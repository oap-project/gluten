/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.extension

import org.apache.gluten.execution.{DeltaScanTransformer, ProjectExecTransformer}
import org.apache.gluten.extension.DeltaRewriteTransformerRules.columnMappingRule
import org.apache.gluten.extension.columnar.RewriteTransformerRules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaParquetFileFormat, NoMapping}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FileFormat

import scala.collection._

class DeltaRewriteTransformerRules extends RewriteTransformerRules {
  override def rules: Seq[Rule[SparkPlan]] = columnMappingRule :: Nil
}

object DeltaRewriteTransformerRules {

  private val COLUMN_MAPPING_RULE_TAG: TreeNodeTag[String] =
    TreeNodeTag[String]("org.apache.gluten.delta.column.mapping")

  private def notAppliedColumnMappingRule(plan: SparkPlan): Boolean = {
    plan.getTagValue(COLUMN_MAPPING_RULE_TAG).isEmpty
  }

  private def tagColumnMappingRule(plan: SparkPlan): Unit = {
    plan.setTagValue(COLUMN_MAPPING_RULE_TAG, null)
  }

  val columnMappingRule: Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformWithSubqueries {
      // If it enables Delta Column Mapping(e.g. nameMapping and idMapping),
      // transform the metadata of Delta into Parquet's,
      // so that gluten can read Delta File using Parquet Reader.
      case p: DeltaScanTransformer
          if isDeltaColumnMappingFileFormat(p.relation.fileFormat) && notAppliedColumnMappingRule(
            p) =>
        transformColumnMappingPlan(p)
    }

  private def isDeltaColumnMappingFileFormat(fileFormat: FileFormat): Boolean = fileFormat match {
    case d: DeltaParquetFileFormat if d.columnMappingMode != NoMapping =>
      true
    case _ =>
      false
  }

  /**
   * This method is only used for Delta ColumnMapping FileFormat(e.g. nameMapping and idMapping)
   * transform the metadata of Delta into Parquet's, each plan should only be transformed once.
   */
  private def transformColumnMappingPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: DeltaScanTransformer =>
      val fmt = plan.relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]

      // transform HadoopFsRelation
      val relation = plan.relation
      val newFsRelation = relation.copy(
        partitionSchema = DeltaColumnMapping.createPhysicalSchema(
          relation.partitionSchema,
          fmt.referenceSchema,
          fmt.columnMappingMode),
        dataSchema = DeltaColumnMapping.createPhysicalSchema(
          relation.dataSchema,
          fmt.referenceSchema,
          fmt.columnMappingMode)
      )(SparkSession.active)
      // transform output's name into physical name so Reader can read data correctly
      // should keep the columns order the same as the origin output
      val originColumnNames = mutable.ListBuffer.empty[String]
      val transformedAttrs = mutable.ListBuffer.empty[Attribute]
      def mapAttribute(attr: Attribute) = {
        val newAttr = if (!plan.isMetadataColumn(attr)) {
          DeltaColumnMapping
            .createPhysicalAttributes(Seq(attr), fmt.referenceSchema, fmt.columnMappingMode)
            .head
        } else {
          attr
        }
        if (!originColumnNames.contains(attr.name)) {
          transformedAttrs += newAttr
          originColumnNames += attr.name
        }
        newAttr
      }
      val newOutput = plan.output.map(o => mapAttribute(o))
      // transform dataFilters
      val newDataFilters = plan.dataFilters.map {
        e =>
          e.transformDown {
            case attr: AttributeReference =>
              mapAttribute(attr)
          }
      }
      // transform partitionFilters
      val newPartitionFilters = plan.partitionFilters.map {
        e =>
          e.transformDown {
            case attr: AttributeReference =>
              mapAttribute(attr)
          }
      }
      // replace tableName in schema with physicalName
      val scanExecTransformer = new DeltaScanTransformer(
        newFsRelation,
        newOutput,
        DeltaColumnMapping.createPhysicalSchema(
          plan.requiredSchema,
          fmt.referenceSchema,
          fmt.columnMappingMode),
        newPartitionFilters,
        plan.optionalBucketSet,
        plan.optionalNumCoalescedBuckets,
        newDataFilters,
        plan.tableIdentifier,
        plan.disableBucketedScan
      )
      scanExecTransformer.copyTagsFrom(plan)
      tagColumnMappingRule(scanExecTransformer)

      // alias physicalName into tableName
      val expr = (transformedAttrs, originColumnNames).zipped.map {
        (attr, columnName) => Alias(attr, columnName)(exprId = attr.exprId)
      }
      val projectExecTransformer = ProjectExecTransformer(expr, scanExecTransformer)
      projectExecTransformer
    case _ => plan
  }
}
