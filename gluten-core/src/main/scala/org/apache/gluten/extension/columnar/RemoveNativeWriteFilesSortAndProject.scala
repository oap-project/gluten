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
package org.apache.gluten.extension.columnar

import org.apache.gluten.GlutenConfig
import org.apache.gluten.execution.{ProjectExecTransformer, SortExecTransformer, WriteFilesExecTransformer}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeMap, AttributeSet, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SortExec, SparkPlan}

/**
 * This rules remove the `V1Writes` added sort and `Empty2Null` in project because:
 *   - Velox table write does not require the data is ordered by partition columns
 *   - Velox table write treats empty string as the `__HIVE_DEFAULT_PARTITION__`, so we do not need
 *     `Empty2Null`
 */
case class RemoveNativeWriteFilesSortAndProject() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(GlutenConfig.REMOVE_NATIVE_WRITE_FILES_SORT_AND_PROJECT)) {
      return plan
    }

    plan.transform {
      case NativeWriteFilesWithSkippingSortAndProject(writeFiles, newChild) =>
        val originalOutput = writeFiles.child.output
        val newOutput = newChild.output
        assert(
          originalOutput.size == newOutput.size,
          s"originalOutput: $originalOutput,\n newOutput: $newOutput")
        val attrMap = AttributeMap(originalOutput.zip(newOutput))
        val newWriteFiles = writeFiles.transformExpressions {
          case attr: Attribute if attrMap.contains(attr) =>
            attr.withExprId(attrMap(attr).exprId)
        }
        newWriteFiles.withNewChildren(newChild :: Nil)
    }
  }
}

object NativeWriteFilesWithSkippingSortAndProject extends Logging {
  private def extractV1WritesProject(plan: SparkPlan): Option[SparkPlan] = {
    plan match {
      // Gluten will never transform ProjectExec if it contains `Empty2Null`.
      // Use `nodeName` to be compatible with older Spark version.
      // We can not remove project exec directly as it may be merged with other project,
      // here we only remove `Empty2Null` expression and try to transform it.
      case p: ProjectExec if p.projectList.exists(_.find(_.nodeName == "Empty2Null").isDefined) =>
        val newProjectList = p.projectList.map {
          expr =>
            expr
              .transform {
                case e if e.nodeName == "Empty2Null" => e.children.head
              }
              .asInstanceOf[NamedExpression]
        }
        val transformer = ProjectExecTransformer(newProjectList, p.child)
        val validationResult = transformer.doValidate()
        if (validationResult.isValid) {
          Some(transformer)
        } else {
          // If we can not transform the project, then we fallback to origin plan which means
          // we also retain the sort operator.
          TransformHints.tagNotTransformable(p, validationResult)
          None
        }
      case _ => None
    }
  }

  private def extractV1WritesSortAndProject(
      plan: SparkPlan,
      partitionColumns: Seq[Attribute]): Option[SparkPlan] = {
    def allSortOrdersFromPartitionColumns(sortOrders: Seq[SortOrder]): Boolean = {
      val partitionColumnsSet = AttributeSet(partitionColumns)
      sortOrders.forall(_.direction == Ascending) &&
      sortOrders.size == partitionColumnsSet.size &&
      sortOrders.map(_.references).forall(attrs => attrs.subsetOf(partitionColumnsSet))
    }

    plan match {
      case sort: SortExec if allSortOrdersFromPartitionColumns(sort.sortOrder) =>
        extractV1WritesProject(sort.child).orElse(Some(sort.child))
      case sort: SortExecTransformer if allSortOrdersFromPartitionColumns(sort.sortOrder) =>
        extractV1WritesProject(sort.child).orElse(Some(sort.child))
      case p: ProjectExec =>
        extractV1WritesProject(p)
      case _ => None
    }
  }

  def unapply(plan: SparkPlan): Option[(WriteFilesExecTransformer, SparkPlan)] = {
    plan match {
      case w: WriteFilesExecTransformer =>
        // TODO: support bucket write
        val childOfV1WritesSortAndProject =
          extractV1WritesSortAndProject(w.child, w.partitionColumns)
        if (childOfV1WritesSortAndProject.isDefined) {
          Some(w, childOfV1WritesSortAndProject.get)
        } else {
          None
        }
      case _ => None
    }
  }
}
