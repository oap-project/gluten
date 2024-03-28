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
package org.apache.spark.sql.execution.datasources.v1

import io.glutenproject.expression.ConverterUtils
import io.glutenproject.substrait.`type`.ColumnTypeNode
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{ExtensionTableBuilder, RelBuilder}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.{CHDatasourceJniWrapper, GlutenFormatWriterInjectsBase, OutputWriter}
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.execution.datasources.utils.MergeTreeDeltaUtil
import org.apache.spark.sql.execution.datasources.v1.clickhouse.MergeTreeOutputWriter
import org.apache.spark.sql.types.StructType

import com.google.common.collect.Lists
import com.google.protobuf.{Any, StringValue}
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.TaskAttemptContext

import java.util.{ArrayList => JList, Map => JMap, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable
case class PlanWithSplitInfo(plan: Array[Byte], splitInfo: Array[Byte])

class CHMergeTreeWriterInjects extends GlutenFormatWriterInjectsBase {

  override def nativeConf(
      options: Map[String, String],
      compressionCodec: String): JMap[String, String] = {
    // pass options to native so that velox can take user-specified conf to write parquet,
    // i.e., compression, block size, block rows.
    val sparkOptions = new mutable.HashMap[String, String]()
    sparkOptions.asJava
  }

  override def createOutputWriter(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext,
      nativeConf: JMap[String, String]): OutputWriter = null

  // scalastyle:off argcount
  override def createOutputWriter(
      path: String,
      database: String,
      tableName: String,
      orderByKeyOption: Option[Seq[String]],
      lowCardKeyOption: Option[Seq[String]],
      minmaxIndexKeyOption: Option[Seq[String]],
      bfIndexKeyOption: Option[Seq[String]],
      setIndexKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]],
      partitionColumns: Seq[String],
      tableSchema: StructType,
      clickhouseTableConfigs: Map[String, String],
      context: TaskAttemptContext,
      nativeConf: JMap[String, String]): OutputWriter = {
    val uuid = UUID.randomUUID.toString

    val planWithSplitInfo = CHMergeTreeWriterInjects.genMergeTreeWriteRel(
      path,
      database,
      tableName,
      orderByKeyOption,
      lowCardKeyOption,
      minmaxIndexKeyOption,
      bfIndexKeyOption,
      setIndexKeyOption,
      primaryKeyOption,
      partitionColumns,
      Seq(),
      ConverterUtils.convertNamedStructJson(tableSchema),
      clickhouseTableConfigs,
      tableSchema.toAttributes // use table schema instead of data schema
    )

    val datasourceJniWrapper = new CHDatasourceJniWrapper()
    val instance =
      datasourceJniWrapper.nativeInitMergeTreeWriterWrapper(
        planWithSplitInfo.plan,
        planWithSplitInfo.splitInfo,
        uuid,
        context.getTaskAttemptID.getTaskID.getId.toString,
        context.getConfiguration.get("mapreduce.task.gluten.mergetree.partition.dir"),
        context.getConfiguration.get("mapreduce.task.gluten.mergetree.bucketid.str")
      )

    new MergeTreeOutputWriter(database, tableName, datasourceJniWrapper, instance, path)
  }
  // scalastyle:on argcount

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    OrcUtils.inferSchema(sparkSession, files, options)
  }

  override def getFormatName(): String = {
    "mergetree"
  }
}

object CHMergeTreeWriterInjects {

  // scalastyle:off argcount
  def genMergeTreeWriteRel(
      path: String,
      database: String,
      tableName: String,
      orderByKeyOption: Option[Seq[String]],
      lowCardKeyOption: Option[Seq[String]],
      minmaxIndexKeyOption: Option[Seq[String]],
      bfIndexKeyOption: Option[Seq[String]],
      setIndexKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]],
      partitionColumns: Seq[String],
      partList: Seq[String],
      tableSchemaJson: String,
      clickhouseTableConfigs: Map[String, String],
      output: Seq[Attribute]): PlanWithSplitInfo = {
    // scalastyle:on argcount
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(output)
    val nameList = ConverterUtils.collectAttributeNamesWithoutExprId(output)
    val columnTypeNodes = output.map {
      attr =>
        if (partitionColumns.exists(_.equals(attr.name))) {
          new ColumnTypeNode(1)
        } else {
          new ColumnTypeNode(0)
        }
    }.asJava

    val (orderByKey, primaryKey) = MergeTreeDeltaUtil.genOrderByAndPrimaryKeyStr(
      orderByKeyOption,
      primaryKeyOption
    )

    val lowCardKey = lowCardKeyOption match {
      case Some(keys) => keys.mkString(",")
      case None => ""
    }
    val minmaxIndexKey = minmaxIndexKeyOption match {
      case Some(keys) => keys.mkString(",")
      case None => ""
    }
    val bfIndexKey = bfIndexKeyOption match {
      case Some(keys) => keys.mkString(",")
      case None => ""
    }
    val setIndexKey = setIndexKeyOption match {
      case Some(keys) => keys.mkString(",")
      case None => ""
    }

    val substraitContext = new SubstraitContext
    val extensionTableNode = ExtensionTableBuilder.makeExtensionTable(
      -1,
      -1,
      database,
      tableName,
      path,
      "",
      orderByKey,
      lowCardKey,
      minmaxIndexKey,
      bfIndexKey,
      setIndexKey,
      primaryKey,
      scala.collection.JavaConverters.seqAsJavaList(partList),
      scala.collection.JavaConverters.seqAsJavaList(
        Seq.range(0L, partList.length).map(long2Long)
      ), // starts and lengths is useless for write
      scala.collection.JavaConverters.seqAsJavaList(Seq.range(0L, partList.length).map(long2Long)),
      tableSchemaJson,
      clickhouseTableConfigs.asJava,
      new JList[String]()
    )

    val optimizationContent = "isMergeTree=1\n"
    val optimization = Any.pack(StringValue.newBuilder.setValue(optimizationContent).build)
    val extensionNode = ExtensionBuilder.makeAdvancedExtension(optimization, null)

    val relNode = RelBuilder.makeReadRel(
      typeNodes,
      nameList,
      columnTypeNodes,
      null,
      extensionNode,
      substraitContext,
      substraitContext.nextOperatorId("readRel"))

    val plan =
      PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode), nameList).toProtobuf

    PlanWithSplitInfo(plan.toByteArray, extensionTableNode.toProtobuf.toByteArray)
  }
}
