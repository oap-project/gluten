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
package org.apache.gluten.execution.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{BlockStripes, FakeRow, OutputWriter}
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.TaskAttemptContext

trait GlutenFormatWriterInjects {
  def createOutputWriter(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext,
      nativeConf: java.util.Map[String, String]): OutputWriter

  // scalastyle:off argcount
  /** For CH MergeTree format */
  def createOutputWriter(
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
      nativeConf: java.util.Map[String, String]): OutputWriter = null
  // scalastyle:on argcount

  def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType]

  def executeWriterWrappedSparkPlan(plan: SparkPlan): RDD[InternalRow]

  def nativeConf(
      options: Map[String, String],
      compressionCodec: String): java.util.Map[String, String]

  def getFormatName(): String

  def getExtendedColumnarPostRule(session: SparkSession): Rule[SparkPlan]
}

trait GlutenRowSplitter {
  def splitBlockByPartitionAndBucket(
      row: FakeRow,
      partitionColIndice: Array[Int],
      hasBucket: Boolean,
      reserve_partition_columns: Boolean = false): BlockStripes
}

object GlutenRowSplitter {
  private var INSTANCE: GlutenRowSplitter = _

  def setInstance(instance: GlutenRowSplitter): Unit = {
    INSTANCE = instance
  }

  def getInstance(): GlutenRowSplitter = {
    if (INSTANCE == null) {
      throw new IllegalStateException("GlutenOutputWriterFactoryCreator is not initialized")
    }
    INSTANCE
  }
}
