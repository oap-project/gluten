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

import org.apache.gluten.GlutenConfig

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{BlockStripes, FakeRow, OutputWriter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.TaskAttemptContext

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable

object GlutenParquetWriterInjects {
  private var INSTANCE: GlutenFormatWriterInjects = _

  def setInstance(instance: GlutenFormatWriterInjects): Unit = {
    INSTANCE = instance
  }
  def getInstance(): GlutenFormatWriterInjects = {
    if (INSTANCE == null) {
      throw new IllegalStateException("GlutenOutputWriterFactoryCreator is not initialized")
    }
    INSTANCE
  }
}
