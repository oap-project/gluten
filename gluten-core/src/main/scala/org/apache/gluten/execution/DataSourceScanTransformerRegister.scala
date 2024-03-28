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
package org.apache.gluten.execution

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

/**
 * Data sources transformer should implement this trait so that they can register an alias to their
 * data source transformer. This allows users to give the data source transformer alias as the
 * format type over the fully qualified class name.
 */
trait DataSourceScanTransformerRegister {

  /**
   * The class name that used to identify what kind of datasource this is。
   *
   * For DataSource V1, it should be the child class name of
   * [[org.apache.spark.sql.execution.datasources.FileIndex]].
   *
   * For DataSource V2, it should be the child class name of
   * [[org.apache.spark.sql.connector.read.Scan]].
   *
   * For example:
   * {{{
   *   override val scanClassName: String = "org.apache.iceberg.spark.source.SparkBatchQueryScan"
   * }}}
   */
  val scanClassName: String

  def createDataSourceTransformer(
      batchScan: FileSourceScanExec,
      newPartitionFilters: Seq[Expression]): FileSourceScanExecTransformerBase = {
    throw new UnsupportedOperationException(
      "This should not be called, please implement this method in child class.");
  }

  def createDataSourceV2Transformer(
      batchScan: BatchScanExec,
      newPartitionFilters: Seq[Expression]): BatchScanExecTransformerBase = {
    throw new UnsupportedOperationException(
      "This should not be called, please implement this method in child class.");
  }
}
