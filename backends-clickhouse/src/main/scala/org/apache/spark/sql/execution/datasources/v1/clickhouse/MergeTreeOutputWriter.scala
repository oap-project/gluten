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
package org.apache.spark.sql.execution.datasources.v1.clickhouse

import org.apache.gluten.vectorized.CHColumnVector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.execution.datasources.{CHDatasourceJniWrapper, FakeRow, OutputWriter}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddFileTags
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer

class MergeTreeOutputWriter(
    database: String,
    tableName: String,
    datasourceJniWrapper: CHDatasourceJniWrapper,
    instance: Long,
    originPath: String)
  extends OutputWriter {

  protected var addFiles: ArrayBuffer[AddFile] = new ArrayBuffer[AddFile]()

  override def write(row: InternalRow): Unit = {
    assert(row.isInstanceOf[FakeRow])
    val nextBatch = row.asInstanceOf[FakeRow].batch

    if (nextBatch.numRows > 0) {
      val col = nextBatch.column(0).asInstanceOf[CHColumnVector]
      datasourceJniWrapper.writeToMergeTree(instance, col.getBlockAddress)
    } // else just ignore this empty block
  }

  override def close(): Unit = {
    val returnedMetrics = datasourceJniWrapper.closeMergeTreeWriter(instance)
    if (returnedMetrics != null && returnedMetrics.nonEmpty) {
      addFiles.appendAll(
        AddFileTags.partsMetricsToAddFile(
          database,
          tableName,
          originPath,
          returnedMetrics,
          Seq(Utils.localHostName())))
    }
  }

  // Do NOT add override keyword for compatibility on spark 3.1.
  def path(): String = {
    originPath
  }

  def getAddFiles(): ArrayBuffer[AddFile] = {
    addFiles
  }
}
