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
package org.apache.gluten.integration.tpc

import org.apache.spark.sql.{QueryRunner, RunResult, SparkSession}

import com.google.common.base.Preconditions
import org.apache.commons.io.FileUtils

import java.io.File

class TpcRunner(val queryResourceFolder: String, val dataPath: String) {
  Preconditions.checkState(
    new File(dataPath).exists(),
    s"Data not found at $dataPath, try using command `<gluten-it> data-gen-only <options>` to generate it first.",
    Array(): _*)

  def createTables(spark: SparkSession): Unit = {
    TpcRunner.createTables(spark, dataPath)
  }

  def runTpcQuery(
      spark: SparkSession,
      desc: String,
      caseId: String,
      explain: Boolean = false,
      metrics: Array[String] = Array(),
      randomKillTasks: Boolean = false): RunResult = {
    val path = "%s/%s.sql".format(queryResourceFolder, caseId)
    QueryRunner.runTpcQuery(spark, desc, path, explain, metrics, randomKillTasks)
  }
}

object TpcRunner {
  def createTables(spark: SparkSession, dataPath: String): Unit = {
    val files = new File(dataPath).listFiles()
    files.foreach(
      file => {
        if (spark.catalog.tableExists(file.getName)) {
          println("Table exists: " + file.getName)
        } else {
          println("Creating catalog table: " + file.getName)
          spark.catalog.createTable(file.getName, file.getAbsolutePath, "parquet")
          try {
            spark.catalog.recoverPartitions(file.getName)
          } catch {
            case _: Throwable =>
          }
        }
      })
  }

  private def delete(path: String): Unit = {
    FileUtils.forceDelete(new File(path))
  }
}
