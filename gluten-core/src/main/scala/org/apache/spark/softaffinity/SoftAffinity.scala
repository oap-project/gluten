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
package org.apache.spark.softaffinity

import org.apache.gluten.softaffinity.{AffinityManager, SoftAffinityManager}
import org.apache.gluten.utils.LogLevelUtil

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.execution.datasources.FilePartition

abstract class Affinity(val manager: AffinityManager) extends LogLevelUtil with Logging {

  private lazy val logLevel: String = manager.logLevel

  protected def internalGetHostLocations(filePath: String): Array[String]
  private def shouldUseSoftAffinity(
      filePaths: Array[String],
      preferredLocations: Array[String]): Boolean = {
    filePaths.nonEmpty && manager.usingSoftAffinity && !manager.checkTargetHosts(preferredLocations)
  }

  /** Get the locations by SoftAffinityManager */
  def getFilePartitionLocations(
      filePaths: Array[String],
      preferredLocations: Array[String]): Array[String] = {
    if (shouldUseSoftAffinity(filePaths, preferredLocations)) {
      internalGetHostLocations(filePaths.min)
    } else {
      preferredLocations
    }
  }

  def getFilePartitionLocations(filePartition: FilePartition): Array[String] = {
    val filePaths = filePartition.files.map(_.filePath.toString)
    val preferredLocations = filePartition.preferredLocations()
    if (shouldUseSoftAffinity(filePaths, preferredLocations)) {
      if (manager.detectDuplicateReading) {
        val locations = manager.askExecutors(filePartition)
        if (locations.nonEmpty) {
          locations.map { case (executor, host) => getCacheTaskLocation(host, executor) }
        } else {
          Array.empty[String]
        }
      } else {
        getFilePartitionLocations(filePaths, preferredLocations)
      }
    } else {
      preferredLocations
    }
  }

  def getLocations(filePath: String)(toTaskLocation: (String, String) => String): Array[String] = {
    val locations = manager.askExecutors(filePath)
    if (locations.nonEmpty) {
      logOnLevel(
        logLevel,
        s"SAMetrics=File $filePath - the expected executors are ${locations.mkString("_")} ")
      locations.map { case (executor, host) => toTaskLocation(host, executor) }
    } else {
      Array.empty[String]
    }
  }

  def getCacheTaskLocation(host: String, executor: String): String = {
    if (host.isEmpty) executor else ExecutorCacheTaskLocation(host, executor).toString
  }

  /** Update the RDD id to SoftAffinityManager */
  def updateFilePartitionLocations(filePartition: FilePartition, rddId: Int): Unit = {
    if (SoftAffinityManager.usingSoftAffinity && SoftAffinityManager.detectDuplicateReading) {
      SoftAffinityManager.updatePartitionMap(filePartition, rddId)
    }
  }
}

object SoftAffinity extends Affinity(SoftAffinityManager) {
  protected def internalGetHostLocations(filePath: String): Array[String] =
    getLocations(filePath)(getCacheTaskLocation)
}
