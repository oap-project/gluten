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
package org.apache.spark.affinity

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.execution.GlutenMergeTreePartition
import org.apache.gluten.softaffinity.{AffinityManager, SoftAffinityManager}

import org.apache.spark.softaffinity.Affinity

abstract class MixedAffinity(manager: AffinityManager) extends Affinity(manager) {

  // Identify locations of forced hosts with this prefix.
  // It's used in TaskLocation to distinguish between forced and soft affinity.
  private val forcedHostLocationTag = "forced_host_"

  def affinityMode: String

  def getNativeMergeTreePartitionLocations(
      filePartition: GlutenMergeTreePartition): Array[String] = {
    getHostLocations(filePartition.relativeTablePath + "/" + filePartition.partList(0).name)
  }

  def getHostLocations(filePath: String): Array[String] = {
    if (manager.usingSoftAffinity) {
      internalGetHostLocations(filePath)
    } else {
      Array.empty[String]
    }
  }

  protected def internalGetHostLocations(filePath: String): Array[String] = {
    affinityMode match {
      case CHBackendSettings.SOFT => getLocations(filePath)(getCacheTaskLocation)
      case CHBackendSettings.FORCE =>
        getLocations(filePath)(
          (host: String, _: String) =>
            if (host.isEmpty) throw new IllegalStateException("Unknown host")
            else s"$forcedHostLocationTag$host")
      case _ => throw new IllegalArgumentException("Unknown affinity mode")
    }
  }
}

/** Get the locations by SoftAffinityManager */
object CHAffinity extends MixedAffinity(SoftAffinityManager) {
  override lazy val affinityMode: String = CHBackendSettings.affinityMode
}
