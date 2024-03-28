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

import org.apache.gluten.substrait.plan.PlanBuilder

case class MergeTreePartSplit(
    name: String,
    dirName: String,
    targetNode: String,
    start: Long,
    length: Long,
    bytesOnDisk: Long) {
  override def toString: String = {
    s"part name: $name, range: $start-${start + length}"
  }
}

case class GlutenMergeTreePartition(
    index: Int,
    engine: String,
    database: String,
    table: String,
    relativeTablePath: String,
    absoluteTablePath: String,
    orderByKey: String,
    lowCardKey: String,
    minmaxIndexKey: String,
    bfIndexKey: String,
    setIndexKey: String,
    primaryKey: String,
    partList: Array[MergeTreePartSplit],
    tableSchemaJson: String,
    clickhouseTableConfigs: Map[String, String],
    plan: Array[Byte] = PlanBuilder.EMPTY_PLAN)
  extends BaseGlutenPartition {
  override def preferredLocations(): Array[String] = {
    Array.empty[String]
  }
}
