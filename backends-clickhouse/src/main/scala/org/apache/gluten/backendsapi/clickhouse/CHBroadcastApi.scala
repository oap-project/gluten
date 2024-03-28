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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.backendsapi.BroadcastApi
import org.apache.gluten.execution.CHBroadcastBuildSideCache

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.GlutenDriverEndpoint

class CHBroadcastApi extends BroadcastApi with Logging {
  override def cleanExecutionBroadcastTable(
      executionId: String,
      broadcastTableIds: java.util.Set[String]): Unit = {
    if (broadcastTableIds != null) {
      broadcastTableIds.forEach(
        resource_id => CHBroadcastBuildSideCache.invalidateBroadcastHashtable(resource_id))
    }
  }

  override def collectExecutionBroadcastTableId(executionId: String, buildTableId: String): Unit = {
    if (executionId != null) {
      GlutenDriverEndpoint.collectResources(executionId, buildTableId)
    } else {
      logWarning(
        s"Can't not trace broadcast hash table data $buildTableId" +
          s" because execution id is null." +
          s" Will clean up until expire time.")
    }
  }
}
