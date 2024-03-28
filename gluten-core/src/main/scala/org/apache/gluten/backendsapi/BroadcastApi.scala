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
package org.apache.gluten.backendsapi

trait BroadcastApi {

  /**
   * Should call by driver. Collect Broadcast Hash Table Ids.
   *
   * @param executionId
   *   execution id
   * @param buildTableId
   *   build table id
   */
  def collectExecutionBroadcastTableId(executionId: String, buildTableId: String): Unit = {}

  /**
   * Should call by executor. On execution end. Clean executor broadcast build hashtable.
   *
   * @param executionId
   *   execution id
   * @param broadcastTableIds
   *   broadcast table ids
   */
  def cleanExecutionBroadcastTable(
      executionId: String,
      broadcastTableIds: java.util.Set[String]): Unit = {}
}
