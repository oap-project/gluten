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
package org.apache.gluten.utils

import org.apache.gluten.GlutenConfig

import org.apache.spark.TaskContext

object DebugUtil {
  // if specify taskId, then only do that task partition
  // if not specify stageId, then do nothing
  // if specify stageId but no partitionId, then do all partitions for that stage
  // if specify stageId and partitionId, then only do that partition for that stage
  def saveInputToFile(): Boolean = {
    if (TaskContext.get().taskAttemptId() == GlutenConfig.getConf.taskId) {
      return true
    }
    if (TaskContext.get().stageId() != GlutenConfig.getConf.taskStageId) {
      return false
    }
    if (GlutenConfig.getConf.taskPartitionId == -1) {
      return true
    }
    if (TaskContext.getPartitionId() == GlutenConfig.getConf.taskPartitionId) {
      return true
    }

    false
  }
}
