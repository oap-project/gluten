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
package io.glutenproject.softaffinity.scheduler

import io.glutenproject.softaffinity.SoftAffinityManager

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded, SparkListenerExecutorRemoved}

class SoftAffinityListener extends SparkListener with Logging {

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    val execId = executorAdded.executorId
    val host = executorAdded.executorInfo.executorHost
    SoftAffinityManager.handleExecutorAdded((execId, host))
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    val execId = executorRemoved.executorId
    SoftAffinityManager.handleExecutorRemoved(execId)
  }
}
