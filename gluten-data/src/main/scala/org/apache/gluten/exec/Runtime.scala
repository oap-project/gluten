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
package org.apache.gluten.exec

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.init.JniUtils

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.TaskResource

import java.util.concurrent.atomic.AtomicBoolean

class Runtime private[exec] () extends TaskResource {
  private val handle = RuntimeJniWrapper.createRuntime(
    BackendsApiManager.getBackendName,
    JniUtils.toNativeConf(
      GlutenConfig.getNativeSessionConf(
        BackendsApiManager.getSettings.getBackendConfigPrefix,
        SQLConf.get.getAllConfs))
  )

  private val released: AtomicBoolean = new AtomicBoolean(false)

  def getHandle: Long = handle

  override def release(): Unit = {
    if (!released.compareAndSet(false, true)) {
      throw new GlutenException(
        s"Runtime instance already released: $getHandle, ${resourceName()}, ${priority()}")
    }
    RuntimeJniWrapper.releaseRuntime(handle)
  }

  override def priority(): Int = 10

  override def resourceName(): String = s"Runtime_" + handle
}
