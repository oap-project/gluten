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

import java.util.ServiceLoader

import scala.collection.JavaConverters

object BackendsApiManager {

  private lazy val backend: Backend = initializeInternal()

  /** Initialize all backends api. */
  private def initializeInternal(): Backend = {
    val discoveredBackends =
      JavaConverters.iterableAsScalaIterable(ServiceLoader.load(classOf[Backend])).toSeq
    if (discoveredBackends.isEmpty) {
      throw new IllegalStateException("Backend implementation not discovered from JVM classpath")
    }
    if (discoveredBackends.size != 1) {
      throw new IllegalStateException(
        s"More than one Backend implementation discovered from JVM classpath: " +
          s"${discoveredBackends.map(_.name()).toList}")
    }
    val backend = discoveredBackends.head
    backend
  }

  /**
   * Automatically detect the backend api.
   * @return
   */
  def initialize(): String = {
    getBackendName
  }

  // Note: Do not make direct if-else checks based on output of the method.
  // Any form of backend-specific code should be avoided from appearing in common module
  // (e.g. gluten-core, gluten-data)
  def getBackendName: String = {
    backend.name()
  }

  def getBuildInfo: BackendBuildInfo = {
    backend.buildInfo()
  }

  def getListenerApiInstance: ListenerApi = {
    backend.listenerApi()
  }

  def getIteratorApiInstance: IteratorApi = {
    backend.iteratorApi()
  }

  def getSparkPlanExecApiInstance: SparkPlanExecApi = {
    backend.sparkPlanExecApi()
  }

  def getTransformerApiInstance: TransformerApi = {
    backend.transformerApi()
  }

  def getValidatorApiInstance: ValidatorApi = {
    backend.validatorApi()
  }

  def getMetricsApiInstance: MetricsApi = {
    backend.metricsApi()
  }

  def getBroadcastApiInstance: BroadcastApi = {
    backend.broadcastApi()
  }

  def getSettings: BackendSettingsApi = {
    backend.settings
  }
}
