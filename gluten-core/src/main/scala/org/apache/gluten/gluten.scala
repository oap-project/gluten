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
package org.apache

import org.apache.gluten.exception.GlutenException

import java.util.Properties

import scala.util.Try

package object gluten {
  protected object BuildInfo {
    private val buildFile = "gluten-build-info.properties"
    private val buildFileStream =
      Thread.currentThread().getContextClassLoader.getResourceAsStream(buildFile)

    if (buildFileStream == null) {
      throw new GlutenException(s"Can not load the core build file: $buildFile")
    }

    val unknown = "<unknown>"
    private val props = new Properties()

    try {
      props.load(buildFileStream)
    } finally {
      Try(buildFileStream.close())
    }

    val version: String = props.getProperty("gluten_version", unknown)
    val gccVersion: String = props.getProperty("gcc_version", unknown)
    val javaVersion: String = props.getProperty("java_version", unknown)
    val scalaVersion: String = props.getProperty("scala_version", unknown)
    val sparkVersion: String = props.getProperty("spark_version", unknown)
    val hadoopVersion: String = props.getProperty("hadoop_version", unknown)
    val branch: String = props.getProperty("branch", unknown)
    val revision: String = props.getProperty("revision", unknown)
    val revisionTime: String = props.getProperty("revision_time", unknown)
    val buildDate: String = props.getProperty("date", unknown)
    val repoUrl: String = props.getProperty("url", unknown)
    val veloxBranch: String = props.getProperty("velox_branch", unknown)
    val veloxRevision: String = props.getProperty("velox_revision", unknown)
    val veloxRevisionTime: String = props.getProperty("velox_revision_time", unknown)
    val chBranch: String = props.getProperty("ch_branch", unknown)
    val chCommit: String = props.getProperty("ch_commit", unknown)
  }

  val VERSION: String = BuildInfo.version
  val GCC_VERSION: String = BuildInfo.gccVersion
  val JAVA_COMPILE_VERSION: String = BuildInfo.javaVersion
  val SCALA_COMPILE_VERSION: String = BuildInfo.scalaVersion
  val SPARK_COMPILE_VERSION: String = BuildInfo.sparkVersion
  val HADOOP_COMPILE_VERSION: String = BuildInfo.hadoopVersion
  val BRANCH: String = BuildInfo.branch
  val REVISION: String = BuildInfo.revision
  val REVISION_TIME: String = BuildInfo.revisionTime
  val BUILD_DATE: String = BuildInfo.buildDate
  val REPO_URL: String = BuildInfo.repoUrl
  val VELOX_BRANCH: String = BuildInfo.veloxBranch
  val VELOX_REVISION: String = BuildInfo.veloxRevision
  val VELOX_REVISION_TIME: String = BuildInfo.veloxRevisionTime
  val CH_BRANCH: String = BuildInfo.chBranch
  val CH_COMMIT: String = BuildInfo.chCommit
}
