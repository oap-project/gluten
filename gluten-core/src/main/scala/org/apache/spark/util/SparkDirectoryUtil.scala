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
package org.apache.spark.util

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

import _root_.org.apache.gluten.exception.GlutenException
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils

import java.io.{File, IOException}
import java.nio.file.Paths

/**
 * Manages Gluten's local directories, for storing jars, libs, spill files, or other temporary
 * stuffs.
 */
object SparkDirectoryUtil extends Logging {
  private val ROOTS = Utils.getConfiguredLocalDirs(SparkEnv.get.conf).flatMap {
    rootDir =>
      try {
        val localDir = Utils.createDirectory(rootDir, "gluten")
        GlutenShutdownManager.addHookForTempDirRemoval(
          () => {
            try FileUtils.forceDelete(localDir)
            catch {
              case e: Exception =>
                throw new GlutenException(e)
            }
          })
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create Gluten local dir in $rootDir. Ignoring this directory.", e)
          None
      }
  }

  private val NAMESPACE_MAPPING: java.util.Map[String, Namespace] = new java.util.HashMap()

  def namespace(name: String): Namespace = synchronized {
    if (NAMESPACE_MAPPING.containsKey(name)) {
      return NAMESPACE_MAPPING.get(name)
    }
    // or create new
    val namespace = new Namespace(ROOTS, name)
    NAMESPACE_MAPPING.put(name, namespace)
    namespace
  }
}

class Namespace(private val parents: Array[File], private val name: String) {
  val all = parents.map {
    root =>
      val path = Paths
        .get(root.getAbsolutePath)
        .resolve(name)
      path.toFile
  }

  private val cycleLooper = Stream.continually(all).flatten.toIterator

  def mkChildDirRoundRobin(childDirName: String): File = synchronized {
    if (!cycleLooper.hasNext) {
      throw new IllegalStateException()
    }
    val subDir = cycleLooper.next()
    if (StringUtils.isEmpty(subDir.getAbsolutePath)) {
      throw new IllegalArgumentException(s"Illegal local dir: $subDir")
    }
    val path = Paths
      .get(subDir.getAbsolutePath)
      .resolve(childDirName)
    val file = path.toFile
    FileUtils.forceMkdir(file)
    file
  }

  def mkChildDirRandomly(childDirName: String): File = {
    val selected = all(scala.util.Random.nextInt(all.length))
    val path = Paths
      .get(selected.getAbsolutePath)
      .resolve(childDirName)
    val file = path.toFile
    FileUtils.forceMkdir(file)
    file
  }

  def mkChildDirs(childDirName: String): Array[File] = {
    all.map {
      subDir =>
        val path = Paths
          .get(subDir.getAbsolutePath)
          .resolve(childDirName)
        val file = path.toFile
        FileUtils.forceMkdir(file)
        file
    }
  }
}
