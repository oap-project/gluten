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

import org.apache.spark.{SparkConf, SparkContext}
import java.io.File
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths

object UserAddedJarUtils {
  def fetchJarFromSpark(
      urlString: String,
      targetDir: String,
      targetFileName: String,
      sparkConf: SparkConf): Unit = synchronized {
    val targetDirHandler = new File(targetDir)
    //TODO: don't fetch when exists
    val targetPath = Paths.get(targetDir + "/" + targetFileName)
    if (Files.notExists(targetPath)) {
      Utils.doFetchFile(urlString, targetDirHandler, targetFileName, sparkConf, null)
    } else {}
  }
}
