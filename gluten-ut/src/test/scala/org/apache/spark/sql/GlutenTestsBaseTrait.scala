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

package org.apache.spark.sql

import io.glutenproject.utils.BackendTestSettings
import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST

trait GlutenTestsBaseTrait {

  protected val rootPath: String = getClass.getResource("/").getPath
  protected val basePath: String = rootPath + "unit-tests-working-home"

  protected val warehouse: String = basePath + "/spark-warehouse"
  protected val metaStorePathAbsolute: String = basePath + "/meta"

  // prefer to use testNameBlackList
  def testNameBlackList: Seq[String] = Seq()

  def shouldRun(testName: String): Boolean = {
    if (testName.startsWith(GLUTEN_TEST)) {
      return true
    }
    if (testNameBlackList.exists(_.equalsIgnoreCase(GlutenTestConstants.IGNORE_ALL))) {
      return false
    }
    if (testNameBlackList.contains(testName)) {
      return false
    }
    if (testName.startsWith("SPARK-")) {
      val issueIDPattern = "SPARK-[0-9]+".r
      val issueID = issueIDPattern.findFirstIn(testName) match {
        case Some(x: String) => x
      }
      if (testNameBlackList.exists(_.startsWith(issueID))) {
        return false
      }
    }

    BackendTestSettings.shouldRun(getClass.getCanonicalName, testName)
  }
}
