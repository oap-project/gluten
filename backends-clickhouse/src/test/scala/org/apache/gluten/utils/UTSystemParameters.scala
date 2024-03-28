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

object UTSystemParameters {

  private val CLICKHOUSE_LIB_PATH_KEY = "clickhouse.lib.path"
  private val CLICKHOUSE_LIB_PATH_DEFAULT_VALUE = "/usr/local/clickhouse/lib/libch.so"

  def clickHouseLibPath: String = {
    System.getProperty(
      UTSystemParameters.CLICKHOUSE_LIB_PATH_KEY,
      UTSystemParameters.CLICKHOUSE_LIB_PATH_DEFAULT_VALUE)
  }

  private val TEST_DATA_PATH_KEY = "gluten.test.data.path"
  private val TEST_DATA_PATH_DEFAULT_VALUE = "/data"

  def testDataPath: String = {
    System.getProperty(
      UTSystemParameters.TEST_DATA_PATH_KEY,
      UTSystemParameters.TEST_DATA_PATH_DEFAULT_VALUE)
  }

  private val TPCDS_DATA_PATH_KEY = "tpcds.data.path"
  private val TPCDS_RELATIVE_DATA_PATH = "tpcds-data-sf1"

  def tpcdsDataPath: String = {
    val result = System.getProperty(UTSystemParameters.TPCDS_DATA_PATH_KEY, null)
    if (result == null) {
      s"$testDataPath/$TPCDS_RELATIVE_DATA_PATH"
    } else {
      result
    }
  }

}
