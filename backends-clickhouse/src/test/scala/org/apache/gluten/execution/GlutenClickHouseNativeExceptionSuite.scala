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
package org.apache.gluten.execution

import org.apache.gluten.GlutenConfig
import org.apache.gluten.memory.alloc.{CHNativeMemoryAllocator, CHReservationListener}
import org.apache.gluten.utils.UTSystemParameters

import org.apache.spark.SparkConf

class GlutenClickHouseNativeExceptionSuite extends GlutenClickHouseWholeStageTransformerSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_LIB_PATH, UTSystemParameters.clickHouseLibPath)
  }

  test("native exception caught by jvm") {
    try {
      val x = new CHNativeMemoryAllocator(100, CHReservationListener.NOOP)
      x.close() // this will incur a native exception
      assert(false)
    } catch {
      case e: Exception =>
        assert(e.getMessage.contains("allocator 100 not found"))
    }
  }
}
