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

package io.glutenproject.e2e.tpc.h.velox

import io.glutenproject.e2e.tpc.h.TpchSuite
import org.apache.spark.SparkConf

class VeloxTpchSuite extends TpchSuite {

  defineTypeModifier(TYPE_MODIFIER_INTEGER_AS_DOUBLE)
  defineTypeModifier(TYPE_MODIFIER_LONG_AS_DOUBLE)
  defineTypeModifier(TYPE_MODIFIER_DATE_AS_DOUBLE)

  override def testConf(): SparkConf = {
    VeloxTpchSuite.testConf
  }

  override def queryResource(): String = {
    VeloxTpchSuite.TPCH_QUERY_RESOURCE
  }
}

object VeloxTpchSuite {
  private val TPCH_QUERY_RESOURCE = "/tpch-queries-noint-nodate"

  private val testConf = VELOX_BACKEND_CONF

}
