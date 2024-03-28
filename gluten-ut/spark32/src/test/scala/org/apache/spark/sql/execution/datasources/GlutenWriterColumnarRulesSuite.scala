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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.GlutenConfig

import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, SaveMode}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class GlutenWriterColumnarRulesSuite extends GlutenSQLTestsBaseTrait {

  class WriterColumnarListener extends QueryExecutionListener {
    var fakeRowAdaptor: Option[FakeRowAdaptor] = None

    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      fakeRowAdaptor = qe.executedPlan.collectFirst { case f: FakeRowAdaptor => f }
    }

    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
  }

  testGluten("writing to noop") {
    withTempDir {
      dir =>
        withSQLConf(GlutenConfig.NATIVE_WRITER_ENABLED.key -> "true") {
          spark.range(0, 100).write.mode(SaveMode.Overwrite).parquet(dir.getPath)
          val listener = new WriterColumnarListener
          spark.listenerManager.register(listener)
          try {
            spark.read.parquet(dir.getPath).write.format("noop").mode(SaveMode.Overwrite).save()
            spark.sparkContext.listenerBus.waitUntilEmpty()
            assert(listener.fakeRowAdaptor.isDefined, "FakeRowAdaptor is not found.")
          } finally {
            spark.listenerManager.unregister(listener)
          }
        }
    }
  }
}
