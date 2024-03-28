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

import org.apache.spark.util.{TaskResource, TaskResources}

import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

class TaskResourceSuite extends AnyFunSuite {
  test("Run unsafe") {
    val out = TaskResources.runUnsafe {
      1
    }
    assert(out == 1)
  }

  test("Run unsafe - task context") {
    TaskResources.runUnsafe {
      assert(TaskResources.inSparkTask())
      assert(TaskResources.getLocalTaskContext() != null)
    }
  }

  test("Run unsafe - register resource") {
    var unregisteredCount = 0
    TaskResources.runUnsafe {
      TaskResources.addResource(
        UUID.randomUUID().toString,
        new TaskResource {
          override def release(): Unit = unregisteredCount += 1

          override def resourceName(): String = "test resource 1"
        })
      TaskResources.addResource(
        UUID.randomUUID().toString,
        new TaskResource {
          override def release(): Unit = unregisteredCount += 1

          override def resourceName(): String = "test resource 2"
        })
    }
    assert(unregisteredCount == 2)
  }
}
