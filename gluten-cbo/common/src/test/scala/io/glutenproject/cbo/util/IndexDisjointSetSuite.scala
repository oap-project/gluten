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
package io.glutenproject.cbo.util

import org.scalatest.funsuite.AnyFunSuite

class IndexDisjointSetSuite extends AnyFunSuite {
  test("Size") {
    val set = IndexDisjointSet[String]()
    set.grow()
    set.grow()
    assert(set.size() == 2)
  }

  test("Union, Set") {
    val set = IndexDisjointSet[String]()
    set.grow()
    set.grow()
    set.grow()
    set.grow()
    set.grow()

    assert(set.setOf(0) == Set(0))
    assert(set.setOf(1) == Set(1))
    assert(set.setOf(2) == Set(2))
    assert(set.setOf(3) == Set(3))
    assert(set.setOf(4) == Set(4))

    set.forward(set.find(2), set.find(3))
    assert(set.find(0) == 0)
    assert(set.find(1) == 1)
    assert(set.find(2) == 3)
    assert(set.find(3) == 3)
    assert(set.find(4) == 4)

    assert(set.setOf(0) == Set(0))
    assert(set.setOf(1) == Set(1))
    assert(set.setOf(2) == Set(2, 3))
    assert(set.setOf(3) == Set(2, 3))
    assert(set.setOf(4) == Set(4))

    set.forward(set.find(4), set.find(3))
    assert(set.find(0) == 0)
    assert(set.find(1) == 1)
    assert(set.find(2) == 3)
    assert(set.find(3) == 3)
    assert(set.find(4) == 3)

    assert(set.setOf(0) == Set(0))
    assert(set.setOf(1) == Set(1))
    assert(set.setOf(2) == Set(2, 3, 4))
    assert(set.setOf(3) == Set(2, 3, 4))
    assert(set.setOf(4) == Set(2, 3, 4))

    set.forward(set.find(3), set.find(0))
    assert(set.find(0) == 0)
    assert(set.find(1) == 1)
    assert(set.find(2) == 0)
    assert(set.find(3) == 0)
    assert(set.find(4) == 0)

    assert(set.setOf(0) == Set(0, 2, 3, 4))
    assert(set.setOf(1) == Set(1))
    assert(set.setOf(2) == Set(0, 2, 3, 4))
    assert(set.setOf(3) == Set(0, 2, 3, 4))
    assert(set.setOf(4) == Set(0, 2, 3, 4))

    set.forward(set.find(2), set.find(1))

    assert(set.find(0) == 1)
    assert(set.find(1) == 1)
    assert(set.find(2) == 1)
    assert(set.find(3) == 1)
    assert(set.find(4) == 1)

    assert(set.setOf(0) == Set(0, 1, 2, 3, 4))
    assert(set.setOf(1) == Set(0, 1, 2, 3, 4))
    assert(set.setOf(2) == Set(0, 1, 2, 3, 4))
    assert(set.setOf(3) == Set(0, 1, 2, 3, 4))
    assert(set.setOf(4) == Set(0, 1, 2, 3, 4))
  }
}
