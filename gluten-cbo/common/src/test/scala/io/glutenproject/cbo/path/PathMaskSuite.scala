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
package io.glutenproject.cbo.path

import io.glutenproject.cbo.CboSuiteBase

import org.scalatest.funsuite.AnyFunSuite

class PathMaskSuite extends AnyFunSuite {

  test("Mask operation - fold 1") {
    val in = PathMask(List(3, -1, 2, 0, 0, 0))
    assert(in.fold(0) == PathMask(List(-1)))
    assert(in.fold(1) == PathMask(List(3, -1, -1, -1)))
    assert(in.fold(2) == PathMask(List(3, -1, 2, -1, -1, 0)))
    assert(in.fold(3) == PathMask(List(3, -1, 2, 0, 0, 0)))
    assert(in.fold(CboPath.INF_DEPTH) == PathMask(List(3, -1, 2, 0, 0, 0)))
  }

  test("Mask operation - fold 2") {
    val in = PathMask(List(3, 2, 0, -1, 0, 1, 0))
    assert(in.fold(0) == PathMask(List(-1)))
    assert(in.fold(1) == PathMask(List(3, -1, -1, -1)))
    assert(in.fold(2) == PathMask(List(3, 2, -1, -1, 0, 1, -1)))
    assert(in.fold(3) == PathMask(List(3, 2, 0, -1, 0, 1, 0)))
    assert(in.fold(CboPath.INF_DEPTH) == PathMask(List(3, 2, 0, -1, 0, 1, 0)))
  }

  test("Mask operation - sub-mask") {
    val in = PathMask(List(3, 2, 0, -1, 0, 1, 0))
    assert(in.subMaskAt(0) == PathMask(List(3, 2, 0, -1, 0, 1, 0)))
    assert(in.subMaskAt(1) == PathMask(List(2, 0, -1)))
    assert(in.subMaskAt(2) == PathMask(List(0)))
    assert(in.subMaskAt(3) == PathMask(List(-1)))
    assert(in.subMaskAt(4) == PathMask(List(0)))
    assert(in.subMaskAt(5) == PathMask(List(1, 0)))
    assert(in.subMaskAt(6) == PathMask(List(0)))
  }

  test("Mask operation - union 1") {
    val in1 = PathMask(List(3, 2, 0, -1, 0, 1, 0))
    val in2 = PathMask(List(-1))
    assert(PathMask.union(List(in1, in2)) == PathMask(List(-1)))
  }

  test("Mask operation - union 2") {
    val in1 = PathMask(List(3, 2, 0, -1, 0, 1, 0))
    val in2 = PathMask(List(3, -1, 0, -1))
    assert(PathMask.union(List(in1, in2)) == PathMask(List(3, -1, 0, -1)))
  }

  test("Mask operation - union 3") {
    val in1 = PathMask(List(3, 2, 0, -1, 0, 1, 0))
    val in2 = PathMask(List(3, -1, 0, 1, 0))
    assert(PathMask.union(List(in1, in2)) == PathMask(List(3, -1, 0, 1, 0)))
  }

  test("Mask operation - union 4") {
    val in1 = PathMask(List(3, 2, 0, -1, 0, 1, 0))
    val in2 = PathMask(List(3, 2, 0, 0, 0, 1, 0))
    assert(PathMask.union(List(in1, in2)) == PathMask(List(3, 2, 0, -1, 0, 1, 0)))
  }

  test("Mask operation - union 5") {
    val in1 = PathMask(List(3, 2, 0, 0, -1, 1, 0))
    val in2 = PathMask(List(3, -1, 2, -1, 0, 1, 0))
    assert(PathMask.union(List(in1, in2)) == PathMask(List(3, -1, -1, 1, 0)))
  }

  test("Mask operation - satisfaction 1") {
    val m1 = PathMask(List(1, 0))
    val m2 = PathMask(List(-1))
    assert(m1.satisfies(m2))
    assert(!m2.satisfies(m1))
  }

  test("Mask operation - satisfaction 2") {
    val m1 = PathMask(List(3, 2, 0, -1, 0, 1, 0))
    val m2 = PathMask(List(3, -1, -1, -1))
    assert(m1.satisfies(m2))
    assert(!m2.satisfies(m1))
  }

  test("Mask operation - satisfaction 3") {
    val m1 = PathMask(List(3, 0, 0, 0))
    val m2 = PathMask(List(3, 0, 0, 0))
    assert(m1.satisfies(m2))
    assert(m2.satisfies(m1))
  }

  test("Mask operation - satisfaction 4") {
    val m1 = PathMask(List(1, -1))
    val m2 = PathMask(List(0))
    assert(!m1.satisfies(m2))
    assert(!m2.satisfies(m1))
  }
}

object PathMaskSuite extends CboSuiteBase {}
