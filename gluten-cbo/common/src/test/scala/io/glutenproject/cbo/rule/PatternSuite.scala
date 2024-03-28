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
package io.glutenproject.cbo.rule

import io.glutenproject.cbo.{rule, Cbo, CboSuiteBase}
import io.glutenproject.cbo.mock.MockCboPath
import io.glutenproject.cbo.path.{CboPath, Pattern}

import org.scalatest.funsuite.AnyFunSuite

class PatternSuite extends AnyFunSuite {
  import PatternSuite._
  test("Match any") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.none())

    val pattern = Pattern.ignore[TestNode].build()
    val path = MockCboPath.mock(cbo, Leaf("n1", 1))
    assert(path.height() == 1)

    assert(pattern.matches(path, 1))
  }

  test("Match ignore") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.none())

    val pattern = Pattern.ignore[TestNode].build()
    val path = MockCboPath.mock(cbo, Leaf("n1", 1))
    assert(path.height() == 1)

    assert(pattern.matches(path, 1))
  }

  test("Match unary") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.none())

    val path = MockCboPath.mock(cbo, Unary("n1", Leaf("n2", 1)))
    assert(path.height() == 2)

    val pattern1 = Pattern.node[TestNode](n => n.isInstanceOf[Unary], Pattern.ignore).build()
    assert(pattern1.matches(path, 1))
    assert(pattern1.matches(path, 2))

    val pattern2 =
      Pattern.node[TestNode](n => n.asInstanceOf[Unary].name == "foo", Pattern.ignore).build()
    assert(!pattern2.matches(path, 1))
    assert(!pattern2.matches(path, 2))
  }

  test("Match binary") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.none())

    val path = MockCboPath.mock(
      cbo,
      Binary("n7", Unary("n1", Unary("n2", Leaf("n3", 1))), Unary("n5", Leaf("n6", 1))))
    assert(path.height() == 4)

    val pattern = Pattern
      .node[TestNode](
        n => n.isInstanceOf[Binary],
        Pattern.node(
          n => n.isInstanceOf[Unary],
          Pattern.node(
            n => n.isInstanceOf[Unary],
            Pattern.ignore
          )
        ),
        Pattern.ignore)
      .build()
    assert(pattern.matches(path, 1))
    assert(pattern.matches(path, 2))
    assert(pattern.matches(path, 3))
    assert(pattern.matches(path, 4))
  }

  test("Matches above a certain depth") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.none())

    val path = MockCboPath.mock(
      cbo,
      Binary("n7", Unary("n1", Unary("n2", Leaf("n3", 1))), Unary("n5", Leaf("n6", 1))))
    assert(path.height() == 4)

    val pattern1 = Pattern
      .node[TestNode](
        n => n.isInstanceOf[Binary],
        Pattern.node(
          n => n.isInstanceOf[Unary],
          Pattern.node(
            n => n.isInstanceOf[Unary],
            Pattern.leaf(
              _.asInstanceOf[Leaf].name == "foo"
            )
          )
        ),
        Pattern.ignore
      )
      .build()

    assert(pattern1.matches(path, 1))
    assert(pattern1.matches(path, 2))
    assert(pattern1.matches(path, 3))
    assert(!pattern1.matches(path, 4))

    val pattern2 = Pattern
      .node[TestNode](
        n => n.isInstanceOf[Binary],
        Pattern.node(
          n => n.isInstanceOf[Unary],
          Pattern.node(
            n => n.isInstanceOf[Unary],
            Pattern.node(
              n => n.isInstanceOf[Unary],
              Pattern.ignore
            )
          )
        ),
        Pattern.ignore
      )
      .build()

    assert(pattern2.matches(path, 1))
    assert(pattern2.matches(path, 2))
    assert(pattern2.matches(path, 3))
    assert(!pattern2.matches(path, 4))
  }
}

object PatternSuite extends CboSuiteBase {
  case class Leaf(name: String, override val selfCost: Long) extends LeafLike {
    override def makeCopy(): LeafLike = this
  }

  case class Unary(name: String, child: TestNode) extends UnaryLike {
    override def selfCost(): Long = 1

    override def withNewChildren(child: TestNode): UnaryLike = copy(child = child)
  }

  case class Binary(name: String, left: TestNode, right: TestNode) extends BinaryLike {
    override def selfCost(): Long = 1

    override def withNewChildren(left: TestNode, right: TestNode): BinaryLike =
      copy(left = left, right = right)
  }

  case class DummyGroup() extends LeafLike {
    override def makeCopy(): rule.PatternSuite.LeafLike = throw new UnsupportedOperationException()
    override def selfCost(): Long = throw new UnsupportedOperationException()
  }

  implicit class PatternImplicits[T <: AnyRef](pattern: Pattern[T]) {
    def matchesAll(paths: Seq[CboPath[T]], depth: Int): Boolean = {
      paths.forall(pattern.matches(_, depth))
    }
  }
}
