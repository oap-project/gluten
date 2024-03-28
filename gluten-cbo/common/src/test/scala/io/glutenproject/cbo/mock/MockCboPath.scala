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
package io.glutenproject.cbo.mock

import io.glutenproject.cbo.{CanonicalNode, Cbo}
import io.glutenproject.cbo.memo.Memo
import io.glutenproject.cbo.path.{CboPath, PathKeySet}

object MockCboPath {
  def mock[T <: AnyRef](cbo: Cbo[T], node: T): CboPath[T] = {
    mock(cbo, node, PathKeySet.trivial)
  }

  def mock[T <: AnyRef](cbo: Cbo[T], node: T, keys: PathKeySet): CboPath[T] = {
    val memo = Memo(cbo)
    val g = memo.memorize(node, cbo.propSetsOf(node))
    val state = memo.newState()
    val groupSupplier = state.asGroupSupplier()
    assert(g.nodes(state).size == 1)
    val can = g.nodes(state).head

    def dfs(n: CanonicalNode[T]): CboPath[T] = {
      if (n.isLeaf()) {
        return CboPath.one(cbo, keys, groupSupplier, n)
      }
      CboPath(
        cbo,
        n,
        n.getChildrenGroups(groupSupplier).map(_.group(groupSupplier)).map {
          cg =>
            assert(cg.nodes(state).size == 1)
            dfs(cg.nodes(state).head)
        }).get
    }
    dfs(can)
  }
}
