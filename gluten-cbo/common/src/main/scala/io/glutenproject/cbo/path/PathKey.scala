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

import java.util.concurrent.atomic.AtomicLong

// Path key is used to identify the corresponding children and parent nodes
// during path finding. One path can have multiple path keys tagged to it
// so it is made possible that we can fetch all interested paths that from
// different wizards within a single path-finding request.
trait PathKey {}

object PathKey {
  case object Trivial extends PathKey

  def random(): PathKey = RandomKey()

  private case class RandomKey private (id: Long) extends PathKey

  private object RandomKey {
    private val nextId = new AtomicLong(0)
    def apply(): PathKey = {
      RandomKey(nextId.getAndIncrement())
    }
  }
}

sealed trait PathKeySet {
  def keys(): Set[PathKey]
}

object PathKeySet {
  private val TRIVIAL = PathKeySetImpl(Set(PathKey.Trivial))

  def apply(keys: Set[PathKey]): PathKeySet = {
    PathKeySetImpl(keys)
  }

  def trivial: PathKeySet = {
    TRIVIAL
  }

  private case class PathKeySetImpl(override val keys: Set[PathKey]) extends PathKeySet {
    assert(keys.nonEmpty, "Path should at least have one key")
  }
}
