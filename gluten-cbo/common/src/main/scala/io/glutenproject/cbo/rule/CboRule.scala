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

trait CboRule[T <: AnyRef] {
  def shift(node: T): Iterable[T]
  def shape(): Shape[T]
}

object CboRule {
  trait Factory[T <: AnyRef] {
    def create(): Seq[CboRule[T]]
  }

  object Factory {
    def reuse[T <: AnyRef](rules: Seq[CboRule[T]]): Factory[T] = new SimpleReuse(rules)

    def none[T <: AnyRef](): Factory[T] = new SimpleReuse[T](List.empty)

    private class SimpleReuse[T <: AnyRef](rules: Seq[CboRule[T]]) extends Factory[T] {
      override def create(): Seq[CboRule[T]] = rules
    }
  }

}
