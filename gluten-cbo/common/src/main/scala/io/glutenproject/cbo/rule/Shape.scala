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

import io.glutenproject.cbo.path.{CboPath, OutputWizard, OutputWizards}

// Shape is an abstraction for all inputs the rule can accept.
// Shape can be specification on pattern, height, or mask
// to represent fuzzy, or precise structure of acceptable inputs.
trait Shape[T <: AnyRef] {
  def wizard(): OutputWizard[T]
  def identify(path: CboPath[T]): Boolean
}

object Shape {}

object Shapes {
  def fixedHeight[T <: AnyRef](height: Int): Shape[T] = {
    new FixedHeight[T](height)
  }

  def none[T <: AnyRef](): Shape[T] = {
    new None()
  }

  private class FixedHeight[T <: AnyRef](height: Int) extends Shape[T] {
    override def wizard(): OutputWizard[T] = OutputWizards.withMaxDepth(height)
    override def identify(path: CboPath[T]): Boolean = path.height() == height
  }

  private class None[T <: AnyRef]() extends Shape[T] {
    override def wizard(): OutputWizard[T] = OutputWizards.none()
    override def identify(path: CboPath[T]): Boolean = false
  }
}
