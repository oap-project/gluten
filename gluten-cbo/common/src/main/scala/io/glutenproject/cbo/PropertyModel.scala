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
package io.glutenproject.cbo

import io.glutenproject.cbo.rule.CboRule

// TODO Use class tags to restrict runtime user-defined class types.

trait Property[T <: AnyRef] {
  def satisfies(other: Property[T]): Boolean
  def definition(): PropertyDef[T, _ <: Property[T]]
}

trait PropertyDef[T <: AnyRef, P <: Property[T]] {
  def getProperty(plan: T): P
  def getChildrenConstraints(constraint: Property[T], plan: T): Seq[P]
}

trait EnforcerRuleFactory[T <: AnyRef] {
  def newEnforcerRules(constraint: Property[T]): Seq[CboRule[T]]
}

trait PropertyModel[T <: AnyRef] {
  def propertyDefs: Seq[PropertyDef[T, _ <: Property[T]]]
  def newEnforcerRuleFactory(propertyDef: PropertyDef[T, _ <: Property[T]]): EnforcerRuleFactory[T]
}

object PropertyModel {}
