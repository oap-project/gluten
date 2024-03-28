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
package io.glutenproject.cbo.property

import io.glutenproject.cbo.{Property, PropertyDef}

trait PropertySet[T <: AnyRef] {
  def get[P <: Property[T]](property: PropertyDef[T, P]): P
  def getMap: Map[PropertyDef[T, _ <: Property[T]], Property[T]]
  def satisfies(other: PropertySet[T]): Boolean
}

object PropertySet {
  def apply[T <: AnyRef](properties: Seq[Property[T]]): PropertySet[T] = {
    val map: Map[PropertyDef[T, _ <: Property[T]], Property[T]] =
      properties.map(p => (p.definition(), p)).toMap
    assert(map.size == properties.size)
    ImmutablePropertySet[T](map)
  }

  def apply[T <: AnyRef](
      map: Map[PropertyDef[T, _ <: Property[T]], Property[T]]): PropertySet[T] = {
    ImmutablePropertySet[T](map)
  }

  implicit class PropertySetImplicits[T <: AnyRef](propSet: PropertySet[T]) {
    def withProp(property: Property[T]): PropertySet[T] = {
      val before = propSet.getMap
      val after = before + (property.definition() -> property)
      assert(after.size == before.size)
      ImmutablePropertySet[T](after)
    }
  }

  private case class ImmutablePropertySet[T <: AnyRef](
      map: Map[PropertyDef[T, _ <: Property[T]], Property[T]])
    extends PropertySet[T] {
    override def getMap: Map[PropertyDef[T, _ <: Property[T]], Property[T]] = map
    override def satisfies(other: PropertySet[T]): Boolean = {
      assert(map.size == other.getMap.size)
      map.forall {
        case (propDef, prop) =>
          prop.satisfies(other.getMap(propDef))
      }
    }

    override def get[P <: Property[T]](propDef: PropertyDef[T, P]): P = {
      assert(map.contains(propDef))
      map(propDef).asInstanceOf[P]
    }

    override def toString: String = map.values.toVector.toString()
  }
}
