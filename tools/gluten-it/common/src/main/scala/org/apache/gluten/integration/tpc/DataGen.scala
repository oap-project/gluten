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
package org.apache.gluten.integration.tpc

import org.apache.spark.sql.types.{DataType, StructField, StructType}

trait DataGen {
  def gen(): Unit
}

abstract class TypeModifier(val predicate: DataType => Boolean, val to: DataType)
  extends Serializable {
  def modValue(value: Any): Any
}

class NoopModifier(t: DataType) extends TypeModifier(_ => true, t) {
  override def modValue(value: Any): Any = value
}

object DataGen {
  def getRowModifier(schema: StructType, typeModifiers: List[TypeModifier]): Int => TypeModifier = {
    val modifiers = schema.fields.map {
      f =>
        val matchedModifiers = typeModifiers.flatMap {
          m =>
            if (m.predicate.apply(f.dataType)) {
              Some(m)
            } else {
              None
            }
        }
        if (matchedModifiers.isEmpty) {
          new NoopModifier(f.dataType)
        } else {
          if (matchedModifiers.size > 1) {
            println(
              s"More than one type modifiers specified for type ${f.dataType}, " +
                s"use first one in the list")
          }
          matchedModifiers.head // use the first one that matches
        }
    }
    i => modifiers(i)
  }

  def modifySchema(schema: StructType, rowModifier: Int => TypeModifier): StructType = {
    val modifiedSchema = new StructType(schema.fields.zipWithIndex.map {
      case (f, i) =>
        val modifier = rowModifier.apply(i)
        StructField(f.name, modifier.to, f.nullable, f.metadata)
    })
    modifiedSchema
  }
}
