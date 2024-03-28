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
package org.apache.spark.sql.utils

import org.apache.gluten.extension.{DefaultExpressionExtensionTransformer, ExpressionExtensionTrait}

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

object ExpressionUtil extends Logging {

  /** Generate the extended expression transformer by conf */
  def extendedExpressionTransformer(
      extendedExpressionTransformer: String
  ): ExpressionExtensionTrait = {
    if (extendedExpressionTransformer.isEmpty) {
      DefaultExpressionExtensionTransformer()
    } else {
      try {
        val extensionConfClass = Utils.classForName(extendedExpressionTransformer)
        extensionConfClass
          .getConstructor()
          .newInstance()
          .asInstanceOf[ExpressionExtensionTrait]
      } catch {
        // Ignore the error if we cannot find the class or when the class has the wrong type.
        case e @ (_: ClassCastException | _: ClassNotFoundException | _: NoClassDefFoundError) =>
          logWarning(
            s"Cannot create extended expression transformer $extendedExpressionTransformer",
            e)
          DefaultExpressionExtensionTransformer()
      }
    }
  }
}
