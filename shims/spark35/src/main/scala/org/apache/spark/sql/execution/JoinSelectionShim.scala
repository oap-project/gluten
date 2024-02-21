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
package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LogicalPlan}

// https://issues.apache.org/jira/browse/SPARK-36745
object JoinSelectionShim {
  object ExtractEquiJoinKeysShim {
    type ReturnType =
      (
          JoinType,
          Seq[Expression],
          Seq[Expression],
          Option[Expression],
          LogicalPlan,
          LogicalPlan,
          JoinHint)
    def unapply(join: Join): Option[ReturnType] = {
      ExtractEquiJoinKeys.unapply(join).map {
        case (
              joinType,
              leftKeys,
              rightKeys,
              otherPredicates,
              predicatesOfJoinKeys,
              left,
              right,
              hint) =>
          (joinType, leftKeys, rightKeys, otherPredicates, left, right, hint)
      }
    }
  }
}
