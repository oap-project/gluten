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
package io.substrait.spark.logical

import org.apache.spark.sql.catalyst.plans.logical._

import io.substrait.relation
import io.substrait.relation.Rel

class AbstractLogicalPlanVisitor extends LogicalPlanVisitor[relation.Rel] {

  protected def t(p: LogicalPlan): relation.Rel =
    throw new UnsupportedOperationException(s"Unable to convert the expression ${p.nodeName}")

  override def visitDistinct(p: Distinct): relation.Rel = t(p)

  override def visitExcept(p: Except): relation.Rel = t(p)

  override def visitExpand(p: Expand): relation.Rel = t(p)

  override def visitRepartition(p: Repartition): relation.Rel = t(p)

  override def visitRepartitionByExpr(p: RepartitionByExpression): relation.Rel = t(p)

  override def visitSample(p: Sample): relation.Rel = t(p)

  override def visitScriptTransform(p: ScriptTransformation): relation.Rel = t(p)

  override def visitUnion(p: Union): relation.Rel = t(p)

  override def visitWindow(p: Window): relation.Rel = t(p)

  override def visitTail(p: Tail): relation.Rel = t(p)

  override def visitGenerate(p: Generate): relation.Rel = t(p)

  override def visitGlobalLimit(p: GlobalLimit): relation.Rel = t(p)

  override def visitIntersect(p: Intersect): relation.Rel = t(p)

  override def visitLocalLimit(p: LocalLimit): relation.Rel = t(p)

  override def visitPivot(p: Pivot): relation.Rel = t(p)

  override def default(p: LogicalPlan): Rel = t(p)

  override def visitAggregate(p: Aggregate): Rel = t(p)

  override def visitFilter(p: Filter): Rel = t(p)

  override def visitJoin(p: Join): Rel = t(p)

  override def visitProject(p: Project): Rel = t(p)

  override def visitSort(sort: Sort): Rel = t(sort)

  override def visitWithCTE(p: WithCTE): Rel = t(p)

  override def visitOffset(p: Offset): Rel = t(p)

  override def visitRebalancePartitions(p: RebalancePartitions): Rel = t(p)
}
