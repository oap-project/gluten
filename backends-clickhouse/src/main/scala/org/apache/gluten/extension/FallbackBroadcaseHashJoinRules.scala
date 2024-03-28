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
package org.apache.gluten.extension

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.columnar._
import org.apache.gluten.extension.columnar.TransformHints.EncodeTransformableTagImplicits
import org.apache.gluten.utils.PhysicalPlanSelector

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeLike, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

import scala.util.control.Breaks.{break, breakable}

// BroadcastHashJoinExec and it's child BroadcastExec will be cut into different QueryStages,
// so the columnar rules will be applied to the two QueryStages separately, and they cannot
// see each other during transformation. In order to prevent BroadcastExec being transformed
// to columnar while BHJ fallbacks, BroadcastExec need to be tagged not transformable when applying
// queryStagePrepRules.
case class FallbackBroadcastHashJoinPrepQueryStage(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = PhysicalPlanSelector.maybe(session, plan) {
    val columnarConf: GlutenConfig = GlutenConfig.getConf
    plan.foreach {
      case bhj: BroadcastHashJoinExec =>
        val buildSidePlan = bhj.buildSide match {
          case BuildLeft => bhj.left
          case BuildRight => bhj.right
        }
        val maybeExchange = buildSidePlan.find {
          case BroadcastExchangeExec(_, _) => true
          case _ => false
        }
        maybeExchange match {
          case Some(exchange @ BroadcastExchangeExec(mode, child)) =>
            val isTransformable =
              if (
                !columnarConf.enableColumnarBroadcastExchange ||
                !columnarConf.enableColumnarBroadcastJoin
              ) {
                ValidationResult.notOk(
                  "columnar broadcast exchange is disabled or " +
                    "columnar broadcast join is disabled")
              } else {
                if (TransformHints.isNotTransformable(bhj)) {
                  ValidationResult.notOk("broadcast join is already tagged as not transformable")
                } else {
                  val bhjTransformer = BackendsApiManager.getSparkPlanExecApiInstance
                    .genBroadcastHashJoinExecTransformer(
                      bhj.leftKeys,
                      bhj.rightKeys,
                      bhj.joinType,
                      bhj.buildSide,
                      bhj.condition,
                      bhj.left,
                      bhj.right,
                      bhj.isNullAwareAntiJoin)
                  val isBhjTransformable = bhjTransformer.doValidate()
                  if (isBhjTransformable.isValid) {
                    val exchangeTransformer = ColumnarBroadcastExchangeExec(mode, child)
                    exchangeTransformer.doValidate()
                  } else {
                    isBhjTransformable
                  }
                }
              }
            TransformHints.tagNotTransformable(bhj, isTransformable)
            TransformHints.tagNotTransformable(exchange, isTransformable)
          case _ =>
          // Skip. This might be the case that the exchange was already
          // executed in earlier stage
        }
      case _ =>
    }
    plan
  }
}

// For similar purpose with FallbackBroadcastHashJoinPrepQueryStage, executed during applying
// columnar rules.
case class FallbackBroadcastHashJoin(session: SparkSession) extends Rule[SparkPlan] {

  private val enableColumnarBroadcastJoin: Boolean =
    GlutenConfig.getConf.enableColumnarBroadcastJoin &&
      GlutenConfig.getConf.enableColumnarBroadcastExchange

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.foreachUp {
      p =>
        try {
          p match {
            case bhj: BroadcastHashJoinExec =>
              // FIXME Hongze: In following codes we perform a lot of if-else conditions to
              //  make sure the broadcast exchange and broadcast hash-join are of same type,
              //  either vanilla or columnar. In order to simplify the codes we have to do
              //  some tricks around C2R and R2C to make them adapt to columnar broadcast.
              //  Currently their doBroadcast() methods just propagate child's broadcast
              //  payloads which is not right in speaking of columnar.
              if (!enableColumnarBroadcastJoin) {
                TransformHints.tagNotTransformable(
                  bhj,
                  "columnar BroadcastJoin is not enabled in BroadcastHashJoinExec")
              } else {
                val isBhjTransformable: ValidationResult = {
                  val transformer = BackendsApiManager.getSparkPlanExecApiInstance
                    .genBroadcastHashJoinExecTransformer(
                      bhj.leftKeys,
                      bhj.rightKeys,
                      bhj.joinType,
                      bhj.buildSide,
                      bhj.condition,
                      bhj.left,
                      bhj.right,
                      isNullAwareAntiJoin = bhj.isNullAwareAntiJoin)
                  transformer.doValidate()
                }
                val buildSidePlan = bhj.buildSide match {
                  case BuildLeft => bhj.left
                  case BuildRight => bhj.right
                }

                val maybeExchange = buildSidePlan
                  .find {
                    case BroadcastExchangeExec(_, _) => true
                    case _ => false
                  }
                  .map(_.asInstanceOf[BroadcastExchangeExec])

                maybeExchange match {
                  case Some(exchange @ BroadcastExchangeExec(mode, child)) =>
                    isBhjTransformable.tagOnFallback(bhj)
                    if (!isBhjTransformable.isValid) {
                      TransformHints.tagNotTransformable(exchange, isBhjTransformable)
                    }
                  case None =>
                    // we are in AQE, find the hidden exchange
                    // FIXME did we consider the case that AQE: OFF && Reuse: ON ?
                    var maybeHiddenExchange: Option[BroadcastExchangeLike] = None
                    breakable {
                      buildSidePlan.foreach {
                        case e: BroadcastExchangeLike =>
                          maybeHiddenExchange = Some(e)
                          break
                        case t: BroadcastQueryStageExec =>
                          t.plan.foreach {
                            case e2: BroadcastExchangeLike =>
                              maybeHiddenExchange = Some(e2)
                              break
                            case r: ReusedExchangeExec =>
                              r.child match {
                                case e2: BroadcastExchangeLike =>
                                  maybeHiddenExchange = Some(e2)
                                  break
                                case _ =>
                              }
                            case _ =>
                          }
                        case _ =>
                      }
                    }
                    // restriction to force the hidden exchange to be found
                    val exchange = maybeHiddenExchange.get
                    // to conform to the underlying exchange's type, columnar or vanilla
                    exchange match {
                      case BroadcastExchangeExec(mode, child) =>
                        TransformHints.tagNotTransformable(
                          bhj,
                          "it's a materialized broadcast exchange or reused broadcast exchange")
                      case ColumnarBroadcastExchangeExec(mode, child) =>
                        if (!isBhjTransformable.isValid) {
                          throw new IllegalStateException(
                            s"BroadcastExchange has already been" +
                              s" transformed to columnar version but BHJ is determined as" +
                              s" non-transformable: ${bhj.toString()}")
                        }
                    }
                }
              }
            case _ =>
          }
        } catch {
          case e: UnsupportedOperationException =>
            TransformHints.tagNotTransformable(
              p,
              s"${e.getMessage}, original Spark plan is " +
                s"${p.getClass}(${p.children.toList.map(_.getClass)})")
        }
    }
    plan
  }
}
