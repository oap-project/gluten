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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.backendsapi.TransformerApi
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.gluten.utils.InputPartitionsUtil
import org.apache.gluten.vectorized.PlanEvaluatorJniWrapper

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.types._
import org.apache.spark.util.TaskResources
import org.apache.spark.util.collection.BitSet

import com.google.protobuf.{Any, Message}

import java.util.{Map => JMap}

class TransformerApiImpl extends TransformerApi with Logging {

  /** Generate Seq[InputPartition] for FileSourceScanExecTransformer. */
  def genInputPartitionSeq(
      relation: HadoopFsRelation,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      bucketedScan: Boolean,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean): Seq[InputPartition] = {
    InputPartitionsUtil(
      relation,
      selectedPartitions,
      output,
      bucketedScan,
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      disableBucketedScan)
      .genInputPartitionSeq()
  }

  override def postProcessNativeConfig(
      nativeConfMap: JMap[String, String],
      backendPrefix: String): Unit = {
    // TODO: IMPLEMENT SPECIAL PROCESS FOR VELOX BACKEND
  }

  override def createDateDiffParamList(
      start: ExpressionNode,
      end: ExpressionNode): Iterable[ExpressionNode] = {
    List(end, start)
  }

  override def createLikeParamList(
      left: ExpressionNode,
      right: ExpressionNode,
      escapeChar: ExpressionNode): Iterable[ExpressionNode] = {
    List(left, right, escapeChar)
  }

  override def createCheckOverflowExprNode(
      args: java.lang.Object,
      substraitExprName: String,
      childNode: ExpressionNode,
      dataType: DecimalType,
      nullable: Boolean,
      nullOnOverflow: Boolean): ExpressionNode = {
    val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
    ExpressionBuilder.makeCast(typeNode, childNode, !nullOnOverflow)
  }

  override def getNativePlanString(substraitPlan: Array[Byte], details: Boolean): String = {
    TaskResources.runUnsafe {
      val jniWrapper = PlanEvaluatorJniWrapper.create()
      jniWrapper.nativePlanString(substraitPlan, details)
    }
  }

  override def packPBMessage(message: Message): Any = Any.pack(message, "")
}
