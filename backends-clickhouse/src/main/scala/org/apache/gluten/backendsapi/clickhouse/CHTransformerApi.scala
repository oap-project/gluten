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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.backendsapi.TransformerApi
import org.apache.gluten.execution.CHHashAggregateExecTransformer
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.substrait.expression.{BooleanLiteralNode, ExpressionBuilder, ExpressionNode}
import org.apache.gluten.utils.{CHInputPartitionsUtil, ExpressionDocUtil}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.DeltaMergeTreeFileFormat
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.BitSet

import com.google.common.collect.Lists
import com.google.protobuf.{Any, Message}

import java.util

class CHTransformerApi extends TransformerApi with Logging {

  /** Generate Seq[InputPartition] for FileSourceScanExecTransformer. */
  def genInputPartitionSeq(
      relation: HadoopFsRelation,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      bucketedScan: Boolean,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean): Seq[InputPartition] = {
    relation.location match {
      case index: TahoeFileIndex
          if relation.fileFormat
            .isInstanceOf[DeltaMergeTreeFileFormat] =>
        // Generate NativeMergeTreePartition for MergeTree
        ClickHouseTableV2
          .partsPartitions(
            index.deltaLog,
            relation,
            selectedPartitions,
            output,
            bucketedScan,
            optionalBucketSet,
            optionalNumCoalescedBuckets,
            disableBucketedScan
          )
      case _: TahoeFileIndex =>
        throw new UnsupportedOperationException("Does not support delta-parquet")
      case _ =>
        // Generate FilePartition for Parquet
        CHInputPartitionsUtil(
          relation,
          selectedPartitions,
          output,
          bucketedScan,
          optionalBucketSet,
          optionalNumCoalescedBuckets,
          disableBucketedScan).genInputPartitionSeq()
    }
  }

  override def postProcessNativeConfig(
      nativeConfMap: util.Map[String, String],
      backendPrefix: String): Unit = {
    val settingPrefix = backendPrefix + ".runtime_settings."
    if (nativeConfMap.getOrDefault("spark.memory.offHeap.enabled", "false").toBoolean) {
      val offHeapSize =
        nativeConfMap.getOrDefault("spark.gluten.memory.offHeap.size.in.bytes", "0").toLong
      if (offHeapSize > 0) {
        // Only set default max_bytes_before_external_sort for CH when it is not set explicitly.
        val sortSpillKey = settingPrefix + "max_bytes_before_external_sort";
        if (!nativeConfMap.containsKey(sortSpillKey)) {
          val sortSpillValue = offHeapSize * 0.5
          nativeConfMap.put(sortSpillKey, sortSpillValue.toLong.toString)
        }

        // Only set default max_bytes_before_external_group_by for CH when it is not set explicitly.
        val groupBySpillKey = settingPrefix + "max_bytes_before_external_group_by";
        if (!nativeConfMap.containsKey(groupBySpillKey)) {
          val groupBySpillValue = offHeapSize * 0.5
          nativeConfMap.put(groupBySpillKey, groupBySpillValue.toLong.toString)
        }

        val maxMemoryUsageKey = settingPrefix + "max_memory_usage";
        if (!nativeConfMap.containsKey(maxMemoryUsageKey)) {
          val maxMemoryUsageValue = offHeapSize
          nativeConfMap.put(maxMemoryUsageKey, maxMemoryUsageValue.toLong.toString)
        }

        // Only set default max_bytes_before_external_join for CH when join_algorithm is grace_hash
        val joinAlgorithmKey = settingPrefix + "join_algorithm";
        if (
          nativeConfMap.containsKey(joinAlgorithmKey) &&
          nativeConfMap.get(joinAlgorithmKey) == "grace_hash"
        ) {
          val joinSpillKey = settingPrefix + "max_bytes_in_join";
          if (!nativeConfMap.containsKey(joinSpillKey)) {
            val joinSpillValue = offHeapSize * 0.7
            nativeConfMap.put(joinSpillKey, joinSpillValue.toLong.toString)
          }
        }
      }
    }

    val injectConfig: (String, String) => Unit = (srcKey, dstKey) => {
      if (nativeConfMap.containsKey(srcKey) && !nativeConfMap.containsKey(dstKey)) {
        nativeConfMap.put(dstKey, nativeConfMap.get(srcKey))
      }
    }

    val hdfsConfigPrefix = backendPrefix + ".runtime_config.hdfs."
    injectConfig("spark.hadoop.input.connect.timeout", hdfsConfigPrefix + "input_connect_timeout")
    injectConfig("spark.hadoop.input.read.timeout", hdfsConfigPrefix + "input_read_timeout")
    injectConfig("spark.hadoop.input.write.timeout", hdfsConfigPrefix + "input_write_timeout")
    injectConfig(
      "spark.hadoop.dfs.client.log.severity",
      hdfsConfigPrefix + "dfs_client_log_severity")

    // TODO: set default to true when metrics could be collected
    // while ch query plan optimization is enabled.
    val planOptKey = settingPrefix + "query_plan_enable_optimizations"
    if (!nativeConfMap.containsKey(planOptKey)) {
      nativeConfMap.put(planOptKey, "false")
    }

    // Respect spark config spark.sql.orc.compression.codec for CH backend
    // TODO: consider compression or orc.compression in table options.
    val orcCompressionKey = settingPrefix + "output_format_orc_compression_method"
    if (!nativeConfMap.containsKey(orcCompressionKey)) {
      if (nativeConfMap.containsKey("spark.sql.orc.compression.codec")) {
        val compression = nativeConfMap.get("spark.sql.orc.compression.codec").toLowerCase()
        compression match {
          case "none" => nativeConfMap.put(orcCompressionKey, "none")
          case "uncompressed" => nativeConfMap.put(orcCompressionKey, "none")
          case "snappy" => nativeConfMap.put(orcCompressionKey, "snappy")
          case "zlib" => nativeConfMap.put(orcCompressionKey, "zlib")
          case "zstd" => nativeConfMap.put(orcCompressionKey, "zstd")
          case "lz4" => nativeConfMap.put(orcCompressionKey, "lz4")
          case _ =>
            throw new UnsupportedOperationException(s"Not supported ORC compression: $compression")
        }
      } else {
        nativeConfMap.put(orcCompressionKey, "snappy")
      }
    }
  }

  override def getSupportExpressionClassName: util.Set[String] = {
    ExpressionDocUtil.supportExpression()
  }

  override def getPlanOutput(plan: SparkPlan): Seq[Attribute] = {
    plan match {
      case hash: HashAggregateExec =>
        // when grouping expression has alias,
        // output name will be different from grouping expressions,
        // so using output attribute instead of grouping expression
        val groupingExpressions = hash.output.splitAt(hash.groupingExpressions.size)._1
        val aggResultAttributes = CHHashAggregateExecTransformer.getAggregateResultAttributes(
          groupingExpressions,
          hash.aggregateExpressions
        )
        if (aggResultAttributes.size == hash.output.size) {
          aggResultAttributes
        } else {
          var output = Seq.empty[Attribute]
          for (i <- hash.output.indices) {
            if (i < groupingExpressions.size) {
              output = output :+ aggResultAttributes(i)
            } else {
              output = output :+ hash.output(i)
            }
          }
          output
        }
      case _ =>
        plan.output
    }

  }

  override def createDateDiffParamList(
      start: ExpressionNode,
      end: ExpressionNode): Iterable[ExpressionNode] = {
    List(ExpressionBuilder.makeStringLiteral("day"), start, end)
  }

  override def createLikeParamList(
      left: ExpressionNode,
      right: ExpressionNode,
      escapeChar: ExpressionNode): Iterable[ExpressionNode] =
    List(left, right)

  override def createCheckOverflowExprNode(
      args: java.lang.Object,
      substraitExprName: String,
      childNode: ExpressionNode,
      dataType: DecimalType,
      nullable: Boolean,
      nullOnOverflow: Boolean): ExpressionNode = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        Seq(dataType, BooleanType),
        ConverterUtils.FunctionConfig.OPT))

    // Just make a fake toType value, because native engine cannot accept datatype itself.
    val toTypeNodes =
      ExpressionBuilder.makeDecimalLiteral(new Decimal().set(0, dataType.precision, dataType.scale))
    val expressionNodes =
      Lists.newArrayList(childNode, new BooleanLiteralNode(nullOnOverflow), toTypeNodes)
    val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }

  override def getNativePlanString(substraitPlan: Array[Byte], details: Boolean): String = {
    throw new UnsupportedOperationException("CH backend does not support this method")
  }

  override def packPBMessage(message: Message): Any = Any.pack(message)
}
