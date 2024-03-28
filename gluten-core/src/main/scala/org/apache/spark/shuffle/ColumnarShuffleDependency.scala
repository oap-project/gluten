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
package org.apache.spark.shuffle

import org.apache.gluten.vectorized.NativePartitioning

import org.apache.spark.{Aggregator, Partitioner, ShuffleDependency, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.execution.metric.SQLMetric

import scala.reflect.ClassTag

/**
 * :: DeveloperApi :: Represents a dependency on the output of a shuffle stage. Note that in the
 * case of shuffle, the RDD is transient since we don't need it on the executor side.
 *
 * @param _rdd
 *   the parent RDD
 * @param partitioner
 *   partitioner used to partition the shuffle output
 * @param serializer
 *   [[org.apache.spark.serializer.Serializer Serializer]] to use. If not set explicitly then the
 *   default serializer, as specified by `spark.serializer` config option, will be used.
 * @param keyOrdering
 *   key ordering for RDD's shuffles
 * @param aggregator
 *   map/reduce-side aggregator for RDD's shuffle
 * @param mapSideCombine
 *   whether to perform partial aggregation (also known as map-side combine)
 * @param shuffleWriterProcessor
 *   the processor to control the write behavior in ShuffleMapTask
 * @param nativePartitioning
 *   hold partitioning parameters needed by native shuffle writer
 * @param metrics
 *   the metrics for the columnar shuffle
 */
class ColumnarShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    override val partitioner: Partitioner,
    override val serializer: Serializer = SparkEnv.get.serializer,
    override val keyOrdering: Option[Ordering[K]] = None,
    override val aggregator: Option[Aggregator[K, V, C]] = None,
    override val mapSideCombine: Boolean = false,
    override val shuffleWriterProcessor: ShuffleWriteProcessor = new ShuffleWriteProcessor,
    val nativePartitioning: NativePartitioning,
    val metrics: Map[String, SQLMetric])
  extends ShuffleDependency[K, V, C](
    _rdd,
    partitioner,
    serializer,
    keyOrdering,
    aggregator,
    mapSideCombine,
    shuffleWriterProcessor) {}
