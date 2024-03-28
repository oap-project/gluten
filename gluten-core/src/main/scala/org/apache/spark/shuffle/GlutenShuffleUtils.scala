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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.vectorized.NativePartitioning

import org.apache.spark.SparkConf
import org.apache.spark.internal.config._
import org.apache.spark.storage.{BlockId, BlockManagerId}
import org.apache.spark.util.random.XORShiftRandom

import java.util.Locale

object GlutenShuffleUtils {
  val SinglePartitioningShortName = "single"
  val RoundRobinPartitioningShortName = "rr"
  val HashPartitioningShortName = "hash"
  val RangePartitioningShortName = "range"

  def getStartPartitionId(partition: NativePartitioning, partitionId: Int): Int = {
    partition.getShortName match {
      case RoundRobinPartitioningShortName =>
        new XORShiftRandom(partitionId).nextInt(partition.getNumPartitions)
      case _ => 0
    }
  }

  def checkCodecValues(codecConf: String, codec: String, validValues: Set[String]): Unit = {
    if (!validValues.contains(codec)) {
      throw new IllegalArgumentException(
        s"The value of $codecConf should be one of " +
          s"${validValues.mkString(", ")}, but was $codec")
    }
  }

  def getCompressionCodec(conf: SparkConf): String = {
    val glutenConfig = GlutenConfig.getConf
    glutenConfig.columnarShuffleCodec match {
      case Some(codec) =>
        val glutenCodecKey = GlutenConfig.COLUMNAR_SHUFFLE_CODEC.key
        if (glutenConfig.columnarShuffleEnableQat) {
          checkCodecValues(glutenCodecKey, codec, GlutenConfig.GLUTEN_QAT_SUPPORTED_CODEC)
        } else if (glutenConfig.columnarShuffleEnableIaa) {
          checkCodecValues(glutenCodecKey, codec, GlutenConfig.GLUTEN_IAA_SUPPORTED_CODEC)
        } else {
          checkCodecValues(
            glutenCodecKey,
            codec,
            BackendsApiManager.getSettings.shuffleSupportedCodec())
        }
        codec
      case None =>
        val sparkCodecKey = IO_COMPRESSION_CODEC.key
        val codec =
          conf
            .get(sparkCodecKey, IO_COMPRESSION_CODEC.defaultValueString)
            .toLowerCase(Locale.ROOT)
        checkCodecValues(
          sparkCodecKey,
          codec,
          BackendsApiManager.getSettings.shuffleSupportedCodec())
        codec
    }
  }

  def getCompressionLevel(conf: SparkConf, codec: String, compressionCodecBackend: String): Int = {
    if ("zstd" == codec && compressionCodecBackend == null) {
      conf.getInt(
        IO_COMPRESSION_ZSTD_LEVEL.key,
        IO_COMPRESSION_ZSTD_LEVEL.defaultValue.getOrElse(1))
    } else {
      // Follow arrow default compression level `kUseDefaultCompressionLevel`
      Int.MinValue
    }
  }

  def getReaderParam[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int
  ): Tuple2[Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])], Boolean] = {
    SparkShimLoader.getSparkShims.getShuffleReaderParam(
      handle,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition)
  }
}
