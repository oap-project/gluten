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
package io.glutenproject.integration.tpc

import org.apache.spark.SparkConf
import org.apache.spark.sql.TypeUtils
import org.apache.spark.sql.types.{DateType, DecimalType, DoubleType, IntegerType, LongType, StringType}

import java.sql.Date

object Constants {

  val VANILLA_CONF: SparkConf = new SparkConf(false)

  val VELOX_CONF: SparkConf = new SparkConf(false)
    .set("spark.gluten.sql.columnar.forceShuffledHashJoin", "true")
    .set("spark.sql.parquet.enableVectorizedReader", "true")
    .set("spark.plugins", "io.glutenproject.GlutenPlugin")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    .set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
    .set("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "0")
    .set(
      "spark.gluten.sql.columnar.physicalJoinOptimizeEnable",
      "false"
    ) // q72 slow if false, q64 fails if true

  val VELOX_WITH_CELEBORN_CONF: SparkConf = new SparkConf(false)
    .set("spark.gluten.sql.columnar.forceShuffledHashJoin", "true")
    .set("spark.sql.parquet.enableVectorizedReader", "true")
    .set("spark.plugins", "io.glutenproject.GlutenPlugin")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.gluten.celeborn.CelebornShuffleManager")
    .set("spark.celeborn.shuffle.writer", "hash")
    .set("spark.celeborn.push.replicate.enabled", "false")
    .set("spark.celeborn.client.shuffle.compression.codec", "none")
    .set("spark.shuffle.service.enabled", "false")
    .set("spark.sql.adaptive.localShuffleReader.enabled", "false")
    .set("spark.dynamicAllocation.enabled", "false")
    .set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
    .set("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "0")
    .set(
      "spark.gluten.sql.columnar.physicalJoinOptimizeEnable",
      "false"
    ) // q72 slow if false, q64 fails if true
    .set("spark.celeborn.push.data.timeout", "600s")
    .set("spark.celeborn.push.limit.inFlight.timeout", "1200s")

  val VELOX_WITH_UNIFFLE_CONF: SparkConf = new SparkConf(false)
    .set("spark.gluten.sql.columnar.forceShuffledHashJoin", "true")
    .set("spark.sql.parquet.enableVectorizedReader", "true")
    .set("spark.plugins", "io.glutenproject.GlutenPlugin")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.gluten.uniffle.GlutenRssShuffleManager")
    .set("spark.rss.coordinator.quorum", "localhost:19999")
    .set("spark.rss.storage.type", "MEMORY_LOCALFILE")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.shuffle.service.enabled", "false")
    .set("spark.sql.adaptive.localShuffleReader.enabled", "false")
    .set("spark.dynamicAllocation.enabled", "false")
    .set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
    .set("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "0")
    .set(
      "spark.gluten.sql.columnar.physicalJoinOptimizeEnable",
      "false"
    )

  @deprecated
  val TYPE_MODIFIER_DATE_AS_DOUBLE: TypeModifier =
    new TypeModifier(TypeUtils.typeAccepts(_, DateType), DoubleType) {
      override def modValue(from: Any): Any = {
        from match {
          case v: Date => v.getTime.asInstanceOf[Double] / 86400.0d / 1000.0d
        }
      }
    }

  @deprecated
  val TYPE_MODIFIER_INTEGER_AS_DOUBLE: TypeModifier =
    new TypeModifier(TypeUtils.typeAccepts(_, IntegerType), DoubleType) {
      override def modValue(from: Any): Any = {
        from match {
          case v: Int => v.asInstanceOf[Double]
        }
      }
    }

  @deprecated
  val TYPE_MODIFIER_LONG_AS_DOUBLE: TypeModifier =
    new TypeModifier(TypeUtils.typeAccepts(_, LongType), DoubleType) {
      override def modValue(from: Any): Any = {
        from match {
          case v: Long => v.asInstanceOf[Double]
        }
      }
    }

  @deprecated
  val TYPE_MODIFIER_DATE_AS_STRING: TypeModifier =
    new TypeModifier(TypeUtils.typeAccepts(_, DateType), StringType) {
      override def modValue(from: Any): Any = {
        from match {
          case v: Date => v.toString
        }
      }
    }

  @deprecated
  val TYPE_MODIFIER_DECIMAL_AS_DOUBLE: TypeModifier =
    new TypeModifier(TypeUtils.decimalAccepts, DoubleType) {
      override def modValue(from: Any): Any = {
        from match {
          case v: java.math.BigDecimal => v.doubleValue()
        }
      }
    }
}
