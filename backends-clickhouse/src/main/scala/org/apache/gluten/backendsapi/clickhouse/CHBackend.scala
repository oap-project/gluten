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

import org.apache.gluten.{CH_BRANCH, CH_COMMIT, GlutenConfig}
import org.apache.gluten.backendsapi._
import org.apache.gluten.expression.WindowFunctionsBuilder
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat._

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, DenseRank, Lag, Lead, NamedExpression, Rank, RowNumber}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}

import java.util.Locale

import scala.util.control.Breaks.{break, breakable}

class CHBackend extends Backend {
  override def name(): String = CHBackend.BACKEND_NAME
  override def buildInfo(): BackendBuildInfo =
    BackendBuildInfo("ClickHouse", CH_BRANCH, CH_COMMIT, "UNKNOWN")
  override def iteratorApi(): IteratorApi = new CHIteratorApi
  override def sparkPlanExecApi(): SparkPlanExecApi = new CHSparkPlanExecApi
  override def transformerApi(): TransformerApi = new CHTransformerApi
  override def validatorApi(): ValidatorApi = new CHValidatorApi
  override def metricsApi(): MetricsApi = new CHMetricsApi
  override def listenerApi(): ListenerApi = new CHListenerApi
  override def broadcastApi(): BroadcastApi = new CHBroadcastApi
  override def settings(): BackendSettingsApi = CHBackendSettings
}

object CHBackend {
  val BACKEND_NAME = "ch"
}

object CHBackendSettings extends BackendSettingsApi with Logging {

  private val GLUTEN_CLICKHOUSE_SEP_SCAN_RDD = "spark.gluten.sql.columnar.separate.scan.rdd.for.ch"
  private val GLUTEN_CLICKHOUSE_SEP_SCAN_RDD_DEFAULT = "false"

  // experimental: when the files count per partition exceeds this threshold,
  // it will put the files into one partition.
  val GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD: String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + CHBackend.BACKEND_NAME +
      ".files.per.partition.threshold"
  val GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD_DEFAULT = "-1"

  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE: String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + CHBackend.BACKEND_NAME +
      ".customized.shuffle.codec.enable"
  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE_DEFAULT = false
  lazy val useCustomizedShuffleCodec: Boolean = SparkEnv.get.conf.getBoolean(
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE,
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE_DEFAULT
  )

  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE: String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + CHBackend.BACKEND_NAME +
      ".customized.buffer.size"
  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE_DEFAULT = 4096
  lazy val customizeBufferSize: Int = SparkEnv.get.conf.getInt(
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE,
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE_DEFAULT
  )

  val GLUTEN_CLICKHOUSE_BROADCAST_CACHE_EXPIRED_TIME: String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + CHBackend.BACKEND_NAME +
      ".broadcast.cache.expired.time"
  // unit: SECONDS, default 1 day
  val GLUTEN_CLICKHOUSE_BROADCAST_CACHE_EXPIRED_TIME_DEFAULT: Int = 86400

  private val GLUTEN_CLICKHOUSE_SHUFFLE_SUPPORTED_CODEC: Set[String] = Set("lz4", "zstd", "snappy")

  // The algorithm for hash partition of the shuffle
  private val GLUTEN_CLICKHOUSE_SHUFFLE_HASH_ALGORITHM: String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + CHBackend.BACKEND_NAME +
      ".shuffle.hash.algorithm"
  // valid values are: cityHash64 or sparkMurmurHash3_32
  private val GLUTEN_CLICKHOUSE_SHUFFLE_HASH_ALGORITHM_DEFAULT = "sparkMurmurHash3_32"
  def shuffleHashAlgorithm: String = {
    val algorithm = SparkEnv.get.conf.get(
      CHBackendSettings.GLUTEN_CLICKHOUSE_SHUFFLE_HASH_ALGORITHM,
      CHBackendSettings.GLUTEN_CLICKHOUSE_SHUFFLE_HASH_ALGORITHM_DEFAULT
    )
    if (!algorithm.equals("cityHash64") && !algorithm.equals("sparkMurmurHash3_32")) {
      CHBackendSettings.GLUTEN_CLICKHOUSE_SHUFFLE_HASH_ALGORITHM_DEFAULT
    } else {
      algorithm
    }
  }

  val GLUTEN_CLICKHOUSE_AFFINITY_MODE: String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + CHBackend.BACKEND_NAME + ".affinity.mode"
  val SOFT: String = "soft"
  val FORCE: String = "force"
  private val GLUTEN_CLICKHOUSE_AFFINITY_MODE_DEFAULT = SOFT

  val GLUTEN_MAX_BLOCK_SIZE: String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + CHBackend.BACKEND_NAME +
      ".runtime_settings.max_block_size"
  // Same as default value in clickhouse
  val GLUTEN_MAX_BLOCK_SIZE_DEFAULT = 65409
  val GLUTEN_MAX_SHUFFLE_READ_BYTES: String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + CHBackend.BACKEND_NAME +
      ".runtime_config.max_source_concatenate_bytes"
  val GLUTEN_MAX_SHUFFLE_READ_BYTES_DEFAULT = -1

  def affinityMode: String = {
    SparkEnv.get.conf
      .get(
        CHBackendSettings.GLUTEN_CLICKHOUSE_AFFINITY_MODE,
        CHBackendSettings.GLUTEN_CLICKHOUSE_AFFINITY_MODE_DEFAULT
      )
      .toLowerCase(Locale.getDefault)
  }

  override def supportFileFormatRead(
      format: ReadFileFormat,
      fields: Array[StructField],
      partTable: Boolean,
      paths: Seq[String]): ValidationResult = {

    def validateFilePath: Boolean = {
      // Fallback to vanilla spark when the input path
      // does not contain the partition info.
      if (partTable && !paths.forall(_.contains("="))) {
        return false
      }
      true
    }

    // Validate if all types are supported.
    def hasComplexType: Boolean = {
      // Collect unsupported types.
      val unsupportedDataTypes = fields.map(_.dataType).collect {
        case _: MapType => "MapType"
        case _: StructType => "StructType"
        case _: ArrayType => "ArrayType"
      }
      for (unsupportedDataType <- unsupportedDataTypes) {
        // scalastyle:off println
        println(
          s"Validation failed for ${this.getClass.toString}" +
            s" due to: data type $unsupportedDataType. in file schema. ")
        // scalastyle:on println
      }
      !unsupportedDataTypes.isEmpty
    }
    format match {
      case ParquetReadFormat =>
        if (validateFilePath) {
          ValidationResult.ok
        } else {
          ValidationResult.notOk("Validate file path failed.")
        }
      case OrcReadFormat => ValidationResult.ok
      case MergeTreeReadFormat => ValidationResult.ok
      case TextReadFormat =>
        if (!hasComplexType) {
          ValidationResult.ok
        } else {
          ValidationResult.notOk("Has complex type.")
        }
      case JsonReadFormat => ValidationResult.ok
      case _ => ValidationResult.notOk(s"Unsupported file format $format")
    }
  }

  override def utilizeShuffledHashJoinHint(): Boolean = true
  override def supportShuffleWithProject(
      outputPartitioning: Partitioning,
      child: SparkPlan): Boolean = {
    child match {
      case hash: HashAggregateExec =>
        if (hash.aggregateExpressions.isEmpty) {
          true
        } else {
          outputPartitioning match {
            case hashPartitioning: HashPartitioning =>
              hashPartitioning.expressions.exists(x => !x.isInstanceOf[AttributeReference])
            case _ =>
              false
          }
        }
      case _ =>
        true
    }
  }

  override def supportSortExec(): Boolean = {
    GlutenConfig.getConf.enableColumnarSort
  }

  override def supportSortMergeJoinExec(): Boolean = {
    false
  }

  override def supportWindowExec(windowFunctions: Seq[NamedExpression]): Boolean = {
    var allSupported = true
    breakable {
      windowFunctions.foreach(
        func => {
          val aliasExpr = func.asInstanceOf[Alias]
          val wExpression = WindowFunctionsBuilder.extractWindowExpression(aliasExpr.child)
          wExpression.windowFunction match {
            case _: RowNumber | _: AggregateExpression | _: Rank | _: Lead | _: Lag |
                _: DenseRank =>
              allSupported = allSupported
            case _ =>
              logDebug(s"Not support window function: ${wExpression.getClass}")
              allSupported = false
              break
          }
        })
    }
    allSupported
  }

  override def supportStructType(): Boolean = true

  override def supportExpandExec(): Boolean = true

  override def excludeScanExecFromCollapsedStage(): Boolean =
    SQLConf.get
      .getConfString(GLUTEN_CLICKHOUSE_SEP_SCAN_RDD, GLUTEN_CLICKHOUSE_SEP_SCAN_RDD_DEFAULT)
      .toBoolean

  /** Get the config prefix for each backend */
  override def getBackendConfigPrefix: String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + CHBackend.BACKEND_NAME

  override def shuffleSupportedCodec(): Set[String] = GLUTEN_CLICKHOUSE_SHUFFLE_SUPPORTED_CODEC
  override def needOutputSchemaForPlan(): Boolean = true

  override def allowDecimalArithmetic: Boolean = !SQLConf.get.decimalOperationsAllowPrecisionLoss

  override def requiredInputFilePaths(): Boolean = true

  override def enableBloomFilterAggFallbackRule(): Boolean = false

  def maxShuffleReadRows(): Long = {
    SparkEnv.get.conf
      .getLong(GLUTEN_MAX_BLOCK_SIZE, GLUTEN_MAX_BLOCK_SIZE_DEFAULT)
  }

  def maxShuffleReadBytes(): Long = {
    SparkEnv.get.conf
      .getLong(GLUTEN_MAX_SHUFFLE_READ_BYTES, GLUTEN_MAX_SHUFFLE_READ_BYTES_DEFAULT)
  }

  override def supportWriteFilesExec(
      format: FileFormat,
      fields: Array[StructField],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String]): ValidationResult =
    ValidationResult.notOk("CH backend is unsupported.")

  override def enableNativeWriteFiles(): Boolean = {
    GlutenConfig.getConf.enableNativeWriter.getOrElse(false)
  }

  override def mergeTwoPhasesHashBaseAggregateIfNeed(): Boolean = true
}
