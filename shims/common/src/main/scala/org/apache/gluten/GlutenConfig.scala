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
package org.apache.gluten

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.internal.SQLConf

import com.google.common.collect.ImmutableList
import org.apache.hadoop.security.UserGroupInformation

import java.util
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

case class GlutenNumaBindingInfo(
    enableNumaBinding: Boolean,
    totalCoreRange: Array[String] = null,
    numCoresPerExecutor: Int = -1) {}

class GlutenConfig(conf: SQLConf) extends Logging {
  import GlutenConfig._

  def enableAnsiMode: Boolean = conf.ansiEnabled

  def enableGluten: Boolean = conf.getConf(GLUTEN_ENABLED)

  def enableAdvancedCbo: Boolean = conf.getConf(ADVANCED_CBO_ENABLED)

  // FIXME the option currently controls both JVM and native validation against a Substrait plan.
  def enableNativeValidation: Boolean = conf.getConf(NATIVE_VALIDATION_ENABLED)

  def enableColumnarBatchScan: Boolean = conf.getConf(COLUMNAR_BATCHSCAN_ENABLED)

  def enableColumnarFileScan: Boolean = conf.getConf(COLUMNAR_FILESCAN_ENABLED)

  def enableColumnarHiveTableScan: Boolean = conf.getConf(COLUMNAR_HIVETABLESCAN_ENABLED)

  def enableVanillaVectorizedReaders: Boolean = conf.getConf(VANILLA_VECTORIZED_READERS_ENABLED)

  def enableColumnarHashAgg: Boolean = conf.getConf(COLUMNAR_HASHAGG_ENABLED)

  def forceToUseHashAgg: Boolean = conf.getConf(COLUMNAR_FORCE_HASHAGG_ENABLED)

  def enableColumnarProject: Boolean = conf.getConf(COLUMNAR_PROJECT_ENABLED)

  def enableColumnarFilter: Boolean = conf.getConf(COLUMNAR_FILTER_ENABLED)

  def enableColumnarSort: Boolean = conf.getConf(COLUMNAR_SORT_ENABLED)

  def enableColumnarWindow: Boolean = conf.getConf(COLUMNAR_WINDOW_ENABLED)

  def veloxColumnarWindowType: String = conf.getConfString(COLUMNAR_VELOX_WINDOW_TYPE.key)

  def enableColumnarShuffledHashJoin: Boolean = conf.getConf(COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED)

  def enableNativeColumnarToRow: Boolean = conf.getConf(COLUMNAR_COLUMNAR_TO_ROW_ENABLED)

  def forceShuffledHashJoin: Boolean = conf.getConf(COLUMNAR_FPRCE_SHUFFLED_HASH_JOIN_ENABLED)

  def enableColumnarSortMergeJoin: Boolean = conf.getConf(COLUMNAR_SORTMERGEJOIN_ENABLED)

  def enableColumnarUnion: Boolean = conf.getConf(COLUMNAR_UNION_ENABLED)

  def enableColumnarExpand: Boolean = conf.getConf(COLUMNAR_EXPAND_ENABLED)

  def enableColumnarBroadcastExchange: Boolean = conf.getConf(COLUMNAR_BROADCAST_EXCHANGE_ENABLED)

  def enableColumnarBroadcastJoin: Boolean = conf.getConf(COLUMNAR_BROADCAST_JOIN_ENABLED)

  def enableColumnarArrowUDF: Boolean = conf.getConf(COLUMNAR_ARROW_UDF_ENABLED)

  def enableColumnarCoalesce: Boolean = conf.getConf(COLUMNAR_COALESCE_ENABLED)

  def columnarTableCacheEnabled: Boolean = conf.getConf(COLUMNAR_TABLE_CACHE_ENABLED)

  def enableRewriteDateTimestampComparison: Boolean =
    conf.getConf(ENABLE_REWRITE_DATE_TIMESTAMP_COMPARISON)

  def enableCommonSubexpressionEliminate: Boolean =
    conf.getConf(ENABLE_COMMON_SUBEXPRESSION_ELIMINATE)

  def enableCountDistinctWithoutExpand: Boolean =
    conf.getConf(ENABLE_COUNT_DISTINCT_WITHOUT_EXPAND)

  def veloxOrcScanEnabled: Boolean =
    conf.getConf(VELOX_ORC_SCAN_ENABLED)

  def forceComplexTypeScanFallbackEnabled: Boolean =
    conf.getConf(VELOX_FORCE_COMPLEX_TYPE_SCAN_FALLBACK)

  def forceOrcCharTypeScanFallbackEnabled: Boolean =
    conf.getConf(VELOX_FORCE_ORC_CHAR_TYPE_SCAN_FALLBACK)

  // whether to use ColumnarShuffleManager
  def isUseColumnarShuffleManager: Boolean =
    conf
      .getConfString("spark.shuffle.manager", "sort")
      .equals("org.apache.spark.shuffle.sort.ColumnarShuffleManager")

  // whether to use CelebornShuffleManager
  def isUseCelebornShuffleManager: Boolean =
    conf
      .getConfString("spark.shuffle.manager", "sort")
      .contains("celeborn")

  def enableColumnarShuffle: Boolean = conf.getConf(COLUMNAR_SHUFFLE_ENABLED)

  def enablePreferColumnar: Boolean = conf.getConf(COLUMNAR_PREFER_ENABLED)

  def enableOneRowRelationColumnar: Boolean = conf.getConf(COLUMNAR_ONE_ROW_RELATION_ENABLED)

  def physicalJoinOptimizationThrottle: Integer =
    conf.getConf(COLUMNAR_PHYSICAL_JOIN_OPTIMIZATION_THROTTLE)

  def enablePhysicalJoinOptimize: Boolean =
    conf.getConf(COLUMNAR_PHYSICAL_JOIN_OPTIMIZATION_ENABLED)

  def logicalJoinOptimizationThrottle: Integer =
    conf.getConf(COLUMNAR_LOGICAL_JOIN_OPTIMIZATION_THROTTLE)

  def enableLogicalJoinOptimize: Boolean =
    conf.getConf(COLUMNAR_LOGICAL_JOIN_OPTIMIZATION_ENABLED)

  def enableScanOnly: Boolean = conf.getConf(COLUMNAR_SCAN_ONLY_ENABLED)

  def tmpFile: Option[String] = conf.getConf(COLUMNAR_TEMP_DIR)

  @deprecated def broadcastCacheTimeout: Int = conf.getConf(COLUMNAR_BROADCAST_CACHE_TIMEOUT)

  def columnarShuffleReallocThreshold: Double = conf.getConf(COLUMNAR_SHUFFLE_REALLOC_THRESHOLD)

  def columnarShuffleMergeThreshold: Double = conf.getConf(SHUFFLE_WRITER_MERGE_THRESHOLD)

  def columnarShuffleCodec: Option[String] = conf.getConf(COLUMNAR_SHUFFLE_CODEC)

  def columnarShuffleCompressionMode: String =
    conf.getConf(COLUMNAR_SHUFFLE_COMPRESSION_MODE)

  def columnarShuffleCodecBackend: Option[String] = conf
    .getConf(COLUMNAR_SHUFFLE_CODEC_BACKEND)
    .filter(Set(GLUTEN_QAT_BACKEND_NAME, GLUTEN_IAA_BACKEND_NAME).contains(_))

  def columnarShuffleEnableQat: Boolean =
    columnarShuffleCodecBackend.contains(GlutenConfig.GLUTEN_QAT_BACKEND_NAME)

  def columnarShuffleEnableIaa: Boolean =
    columnarShuffleCodecBackend.contains(GlutenConfig.GLUTEN_IAA_BACKEND_NAME)

  def columnarShuffleCompressionThreshold: Int =
    conf.getConf(COLUMNAR_SHUFFLE_COMPRESSION_THRESHOLD)

  def maxBatchSize: Int = conf.getConf(COLUMNAR_MAX_BATCH_SIZE)

  def shuffleWriterBufferSize: Int = conf
    .getConf(SHUFFLE_WRITER_BUFFER_SIZE)
    .getOrElse(maxBatchSize)

  def enableColumnarLimit: Boolean = conf.getConf(COLUMNAR_LIMIT_ENABLED)

  def enableColumnarGenerate: Boolean = conf.getConf(COLUMNAR_GENERATE_ENABLED)

  def enableTakeOrderedAndProject: Boolean =
    conf.getConf(COLUMNAR_TAKE_ORDERED_AND_PROJECT_ENABLED)

  def enableNativeBloomFilter: Boolean = conf.getConf(COLUMNAR_NATIVE_BLOOMFILTER_ENABLED)

  def enableNativeHyperLogLogAggregateFunction: Boolean =
    conf.getConf(COLUMNAR_NATIVE_HYPERLOGLOG_AGGREGATE_ENABLED)

  def columnarParquetWriteBlockSize: Long =
    conf.getConf(COLUMNAR_PARQUET_WRITE_BLOCK_SIZE)

  def columnarParquetWriteBlockRows: Long =
    conf.getConf(COLUMNAR_PARQUET_WRITE_BLOCK_ROWS)

  def wholeStageFallbackThreshold: Int = conf.getConf(COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD)

  def queryFallbackThreshold: Int = conf.getConf(COLUMNAR_QUERY_FALLBACK_THRESHOLD)

  def fallbackIgnoreRowToColumnar: Boolean = conf.getConf(COLUMNAR_FALLBACK_IGNORE_ROW_TO_COLUMNAR)

  def fallbackExpressionsThreshold: Int = conf.getConf(COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD)

  def fallbackPreferColumnar: Boolean = conf.getConf(COLUMNAR_FALLBACK_PREFER_COLUMNAR)

  def numaBindingInfo: GlutenNumaBindingInfo = {
    val enableNumaBinding: Boolean = conf.getConf(COLUMNAR_NUMA_BINDING_ENABLED)
    if (!enableNumaBinding) {
      GlutenNumaBindingInfo(enableNumaBinding = false)
    } else {
      val tmp = conf.getConf(COLUMNAR_NUMA_BINDING_CORE_RANGE)
      if (tmp.isEmpty) {
        GlutenNumaBindingInfo(enableNumaBinding = false)
      } else {
        val numCores = conf.getConfString("spark.executor.cores", "1").toInt
        val coreRangeList: Array[String] = tmp.get.split('|').map(_.trim)
        GlutenNumaBindingInfo(enableNumaBinding = true, coreRangeList, numCores)
      }

    }
  }

  def memoryIsolation: Boolean = conf.getConf(COLUMNAR_MEMORY_ISOLATION)

  def offHeapMemorySize: Long = conf.getConf(COLUMNAR_OFFHEAP_SIZE_IN_BYTES)

  def taskOffHeapMemorySize: Long = conf.getConf(COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES)

  def memoryOverAcquiredRatio: Double = conf.getConf(COLUMNAR_MEMORY_OVER_ACQUIRED_RATIO)

  def memoryReservationBlockSize: Long = conf.getConf(COLUMNAR_MEMORY_RESERVATION_BLOCK_SIZE)

  def conservativeTaskOffHeapMemorySize: Long =
    conf.getConf(COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES)

  def enableVeloxCache: Boolean = conf.getConf(COLUMNAR_VELOX_CACHE_ENABLED)

  def veloxMemCacheSize: Long = conf.getConf(COLUMNAR_VELOX_MEM_CACHE_SIZE)

  def veloxSsdCachePath: String = conf.getConf(COLUMNAR_VELOX_SSD_CACHE_PATH)

  def veloxSsdCacheSize: Long = conf.getConf(COLUMNAR_VELOX_SSD_CACHE_SIZE)

  def veloxSsdCacheShards: Integer = conf.getConf(COLUMNAR_VELOX_SSD_CACHE_SHARDS)

  def veloxSsdCacheIOThreads: Integer = conf.getConf(COLUMNAR_VELOX_SSD_CACHE_IO_THREADS)

  def veloxSsdODirectEnabled: Boolean = conf.getConf(COLUMNAR_VELOX_SSD_ODIRECT_ENABLED)

  def veloxConnectorIOThreads: Integer = conf.getConf(COLUMNAR_VELOX_CONNECTOR_IO_THREADS)

  def veloxSplitPreloadPerDriver: Integer = conf.getConf(COLUMNAR_VELOX_SPLIT_PRELOAD_PER_DRIVER)

  def veloxSpillStrategy: String = conf.getConf(COLUMNAR_VELOX_SPILL_STRATEGY)

  def veloxMaxSpillFileSize: Long = conf.getConf(COLUMNAR_VELOX_MAX_SPILL_FILE_SIZE)

  def veloxSpillFileSystem: String = conf.getConf(COLUMNAR_VELOX_SPILL_FILE_SYSTEM)

  def veloxBloomFilterExpectedNumItems: Long =
    conf.getConf(COLUMNAR_VELOX_BLOOM_FILTER_EXPECTED_NUM_ITEMS)

  def veloxBloomFilterNumBits: Long = conf.getConf(COLUMNAR_VELOX_BLOOM_FILTER_NUM_BITS)

  def veloxBloomFilterMaxNumBits: Long = conf.getConf(COLUMNAR_VELOX_BLOOM_FILTER_MAX_NUM_BITS)

  def chColumnarShufflePreferSpill: Boolean = conf.getConf(COLUMNAR_CH_SHUFFLE_PREFER_SPILL_ENABLED)

  def chColumnarShuffleSpillThreshold: Long = conf.getConf(COLUMNAR_CH_SHUFFLE_SPILL_THRESHOLD)

  def chColumnarThrowIfMemoryExceed: Boolean = conf.getConf(COLUMNAR_CH_THROW_IF_MEMORY_EXCEED)

  def chColumnarFlushBlockBufferBeforeEvict: Boolean =
    conf.getConf(COLUMNAR_CH_FLUSH_BLOCK_BUFFER_BEFORE_EVICT)

  def cartesianProductTransformerEnabled: Boolean =
    conf.getConf(CARTESIAN_PRODUCT_TRANSFORMER_ENABLED)

  def broadcastNestedLoopJoinTransformerTransformerEnabled: Boolean =
    conf.getConf(BROADCAST_NESTED_LOOP_JOIN_TRANSFORMER_ENABLED)

  def transformPlanLogLevel: String = conf.getConf(TRANSFORM_PLAN_LOG_LEVEL)

  def substraitPlanLogLevel: String = conf.getConf(SUBSTRAIT_PLAN_LOG_LEVEL)

  def validationLogLevel: String = conf.getConf(VALIDATION_LOG_LEVEL)

  def softAffinityLogLevel: String = conf.getConf(SOFT_AFFINITY_LOG_LEVEL)

  // A comma-separated list of classes for the extended columnar pre rules
  def extendedColumnarTransformRules: String = conf.getConf(EXTENDED_COLUMNAR_TRANSFORM_RULES)

  // A comma-separated list of classes for the extended columnar post rules
  def extendedColumnarPostRules: String = conf.getConf(EXTENDED_COLUMNAR_POST_RULES)

  def extendedExpressionTransformer: String = conf.getConf(EXTENDED_EXPRESSION_TRAN_CONF)

  def expressionBlacklist: Set[String] = {
    val blacklist = conf.getConf(EXPRESSION_BLACK_LIST)
    val blacklistSet: Set[String] = if (blacklist.isDefined) {
      blacklist.get.toLowerCase(Locale.ROOT).trim.split(",").toSet
    } else {
      Set.empty
    }

    if (conf.getConf(FALLBACK_REGEXP_EXPRESSIONS)) {
      val regexpList = "rlike,regexp_replace,regexp_extract,regexp_extract_all,split"
      regexpList.trim.split(",").toSet ++ blacklistSet
    } else {
      blacklistSet
    }
  }

  def printStackOnValidationFailure: Boolean =
    conf.getConf(VALIDATION_PRINT_FAILURE_STACK_)

  def enableFallbackReport: Boolean = conf.getConf(FALLBACK_REPORTER_ENABLED)

  def enableVeloxUserExceptionStacktrace: Boolean =
    conf.getConf(COLUMNAR_VELOX_ENABLE_USER_EXCEPTION_STACKTRACE)

  def memoryUseHugePages: Boolean =
    conf.getConf(COLUMNAR_VELOX_MEMORY_USE_HUGE_PAGES)

  def debug: Boolean = conf.getConf(DEBUG_ENABLED)
  def debugKeepJniWorkspace: Boolean =
    conf.getConf(DEBUG_ENABLED) && conf.getConf(DEBUG_KEEP_JNI_WORKSPACE)
  def taskStageId: Int = conf.getConf(BENCHMARK_TASK_STAGEID)
  def taskPartitionId: Int = conf.getConf(BENCHMARK_TASK_PARTITIONID)
  def taskId: Long = conf.getConf(BENCHMARK_TASK_TASK_ID)
  def textInputMaxBlockSize: Long = conf.getConf(TEXT_INPUT_ROW_MAX_BLOCK_SIZE)
  def textIputEmptyAsDefault: Boolean = conf.getConf(TEXT_INPUT_EMPTY_AS_DEFAULT)
  def enableParquetRowGroupMaxMinIndex: Boolean =
    conf.getConf(ENABLE_PARQUET_ROW_GROUP_MAX_MIN_INDEX)

  def enableVeloxFlushablePartialAggregation: Boolean =
    conf.getConf(VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED)
  def maxFlushableAggregationMemoryRatio: Option[Double] =
    conf.getConf(MAX_PARTIAL_AGGREGATION_MEMORY_RATIO)
  def maxExtendedFlushableAggregationMemoryRatio: Option[Double] =
    conf.getConf(MAX_PARTIAL_AGGREGATION_MEMORY_RATIO)
  def abandonFlushableAggregationMinPct: Option[Int] =
    conf.getConf(ABANDON_PARTIAL_AGGREGATION_MIN_PCT)
  def abandonFlushableAggregationMinRows: Option[Int] =
    conf.getConf(ABANDON_PARTIAL_AGGREGATION_MIN_ROWS)

  // Please use `BackendsApiManager.getSettings.enableNativeWriteFiles()` instead
  def enableNativeWriter: Option[Boolean] = conf.getConf(NATIVE_WRITER_ENABLED)

  def directorySizeGuess: Option[Int] =
    conf.getConf(DIRECTORY_SIZE_GUESS)
  def filePreloadThreshold: Option[Int] =
    conf.getConf(FILE_PRELOAD_THRESHOLD)
  def prefetchRowGroups: Option[Int] =
    conf.getConf(PREFETCH_ROW_GROUPS)
  def loadQuantum: Option[Int] =
    conf.getConf(LOAD_QUANTUM)
  def maxCoalescedDistanceBytes: Option[Int] =
    conf.getConf(MAX_COALESCED_DISTANCE_BYTES)
  def maxCoalescedBytes: Option[Int] =
    conf.getConf(MAX_COALESCED_BYTES)
  def cachePrefetchMinPct: Option[Int] =
    conf.getConf(CACHE_PREFETCH_MINPCT)

  def enableColumnarProjectCollapse: Boolean = conf.getConf(ENABLE_COLUMNAR_PROJECT_COLLAPSE)

  def awsSdkLogLevel: String = conf.getConf(AWS_SDK_LOG_LEVEL)

  def enableCastAvgAggregateFunction: Boolean = conf.getConf(COLUMNAR_NATIVE_CAST_AGGREGATE_ENABLED)
}

object GlutenConfig {
  import SQLConf._

  var GLUTEN_ENABLE_BY_DEFAULT = true
  val GLUTEN_ENABLE_KEY = "spark.gluten.enabled"
  val GLUTEN_LIB_NAME = "spark.gluten.sql.columnar.libname"
  val GLUTEN_LIB_PATH = "spark.gluten.sql.columnar.libpath"
  val GLUTEN_EXECUTOR_LIB_PATH = "spark.gluten.sql.columnar.executor.libpath"

  // Hive configurations.
  val SPARK_PREFIX = "spark."
  val HIVE_EXEC_ORC_STRIPE_SIZE = "hive.exec.orc.stripe.size"
  val SPARK_HIVE_EXEC_ORC_STRIPE_SIZE: String = SPARK_PREFIX + HIVE_EXEC_ORC_STRIPE_SIZE
  val HIVE_EXEC_ORC_ROW_INDEX_STRIDE = "hive.exec.orc.row.index.stride"
  val SPARK_HIVE_EXEC_ORC_ROW_INDEX_STRIDE: String = SPARK_PREFIX + HIVE_EXEC_ORC_ROW_INDEX_STRIDE
  val HIVE_EXEC_ORC_COMPRESS = "hive.exec.orc.compress"
  val SPARK_HIVE_EXEC_ORC_COMPRESS: String = SPARK_PREFIX + HIVE_EXEC_ORC_COMPRESS
  val SPARK_SQL_PARQUET_COMPRESSION_CODEC: String = "spark.sql.parquet.compression.codec"
  val PARQUET_BLOCK_SIZE: String = "parquet.block.size"
  val PARQUET_BLOCK_ROWS: String = "parquet.block.rows"
  val PARQUET_GZIP_WINDOW_SIZE: String = "parquet.gzip.windowSize"
  // Hadoop config
  val HADOOP_PREFIX = "spark.hadoop."

  // S3 config
  val S3A_PREFIX = "fs.s3a."
  val S3_ACCESS_KEY = "fs.s3a.access.key"
  val SPARK_S3_ACCESS_KEY: String = HADOOP_PREFIX + S3_ACCESS_KEY
  val S3_SECRET_KEY = "fs.s3a.secret.key"
  val SPARK_S3_SECRET_KEY: String = HADOOP_PREFIX + S3_SECRET_KEY
  val S3_ENDPOINT = "fs.s3a.endpoint"
  val SPARK_S3_ENDPOINT: String = HADOOP_PREFIX + S3_ENDPOINT
  val S3_CONNECTION_SSL_ENABLED = "fs.s3a.connection.ssl.enabled"
  val SPARK_S3_CONNECTION_SSL_ENABLED: String = HADOOP_PREFIX + S3_CONNECTION_SSL_ENABLED
  val S3_PATH_STYLE_ACCESS = "fs.s3a.path.style.access"
  val SPARK_S3_PATH_STYLE_ACCESS: String = HADOOP_PREFIX + S3_PATH_STYLE_ACCESS
  val S3_USE_INSTANCE_CREDENTIALS = "fs.s3a.use.instance.credentials"
  val SPARK_S3_USE_INSTANCE_CREDENTIALS: String = HADOOP_PREFIX + S3_USE_INSTANCE_CREDENTIALS
  val S3_IAM_ROLE = "fs.s3a.iam.role"
  val SPARK_S3_IAM: String = HADOOP_PREFIX + S3_IAM_ROLE
  val S3_IAM_ROLE_SESSION_NAME = "fs.s3a.iam.role.session.name"
  val SPARK_S3_IAM_SESSION_NAME: String = HADOOP_PREFIX + S3_IAM_ROLE_SESSION_NAME

  // Hardware acceleraters backend
  val GLUTEN_SHUFFLE_CODEC_BACKEND = "spark.gluten.sql.columnar.shuffle.codecBackend"
  // ABFS config
  val ABFS_ACCOUNT_KEY = "hadoop.fs.azure.account.key"
  val SPARK_ABFS_ACCOUNT_KEY: String = "spark." + ABFS_ACCOUNT_KEY

  // GCS config
  val GCS_PREFIX = "fs.gs."
  val GCS_STORAGE_ROOT_URL = "fs.gs.storage.root.url"
  val SPARK_GCS_STORAGE_ROOT_URL: String = HADOOP_PREFIX + GCS_STORAGE_ROOT_URL
  val GCS_AUTH_TYPE = "fs.gs.auth.type"
  val SPARK_GCS_AUTH_TYPE: String = HADOOP_PREFIX + GCS_AUTH_TYPE
  val GCS_AUTH_SERVICE_ACCOUNT_JSON_KEYFILE = "fs.gs.auth.service.account.json.keyfile"
  val SPARK_GCS_AUTH_SERVICE_ACCOUNT_JSON_KEYFILE: String =
    HADOOP_PREFIX + GCS_AUTH_SERVICE_ACCOUNT_JSON_KEYFILE

  // QAT config
  val GLUTEN_QAT_BACKEND_NAME = "qat"
  val GLUTEN_QAT_SUPPORTED_CODEC: Set[String] = Set("gzip", "zstd")
  // IAA config
  val GLUTEN_IAA_BACKEND_NAME = "iaa"
  val GLUTEN_IAA_SUPPORTED_CODEC: Set[String] = Set("gzip")

  val GLUTEN_CONFIG_PREFIX = "spark.gluten.sql.columnar.backend."

  // Private Spark configs.
  val GLUTEN_OFFHEAP_SIZE_KEY = "spark.memory.offHeap.size"
  val GLUTEN_OFFHEAP_ENABLED = "spark.memory.offHeap.enabled"

  // For Soft Affinity Scheduling
  // Enable Soft Affinity Scheduling, defalut value is false
  val GLUTEN_SOFT_AFFINITY_ENABLED = "spark.gluten.soft-affinity.enabled"
  val GLUTEN_SOFT_AFFINITY_ENABLED_DEFAULT_VALUE = false
  // Calculate the number of the replcations for scheduling to the target executors per file
  val GLUTEN_SOFT_AFFINITY_REPLICATIONS_NUM = "spark.gluten.soft-affinity.replications.num"
  val GLUTEN_SOFT_AFFINITY_REPLICATIONS_NUM_DEFAULT_VALUE = 2
  // For on HDFS, if there are already target hosts,
  // and then prefer to use the orginal target hosts to schedule
  val GLUTEN_SOFT_AFFINITY_MIN_TARGET_HOSTS = "spark.gluten.soft-affinity.min.target-hosts"
  val GLUTEN_SOFT_AFFINITY_MIN_TARGET_HOSTS_DEFAULT_VALUE = 1

  // Enable Soft Affinity duplicate reading detection, defalut value is true
  val GLUTEN_SOFT_AFFINITY_DUPLICATE_READING_DETECT_ENABLED =
    "spark.gluten.soft-affinity.duplicateReadingDetect.enabled"
  val GLUTEN_SOFT_AFFINITY_DUPLICATE_READING_DETECT_ENABLED_DEFAULT_VALUE = true
  // Enable Soft Affinity duplicate reading detection, defalut value is 10000
  val GLUTEN_SOFT_AFFINITY_MAX_DUPLICATE_READING_RECORDS =
    "spark.gluten.soft-affinity.maxDuplicateReading.records"
  val GLUTEN_SOFT_AFFINITY_MAX_DUPLICATE_READING_RECORDS_DEFAULT_VALUE = 10000

  // Pass through to native conf
  val GLUTEN_SAVE_DIR = "spark.gluten.saveDir"

  val GLUTEN_DEBUG_MODE = "spark.gluten.sql.debug"
  val GLUTEN_DEBUG_KEEP_JNI_WORKSPACE = "spark.gluten.sql.debug.keepJniWorkspace"

  // Added back to Spark Conf during executor initialization
  val GLUTEN_OFFHEAP_SIZE_IN_BYTES_KEY = "spark.gluten.memory.offHeap.size.in.bytes"
  val GLUTEN_TASK_OFFHEAP_SIZE_IN_BYTES_KEY = "spark.gluten.memory.task.offHeap.size.in.bytes"
  val GLUTEN_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES_KEY =
    "spark.gluten.memory.conservative.task.offHeap.size.in.bytes"

  // Batch size.
  val GLUTEN_MAX_BATCH_SIZE_KEY = "spark.gluten.sql.columnar.maxBatchSize"

  // Shuffle Writer buffer size.
  val GLUTEN_SHUFFLE_WRITER_BUFFER_SIZE = "spark.gluten.shuffleWriter.bufferSize"

  val GLUTEN_SHUFFLE_WRITER_MERGE_THRESHOLD = "spark.gluten.sql.columnar.shuffle.merge.threshold"

  // Controls whether to load DLL from jars. User can get dependent native libs packed into a jar
  // by executing dev/package.sh. Then, with that jar configured, Gluten can load the native libs
  // at runtime. This config is just for velox backend. And it is NOT applicable to the situation
  // where deployed gluten jar is generated through static build (e.g., Gluten's release jar).
  val GLUTEN_LOAD_LIB_FROM_JAR = "spark.gluten.loadLibFromJar"
  val GLUTEN_LOAD_LIB_FROM_JAR_DEFAULT = false

  // Expired time of execution with resource relation has cached
  val GLUTEN_RESOURCE_RELATION_EXPIRED_TIME = "spark.gluten.execution.resource.expired.time"
  // unit: SECONDS, default 1 day
  val GLUTEN_RESOURCE_RELATION_EXPIRED_TIME_DEFAULT: Int = 86400

  // Supported hive/python/scala udf names
  val GLUTEN_SUPPORTED_HIVE_UDFS = "spark.gluten.supported.hive.udfs"
  val GLUTEN_SUPPORTED_PYTHON_UDFS = "spark.gluten.supported.python.udfs"
  val GLUTEN_SUPPORTED_SCALA_UDFS = "spark.gluten.supported.scala.udfs"

  val GLUTEN_EXTENDED_EXPRESSION_TRAN_CONF =
    "spark.gluten.sql.columnar.extended.expressions.transformer"

  // This is an internal config property set by Gluten. It is used to hold default session timezone
  // and will be really used by Gluten only if `spark.sql.session.timeZone` is not set.
  val GLUTEN_DEFAULT_SESSION_TIMEZONE_KEY = "spark.gluten.sql.session.timeZone.default"

  // Principal of current user
  val GLUTEN_UGI_USERNAME = "spark.gluten.ugi.username"
  // Tokens of current user, split by `\0`
  val GLUTEN_UGI_TOKENS = "spark.gluten.ugi.tokens"

  val GLUTEN_UI_ENABLED = "spark.gluten.ui.enabled"

  var ins: GlutenConfig = _

  def getConf: GlutenConfig = {
    new GlutenConfig(SQLConf.get)
  }

  @deprecated
  def getTempFile: String = synchronized {
    if (ins != null && ins.tmpFile.nonEmpty) {
      ins.tmpFile.get
    } else {
      System.getProperty("java.io.tmpdir")
    }
  }

  /** Get dynamic configs. */
  def getNativeSessionConf(
      backendPrefix: String,
      conf: scala.collection.Map[String, String]): util.Map[String, String] = {
    val nativeConfMap = new util.HashMap[String, String]()
    val keys = ImmutableList.of(
      GLUTEN_DEBUG_MODE,
      GLUTEN_SAVE_DIR,
      GLUTEN_TASK_OFFHEAP_SIZE_IN_BYTES_KEY,
      GLUTEN_MAX_BATCH_SIZE_KEY,
      GLUTEN_SHUFFLE_WRITER_BUFFER_SIZE,
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      GLUTEN_DEFAULT_SESSION_TIMEZONE_KEY,
      SQLConf.LEGACY_SIZE_OF_NULL.key,
      "spark.io.compression.codec",
      COLUMNAR_VELOX_BLOOM_FILTER_EXPECTED_NUM_ITEMS.key,
      COLUMNAR_VELOX_BLOOM_FILTER_NUM_BITS.key,
      COLUMNAR_VELOX_BLOOM_FILTER_MAX_NUM_BITS.key,
      // s3 config
      SPARK_S3_ACCESS_KEY,
      SPARK_S3_SECRET_KEY,
      SPARK_S3_ENDPOINT,
      SPARK_S3_CONNECTION_SSL_ENABLED,
      SPARK_S3_PATH_STYLE_ACCESS,
      SPARK_S3_USE_INSTANCE_CREDENTIALS,
      SPARK_S3_IAM,
      SPARK_S3_IAM_SESSION_NAME,
      AWS_SDK_LOG_LEVEL.key,
      // gcs config
      SPARK_GCS_STORAGE_ROOT_URL,
      SPARK_GCS_AUTH_TYPE,
      SPARK_GCS_AUTH_SERVICE_ACCOUNT_JSON_KEYFILE
    )
    keys.forEach(
      k => {
        if (conf.contains(k)) {
          nativeConfMap.put(k, conf(k))
        }
      })

    val keyWithDefault = ImmutableList.of(
      (SQLConf.CASE_SENSITIVE.key, "false"),
      (SQLConf.IGNORE_MISSING_FILES.key, "false")
    )
    keyWithDefault.forEach(e => nativeConfMap.put(e._1, conf.getOrElse(e._1, e._2)))

    // Backend's dynamic session conf only.
    conf
      .filter(entry => entry._1.startsWith(backendPrefix) && !SQLConf.isStaticConfigKey(entry._1))
      .foreach(entry => nativeConfMap.put(entry._1, entry._2))

    // Pass the latest tokens to native
    nativeConfMap.put(
      GLUTEN_UGI_TOKENS,
      UserGroupInformation.getCurrentUser.getTokens.asScala
        .map(_.encodeToUrlString)
        .mkString("\u0000"))
    nativeConfMap.put(GLUTEN_UGI_USERNAME, UserGroupInformation.getCurrentUser.getUserName)

    // return
    nativeConfMap
  }

  /**
   * Get static and dynamic configs. Some of the config is dynamic in spark, but is static in
   * gluten, these will be used to construct HiveConnector which intends reused in velox
   */
  def getNativeBackendConf(
      backendPrefix: String,
      conf: scala.collection.Map[String, String]): util.Map[String, String] = {

    val nativeConfMap = new util.HashMap[String, String]()

    // some configs having default values
    val keyWithDefault = ImmutableList.of(
      (SPARK_S3_ACCESS_KEY, ""),
      (SPARK_S3_SECRET_KEY, ""),
      (SPARK_S3_ENDPOINT, "localhost:9000"),
      (SPARK_S3_CONNECTION_SSL_ENABLED, "false"),
      (SPARK_S3_PATH_STYLE_ACCESS, "true"),
      (SPARK_S3_USE_INSTANCE_CREDENTIALS, "false"),
      (SPARK_S3_IAM, ""),
      (SPARK_S3_IAM_SESSION_NAME, ""),
      (
        COLUMNAR_VELOX_CONNECTOR_IO_THREADS.key,
        COLUMNAR_VELOX_CONNECTOR_IO_THREADS.defaultValueString),
      (COLUMNAR_SHUFFLE_CODEC.key, ""),
      (COLUMNAR_SHUFFLE_CODEC_BACKEND.key, ""),
      ("spark.hadoop.input.connect.timeout", "180000"),
      ("spark.hadoop.input.read.timeout", "180000"),
      ("spark.hadoop.input.write.timeout", "180000"),
      ("spark.hadoop.dfs.client.log.severity", "INFO"),
      ("spark.sql.orc.compression.codec", "snappy"),
      (
        COLUMNAR_VELOX_FILE_HANDLE_CACHE_ENABLED.key,
        COLUMNAR_VELOX_FILE_HANDLE_CACHE_ENABLED.defaultValueString),
      (AWS_SDK_LOG_LEVEL.key, AWS_SDK_LOG_LEVEL.defaultValueString)
    )
    keyWithDefault.forEach(e => nativeConfMap.put(e._1, conf.getOrElse(e._1, e._2)))

    val keys = ImmutableList.of(
      GLUTEN_DEBUG_MODE,
      // datasource config
      SPARK_SQL_PARQUET_COMPRESSION_CODEC,
      // datasource config end

      GLUTEN_OFFHEAP_SIZE_IN_BYTES_KEY,
      GLUTEN_TASK_OFFHEAP_SIZE_IN_BYTES_KEY,
      GLUTEN_OFFHEAP_ENABLED
    )
    keys.forEach(
      k => {
        if (conf.contains(k)) {
          nativeConfMap.put(k, conf(k))
        }
      })

    conf
      .filter(_._1.startsWith(backendPrefix))
      .foreach(entry => nativeConfMap.put(entry._1, entry._2))

    // put in all S3 configs
    conf
      .filter(_._1.startsWith(HADOOP_PREFIX + S3A_PREFIX))
      .foreach(entry => nativeConfMap.put(entry._1, entry._2))

    conf
      .filter(_._1.startsWith(SPARK_ABFS_ACCOUNT_KEY))
      .foreach(entry => nativeConfMap.put(entry._1, entry._2))

    // return
    nativeConfMap
  }

  val GLUTEN_ENABLED =
    buildConf(GLUTEN_ENABLE_KEY)
      .internal()
      .doc("Whether to enable gluten. Default value is true. Just an experimental property." +
        " Recommend to enable/disable Gluten through the setting for spark.plugins.")
      .booleanConf
      .createWithDefault(GLUTEN_ENABLE_BY_DEFAULT)

  val ADVANCED_CBO_ENABLED =
    buildConf("spark.gluten.sql.advanced.cbo.enabled")
      .doc(
        "Experimental: Enables Gluten's advanced CBO features during physical planning. " +
          "E.g, More efficient fallback strategy, etc. The option can be turned on and off " +
          "individually despite vanilla Spark's CBO settings. Note, Gluten's query optimizer " +
          "may still adopt a subset of its advanced CBO capabilities even this option " +
          "is off. Enabling it would cause Gluten consider using CBO for optimization " +
          "more aggressively. Note, this feature is still in development and may not bring " +
          "performance profits.")
      .booleanConf
      .createWithDefault(false)

  // FIXME the option currently controls both JVM and native validation against a Substrait plan.
  val NATIVE_VALIDATION_ENABLED =
    buildConf("spark.gluten.sql.enable.native.validation")
      .internal()
      .doc(
        "This is tmp config to specify whether to enable the native validation based on " +
          "Substrait plan. After the validations in all backends are correctly implemented, " +
          "this config should be removed.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_BATCHSCAN_ENABLED =
    buildConf("spark.gluten.sql.columnar.batchscan")
      .internal()
      .doc("Enable or disable columnar batchscan.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_FILESCAN_ENABLED =
    buildConf("spark.gluten.sql.columnar.filescan")
      .internal()
      .doc("Enable or disable columnar filescan.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_HIVETABLESCAN_ENABLED =
    buildConf("spark.gluten.sql.columnar.hivetablescan")
      .internal()
      .doc("Enable or disable columnar hivetablescan.")
      .booleanConf
      .createWithDefault(true)

  val VANILLA_VECTORIZED_READERS_ENABLED =
    buildConf("spark.gluten.sql.columnar.enableVanillaVectorizedReaders")
      .internal()
      .doc("Enable or disable vanilla vectorized scan.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_HASHAGG_ENABLED =
    buildConf("spark.gluten.sql.columnar.hashagg")
      .internal()
      .doc("Enable or disable columnar hashagg.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_FORCE_HASHAGG_ENABLED =
    buildConf("spark.gluten.sql.columnar.force.hashagg")
      .internal()
      .doc("Whether to force to use gluten's hash agg for replacing vanilla spark's sort agg.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_PROJECT_ENABLED =
    buildConf("spark.gluten.sql.columnar.project")
      .internal()
      .doc("Enable or disable columnar project.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_FILTER_ENABLED =
    buildConf("spark.gluten.sql.columnar.filter")
      .internal()
      .doc("Enable or disable columnar filter.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_SORT_ENABLED =
    buildConf("spark.gluten.sql.columnar.sort")
      .internal()
      .doc("Enable or disable columnar sort.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_WINDOW_ENABLED =
    buildConf("spark.gluten.sql.columnar.window")
      .internal()
      .doc("Enable or disable columnar window.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_VELOX_WINDOW_TYPE =
    buildConf("spark.gluten.sql.columnar.backend.velox.window.type")
      .internal()
      .doc(
        "Velox backend supports both SortWindow and" +
          " StreamingWindow operators." +
          " The StreamingWindow operator skips the sorting step" +
          " in the input but does not support spill." +
          " On the other hand, the SortWindow operator is " +
          "responsible for sorting the input data within the" +
          " Window operator and also supports spill.")
      .stringConf
      .checkValues(Set("streaming", "sort"))
      .createWithDefault("streaming")

  val COLUMNAR_PREFER_STREAMING_AGGREGATE =
    buildConf("spark.gluten.sql.columnar.preferStreamingAggregate")
      .internal()
      .doc(
        "Velox backend supports `StreamingAggregate`. `StreamingAggregate` uses the less " +
          "memory as it does not need to hold all groups in memory, so it could avoid spill. " +
          "When true and the child output ordering satisfies the grouping key then " +
          "Gluten will choose `StreamingAggregate` as the native operator.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_FPRCE_SHUFFLED_HASH_JOIN_ENABLED =
    buildConf("spark.gluten.sql.columnar.forceShuffledHashJoin")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED =
    buildConf("spark.gluten.sql.columnar.shuffledHashJoin")
      .internal()
      .doc("Enable or disable columnar shuffledHashJoin.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_COLUMNAR_TO_ROW_ENABLED =
    buildConf("spark.gluten.sql.columnar.columnarToRow")
      .internal()
      .doc("Enable or disable columnar columnarToRow.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_SORTMERGEJOIN_ENABLED =
    buildConf("spark.gluten.sql.columnar.sortMergeJoin")
      .internal()
      .doc("Enable or disable columnar sortMergeJoin. " +
        "This should be set with preferSortMergeJoin=false.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_UNION_ENABLED =
    buildConf("spark.gluten.sql.columnar.union")
      .internal()
      .doc("Enable or disable columnar union.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_EXPAND_ENABLED =
    buildConf("spark.gluten.sql.columnar.expand")
      .internal()
      .doc("Enable or disable columnar expand.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_BROADCAST_EXCHANGE_ENABLED =
    buildConf("spark.gluten.sql.columnar.broadcastExchange")
      .internal()
      .doc("Enable or disable columnar broadcastExchange.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_BROADCAST_JOIN_ENABLED =
    buildConf("spark.gluten.sql.columnar.broadcastJoin")
      .internal()
      .doc("Enable or disable columnar broadcastJoin.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_ARROW_UDF_ENABLED =
    buildConf("spark.gluten.sql.columnar.arrowUdf")
      .internal()
      .doc("Enable or disable columnar arrow udf.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_COALESCE_ENABLED =
    buildConf("spark.gluten.sql.columnar.coalesce")
      .internal()
      .doc("Enable or disable columnar coalesce.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_SHUFFLE_ENABLED =
    buildConf("spark.gluten.sql.columnar.shuffle")
      .internal()
      .doc("Enable or disable columnar shuffle.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_PREFER_ENABLED =
    buildConf("spark.gluten.sql.columnar.preferColumnar")
      .internal()
      .doc("Prefer to use columnar operators if set to true.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_ONE_ROW_RELATION_ENABLED =
    buildConf("spark.gluten.sql.columnar.oneRowRelation")
      .internal()
      .doc("Enable or disable columnar `OneRowRelation`.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_TABLE_CACHE_ENABLED =
    buildConf("spark.gluten.sql.columnar.tableCache")
      .internal()
      .doc("Enable or disable columnar table cache.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_PHYSICAL_JOIN_OPTIMIZATION_THROTTLE =
    buildConf("spark.gluten.sql.columnar.physicalJoinOptimizationLevel")
      .internal()
      .doc("Fallback to row operators if there are several continuous joins.")
      .intConf
      .createWithDefault(12)

  val COLUMNAR_PHYSICAL_JOIN_OPTIMIZATION_ENABLED =
    buildConf("spark.gluten.sql.columnar.physicalJoinOptimizeEnable")
      .internal()
      .doc("Enable or disable columnar physicalJoinOptimize.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_LOGICAL_JOIN_OPTIMIZATION_THROTTLE =
    buildConf("spark.gluten.sql.columnar.logicalJoinOptimizationLevel")
      .internal()
      .doc("Fallback to row operators if there are several continuous joins.")
      .intConf
      .createWithDefault(12)

  val COLUMNAR_LOGICAL_JOIN_OPTIMIZATION_ENABLED =
    buildConf("spark.gluten.sql.columnar.logicalJoinOptimizeEnable")
      .internal()
      .doc("Enable or disable columnar logicalJoinOptimize.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_SCAN_ONLY_ENABLED =
    buildConf("spark.gluten.sql.columnar.scanOnly")
      .internal()
      .doc("When enabled, only scan and the filter after scan will be offloaded to native.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_TEMP_DIR =
    buildConf("spark.gluten.sql.columnar.tmp_dir")
      .internal()
      .doc("A folder to store the codegen files.")
      .stringConf
      .createOptional

  val COLUMNAR_BROADCAST_CACHE_TIMEOUT =
    buildConf("spark.sql.columnar.sort.broadcast.cache.timeout")
      .internal()
      .doc("Deprecated")
      .intConf
      .createWithDefault(-1)

  val COLUMNAR_SHUFFLE_REALLOC_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.shuffle.realloc.threshold")
      .internal()
      .doubleConf
      .checkValue(v => v >= 0 && v <= 1, "Buffer reallocation threshold must between [0, 1]")
      .createWithDefault(0.25)

  val COLUMNAR_SHUFFLE_CODEC =
    buildConf("spark.gluten.sql.columnar.shuffle.codec")
      .internal()
      .doc(
        "By default, the supported codecs are lz4 and zstd. " +
          "When spark.gluten.sql.columnar.shuffle.codecBackend=qat," +
          "the supported codecs are gzip and zstd. " +
          "When spark.gluten.sql.columnar.shuffle.codecBackend=iaa," +
          "the supported codec is gzip.")
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .createOptional

  val COLUMNAR_SHUFFLE_CODEC_BACKEND =
    buildConf(GlutenConfig.GLUTEN_SHUFFLE_CODEC_BACKEND)
      .internal()
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .createOptional

  val COLUMNAR_SHUFFLE_COMPRESSION_MODE =
    buildConf("spark.gluten.sql.columnar.shuffle.compressionMode")
      .internal()
      .doc("buffer means compress each buffer to pre allocated big buffer," +
        "rowvector means to copy the buffers to a big buffer, and then compress the buffer")
      .stringConf
      .checkValues(Set("buffer", "rowvector"))
      .createWithDefault("buffer")

  val COLUMNAR_SHUFFLE_COMPRESSION_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.shuffle.compression.threshold")
      .internal()
      .doc("If number of rows in a batch falls below this threshold," +
        " will copy all buffers into one buffer to compress.")
      .intConf
      .createWithDefault(100)

  val SHUFFLE_WRITER_MERGE_THRESHOLD =
    buildConf(GLUTEN_SHUFFLE_WRITER_MERGE_THRESHOLD)
      .internal()
      .doubleConf
      .checkValue(v => v >= 0 && v <= 1, "Shuffle writer merge threshold must between [0, 1]")
      .createWithDefault(0.25)

  val COLUMNAR_MAX_BATCH_SIZE =
    buildConf(GLUTEN_MAX_BATCH_SIZE_KEY)
      .internal()
      .intConf
      .checkValue(_ > 0, s"$GLUTEN_MAX_BATCH_SIZE_KEY must be positive.")
      .createWithDefault(4096)

  // if not set, use COLUMNAR_MAX_BATCH_SIZE instead
  val SHUFFLE_WRITER_BUFFER_SIZE =
    buildConf(GLUTEN_SHUFFLE_WRITER_BUFFER_SIZE)
      .internal()
      .intConf
      .checkValue(_ > 0, s"$GLUTEN_SHUFFLE_WRITER_BUFFER_SIZE must be positive.")
      .createOptional

  val COLUMNAR_LIMIT_ENABLED =
    buildConf("spark.gluten.sql.columnar.limit")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_GENERATE_ENABLED =
    buildConf("spark.gluten.sql.columnar.generate")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_TAKE_ORDERED_AND_PROJECT_ENABLED =
    buildConf("spark.gluten.sql.columnar.takeOrderedAndProject")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_NATIVE_BLOOMFILTER_ENABLED =
    buildConf("spark.gluten.sql.native.bloomFilter")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_NATIVE_HYPERLOGLOG_AGGREGATE_ENABLED =
    buildConf("spark.gluten.sql.native.hyperLogLog.Aggregate")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_PARQUET_WRITE_BLOCK_SIZE =
    buildConf("spark.gluten.sql.columnar.parquet.write.blockSize")
      .internal()
      .longConf
      .createWithDefault(128 * 1024 * 1024)

  val COLUMNAR_PARQUET_WRITE_BLOCK_ROWS =
    buildConf("spark.gluten.sql.native.parquet.write.blockRows")
      .internal()
      .longConf
      .createWithDefault(100 * 1000 * 1000)

  val COLUMNAR_QUERY_FALLBACK_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.query.fallback.threshold")
      .internal()
      .doc("The threshold for whether query will fall back " +
        "by counting the number of ColumnarToRow & vanilla leaf node.")
      .intConf
      .createWithDefault(-1)

  val COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.wholeStage.fallback.threshold")
      .internal()
      .doc("The threshold for whether whole stage will fall back in AQE supported case " +
        "by counting the number of ColumnarToRow & vanilla leaf node.")
      .intConf
      .createWithDefault(-1)

  val COLUMNAR_FALLBACK_IGNORE_ROW_TO_COLUMNAR =
    buildConf("spark.gluten.sql.columnar.fallback.ignoreRowToColumnar")
      .internal()
      .doc(
        "When true, the fallback policy ignores the RowToColumnar when counting fallback number.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.fallback.expressions.threshold")
      .internal()
      .doc("Fall back filter/project if number of nested expressions reaches this threshold," +
        " considering Spark codegen can bring better performance for such case.")
      .intConf
      .createWithDefault(50)

  val COLUMNAR_FALLBACK_PREFER_COLUMNAR =
    buildConf("spark.gluten.sql.columnar.fallback.preferColumnar")
      .internal()
      .doc(
        "When true, the fallback policy prefers to use Gluten plan rather than vanilla " +
          "Spark plan if the both of them contains ColumnarToRow and the vanilla Spark plan " +
          "ColumnarToRow number is not smaller than Gluten plan.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_NUMA_BINDING_ENABLED =
    buildConf("spark.gluten.sql.columnar.numaBinding")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_NUMA_BINDING_CORE_RANGE =
    buildConf("spark.gluten.sql.columnar.coreRange")
      .internal()
      .stringConf
      .createOptional

  val COLUMNAR_OFFHEAP_SIZE_IN_BYTES =
    buildConf(GlutenConfig.GLUTEN_OFFHEAP_SIZE_IN_BYTES_KEY)
      .internal()
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES =
    buildConf(GlutenConfig.GLUTEN_TASK_OFFHEAP_SIZE_IN_BYTES_KEY)
      .internal()
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES =
    buildConf(GlutenConfig.GLUTEN_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES_KEY)
      .internal()
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_MEMORY_ISOLATION =
    buildConf("spark.gluten.memory.isolation")
      .internal()
      .doc("Enable isolated memory mode. If true, Gluten controls the maximum off-heap memory " +
        "can be used by each task to X, X = executor memory / max task slots. It's recommended " +
        "to set true if Gluten serves concurrent queries within a single session, since not all " +
        "memory Gluten allocated is guaranteed to be spillable. In the case, the feature should " +
        "be enabled to avoid OOM.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_MEMORY_OVER_ACQUIRED_RATIO =
    buildConf("spark.gluten.memory.overAcquiredMemoryRatio")
      .internal()
      .doc("If larger than 0, Velox backend will try over-acquire this ratio of the total " +
        "allocated memory as backup to avoid OOM.")
      .doubleConf
      .checkValue(d => d >= 0.0d, "Over-acquired ratio should be larger than or equals 0")
      .createWithDefault(0.3d)

  val COLUMNAR_MEMORY_RESERVATION_BLOCK_SIZE =
    buildConf("spark.gluten.memory.reservationBlockSize")
      .internal()
      .doc("Block size of native reservation listener reserve memory from Spark.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("8MB")

  // velox caching options
  val COLUMNAR_VELOX_CACHE_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cacheEnabled")
      .internal()
      .doc("Enable Velox cache, default off")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_MEM_CACHE_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.memCacheSize")
      .internal()
      .doc("The memory cache size")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_SSD_CACHE_PATH =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCachePath")
      .internal()
      .doc("The folder to store the cache files, better on SSD")
      .stringConf
      .createWithDefault("/tmp")

  val COLUMNAR_VELOX_SSD_CACHE_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCacheSize")
      .internal()
      .doc("The SSD cache size, will do memory caching only if this value = 0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_SSD_CACHE_SHARDS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCacheShards")
      .internal()
      .doc("The cache shards")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_SSD_CACHE_IO_THREADS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCacheIOThreads")
      .internal()
      .doc("The IO threads for cache promoting")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_SSD_ODIRECT_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdODirect")
      .internal()
      .doc("The O_DIRECT flag for cache writing")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_CONNECTOR_IO_THREADS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.IOThreads")
      .internal()
      .doc("The IO threads for connector split preloading")
      .intConf
      .createWithDefault(0)

  val COLUMNAR_VELOX_ASYNC_TIMEOUT =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.asyncTimeoutOnTaskStopping")
      .internal()
      .doc(
        "Timeout for asynchronous execution when task is being stopped in Velox backend. " +
          "It's recommended to set to a number larger than network connection timeout that the " +
          "possible aysnc tasks are relying on.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(30000)

  val COLUMNAR_VELOX_SPLIT_PRELOAD_PER_DRIVER =
    buildConf("spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver")
      .internal()
      .doc("The split preload per task")
      .intConf
      .createWithDefault(2)

  val COLUMNAR_VELOX_GLOG_VERBOSE_LEVEL =
    buildConf("spark.gluten.sql.columnar.backend.velox.glogVerboseLevel")
      .internal()
      .doc("Set glog verbose level in Velox backend, same as FLAGS_v.")
      .intConf
      .createWithDefault(0)

  val COLUMNAR_VELOX_GLOG_SEVERITY_LEVEL =
    buildConf("spark.gluten.sql.columnar.backend.velox.glogSeverityLevel")
      .internal()
      .doc("Set glog severity level in Velox backend, same as FLAGS_minloglevel.")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_SPILL_STRATEGY =
    buildConf("spark.gluten.sql.columnar.backend.velox.spillStrategy")
      .internal()
      .doc("none: Disable spill on Velox backend; " +
        "auto: Let Spark memory manager manage Velox's spilling")
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValues(Set("none", "auto"))
      .createWithDefault("auto")

  val COLUMNAR_VELOX_MAX_SPILL_FILE_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxSpillFileSize")
      .internal()
      .doc("The maximum size of a single spill file created")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_SPILL_FILE_SYSTEM =
    buildConf("spark.gluten.sql.columnar.backend.velox.spillFileSystem")
      .internal()
      .doc(
        "The filesystem used to store spill data. local: The local file system. " +
          "heap-over-local: Write file to JVM heap if having extra heap space. " +
          "Otherwise write to local file system.")
      .stringConf
      .checkValues(Set("local", "heap-over-local"))
      .createWithDefaultString("local")

  val MAX_PARTITION_PER_WRITERS_SESSION =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxPartitionsPerWritersSession")
      .internal()
      .doc("Maximum number of partitions per a single table writer instance.")
      .intConf
      .checkValue(_ > 0, "must be a positive number")
      .createWithDefault(10000)

  val COLUMNAR_CH_SHUFFLE_PREFER_SPILL_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.ch.shuffle.preferSpill")
      .internal()
      .doc(
        "Whether to spill the partition buffers when buffers are full. " +
          "If false, the partition buffers will be cached in memory first, " +
          "and the cached buffers will be spilled when reach maximum memory.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_CH_SHUFFLE_SPILL_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.backend.ch.spillThreshold")
      .internal()
      .doc("Shuffle spill threshold on ch backend")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0MB")

  val COLUMNAR_CH_THROW_IF_MEMORY_EXCEED =
    buildConf("spark.gluten.sql.columnar.backend.ch.throwIfMemoryExceed")
      .internal()
      .doc("Throw exception if memory exceeds threshold on ch backend.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_CH_FLUSH_BLOCK_BUFFER_BEFORE_EVICT =
    buildConf("spark.gluten.sql.columnar.backend.ch.flushBlockBufferBeforeEvict")
      .internal()
      .doc("Whether to flush partition_block_buffer before execute evict in CH PartitionWriter.")
      .booleanConf
      .createWithDefault(false)

  val TRANSFORM_PLAN_LOG_LEVEL =
    buildConf("spark.gluten.sql.transform.logLevel")
      .internal()
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(
        logLevel => Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(logLevel),
        "Valid values are 'trace', 'debug', 'info', 'warn' and 'error'.")
      .createWithDefault("DEBUG")

  val SUBSTRAIT_PLAN_LOG_LEVEL =
    buildConf("spark.gluten.sql.substrait.plan.logLevel")
      .internal()
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(
        logLevel => Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(logLevel),
        "Valid values are 'trace', 'debug', 'info', 'warn' and 'error'.")
      .createWithDefault("DEBUG")

  val VALIDATION_LOG_LEVEL =
    buildConf("spark.gluten.sql.validation.logLevel")
      .internal()
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(
        logLevel => Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(logLevel),
        "Valid values are 'trace', 'debug', 'info', 'warn' and 'error'.")
      .createWithDefault("INFO")

  val VALIDATION_PRINT_FAILURE_STACK_ =
    buildConf("spark.gluten.sql.validation.printStackOnFailure")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val SOFT_AFFINITY_LOG_LEVEL =
    buildConf("spark.gluten.soft-affinity.logLevel")
      .internal()
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(
        logLevel => Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(logLevel),
        "Valid values are 'trace', 'debug', 'info', 'warn' and 'error'.")
      .createWithDefault("DEBUG")

  val DEBUG_ENABLED =
    buildConf(GLUTEN_DEBUG_MODE)
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DEBUG_KEEP_JNI_WORKSPACE =
    buildConf(GLUTEN_DEBUG_KEEP_JNI_WORKSPACE)
      .internal()
      .booleanConf
      .createWithDefault(false)

  val BENCHMARK_TASK_STAGEID =
    buildConf("spark.gluten.sql.benchmark_task.stageId")
      .internal()
      .intConf
      .createWithDefault(-1)

  val BENCHMARK_TASK_PARTITIONID =
    buildConf("spark.gluten.sql.benchmark_task.partitionId")
      .internal()
      .intConf
      .createWithDefault(-1)

  val BENCHMARK_TASK_TASK_ID =
    buildConf("spark.gluten.sql.benchmark_task.taskId")
      .internal()
      .longConf
      .createWithDefault(-1L)

  val NATIVE_WRITER_ENABLED =
    buildConf("spark.gluten.sql.native.writer.enabled")
      .internal()
      .doc("This is config to specify whether to enable the native columnar parquet/orc writer")
      .booleanConf
      .createOptional

  val NATIVE_WRITE_FILES_COLUMN_METADATA_EXCLUSION_LIST =
    buildConf("spark.gluten.sql.native.writeColumnMetadataExclusionList")
      .doc(
        "Native write files does not support column metadata. Metadata in list would be " +
          "removed to support native write files. Multiple values separated by commas.")
      .stringConf
      .createWithDefault("comment")

  val REMOVE_NATIVE_WRITE_FILES_SORT_AND_PROJECT =
    buildConf("spark.gluten.sql.removeNativeWriteFilesSortAndProject")
      .internal()
      .doc("When true, Gluten will remove the vanilla Spark V1Writes added sort and project " +
        "for velox backend.")
      .booleanConf
      .createWithDefault(true)

  val UT_STATISTIC =
    buildConf("spark.gluten.sql.ut.statistic")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val EXTENDED_COLUMNAR_TRANSFORM_RULES =
    buildConf("spark.gluten.sql.columnar.extended.columnar.transform.rules")
      .withAlternative("spark.gluten.sql.columnar.extended.columnar.pre.rules")
      .doc("A comma-separated list of classes for the extended columnar transform rules.")
      .stringConf
      .createWithDefaultString("")

  val EXTENDED_COLUMNAR_POST_RULES =
    buildConf("spark.gluten.sql.columnar.extended.columnar.post.rules")
      .doc("A comma-separated list of classes for the extended columnar post rules.")
      .stringConf
      .createWithDefaultString("")

  val EXTENDED_EXPRESSION_TRAN_CONF =
    buildConf(GLUTEN_EXTENDED_EXPRESSION_TRAN_CONF)
      .doc("A class for the extended expressions transformer.")
      .stringConf
      .createWithDefaultString("")

  val EXPRESSION_BLACK_LIST =
    buildConf("spark.gluten.expression.blacklist")
      .doc("A black list of expression to skip transform, multiple values separated by commas.")
      .stringConf
      .createOptional

  val FALLBACK_REGEXP_EXPRESSIONS =
    buildConf("spark.gluten.sql.fallbackRegexpExpressions")
      .doc(
        "If true, fall back all regexp expressions. There are a few incompatible cases" +
          " between RE2 (used by native engine) and java.util.regex (used by Spark). User should" +
          " enable this property if their incompatibility is intolerable.")
      .booleanConf
      .createWithDefault(false)

  val FALLBACK_REPORTER_ENABLED =
    buildConf("spark.gluten.sql.columnar.fallbackReporter")
      .doc("When true, enable fallback reporter rule to print fallback reason")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_VELOX_ENABLE_USER_EXCEPTION_STACKTRACE =
    buildConf("spark.gluten.sql.columnar.backend.velox.enableUserExceptionStacktrace")
      .internal()
      .doc("Enable the stacktrace for user type of VeloxException")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_VELOX_MEMORY_USE_HUGE_PAGES =
    buildConf("spark.gluten.sql.columnar.backend.velox.memoryUseHugePages")
      .internal()
      .doc("Use explicit huge pages for Velox memory allocation.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_ENABLE_SYSTEM_EXCEPTION_STACKTRACE =
    buildConf("spark.gluten.sql.columnar.backend.velox.enableSystemExceptionStacktrace")
      .internal()
      .doc("Enable the stacktrace for system type of VeloxException")
      .booleanConf
      .createWithDefault(true)

  val TEXT_INPUT_ROW_MAX_BLOCK_SIZE =
    buildConf("spark.gluten.sql.text.input.max.block.size")
      .internal()
      .doc("the max block size for text input rows")
      .longConf
      .createWithDefault(8192);

  val TEXT_INPUT_EMPTY_AS_DEFAULT =
    buildConf("spark.gluten.sql.text.input.empty.as.default")
      .internal()
      .doc("treat empty fields in CSV input as default values.")
      .booleanConf
      .createWithDefault(false);

  val ENABLE_PARQUET_ROW_GROUP_MAX_MIN_INDEX =
    buildConf("spark.gluten.sql.parquet.maxmin.index")
      .internal()
      .doc("Enable row group max min index for parquet file scan")
      .booleanConf
      .createWithDefault(false)

  val VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation")
      .internal()
      .doc(
        "Enable flushable aggregation. If true, Gluten will try converting regular aggregation " +
          "into Velox's flushable aggregation when applicable. A flushable aggregation could " +
          "emit intermediate result at anytime when memory is full / data reduction ratio is low."
      )
      .booleanConf
      .createWithDefault(true)

  val MAX_PARTIAL_AGGREGATION_MEMORY_RATIO =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxPartialAggregationMemoryRatio")
      .internal()
      .doc(
        "Set the max memory of partial aggregation as "
          + "maxPartialAggregationMemoryRatio of offheap size. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false."
      )
      .doubleConf
      .createOptional

  val MAX_EXTENDED_PARTIAL_AGGREGATION_MEMORY_RATIO =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxExtendedPartialAggregationMemoryRatio")
      .internal()
      .doc(
        "Set the max extended memory of partial aggregation as "
          + "maxExtendedPartialAggregationMemoryRatio of offheap size. Note: this option only " +
          "works when flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false."
      )
      .doubleConf
      .createOptional

  val ABANDON_PARTIAL_AGGREGATION_MIN_PCT =
    buildConf("spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinPct")
      .internal()
      .doc(
        "If partial aggregation input rows number greater than this value, "
          + " partial aggregation may be early abandoned. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false.")
      .intConf
      .createOptional

  val ABANDON_PARTIAL_AGGREGATION_MIN_ROWS =
    buildConf("spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinRows")
      .internal()
      .doc(
        "If partial aggregation aggregationPct greater than this value, "
          + "partial aggregation may be early abandoned. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false.")
      .intConf
      .createOptional

  val ENABLE_REWRITE_DATE_TIMESTAMP_COMPARISON =
    buildConf("spark.gluten.sql.rewrite.dateTimestampComparison")
      .internal()
      .doc("Rewrite the comparision between date and timestamp to timestamp comparison."
        + "For example `from_unixtime(ts) > date` will be rewritten to `ts > to_unixtime(date)`")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_COLUMNAR_PROJECT_COLLAPSE =
    buildConf("spark.gluten.sql.columnar.project.collapse")
      .internal()
      .doc("Combines two columnar project operators into one and perform alias substitution")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_COMMON_SUBEXPRESSION_ELIMINATE =
    buildConf("spark.gluten.sql.commonSubexpressionEliminate")
      .internal()
      .doc(
        "Eliminate common subexpressions in logical plan to avoid multiple evaluation of the same"
          + "expression, may improve performance")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_COUNT_DISTINCT_WITHOUT_EXPAND =
    buildConf("spark.gluten.sql.countDistinctWithoutExpand")
      .internal()
      .doc(
        "Convert Count Distinct to a UDAF called count_distinct to " +
          "prevent SparkPlanner converting it to Expand+Count. WARNING: " +
          "When enabled, count distinct queries will fail to fallback!!!")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_BLOOM_FILTER_EXPECTED_NUM_ITEMS =
    buildConf("spark.gluten.sql.columnar.backend.velox.bloomFilter.expectedNumItems")
      .internal()
      .doc("The default number of expected items for the velox bloomfilter: " +
        "'spark.bloom_filter.expected_num_items'")
      .longConf
      .createWithDefault(1000000L)

  val COLUMNAR_VELOX_BLOOM_FILTER_NUM_BITS =
    buildConf("spark.gluten.sql.columnar.backend.velox.bloomFilter.numBits")
      .internal()
      .doc("The default number of bits to use for the velox bloom filter: " +
        "'spark.bloom_filter.num_bits'")
      .longConf
      .createWithDefault(8388608L)

  val COLUMNAR_VELOX_BLOOM_FILTER_MAX_NUM_BITS =
    buildConf("spark.gluten.sql.columnar.backend.velox.bloomFilter.maxNumBits")
      .internal()
      .doc("The max number of bits to use for the velox bloom filter: " +
        "'spark.bloom_filter.max_num_bits'")
      .longConf
      .createWithDefault(4194304L)

  val COLUMNAR_VELOX_FILE_HANDLE_CACHE_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.fileHandleCacheEnabled")
      .internal()
      .doc("Disables caching if false. File handle cache should be disabled " +
        "if files are mutable, i.e. file content may change while file path stays the same.")
      .booleanConf
      .createWithDefault(false)

  val CARTESIAN_PRODUCT_TRANSFORMER_ENABLED =
    buildConf("spark.gluten.sql.cartesianProductTransformerEnabled")
      .internal()
      .doc("Config to enable CartesianProductExecTransformer.")
      .booleanConf
      .createWithDefault(true)

  val BROADCAST_NESTED_LOOP_JOIN_TRANSFORMER_ENABLED =
    buildConf("spark.gluten.sql.broadcastNestedLoopJoinTransformerEnabled")
      .internal()
      .doc("Config to enable BroadcastNestedLoopJoinExecTransformer.")
      .booleanConf
      .createWithDefault(true)

  val CACHE_WHOLE_STAGE_TRANSFORMER_CONTEXT =
    buildConf("spark.gluten.sql.cacheWholeStageTransformerContext")
      .internal()
      .doc("When true, `WholeStageTransformer` will cache the `WholeStageTransformerContext` " +
        "when executing. It is used to get substrait plan node and native plan string.")
      .booleanConf
      .createWithDefault(false)

  val INJECT_NATIVE_PLAN_STRING_TO_EXPLAIN =
    buildConf("spark.gluten.sql.injectNativePlanStringToExplain")
      .internal()
      .doc("When true, Gluten will inject native plan tree to explain string inside " +
        "`WholeStageTransformerContext`.")
      .booleanConf
      .createWithDefault(false)

  val DIRECTORY_SIZE_GUESS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.directorySizeGuess")
      .internal()
      .doc("Set the directory size guess for velox file scan")
      .intConf
      .createOptional

  val FILE_PRELOAD_THRESHOLD =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.filePreloadThreshold")
      .internal()
      .doc("Set the file preload threshold for velox file scan")
      .intConf
      .createOptional

  val PREFETCH_ROW_GROUPS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.prefetchRowGroups")
      .internal()
      .doc("Set the prefetch row groups for velox file scan")
      .intConf
      .createOptional

  val LOAD_QUANTUM =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.loadQuantum")
      .internal()
      .doc("Set the load quantum for velox file scan")
      .intConf
      .createOptional

  val MAX_COALESCED_DISTANCE_BYTES =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.maxCoalescedDistanceBytes")
      .internal()
      .doc(" Set the max coalesced distance bytes for velox file scan")
      .intConf
      .createOptional

  val MAX_COALESCED_BYTES =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.maxCoalescedBytes")
      .internal()
      .doc("Set the max coalesced bytes for velox file scan")
      .intConf
      .createOptional

  val CACHE_PREFETCH_MINPCT =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cachePrefetchMinPct")
      .internal()
      .doc("Set prefetch cache min pct for velox file scan")
      .intConf
      .createOptional

  val AWS_SDK_LOG_LEVEL =
    buildConf("spark.gluten.velox.awsSdkLogLevel")
      .internal()
      .doc("Log granularity of AWS C++ SDK in velox.")
      .stringConf
      .createWithDefault("FATAL")

  val VELOX_ORC_SCAN_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.orc.scan.enabled")
      .internal()
      .doc("Enable velox orc scan. If disabled, vanilla spark orc scan will be used.")
      .booleanConf
      .createWithDefault(true)

  val VELOX_FORCE_COMPLEX_TYPE_SCAN_FALLBACK =
    buildConf("spark.gluten.sql.complexType.scan.fallback.enabled")
      .internal()
      .doc("Force fallback for complex type scan, including struct, map, array.")
      .booleanConf
      .createWithDefault(true)

  val VELOX_FORCE_ORC_CHAR_TYPE_SCAN_FALLBACK =
    buildConf("spark.gluten.sql.orc.charType.scan.fallback.enabled")
      .internal()
      .doc("Force fallback for orc char type scan.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_NATIVE_CAST_AGGREGATE_ENABLED =
    buildConf("spark.gluten.sql.columnar.cast.avg")
      .internal()
      .booleanConf
      .createWithDefault(true)
}
