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
import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.memory.alloc.CHNativeMemoryAllocators
import org.apache.gluten.memory.memtarget.{MemoryTarget, Spiller, Spillers}
import org.apache.gluten.vectorized._

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SparkDirectoryUtil, Utils}

import java.io.IOException
import java.util
import java.util.{Locale, UUID}

class CHColumnarShuffleWriter[K, V](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, V],
    mapId: Long,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends ShuffleWriter[K, V]
  with Logging {

  private val dep = handle.dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]]

  private val conf = SparkEnv.get.conf

  private val blockManager = SparkEnv.get.blockManager
  private val localDirs = SparkDirectoryUtil
    .namespace("ch-shuffle-write")
    .mkChildDirs(UUID.randomUUID().toString)
    .map(_.getAbsolutePath)
    .mkString(",")
  private val subDirsPerLocalDir = blockManager.diskBlockManager.subDirsPerLocalDir
  private val splitSize = GlutenConfig.getConf.maxBatchSize
  private val customizedCompressCodec =
    GlutenShuffleUtils.getCompressionCodec(conf).toUpperCase(Locale.ROOT)
  private val preferSpill = GlutenConfig.getConf.chColumnarShufflePreferSpill
  private val throwIfMemoryExceed = GlutenConfig.getConf.chColumnarThrowIfMemoryExceed
  private val flushBlockBufferBeforeEvict =
    GlutenConfig.getConf.chColumnarFlushBlockBufferBeforeEvict
  private val spillThreshold = GlutenConfig.getConf.chColumnarShuffleSpillThreshold
  private val jniWrapper = new CHShuffleSplitterJniWrapper
  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false
  private var mapStatus: MapStatus = _
  private var nativeSplitter: Long = 0

  private var splitResult: CHSplitResult = _

  private var partitionLengths: Array[Long] = _

  private var rawPartitionLengths: Array[Long] = _

  private var firstRecordBatch: Boolean = true

  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    internalCHWrite(records)
  }

  private def internalCHWrite(records: Iterator[Product2[K, V]]): Unit = {
    val splitterJniWrapper: CHShuffleSplitterJniWrapper = jniWrapper
    if (!records.hasNext) {
      partitionLengths = new Array[Long](dep.partitioner.numPartitions)
      shuffleBlockResolver.writeMetadataFileAndCommit(
        dep.shuffleId,
        mapId,
        partitionLengths,
        Array[Long](),
        null)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
      return
    }
    val dataTmp = Utils.tempFileWith(shuffleBlockResolver.getDataFile(dep.shuffleId, mapId))
    if (nativeSplitter == 0) {
      nativeSplitter = splitterJniWrapper.make(
        dep.nativePartitioning,
        dep.shuffleId,
        mapId,
        splitSize,
        customizedCompressCodec,
        dataTmp.getAbsolutePath,
        localDirs,
        subDirsPerLocalDir,
        preferSpill,
        spillThreshold,
        CHBackendSettings.shuffleHashAlgorithm,
        throwIfMemoryExceed,
        flushBlockBufferBeforeEvict
      )
      CHNativeMemoryAllocators.createSpillable(
        "ShuffleWriter",
        new Spiller() {
          override def spill(self: MemoryTarget, size: Long): Long = {
            if (nativeSplitter == 0) {
              throw new IllegalStateException(
                "Fatal: spill() called before a shuffle writer " +
                  "is created. This behavior should be optimized by moving memory " +
                  "allocations from make() to split()")
            }
            logInfo(s"Gluten shuffle writer: Trying to spill $size bytes of data")
            val spilled = splitterJniWrapper.evict(nativeSplitter);
            logInfo(s"Gluten shuffle writer: Spilled $spilled / $size bytes of data")
            spilled
          }

          override def applicablePhases(): util.Set[Spiller.Phase] = Spillers.PHASE_SET_SPILL_ONLY
        }
      )
    }
    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        firstRecordBatch = false
        val col = cb.column(0).asInstanceOf[CHColumnVector]
        val block = col.getBlockAddress
        splitterJniWrapper
          .split(nativeSplitter, block)
        dep.metrics("numInputRows").add(cb.numRows)
        dep.metrics("inputBatches").add(1)
        writeMetrics.incRecordsWritten(cb.numRows)
      }
    }
    splitResult = splitterJniWrapper.stop(nativeSplitter)

    dep.metrics("splitTime").add(splitResult.getSplitTime)
    dep.metrics("IOTime").add(splitResult.getDiskWriteTime)
    dep.metrics("serializeTime").add(splitResult.getSerializationTime)
    dep.metrics("spillTime").add(splitResult.getTotalSpillTime)
    dep.metrics("compressTime").add(splitResult.getTotalCompressTime)
    dep.metrics("computePidTime").add(splitResult.getTotalComputePidTime)
    dep.metrics("bytesSpilled").add(splitResult.getTotalBytesSpilled)
    dep.metrics("dataSize").add(splitResult.getTotalBytesWritten)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime + splitResult.getTotalSpillTime)

    partitionLengths = splitResult.getPartitionLengths
    rawPartitionLengths = splitResult.getRawPartitionLengths
    try {
      shuffleBlockResolver.writeMetadataFileAndCommit(
        dep.shuffleId,
        mapId,
        partitionLengths,
        Array[Long](),
        dataTmp)
    } finally {
      if (dataTmp.exists() && !dataTmp.delete()) {
        logError(s"Error while deleting temp file ${dataTmp.getAbsolutePath}")
      }
    }

    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        None
      }
      stopping = true
      if (success) {
        Option(mapStatus)
      } else {
        None
      }
    } finally {
      if (nativeSplitter != 0) {
        closeCHSplitter()
        nativeSplitter = 0
      }
    }
  }

  private def closeCHSplitter(): Unit = {
    jniWrapper.close(nativeSplitter)
  }

  // VisibleForTesting
  def getPartitionLengths: Array[Long] = partitionLengths

}
