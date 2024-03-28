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
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.memory.memtarget.MemoryTarget
import org.apache.gluten.memory.memtarget.Spiller
import org.apache.gluten.memory.memtarget.Spillers
import org.apache.gluten.memory.nmm.NativeMemoryManagers
import org.apache.gluten.vectorized._

import org.apache.spark._
import org.apache.spark.memory.SparkMemoryUtil
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SparkResourceUtil

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf

import java.io.IOException
import java.util

class VeloxCelebornHashBasedColumnarShuffleWriter[K, V](
    shuffleId: Int,
    handle: CelebornShuffleHandle[K, V, V],
    context: TaskContext,
    celebornConf: CelebornConf,
    client: ShuffleClient,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends CelebornHashBasedColumnarShuffleWriter[K, V](
    shuffleId,
    handle,
    context,
    celebornConf,
    client,
    writeMetrics) {

  private val jniWrapper = ShuffleWriterJniWrapper.create()

  private var splitResult: SplitResult = _

  private lazy val nativeBufferSize = {
    val bufferSize = GlutenConfig.getConf.shuffleWriterBufferSize
    val maxBatchSize = GlutenConfig.getConf.maxBatchSize
    if (bufferSize > maxBatchSize) {
      logInfo(
        s"${GlutenConfig.SHUFFLE_WRITER_BUFFER_SIZE.key} ($bufferSize) exceeds max " +
          s" batch size. Limited to ${GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key} ($maxBatchSize).")
      maxBatchSize
    } else {
      bufferSize
    }
  }

  private def availableOffHeapPerTask(): Long = {
    val perTask =
      SparkMemoryUtil.getCurrentAvailableOffHeapMemory / SparkResourceUtil.getTaskSlots(conf)
    perTask
  }

  @throws[IOException]
  override def internalWrite(records: Iterator[Product2[K, V]]): Unit = {
    if (!records.hasNext) {
      handleEmptyIterator()
      return
    }

    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        val handle = ColumnarBatches.getNativeHandle(cb)
        if (nativeShuffleWriter == -1L) {
          nativeShuffleWriter = jniWrapper.makeForRSS(
            dep.nativePartitioning,
            nativeBufferSize,
            customizedCompressionCodec,
            compressionLevel,
            bufferCompressThreshold,
            GlutenConfig.getConf.columnarShuffleCompressionMode,
            clientPushBufferMaxSize,
            celebornPartitionPusher,
            NativeMemoryManagers
              .create(
                "CelebornShuffleWriter",
                new Spiller() {
                  override def spill(self: MemoryTarget, size: Long): Long = {
                    if (nativeShuffleWriter == -1L) {
                      throw new IllegalStateException(
                        "Fatal: spill() called before a celeborn shuffle writer " +
                          "is created. This behavior should be" +
                          "optimized by moving memory " +
                          "allocations from make() to split()")
                    }
                    logInfo(s"Gluten shuffle writer: Trying to push $size bytes of data")
                    // fixme pass true when being called by self
                    val pushed =
                      jniWrapper.nativeEvict(nativeShuffleWriter, size, false)
                    logInfo(s"Gluten shuffle writer: Pushed $pushed / $size bytes of data")
                    pushed
                  }

                  override def applicablePhases(): util.Set[Spiller.Phase] =
                    Spillers.PHASE_SET_SPILL_ONLY
                }
              )
              .getNativeInstanceHandle,
            handle,
            context.taskAttemptId(),
            GlutenShuffleUtils.getStartPartitionId(dep.nativePartitioning, context.partitionId),
            "celeborn",
            GlutenConfig.getConf.columnarShuffleReallocThreshold
          )
        }
        val startTime = System.nanoTime()
        jniWrapper.split(nativeShuffleWriter, cb.numRows, handle, availableOffHeapPerTask())
        dep.metrics("splitTime").add(System.nanoTime() - startTime)
        dep.metrics("numInputRows").add(cb.numRows)
        dep.metrics("inputBatches").add(1)
        // This metric is important, AQE use it to decide if EliminateLimit
        writeMetrics.incRecordsWritten(cb.numRows())
      }
    }

    val startTime = System.nanoTime()
    assert(nativeShuffleWriter != -1L)
    splitResult = jniWrapper.stop(nativeShuffleWriter)

    dep
      .metrics("splitTime")
      .add(
        System.nanoTime() - startTime - splitResult.getTotalPushTime -
          splitResult.getTotalWriteTime -
          splitResult.getTotalCompressTime)
    dep.metrics("dataSize").add(splitResult.getRawPartitionLengths.sum)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime + splitResult.getTotalPushTime)

    partitionLengths = splitResult.getPartitionLengths

    pushMergedDataToCeleborn()
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  override def closeShuffleWriter(): Unit = {
    jniWrapper.close(nativeShuffleWriter)
  }
}
