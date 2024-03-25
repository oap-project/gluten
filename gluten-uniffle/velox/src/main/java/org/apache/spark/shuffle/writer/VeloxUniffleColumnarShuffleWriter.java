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
package org.apache.spark.shuffle.writer;

import io.glutenproject.GlutenConfig;
import io.glutenproject.columnarbatch.ColumnarBatches;
import io.glutenproject.memory.memtarget.MemoryTarget;
import io.glutenproject.memory.memtarget.Spiller;
import io.glutenproject.memory.memtarget.Spillers;
import io.glutenproject.memory.nmm.NativeMemoryManagers;
import io.glutenproject.vectorized.ShuffleWriterJniWrapper;
import io.glutenproject.vectorized.SplitResult;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.SparkMemoryUtil;
import org.apache.spark.shuffle.ColumnarShuffleDependency;
import org.apache.spark.shuffle.GlutenShuffleUtils;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.SparkResourceUtil;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import scala.Product2;
import scala.collection.Iterator;

public class VeloxUniffleColumnarShuffleWriter<K, V> extends RssShuffleWriter<K, V, V> {

  private static final Logger LOG =
      LoggerFactory.getLogger(VeloxUniffleColumnarShuffleWriter.class);

  private long nativeShuffleWriter = 0L;
  private int compressThreshold = GlutenConfig.getConf().columnarShuffleCompressionThreshold();
  private double reallocThreshold = GlutenConfig.getConf().columnarShuffleReallocThreshold();
  private String compressionCodec;
  private int compressionLevel;
  private int partitionId;

  private ShuffleWriterJniWrapper jniWrapper = ShuffleWriterJniWrapper.create();
  private SplitResult splitResult;
  private int nativeBufferSize = GlutenConfig.getConf().maxBatchSize();
  private int bufferSize;
  private PartitionPusher partitionPusher;

  private final ColumnarShuffleDependency<K, V, V> columnarDep;
  private final SparkConf sparkConf;

  private long availableOffHeapPerTask() {
    return SparkMemoryUtil.getCurrentAvailableOffHeapMemory()
        / SparkResourceUtil.getTaskSlots(sparkConf);
  }

  public VeloxUniffleColumnarShuffleWriter(
      int partitionId,
      String appId,
      int shuffleId,
      String taskId,
      long taskAttemptId,
      ShuffleWriteMetrics shuffleWriteMetrics,
      RssShuffleManager shuffleManager,
      SparkConf sparkConf,
      ShuffleWriteClient shuffleWriteClient,
      RssShuffleHandle<K, V, V> rssHandle,
      Function<String, Boolean> taskFailureCallback,
      TaskContext context) {
    super(
        appId,
        shuffleId,
        taskId,
        taskAttemptId,
        shuffleWriteMetrics,
        shuffleManager,
        sparkConf,
        shuffleWriteClient,
        rssHandle,
        taskFailureCallback,
        context);
    columnarDep = (ColumnarShuffleDependency<K, V, V>) rssHandle.getDependency();
    this.partitionId = partitionId;
    this.sparkConf = sparkConf;
    bufferSize =
        (int)
            sparkConf.getSizeAsBytes(
                RssSparkConfig.RSS_WRITER_BUFFER_SIZE.key(),
                RssSparkConfig.RSS_WRITER_BUFFER_SIZE.defaultValue().get());
    compressionCodec = GlutenShuffleUtils.getCompressionCodec(sparkConf);
    compressionLevel = GlutenShuffleUtils.getCompressionLevel(sparkConf, compressionCodec, null);
  }

  @Override
  protected void writeImpl(Iterator<Product2<K, V>> records) throws IOException {
    if (!records.hasNext() && !isMemoryShuffleEnabled) {
      super.sendCommit();
      return;
    }
    // writer already init
    partitionPusher = new PartitionPusher(this);
    while (records.hasNext()) {
      ColumnarBatch cb = (ColumnarBatch) (records.next()._2());
      if (cb.numRows() == 0 || cb.numCols() == 0) {
        LOG.info("Skip ColumnarBatch of 0 rows or 0 cols");
      } else {
        long handle = ColumnarBatches.getNativeHandle(cb);
        if (nativeShuffleWriter == 0) {
          nativeShuffleWriter =
              jniWrapper.makeForRSS(
                  columnarDep.nativePartitioning(),
                  nativeBufferSize,
                  // use field do this
                  compressionCodec,
                  compressionLevel,
                  compressThreshold,
                  GlutenConfig.getConf().columnarShuffleCompressionMode(),
                  bufferSize,
                  partitionPusher,
                  NativeMemoryManagers.create(
                          "UniffleShuffleWriter",
                          new Spiller() {
                            @Override
                            public long spill(MemoryTarget self, long size) {
                              if (nativeShuffleWriter == -1) {
                                throw new IllegalStateException(
                                    "Fatal: spill() called before a shuffle shuffle writer "
                                        + "evaluator is created. This behavior should be"
                                        + "optimized by moving memory "
                                        + "allocations from make() to split()");
                              }
                              LOG.info(
                                  "Gluten shuffle writer: Trying to push {} bytes of data", size);
                              long pushed =
                                  jniWrapper.nativeEvict(nativeShuffleWriter, size, false);
                              LOG.info(
                                  "Gluten shuffle writer: Pushed {} / {} bytes of data",
                                  pushed,
                                  size);
                              return pushed;
                            }

                            @Override
                            public Set<Phase> applicablePhases() {
                              return Spillers.PHASE_SET_SPILL_ONLY;
                            }
                          })
                      .getNativeInstanceHandle(),
                  handle,
                  taskAttemptId,
                  GlutenShuffleUtils.getStartPartitionId(
                      columnarDep.nativePartitioning(), partitionId),
                  "uniffle",
                  reallocThreshold);
        }
        long startTime = System.nanoTime();
        long bytes =
            jniWrapper.split(nativeShuffleWriter, cb.numRows(), handle, availableOffHeapPerTask());
        LOG.debug("jniWrapper.split rows {}, split bytes {}", cb.numRows(), bytes);
        columnarDep.metrics().get("dataSize").get().add(bytes);
        // this metric replace part of uniffle shuffle write time
        columnarDep.metrics().get("splitTime").get().add(System.nanoTime() - startTime);
        columnarDep.metrics().get("numInputRows").get().add(cb.numRows());
        columnarDep.metrics().get("inputBatches").get().add(1);
        shuffleWriteMetrics.incRecordsWritten(cb.numRows());
      }
    }

    long startTime = System.nanoTime();
    if (nativeShuffleWriter != -1L) {
      splitResult = jniWrapper.stop(nativeShuffleWriter);
    }
    LOG.info("nativeShuffleWriter value {}", nativeShuffleWriter);
    splitResult = jniWrapper.stop(nativeShuffleWriter);
    columnarDep
        .metrics()
        .get("splitTime")
        .get()
        .add(
            System.nanoTime()
                - startTime
                - splitResult.getTotalPushTime()
                - splitResult.getTotalWriteTime()
                - splitResult.getTotalCompressTime());

    shuffleWriteMetrics.incBytesWritten(splitResult.getTotalBytesWritten());
    shuffleWriteMetrics.incWriteTime(
        splitResult.getTotalWriteTime() + splitResult.getTotalPushTime());
    // partitionLengths is calculate in uniffle side

    long pushMergedDataTime = System.nanoTime();
    // clear all
    sendRestBlockAndWait();
    if (!isMemoryShuffleEnabled) {
      super.sendCommit();
    }
    long writeDurationMs = System.nanoTime() - pushMergedDataTime;
    shuffleWriteMetrics.incWriteTime(writeDurationMs);
    LOG.info(
        "Finish write shuffle  with rest write {} ms",
        TimeUnit.MILLISECONDS.toNanos(writeDurationMs));
  }

  private void sendRestBlockAndWait() {
    List<ShuffleBlockInfo> shuffleBlockInfos = super.getBufferManager().clear();
    super.processShuffleBlockInfos(shuffleBlockInfos);
    // make checkBlockSendResult no arguments
    super.internalCheckBlockSendResult();
  }

  public int doAddByte(int partitionId, byte[] data, int length) {
    List<ShuffleBlockInfo> shuffleBlockInfos =
        super.getBufferManager()
            .addPartitionData(partitionId, data, length, System.currentTimeMillis());
    super.processShuffleBlockInfos(shuffleBlockInfos);
    return length;
  }
}
