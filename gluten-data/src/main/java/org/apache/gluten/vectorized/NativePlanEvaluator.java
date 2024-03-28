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
package org.apache.gluten.vectorized;

import org.apache.gluten.backendsapi.BackendsApiManager;
import org.apache.gluten.exec.Runtime;
import org.apache.gluten.exec.Runtimes;
import org.apache.gluten.memory.memtarget.MemoryTarget;
import org.apache.gluten.memory.memtarget.Spiller;
import org.apache.gluten.memory.memtarget.Spillers;
import org.apache.gluten.memory.nmm.NativeMemoryManager;
import org.apache.gluten.memory.nmm.NativeMemoryManagers;
import org.apache.gluten.utils.DebugUtil;
import org.apache.gluten.validate.NativePlanValidationInfo;

import org.apache.spark.TaskContext;
import org.apache.spark.util.SparkDirectoryUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class NativePlanEvaluator {

  private final PlanEvaluatorJniWrapper jniWrapper;

  private NativePlanEvaluator() {
    jniWrapper = PlanEvaluatorJniWrapper.create();
  }

  public static NativePlanEvaluator create() {
    return new NativePlanEvaluator();
  }

  public NativePlanValidationInfo doNativeValidateWithFailureReason(byte[] subPlan) {
    return jniWrapper.nativeValidateWithFailureReason(subPlan);
  }

  public void injectWriteFilesTempPath(String path) {
    jniWrapper.injectWriteFilesTempPath(path.getBytes(StandardCharsets.UTF_8));
  }

  // Used by WholeStageTransform to create the native computing pipeline and
  // return a columnar result iterator.
  public GeneralOutIterator createKernelWithBatchIterator(
      byte[] wsPlan, byte[][] splitInfo, List<GeneralInIterator> iterList, int partitionIndex)
      throws RuntimeException, IOException {
    final AtomicReference<ColumnarBatchOutIterator> outIterator = new AtomicReference<>();
    final NativeMemoryManager nmm =
        NativeMemoryManagers.create(
            "WholeStageIterator",
            new Spiller() {
              @Override
              public long spill(MemoryTarget self, long size) {
                ColumnarBatchOutIterator instance =
                    Optional.of(outIterator.get())
                        .orElseThrow(
                            () ->
                                new IllegalStateException(
                                    "Fatal: spill() called before a output iterator "
                                        + "is created. This behavior should be optimized "
                                        + "by moving memory allocations from create() to "
                                        + "hasNext()/next()"));
                return instance.spill(size);
              }

              @Override
              public Set<Phase> applicablePhases() {
                return Spillers.PHASE_SET_SPILL_ONLY;
              }
            });
    final long memoryManagerHandle = nmm.getNativeInstanceHandle();

    final String spillDirPath =
        SparkDirectoryUtil.namespace("gluten-spill")
            .mkChildDirRoundRobin(UUID.randomUUID().toString())
            .getAbsolutePath();

    long iterHandle =
        jniWrapper.nativeCreateKernelWithIterator(
            memoryManagerHandle,
            wsPlan,
            splitInfo,
            iterList.toArray(new GeneralInIterator[0]),
            TaskContext.get().stageId(),
            partitionIndex, // TaskContext.getPartitionId(),
            TaskContext.get().taskAttemptId(),
            DebugUtil.saveInputToFile(),
            BackendsApiManager.getSparkPlanExecApiInstance().rewriteSpillPath(spillDirPath));
    outIterator.set(createOutIterator(Runtimes.contextInstance(), iterHandle, nmm));
    return outIterator.get();
  }

  private ColumnarBatchOutIterator createOutIterator(
      Runtime runtime, long iterHandle, NativeMemoryManager nmm) throws IOException {
    return new ColumnarBatchOutIterator(runtime, iterHandle, nmm);
  }
}
