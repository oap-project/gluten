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

import org.apache.gluten.exec.Runtime;
import org.apache.gluten.exec.RuntimeAware;
import org.apache.gluten.exec.Runtimes;
import org.apache.gluten.validate.NativePlanValidationInfo;

/**
 * This class is implemented in JNI. This provides the Java interface to invoke functions in JNI.
 * This file is used to generate the .h files required for jni. Avoid all external dependencies in
 * this file.
 */
public class PlanEvaluatorJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  private PlanEvaluatorJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static PlanEvaluatorJniWrapper create() {
    return new PlanEvaluatorJniWrapper(Runtimes.contextInstance());
  }

  @Override
  public long handle() {
    return runtime.getHandle();
  }

  /**
   * Validate the Substrait plan in native compute engine.
   *
   * @param subPlan the Substrait plan in binary format.
   * @return whether the computing of this plan is supported in native and related info.
   */
  native NativePlanValidationInfo nativeValidateWithFailureReason(byte[] subPlan);

  public native String nativePlanString(byte[] substraitPlan, Boolean details);

  public native void injectWriteFilesTempPath(byte[] path);

  /**
   * Create a native compute kernel and return a columnar result iterator.
   *
   * @param memoryManagerHandle NativeMemoryManager instance handle
   * @return iterator instance id
   */
  public native long nativeCreateKernelWithIterator(
      long memoryManagerHandle,
      byte[] wsPlan,
      byte[][] splitInfo,
      GeneralInIterator[] batchItr,
      int stageId,
      int partitionId,
      long taskId,
      boolean saveInputToFile,
      String spillDir)
      throws RuntimeException;
}
