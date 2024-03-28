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

/**
 * This class is implemented in JNI. This provides the Java interface to invoke functions in JNI.
 * This file is used to generate the .h files required for jni. Avoid all external dependencies in
 * this file.
 */
public class ExpressionEvaluatorJniWrapper {

  /** Call initNative to initialize native computing. */
  native void nativeInitNative(byte[] confAsPlan);

  /** Call finalizeNative to finalize native computing. */
  native void nativeFinalizeNative();

  /**
   * Validate the Substrait plan in native compute engine.
   *
   * @param subPlan the Substrait plan in binary format.
   * @return whether the computing of this plan is supported in native.
   */
  native boolean nativeDoValidate(byte[] subPlan);

  /**
   * Create a native compute kernel and return a columnar result iterator.
   *
   * @param allocatorId allocator id
   * @return iterator instance id
   */
  public native long nativeCreateKernelWithIterator(
      long allocatorId,
      byte[] wsPlan,
      byte[][] splitInfo,
      GeneralInIterator[] batchItr,
      byte[] confArray,
      boolean materializeInput);

  /**
   * Closes the projector referenced by nativeHandler.
   *
   * @param nativeHandler nativeHandler that needs to be closed
   */
  native void nativeClose(long nativeHandler);
}
