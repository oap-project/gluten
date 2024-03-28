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

import org.apache.gluten.GlutenConfig;
import org.apache.gluten.backendsapi.BackendsApiManager;
import org.apache.gluten.memory.alloc.CHNativeMemoryAllocators;
import org.apache.gluten.substrait.expression.ExpressionBuilder;
import org.apache.gluten.substrait.expression.StringMapNode;
import org.apache.gluten.substrait.extensions.AdvancedExtensionNode;
import org.apache.gluten.substrait.extensions.ExtensionBuilder;
import org.apache.gluten.substrait.plan.PlanBuilder;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.internal.SQLConf;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.JavaConverters;

public class CHNativeExpressionEvaluator {
  private final ExpressionEvaluatorJniWrapper jniWrapper;

  public CHNativeExpressionEvaluator() {
    jniWrapper = new ExpressionEvaluatorJniWrapper();
  }

  // Used to initialize the native computing.
  public void initNative(SparkConf conf) {
    Tuple2<String, String>[] all = conf.getAll();
    Map<String, String> confMap =
        Arrays.stream(all).collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
    String prefix = BackendsApiManager.getSettings().getBackendConfigPrefix();
    Map<String, String> nativeConfMap =
        GlutenConfig.getNativeBackendConf(prefix, JavaConverters.mapAsScalaMap(confMap));

    // Get the customer config from SparkConf for each backend
    BackendsApiManager.getTransformerApiInstance().postProcessNativeConfig(nativeConfMap, prefix);

    jniWrapper.nativeInitNative(buildNativeConf(nativeConfMap));
  }

  public void finalizeNative() {
    jniWrapper.nativeFinalizeNative();
  }

  // Used to validate the Substrait plan in native compute engine.
  public boolean doValidate(byte[] subPlan) {
    return jniWrapper.nativeDoValidate(subPlan);
  }

  private byte[] buildNativeConf(Map<String, String> confs) {
    StringMapNode stringMapNode = ExpressionBuilder.makeStringMap(confs);
    AdvancedExtensionNode extensionNode =
        ExtensionBuilder.makeAdvancedExtension(
            BackendsApiManager.getTransformerApiInstance()
                .packPBMessage(stringMapNode.toProtobuf()));
    return PlanBuilder.makePlan(extensionNode).toProtobuf().toByteArray();
  }

  private Map<String, String> getNativeBackendConf() {
    return GlutenConfig.getNativeBackendConf(
        BackendsApiManager.getSettings().getBackendConfigPrefix(), SQLConf.get().getAllConfs());
  }

  // Used by WholeStageTransform to create the native computing pipeline and
  // return a columnar result iterator.
  public GeneralOutIterator createKernelWithBatchIterator(
      byte[] wsPlan,
      byte[][] splitInfo,
      List<GeneralInIterator> iterList,
      boolean materializeInput) {
    long allocId = CHNativeMemoryAllocators.contextInstance().getNativeInstanceId();
    long handle =
        jniWrapper.nativeCreateKernelWithIterator(
            allocId,
            wsPlan,
            splitInfo,
            iterList.toArray(new GeneralInIterator[0]),
            buildNativeConf(getNativeBackendConf()),
            materializeInput);
    return createOutIterator(handle);
  }

  // Only for UT.
  public GeneralOutIterator createKernelWithBatchIterator(
      long allocId, byte[] wsPlan, byte[][] splitInfo, List<GeneralInIterator> iterList) {
    long handle =
        jniWrapper.nativeCreateKernelWithIterator(
            allocId,
            wsPlan,
            splitInfo,
            iterList.toArray(new GeneralInIterator[0]),
            buildNativeConf(getNativeBackendConf()),
            false);
    return createOutIterator(handle);
  }

  private GeneralOutIterator createOutIterator(long nativeHandle) {
    return new BatchIterator(nativeHandle);
  }
}
