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

import org.apache.gluten.execution.ColumnarNativeIterator;
import org.apache.gluten.substrait.plan.PlanNode;

import io.substrait.proto.Plan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class SimpleExpressionEval implements AutoCloseable, Iterator<Long> {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleExpressionEval.class);

  private final Long instance;

  public SimpleExpressionEval(ColumnarNativeIterator blockStream, PlanNode planNode) {
    Plan plan = planNode.toProtobuf();
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("SimpleExpressionEval exec plan: %s", plan.toString()));
    }
    byte[] planData = plan.toByteArray();
    instance = createNativeInstance(blockStream, planData);
  }

  private static native long createNativeInstance(ColumnarNativeIterator blockStream, byte[] plan);

  private static native void nativeClose(long instance);

  private static native boolean nativeHasNext(long instance);

  private static native long nativeNext(long instance);

  @Override
  public boolean hasNext() {
    return nativeHasNext(instance);
  }

  @Override
  public Long next() {
    return nativeNext(instance);
  }

  @Override
  public void close() throws Exception {
    nativeClose(instance);
  }
}
