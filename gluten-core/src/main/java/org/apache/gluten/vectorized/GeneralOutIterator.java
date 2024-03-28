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

import org.apache.gluten.exception.GlutenException;
import org.apache.gluten.metrics.IMetrics;

import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class GeneralOutIterator
    implements AutoCloseable, Serializable, Iterator<ColumnarBatch> {
  protected final AtomicBoolean closed = new AtomicBoolean(false);

  public GeneralOutIterator() {}

  @Override
  public final boolean hasNext() {
    try {
      return hasNextInternal();
    } catch (Exception e) {
      throw new GlutenException(e);
    }
  }

  @Override
  public final ColumnarBatch next() {
    try {
      return nextInternal();
    } catch (Exception e) {
      throw new GlutenException(e);
    }
  }

  public final IMetrics getMetrics() throws Exception {
    return getMetricsInternal();
  }

  @Override
  public final void close() {
    if (closed.compareAndSet(false, true)) {
      closeInternal();
    }
  }

  public abstract String getId();

  protected abstract void closeInternal();

  protected abstract boolean hasNextInternal() throws Exception;

  protected abstract ColumnarBatch nextInternal() throws Exception;

  protected abstract IMetrics getMetricsInternal() throws Exception;
}
