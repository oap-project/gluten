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
package org.apache.gluten.columnarbatch;

import org.apache.gluten.exception.GlutenException;
import org.apache.gluten.exec.Runtime;
import org.apache.gluten.exec.Runtimes;
import org.apache.gluten.memory.arrowalloc.ArrowBufferAllocators;
import org.apache.gluten.memory.nmm.NativeMemoryManager;
import org.apache.gluten.utils.ArrowAbiUtil;
import org.apache.gluten.utils.ArrowUtil;
import org.apache.gluten.utils.ImplicitClass;
import org.apache.gluten.vectorized.ArrowWritableColumnVector;

import com.google.common.base.Preconditions;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class ColumnarBatches {
  private static final Field FIELD_COLUMNS;

  static {
    try {
      Field f = ColumnarBatch.class.getDeclaredField("columns");
      f.setAccessible(true);
      FIELD_COLUMNS = f;
    } catch (NoSuchFieldException e) {
      throw new GlutenException(e);
    }
  }

  private ColumnarBatches() {}

  enum BatchType {
    LIGHT,
    HEAVY
  }

  private static BatchType identifyBatchType(ColumnarBatch batch) {
    if (batch.numCols() == 0) {
      // zero-column batch considered as heavy batch
      return BatchType.HEAVY;
    }

    final ColumnVector col0 = batch.column(0);
    if (col0 instanceof IndicatorVector) {
      // it's likely a light batch
      for (int i = 1; i < batch.numCols(); i++) {
        ColumnVector col = batch.column(i);
        if (!(col instanceof PlaceholderVector)) {
          throw new IllegalStateException(
              "Light batch should consist of one indicator vector "
                  + "and (numCols - 1) placeholder vectors");
        }
      }
      return BatchType.LIGHT;
    }

    // it's likely a heavy batch
    for (int i = 0; i < batch.numCols(); i++) {
      ColumnVector col = batch.column(i);
      if (!(col instanceof ArrowWritableColumnVector)) {
        throw new IllegalStateException("Heavy batch should consist of arrow vectors");
      }
    }
    return BatchType.HEAVY;
  }

  private static void transferVectors(ColumnarBatch from, ColumnarBatch target) {
    try {
      if (target.numCols() != from.numCols()) {
        throw new IllegalStateException();
      }
      final ColumnVector[] vectors = (ColumnVector[]) FIELD_COLUMNS.get(target);
      for (int i = 0; i < target.numCols(); i++) {
        vectors[i] = from.column(i);
      }
    } catch (IllegalAccessException e) {
      throw new GlutenException(e);
    }
  }

  /** Heavy batch: Data is readable from JVM and formatted as Arrow data. */
  public static boolean isHeavyBatch(ColumnarBatch batch) {
    return identifyBatchType(batch) == BatchType.HEAVY;
  }

  /**
   * Light batch: Data is not readable from JVM, a long int handle (which is a pointer usually) is
   * used to bind the batch to a native side implementation.
   */
  public static boolean isLightBatch(ColumnarBatch batch) {
    return identifyBatchType(batch) == BatchType.LIGHT;
  }

  /**
   * This method will always return a velox based ColumnarBatch. This method will close the input
   * column batch.
   */
  public static ColumnarBatch select(
      NativeMemoryManager nmm, ColumnarBatch batch, int[] columnIndices) {
    switch (identifyBatchType(batch)) {
      case LIGHT:
        final IndicatorVector iv = getIndicatorVector(batch);
        long outputBatchHandle =
            ColumnarBatchJniWrapper.create()
                .select(nmm.getNativeInstanceHandle(), iv.handle(), columnIndices);
        return create(iv.runtime(), outputBatchHandle);
      case HEAVY:
        return new ColumnarBatch(
            Arrays.stream(columnIndices).mapToObj(batch::column).toArray(ColumnVector[]::new),
            batch.numRows());
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Ensure the input batch is offloaded as native-based columnar batch (See {@link IndicatorVector}
   * and {@link PlaceholderVector}). This method will close the input column batch after offloaded.
   */
  public static ColumnarBatch ensureOffloaded(BufferAllocator allocator, ColumnarBatch batch) {
    if (ColumnarBatches.isLightBatch(batch)) {
      return batch;
    }
    return offload(allocator, batch);
  }

  /**
   * Ensure the input batch is loaded as Arrow-based Java columnar batch. ABI-based sharing will
   * take place if loading is required, which means when the input batch is not loaded yet. This
   * method will close the input column batch after loaded.
   */
  public static ColumnarBatch ensureLoaded(BufferAllocator allocator, ColumnarBatch batch) {
    if (isHeavyBatch(batch)) {
      return batch;
    }
    return load(allocator, batch);
  }

  private static ColumnarBatch load(BufferAllocator allocator, ColumnarBatch input) {
    if (!ColumnarBatches.isLightBatch(input)) {
      throw new IllegalArgumentException(
          "Input is not light columnar batch. "
              + "Please consider to use vanilla spark's row based input by setting one of the below"
              + " configs: \n"
              + "spark.sql.parquet.enableVectorizedReader=false\n"
              + "spark.sql.inMemoryColumnarStorage.enableVectorizedReader=false\n"
              + "spark.sql.orc.enableVectorizedReader=false\n");
    }
    IndicatorVector iv = (IndicatorVector) input.column(0);
    try (ArrowSchema cSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray cArray = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        CDataDictionaryProvider provider = new CDataDictionaryProvider()) {
      ColumnarBatchJniWrapper.forRuntime(iv.runtime())
          .exportToArrow(iv.handle(), cSchema.memoryAddress(), cArray.memoryAddress());

      Data.exportSchema(
          allocator, ArrowUtil.toArrowSchema(cSchema, allocator, provider), provider, arrowSchema);

      ColumnarBatch output =
          ArrowAbiUtil.importToSparkColumnarBatch(allocator, arrowSchema, cArray);

      // Follow gluten input's reference count. This might be optimized using
      // automatic clean-up or once the extensibility of ColumnarBatch is enriched
      IndicatorVector giv = (IndicatorVector) input.column(0);
      ImplicitClass.ArrowColumnarBatchRetainer retainer =
          new ImplicitClass.ArrowColumnarBatchRetainer(output);
      for (long i = 0; i < (giv.refCnt() - 1); i++) {
        retainer.retain();
      }

      // close the input one
      for (long i = 0; i < giv.refCnt(); i++) {
        input.close();
      }

      // populate new vectors to input
      transferVectors(output, input);
      return input;
    }
  }

  private static ColumnarBatch offload(BufferAllocator allocator, ColumnarBatch input) {
    if (!isHeavyBatch(input)) {
      throw new IllegalArgumentException("batch is not Arrow columnar batch");
    }
    if (input.numCols() == 0) {
      throw new IllegalArgumentException("batch with zero columns cannot be offloaded");
    }
    final Runtime runtime = Runtimes.contextInstance();
    try (ArrowArray cArray = ArrowArray.allocateNew(allocator);
        ArrowSchema cSchema = ArrowSchema.allocateNew(allocator)) {
      ArrowAbiUtil.exportFromSparkColumnarBatch(
          ArrowBufferAllocators.contextInstance(), input, cSchema, cArray);
      long handle =
          ColumnarBatchJniWrapper.forRuntime(runtime)
              .createWithArrowArray(cSchema.memoryAddress(), cArray.memoryAddress());
      ColumnarBatch output = ColumnarBatches.create(runtime, handle);

      // Follow input's reference count. This might be optimized using
      // automatic clean-up or once the extensibility of ColumnarBatch is enriched
      long refCnt = getRefCntHeavy(input);
      final IndicatorVector giv = (IndicatorVector) output.column(0);
      for (long i = 0; i < (refCnt - 1); i++) {
        giv.retain();
      }

      // close the input one
      for (long i = 0; i < refCnt; i++) {
        input.close();
      }

      // populate new vectors to input
      transferVectors(output, input);
      return input;
    }
  }

  public static Iterator<InternalRow> emptyRowIterator(int numRows) {
    final int maxRows = numRows;
    return new Iterator<InternalRow>() {
      int rowId = 0;

      @Override
      public boolean hasNext() {
        return rowId < maxRows;
      }

      @Override
      public InternalRow next() {
        if (rowId >= maxRows) {
          throw new NoSuchElementException();
        }
        rowId++;
        return new UnsafeRow(0);
      }
    };
  }

  private static long getRefCntLight(ColumnarBatch input) {
    if (!isLightBatch(input)) {
      throw new UnsupportedOperationException("Input batch is not light batch");
    }
    IndicatorVector iv = (IndicatorVector) input.column(0);
    return iv.refCnt();
  }

  private static long getRefCntHeavy(ColumnarBatch input) {
    if (!isHeavyBatch(input)) {
      throw new UnsupportedOperationException("Input batch is not heavy batch");
    }
    if (input.numCols() == 0) {
      throw new IllegalArgumentException(
          "batch with zero columns doesn't have meaningful " + "reference count");
    }
    long refCnt = -1L;
    for (int i = 0; i < input.numCols(); i++) {
      ArrowWritableColumnVector col = ((ArrowWritableColumnVector) input.column(i));
      long colRefCnt = col.refCnt();
      if (refCnt == -1L) {
        refCnt = colRefCnt;
      } else {
        if (colRefCnt != refCnt) {
          throw new IllegalStateException();
        }
      }
    }
    if (refCnt == -1L) {
      throw new IllegalStateException();
    }
    return refCnt;
  }

  private static long getRefCnt(ColumnarBatch input) {
    switch (identifyBatchType(input)) {
      case LIGHT:
        return getRefCntLight(input);
      case HEAVY:
        return getRefCntHeavy(input);
      default:
        throw new IllegalStateException();
    }
  }

  public static void forceClose(ColumnarBatch input) {
    for (long i = 0; i < getRefCnt(input); i++) {
      input.close();
    }
  }

  private static IndicatorVector getIndicatorVector(ColumnarBatch input) {
    if (!isLightBatch(input)) {
      throw new UnsupportedOperationException("Input batch is not light batch");
    }
    return (IndicatorVector) input.column(0);
  }

  /**
   * Combine multiple columnar batches horizontally, assuming each of them is already offloaded.
   * Otherwise {@link UnsupportedOperationException} will be thrown.
   */
  public static long compose(ColumnarBatch... batches) {
    IndicatorVector[] ivs =
        Arrays.stream(batches)
            .map(ColumnarBatches::getIndicatorVector)
            .toArray(IndicatorVector[]::new);
    // We assume all input batches should be managed by same Runtime.
    // FIXME: The check could be removed to adopt ownership-transfer semantic
    final Runtime[] ctxs =
        Arrays.stream(ivs).map(IndicatorVector::runtime).distinct().toArray(Runtime[]::new);
    Preconditions.checkState(
        ctxs.length == 1, "All input batches should be managed by same Runtime.");
    final long[] handles = Arrays.stream(ivs).mapToLong(IndicatorVector::handle).toArray();
    return ColumnarBatchJniWrapper.forRuntime(ctxs[0]).compose(handles);
  }

  public static ColumnarBatch create(Runtime runtime, long nativeHandle) {
    final IndicatorVector iv = new IndicatorVector(runtime, nativeHandle);
    int numColumns = Math.toIntExact(iv.getNumColumns());
    int numRows = Math.toIntExact(iv.getNumRows());
    if (numColumns == 0) {
      return new ColumnarBatch(new ColumnVector[0], numRows);
    }
    final ColumnVector[] columnVectors = new ColumnVector[numColumns];
    columnVectors[0] = iv;
    long numPlaceholders = numColumns - 1;
    for (int i = 0; i < numPlaceholders; i++) {
      final PlaceholderVector pv = PlaceholderVector.INSTANCE;
      columnVectors[i + 1] = pv;
    }
    return new ColumnarBatch(columnVectors, numRows);
  }

  public static void retain(ColumnarBatch b) {
    switch (identifyBatchType(b)) {
      case LIGHT:
        IndicatorVector iv = (IndicatorVector) b.column(0);
        iv.retain();
        return;
      case HEAVY:
        for (int i = 0; i < b.numCols(); i++) {
          ArrowWritableColumnVector col = ((ArrowWritableColumnVector) b.column(i));
          col.retain();
        }
        return;
      default:
        throw new IllegalStateException();
    }
  }

  public static void release(ColumnarBatch b) {
    b.close();
  }

  public static long getNativeHandle(ColumnarBatch batch) {
    return getIndicatorVector(batch).handle();
  }

  public static Runtime getRuntime(ColumnarBatch batch) {
    return getIndicatorVector(batch).runtime();
  }

  public static Runtime getRuntime(List<ColumnarBatch> batch) {
    final Set<Runtime> all = new HashSet<>();
    batch.forEach(b -> all.add(getRuntime(b)));
    if (all.size() != 1) {
      throw new IllegalArgumentException(
          "The input columnar batches has different associated runtimes");
    }
    return all.toArray(new Runtime[0])[0];
  }
}
