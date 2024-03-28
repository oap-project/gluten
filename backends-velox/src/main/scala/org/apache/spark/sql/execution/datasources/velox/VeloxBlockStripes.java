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
package org.apache.spark.sql.execution.datasources.velox;

import org.apache.gluten.exec.Runtimes;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.datasources.BlockStripe;
import org.apache.spark.sql.execution.datasources.BlockStripes;
import org.apache.gluten.columnarbatch.ColumnarBatches;

import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public class VeloxBlockStripes extends BlockStripes {

  private int index = 0;

  public VeloxBlockStripes(BlockStripes bs) {
    super(bs.originBlockAddress,
        bs.blockAddresses, bs.headingRowIndice, bs.originBlockNumColumns,
        bs.rowBytes);
  }

  @Override
  public @NotNull Iterator<BlockStripe> iterator() {
    return new Iterator<BlockStripe>() {

      @Override
      public boolean hasNext() {
        return index < 1;
      }

      @Override
      public BlockStripe next() {
        index += 1;
        return new BlockStripe() {
          @Override
          public ColumnarBatch getColumnarBatch() {
            return ColumnarBatches.create(Runtimes.contextInstance(), blockAddresses[0]);
          }

          @Override
          public InternalRow getHeadingRow() {
            UnsafeRow row = new UnsafeRow(originBlockNumColumns);
            row.pointTo(rowBytes, rowBytes.length);
            return row;
          }
        };
      }
    };
  }


  @Override
  public void release() {

  }
}

