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
package org.apache.spark.sql.execution.datasources.v1;

import org.apache.gluten.row.SparkRowInfo;
import org.apache.gluten.vectorized.CHBlockConverterJniWrapper;
import org.apache.gluten.vectorized.CHNativeBlock;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.BlockStripe;
import org.apache.spark.sql.execution.datasources.BlockStripes;
import org.apache.spark.sql.execution.utils.CHExecUtil;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;


public class CHBlockStripes extends BlockStripes {

    private final SparkRowInfo sparkRowInfo;
    private final scala.collection.Iterator<InternalRow> headingRowIter;
    private int index = -1;

    public CHBlockStripes(BlockStripes bs) {
        super(bs.originBlockAddress,
                bs.blockAddresses, bs.headingRowIndice, bs.originBlockNumColumns);
        sparkRowInfo = CHBlockConverterJniWrapper.convertColumnarToRow(
                originBlockAddress, headingRowIndice);
        headingRowIter = CHExecUtil.getRowIterFromSparkRowInfo(
                sparkRowInfo, originBlockNumColumns, headingRowIndice.length);
    }

    @Override
    public @NotNull Iterator<BlockStripe> iterator() {
        return new Iterator<BlockStripe>() {

            @Override
            public boolean hasNext() {
                return headingRowIter.hasNext();
            }

            @Override
            public BlockStripe next() {
                index += 1;
                InternalRow headingRow = headingRowIter.next();
                return new BlockStripe() {
                    @Override
                    public ColumnarBatch getColumnarBatch() {
                        CHNativeBlock nativeBlock = new CHNativeBlock(blockAddresses[index]);
                        return nativeBlock.toColumnarBatch();
                    }

                    @Override
                    public InternalRow getHeadingRow() {
                        return headingRow;
                    }
                };
            }
        };
    }


    @Override
    public void release() {
        for (long address : blockAddresses) {
            CHBlockConverterJniWrapper.freeBlock(address);
        }
    }
}

