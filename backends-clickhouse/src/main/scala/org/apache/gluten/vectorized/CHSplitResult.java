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

public class CHSplitResult extends SplitResult {
    private final long splitTime;
    private final long diskWriteTime;
    private final long serializationTime;

    public CHSplitResult(long totalComputePidTime,
                         long totalWriteTime,
                         long totalEvictTime,
                         long totalCompressTime,
                         long totalBytesWritten,
                         long totalBytesEvicted,
                         long[] partitionLengths,
                         long[] rawPartitionLengths,
                         long splitTime,
                         long diskWriteTime,
                         long serializationTime) {
        super(totalComputePidTime,
                totalWriteTime,
                totalEvictTime,
                totalCompressTime,
                totalBytesWritten,
                totalBytesEvicted,
                partitionLengths,
                rawPartitionLengths);
        this.splitTime = splitTime;
        this.diskWriteTime = diskWriteTime;
        this.serializationTime = serializationTime;
    }

    public long getSplitTime() {
        return splitTime;
    }

    public long getDiskWriteTime() {
        return diskWriteTime;
    }

    public long getSerializationTime() {
        return serializationTime;
    }
}
