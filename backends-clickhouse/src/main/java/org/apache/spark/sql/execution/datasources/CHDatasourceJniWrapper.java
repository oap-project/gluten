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
package org.apache.spark.sql.execution.datasources;

public class CHDatasourceJniWrapper {

  public native long nativeInitFileWriterWrapper(
      String filePath, String[] preferredColumnNames, String formatHint);

  public native long nativeInitMergeTreeWriterWrapper(
      byte[] plan,
      byte[] splitInfo,
      String uuid,
      String taskId,
      String partition_dir,
      String bucket_dir);

  public native String nativeMergeMTParts(
      byte[] plan,
      byte[] splitInfo,
      String uuid,
      String taskId,
      String partition_dir,
      String bucket_dir);

  public native void write(long instanceId, long blockAddress);

  public native void writeToMergeTree(long instanceId, long blockAddress);

  public native void close(long instanceId);

  public native String closeMergeTreeWriter(long instanceId);

  /*-
   * The input block is already sorted by partition columns + bucket expressions. (check
   * org.apache.spark.sql.execution.datasources.FileFormatWriter#write)
   * However, the input block may contain parts(we call it stripe here) belonging to
   * different partition/buckets.
   *
   * If bucketing is enabled, the input block's last column is guaranteed to be _bucket_value_.
   *
   * This function splits the input block in to several blocks, each of which belonging
   * to the same partition/bucket. Notice the stripe will NOT contain partition columns
   *
   * Since all rows in a stripe share the same partition/bucket,
   * we only need to check the heading row.
   * So, for each stripe, the native code also returns each stripe's first row's index.
   * Caller can use these indice to get UnsafeRows from the input block,
   * to help FileFormatDataWriter to aware partition/bucket changes.
   */
  public static native BlockStripes splitBlockByPartitionAndBucket(
      long blockAddress,
      int[] partitionColIndice,
      boolean hasBucket,
      boolean reserve_partition_columns);
}
