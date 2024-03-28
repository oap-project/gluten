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
package org.apache.gluten.utils

import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.{FilePartition, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.util.collection.BitSet

case class InputPartitionsUtil(
    relation: HadoopFsRelation,
    selectedPartitions: Array[PartitionDirectory],
    output: Seq[Attribute],
    bucketedScan: Boolean,
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    disableBucketedScan: Boolean)
  extends Logging {

  def genInputPartitionSeq(): Seq[InputPartition] = {
    if (bucketedScan) {
      genBucketedInputPartitionSeq()
    } else {
      genNonBuckedInputPartitionSeq()
    }
  }

  private def genNonBuckedInputPartitionSeq(): Seq[InputPartition] = {
    val openCostInBytes = relation.sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes =
      FilePartition.maxSplitBytes(relation.sparkSession, selectedPartitions)
    logInfo(
      s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
        s"open cost is considered as scanning $openCostInBytes bytes.")

    val splitFiles = selectedPartitions
      .flatMap {
        partition =>
          SparkShimLoader.getSparkShims.getFileStatus(partition).flatMap {
            file =>
              // getPath() is very expensive so we only want to call it once in this block:
              val filePath = file.getPath
              val isSplitable =
                relation.fileFormat.isSplitable(relation.sparkSession, relation.options, filePath)
              SparkShimLoader.getSparkShims.splitFiles(
                sparkSession = relation.sparkSession,
                file = file,
                filePath = filePath,
                isSplitable = isSplitable,
                maxSplitBytes = maxSplitBytes,
                partitionValues = partition.values)
          }
      }
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)
  }

  private def genBucketedInputPartitionSeq(): Seq[InputPartition] = {
    val bucketSpec = relation.bucketSpec.get
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    val filesGroupedToBuckets =
      SparkShimLoader.getSparkShims.filesGroupedToBuckets(selectedPartitions)

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      filesGroupedToBuckets.filter(f => bucketSet.get(f._1))
    } else {
      filesGroupedToBuckets
    }

    optionalNumCoalescedBuckets
      .map {
        numCoalescedBuckets =>
          logInfo(s"Coalescing to $numCoalescedBuckets buckets")
          val coalescedBuckets = prunedFilesGroupedToBuckets.groupBy(_._1 % numCoalescedBuckets)
          Seq.tabulate(numCoalescedBuckets) {
            bucketId =>
              val partitionedFiles = coalescedBuckets
                .get(bucketId)
                .map {
                  _.values.flatten.toArray
                }
                .getOrElse(Array.empty)
              FilePartition(bucketId, partitionedFiles)
          }
      }
      .getOrElse {
        Seq.tabulate(bucketSpec.numBuckets) {
          bucketId =>
            FilePartition(bucketId, prunedFilesGroupedToBuckets.getOrElse(bucketId, Array.empty))
        }
      }
  }

  private def toAttribute(colName: String): Option[Attribute] = {
    output.find(_.name == colName)
  }
}
