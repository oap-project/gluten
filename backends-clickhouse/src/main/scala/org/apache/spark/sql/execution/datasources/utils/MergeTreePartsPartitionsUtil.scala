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
package org.apache.spark.sql.execution.datasources.utils

import org.apache.gluten.execution.{GlutenMergeTreePartition, MergeTreePartSplit}
import org.apache.gluten.expression.ConverterUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.delta.ClickhouseSnapshot
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.DeltaMergeTreeFileFormat
import org.apache.spark.util.collection.BitSet

import scala.collection.mutable.ArrayBuffer

// scalastyle:off argcount
object MergeTreePartsPartitionsUtil extends Logging {

  def getMergeTreePartsPartitions(
      relation: HadoopFsRelation,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      bucketedScan: Boolean,
      sparkSession: SparkSession,
      table: ClickHouseTableV2,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean): Seq[InputPartition] = {
    if (
      !relation.location.isInstanceOf[TahoeFileIndex] || !relation.fileFormat
        .isInstanceOf[DeltaMergeTreeFileFormat]
    ) {
      throw new IllegalStateException()
    }
    val fileIndex = relation.location.asInstanceOf[TahoeFileIndex]

    val partitions = new ArrayBuffer[InputPartition]
    val (database, tableName) = if (table.catalogTable.isDefined) {
      (table.catalogTable.get.identifier.database.get, table.catalogTable.get.identifier.table)
    } else {
      // for file_format.`file_path`
      ("default", "file_format")
    }
    val engine = "MergeTree"
    val relativeTablePath = fileIndex.deltaLog.dataPath.toUri.getPath.substring(1)
    val absoluteTablePath = fileIndex.deltaLog.dataPath.toUri.toString

    val (orderByKey, primaryKey) =
      MergeTreeDeltaUtil.genOrderByAndPrimaryKeyStr(table.orderByKeyOption, table.primaryKeyOption)

    val lowCardKey = table.lowCardKeyOption match {
      case Some(keys) => keys.mkString(",")
      case None => ""
    }

    val minmaxIndexKey = table.minmaxIndexKeyOption match {
      case Some(keys) => keys.mkString(",")
      case None => ""
    }

    val bfIndexKey = table.bfIndexKeyOption match {
      case Some(keys) => keys.mkString(",")
      case None => ""
    }

    val setIndexKey = table.setIndexKeyOption match {
      case Some(keys) => keys.mkString(",")
      case None => ""
    }

    val tableSchemaJson = ConverterUtils.convertNamedStructJson(table.schema())

    // bucket table
    if (table.bucketOption.isDefined && bucketedScan) {
      genBucketedInputPartitionSeq(
        engine,
        database,
        tableName,
        relativeTablePath,
        absoluteTablePath,
        table.bucketOption.get,
        optionalBucketSet,
        optionalNumCoalescedBuckets,
        selectedPartitions,
        tableSchemaJson,
        partitions,
        orderByKey,
        lowCardKey,
        minmaxIndexKey,
        bfIndexKey,
        setIndexKey,
        primaryKey,
        table.clickhouseTableConfigs,
        sparkSession
      )
    } else {
      genInputPartitionSeq(
        engine,
        database,
        tableName,
        relativeTablePath,
        absoluteTablePath,
        optionalBucketSet,
        selectedPartitions,
        tableSchemaJson,
        partitions,
        orderByKey,
        lowCardKey,
        minmaxIndexKey,
        bfIndexKey,
        setIndexKey,
        primaryKey,
        table.clickhouseTableConfigs,
        sparkSession
      )
    }
    partitions
  }

  def genInputPartitionSeq(
      engine: String,
      database: String,
      tableName: String,
      relativeTablePath: String,
      absoluteTablePath: String,
      optionalBucketSet: Option[BitSet],
      selectedPartitions: Array[PartitionDirectory],
      tableSchemaJson: String,
      partitions: ArrayBuffer[InputPartition],
      orderByKey: String,
      lowCardKey: String,
      minmaxIndexKey: String,
      bfIndexKey: String,
      setIndexKey: String,
      primaryKey: String,
      clickhouseTableConfigs: Map[String, String],
      sparkSession: SparkSession): Unit = {

    val selectPartsFiles = selectedPartitions
      .flatMap(
        partition =>
          partition.files.map(
            fs => {
              val path = fs.getPath.toString

              val ret = ClickhouseSnapshot.pathToAddMTPCache.getIfPresent(path)
              if (ret == null) {
                val keys = ClickhouseSnapshot.pathToAddMTPCache.asMap().keySet()
                val keySample = keys.isEmpty() match {
                  case true => "<empty>"
                  case false => keys.iterator().next()
                }
                throw new IllegalStateException(
                  "Can't find AddMergeTreeParts from cache pathToAddMTPCache for key: " +
                    path + ". This happens when too many new entries are added to " +
                    "pathToAddMTPCache during current query. " +
                    "Try rerun current query. KeySample: " + keySample
                )
              }
              ret
            }))
      .toSeq
    if (selectPartsFiles.isEmpty) {
      return
    }

    val maxSplitBytes = getMaxSplitBytes(sparkSession, selectPartsFiles)
    val total_marks = selectPartsFiles.map(p => p.marks).sum
    val total_Bytes = selectPartsFiles.map(p => p.size).sum
    val markCntPerPartition = maxSplitBytes * total_marks / total_Bytes + 1

    val bucketingEnabled = sparkSession.sessionState.conf.bucketingEnabled
    val shouldProcess: String => Boolean = optionalBucketSet match {
      case Some(bucketSet) if bucketingEnabled =>
        name =>
          // find bucket it in name pattern of:
          // "partition_col=1/00001/373c9386-92a4-44ef-baaf-a67e1530b602_0_006"
          name.split("/").dropRight(1).filterNot(_.contains("=")).map(_.toInt).forall(bucketSet.get)
      case _ =>
        _ => true
    }

    logInfo(s"Planning scan with bin packing, max mark: $markCntPerPartition")
    val splitFiles = selectPartsFiles
      .flatMap {
        part =>
          if (shouldProcess(part.name)) {
            (0L until part.marks by markCntPerPartition).map {
              offset =>
                val remaining = part.marks - offset
                val size = if (remaining > markCntPerPartition) markCntPerPartition else remaining
                MergeTreePartSplit(
                  part.name,
                  part.dirName,
                  part.targetNode,
                  offset,
                  size,
                  size * part.size / part.marks)
            }
          } else {
            None
          }
      }

    var currentSize = 0L
    val currentFiles = new ArrayBuffer[MergeTreePartSplit]

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        val newPartition = GlutenMergeTreePartition(
          partitions.size,
          engine,
          database,
          tableName,
          relativeTablePath,
          absoluteTablePath,
          orderByKey,
          lowCardKey,
          minmaxIndexKey,
          bfIndexKey,
          setIndexKey,
          primaryKey,
          currentFiles.toArray,
          tableSchemaJson,
          clickhouseTableConfigs
        )
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    // generate `Seq[InputPartition]` by file size
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    // val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    // Assign files to partitions using "Next Fit Decreasing"
    splitFiles.foreach {
      parts =>
        if ((currentSize + parts.bytesOnDisk > maxSplitBytes)) {
          closePartition()
        }
        // Add the given file to the current partition.
        currentSize += parts.bytesOnDisk + openCostInBytes
        currentFiles += parts
    }
    closePartition()
  }

  /** Generate bucket partition */
  def genBucketedInputPartitionSeq(
      engine: String,
      database: String,
      tableName: String,
      relativeTablePath: String,
      absoluteTablePath: String,
      bucketSpec: BucketSpec,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      selectedPartitions: Array[PartitionDirectory],
      tableSchemaJson: String,
      partitions: ArrayBuffer[InputPartition],
      orderByKey: String,
      lowCardKey: String,
      minmaxIndexKey: String,
      bfIndexKey: String,
      setIndexKey: String,
      primaryKey: String,
      clickhouseTableConfigs: Map[String, String],
      sparkSession: SparkSession): Unit = {

    val selectPartsFiles = selectedPartitions
      .flatMap(
        partition =>
          partition.files.map(
            fs => {
              val path = fs.getPath.toString
              val ret = ClickhouseSnapshot.pathToAddMTPCache.getIfPresent(path)
              if (ret == null) {
                val keys = ClickhouseSnapshot.pathToAddMTPCache.asMap().keySet()
                val keySample = keys.isEmpty() match {
                  case true => "<empty>"
                  case false => keys.iterator().next()
                }
                throw new IllegalStateException(
                  "Can't find AddMergeTreeParts from cache pathToAddMTPCache for key: " +
                    path + ". This happens when too many new entries are added to " +
                    "pathToAddMTPCache during current query. " +
                    "Try rerun current query. KeySample: " + keySample)
              }
              ret
            }))
      .toSeq

    if (selectPartsFiles.isEmpty) {
      return
    }

    val bucketGroupParts = selectPartsFiles.groupBy(p => Integer.parseInt(p.bucketNum))

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      bucketGroupParts.filter(f => bucketSet.get(f._1))
    } else {
      bucketGroupParts
    }

    if (optionalNumCoalescedBuckets.isDefined) {
      throw new UnsupportedOperationException(
        "Currently CH backend can't support coalesced buckets.")
    }
    Seq.tabulate(bucketSpec.numBuckets) {
      bucketId =>
        val currBucketParts: Seq[AddMergeTreeParts] =
          prunedFilesGroupedToBuckets.getOrElse(bucketId, Seq.empty)
        if (!currBucketParts.isEmpty) {
          val currentFiles = currBucketParts.map {
            part =>
              MergeTreePartSplit(part.name, part.dirName, part.targetNode, 0, part.marks, part.size)
          }
          val newPartition = GlutenMergeTreePartition(
            partitions.size,
            engine,
            database,
            tableName,
            relativeTablePath,
            absoluteTablePath,
            orderByKey,
            lowCardKey,
            minmaxIndexKey,
            bfIndexKey,
            setIndexKey,
            primaryKey,
            currentFiles.toArray,
            tableSchemaJson,
            clickhouseTableConfigs
          )
          partitions += newPartition
        }
    }
  }

  def getMaxSplitBytes(sparkSession: SparkSession, selectedParts: Seq[AddMergeTreeParts]): Long = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val minPartitionNum = sparkSession.sessionState.conf.filesMinPartitionNum
      .getOrElse(sparkSession.leafNodeDefaultParallelism)
    val totalBytes = selectedParts.map(_.size + openCostInBytes).sum
    val bytesPerCore = totalBytes / minPartitionNum

    Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
  }
}
// scalastyle:on argcount
