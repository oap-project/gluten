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
package org.apache.spark.sql.delta

import org.apache.gluten.execution.ColumnarToRowExecBase

import org.apache.spark.SparkException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.constraints.{Constraint, Constraints}
import org.apache.spark.sql.delta.files.MergeTreeCommitProtocol
import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FakeRowAdaptor, FileFormatWriter, WriteJobStatsTracker}
import org.apache.spark.sql.execution.datasources.v1.clickhouse.MergeTreeFileFormatWriter
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.DeltaMergeTreeFileFormat
import org.apache.spark.util.{Clock, SerializableConfiguration}

import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.mutable.ListBuffer

object ClickhouseOptimisticTransaction {}
class ClickhouseOptimisticTransaction(
    override val deltaLog: DeltaLog,
    override val snapshot: Snapshot)(implicit override val clock: Clock)
  extends OptimisticTransaction(deltaLog, snapshot) {

  def this(deltaLog: DeltaLog, snapshotOpt: Option[Snapshot] = None)(implicit clock: Clock) {
    this(
      deltaLog,
      snapshotOpt.getOrElse(deltaLog.update())
    )
  }

  override def writeFiles(
      inputData: Dataset[_],
      writeOptions: Option[DeltaOptions],
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    hasWritten = true

    val spark = inputData.sparkSession
    val (data, partitionSchema) = performCDCPartition(inputData)
    val outputPath = deltaLog.dataPath

    val (queryExecution, output, generatedColumnConstraints, _) =
      normalizeData(deltaLog, data)
    val partitioningColumns = getPartitioningColumns(partitionSchema, output)

    val committer = new MergeTreeCommitProtocol("delta-mergetree", outputPath.toString, None)

    // val (optionalStatsTracker, _) = getOptionalStatsTrackerAndStatsCollection(output, outputPath,
    //   partitionSchema, data)
    val (optionalStatsTracker, _) = (None, None)

    val constraints =
      Constraints.getAll(metadata, spark) ++ generatedColumnConstraints ++ additionalConstraints

    SQLExecution.withNewExecutionId(queryExecution, Option("deltaTransactionalWrite")) {
      val outputSpec = FileFormatWriter.OutputSpec(outputPath.toString, Map.empty, output)

      val queryPlan = queryExecution.executedPlan
      val newQueryPlan = queryPlan match {
        // if the child is columnar, we can just wrap&transfer the columnar data
        case c2r: ColumnarToRowExecBase =>
          FakeRowAdaptor(c2r.child)
        // If the child is aqe, we make aqe "support columnar",
        // then aqe itself will guarantee to generate columnar outputs.
        // So FakeRowAdaptor will always consumes columnar data,
        // thus avoiding the case of c2r->aqe->r2c->writer
        case aqe: AdaptiveSparkPlanExec =>
          FakeRowAdaptor(
            AdaptiveSparkPlanExec(
              aqe.inputPlan,
              aqe.context,
              aqe.preprocessingRules,
              aqe.isSubquery,
              supportsColumnar = true
            ))
        case other => FakeRowAdaptor(other)
      }

      val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()

      if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
        val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
          new SerializableConfiguration(deltaLog.newDeltaHadoopConf()),
          BasicWriteJobStatsTracker.metrics)
//        registerSQLMetrics(spark, basicWriteJobStatsTracker.driverSideMetrics)
        statsTrackers.append(basicWriteJobStatsTracker)
      }

      // Retain only a minimal selection of Spark writer options to avoid any potential
      // compatibility issues
      val options = writeOptions match {
        case None => Map.empty[String, String]
        case Some(writeOptions) =>
          writeOptions.options.filterKeys {
            key =>
              key.equalsIgnoreCase(DeltaOptions.MAX_RECORDS_PER_FILE) ||
              key.equalsIgnoreCase(DeltaOptions.COMPRESSION)
          }.toMap
      }

      try {
        val tableV2 = ClickHouseTableV2.getTable(deltaLog)
        MergeTreeFileFormatWriter.write(
          sparkSession = spark,
          plan = newQueryPlan,
          fileFormat = new DeltaMergeTreeFileFormat(
            metadata,
            tableV2.dataBaseName,
            tableV2.tableName,
            tableV2.orderByKeyOption,
            tableV2.lowCardKeyOption,
            tableV2.minmaxIndexKeyOption,
            tableV2.bfIndexKeyOption,
            tableV2.setIndexKeyOption,
            tableV2.primaryKeyOption,
            tableV2.clickhouseTableConfigs,
            tableV2.partitionColumns
          ),
          // formats.
          committer = committer,
          outputSpec = outputSpec,
          // scalastyle:off deltahadoopconfiguration
          hadoopConf =
            spark.sessionState.newHadoopConfWithOptions(metadata.configuration ++ deltaLog.options),
          // scalastyle:on deltahadoopconfiguration
          orderByKeyOption = tableV2.orderByKeyOption,
          lowCardKeyOption = tableV2.lowCardKeyOption,
          minmaxIndexKeyOption = tableV2.minmaxIndexKeyOption,
          bfIndexKeyOption = tableV2.bfIndexKeyOption,
          setIndexKeyOption = tableV2.setIndexKeyOption,
          primaryKeyOption = tableV2.primaryKeyOption,
          partitionColumns = partitioningColumns,
          bucketSpec = tableV2.bucketOption,
          statsTrackers = optionalStatsTracker.toSeq ++ statsTrackers,
          options = options,
          constraints = constraints
        )
      } catch {
        case s: SparkException =>
          // Pull an InvariantViolationException up to the top level if it was the root cause.
          val violationException = ExceptionUtils.getRootCause(s)
          if (violationException.isInstanceOf[InvariantViolationException]) {
            throw violationException
          } else {
            throw s
          }
      }
    }

    //    val resultFiles = committer.addedStatuses
    //      .map {
    //        a =>
    //          a.copy(stats = optionalStatsTracker
    //            .map(_.recordedStats(new Path(new URI(a.path)).getName))
    //            .getOrElse(a.stats))
    //      }
    /*
      .filter {
        // In some cases, we can write out an empty `inputData`.
        // Some examples of this (though, they
        // may be fixed in the future) are the MERGE command when you delete with empty source, or
        // empty target, or on disjoint tables. This is hard to catch before the write without
        // collecting the DF ahead of time. Instead, we can return only the AddFiles that
        // a) actually add rows, or
        // b) don't have any stats so we don't know the number of rows at all
        case a: AddFile => a.numLogicalRecords.forall(_ > 0)
        case _ => true
      }
     */

    committer.addedStatuses.toSeq ++ committer.changeFiles
  }
}
