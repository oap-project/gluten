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
package org.apache.spark.sql.execution.datasources.v1.clickhouse

import org.apache.spark.{TaskContext, TaskOutputFileAlreadyExistException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.delta.constraints.Constraint
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.FileFormatWriter.{processStats, ConcurrentOutputWriterSpec, OutputSpec}
import org.apache.spark.sql.execution.datasources.v1.GlutenMergeTreeWriterInjects
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{SerializableConfiguration, Utils}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import java.util.{Date, UUID}

/** Reference to the 'FileFormatWriter' of the spark */
object MergeTreeFileFormatWriter extends Logging {

  // scalastyle:off argcount

  def write(
      sparkSession: SparkSession,
      plan: SparkPlan,
      fileFormat: FileFormat,
      committer: FileCommitProtocol,
      outputSpec: OutputSpec,
      hadoopConf: Configuration,
      orderByKeyOption: Option[Seq[String]],
      lowCardKeyOption: Option[Seq[String]],
      minmaxIndexKeyOption: Option[Seq[String]],
      bfIndexKeyOption: Option[Seq[String]],
      setIndexKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]],
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      statsTrackers: Seq[WriteJobStatsTracker],
      options: Map[String, String],
      constraints: Seq[Constraint],
      numStaticPartitionCols: Int = 0): Set[String] = {

    assert(plan.isInstanceOf[IFakeRowAdaptor])

    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])

    val outputPath = new Path(outputSpec.outputPath)
    val outputPathName = outputPath.toString

    FileOutputFormat.setOutputPath(job, outputPath)

    val partitionSet = AttributeSet(partitionColumns)
    // cleanup the internal metadata information of
    // the file source metadata attribute if any before write out
    // val finalOutputSpec = outputSpec.copy(outputColumns = outputSpec.outputColumns
    //   .map(FileSourceMetadataAttribute.cleanupFileSourceMetadataInformation))
    val finalOutputSpec = outputSpec.copy(outputColumns = outputSpec.outputColumns)
    val dataColumns = finalOutputSpec.outputColumns.filterNot(partitionSet.contains)

    // TODO: check whether it needs to use `convertEmptyToNullIfNeeded` to convert empty to null
    val empty2NullPlan = plan // convertEmptyToNullIfNeeded(plan, partitionColumns, constraints)

    val writerBucketSpec = bucketSpec.map {
      spec =>
        val bucketColumns = spec.bucketColumnNames.map(c => dataColumns.find(_.name == c).get)
        // Spark bucketed table: use `HashPartitioning.partitionIdExpression` as bucket id
        // expression, so that we can guarantee the data distribution is same between shuffle and
        // bucketed data source, which enables us to only shuffle one side when join a bucketed
        // table and a normal one.
        val bucketIdExpression =
          HashPartitioning(bucketColumns, spec.numBuckets).partitionIdExpression
        MergeTreeWriterBucketSpec(bucketIdExpression, (_: Int) => "")
    }
    val sortColumns = bucketSpec.toSeq.flatMap {
      spec => spec.sortColumnNames.map(c => dataColumns.find(_.name == c).get)
    }

    val caseInsensitiveOptions = CaseInsensitiveMap(options)

    val dataSchema = dataColumns.toStructType
    DataSourceUtils.verifySchema(fileFormat, dataSchema)
    // Note: prepareWrite has side effect. It sets "job".
    val outputWriterFactory =
      fileFormat.prepareWrite(sparkSession, job, caseInsensitiveOptions, dataSchema)

    val description = new MergeTreeWriteJobDescription(
      uuid = UUID.randomUUID.toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = finalOutputSpec.outputColumns,
      dataColumns = dataColumns,
      partitionColumns = partitionColumns,
      bucketSpec = writerBucketSpec,
      path = outputPathName,
      customPartitionLocations = finalOutputSpec.customPartitionLocations,
      maxRecordsPerFile = caseInsensitiveOptions
        .get("maxRecordsPerFile")
        .map(_.toLong)
        .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions
        .get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone),
      statsTrackers = statsTrackers
    )

    // We should first sort by partition columns, then bucket id, and finally sorting columns.
    val requiredOrdering = partitionColumns.drop(numStaticPartitionCols) ++
      writerBucketSpec.map(_.bucketIdExpression) ++ sortColumns
    // the sort order doesn't matter
    val actualOrdering = empty2NullPlan.outputOrdering.map(_.child)
    val orderingMatched = if (requiredOrdering.length > actualOrdering.length) {
      false
    } else {
      requiredOrdering.zip(actualOrdering).forall {
        case (requiredOrder, childOutputOrder) =>
          requiredOrder.semanticEquals(childOutputOrder)
      }
    }

    SQLExecution.checkSQLExecutionId(sparkSession)

    // propagate the description UUID into the jobs, so that committers
    // get an ID guaranteed to be unique.
    job.getConfiguration.set("spark.sql.sources.writeJobUUID", description.uuid)

    // This call shouldn't be put into the `try` block below because it only initializes and
    // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
    committer.setupJob(job)

    def nativeWrap(plan: SparkPlan) = {
      var wrapped: SparkPlan = plan
      if (writerBucketSpec.isDefined) {
        // We need to add the bucket id expression to the output of the sort plan,
        // so that we can use backend to calculate the bucket id for each row.
        wrapped = ProjectExec(
          wrapped.output :+ Alias(writerBucketSpec.get.bucketIdExpression, "__bucket_value__")(),
          wrapped)
        // TODO: to optimize, bucket value is computed twice here
      }

      val nativeFormat = sparkSession.sparkContext.getLocalProperty("nativeFormat")
      (GlutenMergeTreeWriterInjects.getInstance().executeWriterWrappedSparkPlan(wrapped), None)
    }

    try {
      val (rdd, concurrentOutputWriterSpec) = if (orderingMatched) {
        nativeWrap(empty2NullPlan)
      } else {
        // SPARK-21165: the `requiredOrdering` is based on the attributes from analyzed plan, and
        // the physical plan may have different attribute ids due to optimizer removing some
        // aliases. Here we bind the expression ahead to avoid potential attribute ids mismatch.
        val orderingExpr = bindReferences(
          requiredOrdering.map(SortOrder(_, Ascending)),
          finalOutputSpec.outputColumns)
        val sortPlan = SortExec(orderingExpr, global = false, child = empty2NullPlan)

        val maxWriters = sparkSession.sessionState.conf.maxConcurrentOutputFileWriters
        var concurrentWritersEnabled = maxWriters > 0 && sortColumns.isEmpty
        if (concurrentWritersEnabled) {
          log.warn(
            s"spark.sql.maxConcurrentOutputFileWriters(being set to $maxWriters) will be " +
              "ignored when native writer is being active. No concurrent Writers.")
          concurrentWritersEnabled = false
        }

        if (concurrentWritersEnabled) {
          (
            empty2NullPlan.execute(),
            Some(ConcurrentOutputWriterSpec(maxWriters, () => sortPlan.createSorter())))
        } else {
          nativeWrap(sortPlan)
        }
      }

      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      val rddWithNonEmptyPartitions = if (rdd.partitions.length == 0) {
        sparkSession.sparkContext.parallelize(Array.empty[InternalRow], 1)
      } else {
        rdd
      }

      val jobIdInstant = new Date().getTime
      val ret = new Array[MergeTreeWriteTaskResult](rddWithNonEmptyPartitions.partitions.length)
      sparkSession.sparkContext.runJob(
        rddWithNonEmptyPartitions,
        (taskContext: TaskContext, iter: Iterator[InternalRow]) => {
          executeTask(
            description = description,
            jobIdInstant = jobIdInstant,
            sparkStageId = taskContext.stageId(),
            sparkPartitionId = taskContext.partitionId(),
            sparkAttemptNumber = taskContext.taskAttemptId().toInt & Integer.MAX_VALUE,
            committer,
            iterator = iter,
            concurrentOutputWriterSpec = concurrentOutputWriterSpec
          )
        },
        rddWithNonEmptyPartitions.partitions.indices,
        (index, res: MergeTreeWriteTaskResult) => {
          committer.onTaskCommit(res.commitMsg)
          ret(index) = res
        }
      )

      val commitMsgs = ret.map(_.commitMsg)

      logInfo(s"Start to commit write Job ${description.uuid}.")
      val (_, duration) = Utils.timeTakenMs(committer.commitJob(job, commitMsgs))
      logInfo(s"Write Job ${description.uuid} committed. Elapsed time: $duration ms.")

      processStats(description.statsTrackers, ret.map(_.summary.stats), duration)
      logInfo(s"Finished processing stats for write job ${description.uuid}.")

      // return a set of all the partition paths that were updated during this job
      ret.map(_.summary.updatedPartitions).reduceOption(_ ++ _).getOrElse(Set.empty)
    } catch {
      case cause: Throwable =>
        logError(s"Aborting job ${description.uuid}.", cause)
        committer.abortJob(job)
        throw QueryExecutionErrors.jobAbortedError(cause)
    }
  }
  // scalastyle:on argcount

  def executeTask(
      description: MergeTreeWriteJobDescription,
      jobIdInstant: Long,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow],
      concurrentOutputWriterSpec: Option[ConcurrentOutputWriterSpec]
  ): MergeTreeWriteTaskResult = {

    val jobId = SparkHadoopWriterUtils.createJobID(new Date(jobIdInstant), sparkStageId)
    val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

    // Set up the attempt context required to use in the output committer.
    val taskAttemptContext: TaskAttemptContext = {
      // Set up the configuration object
      val hadoopConf = description.serializableHadoopConf.value
      hadoopConf.set("mapreduce.job.id", jobId.toString)
      hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
      hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
      hadoopConf.setBoolean("mapreduce.task.ismap", true)
      hadoopConf.setInt("mapreduce.task.partition", 0)

      new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    }

    committer.setupTask(taskAttemptContext)

    val dataWriter =
      if (sparkPartitionId != 0 && !iterator.hasNext) {
        // In case of empty job,
        // leave first partition to save meta for file format like parquet/orc.
        new MergeTreeEmptyDirectoryDataWriter(description, taskAttemptContext, committer)
      } else if (description.partitionColumns.isEmpty && description.bucketSpec.isEmpty) {
        new MergeTreeSingleDirectoryDataWriter(description, taskAttemptContext, committer)
      } else {
        concurrentOutputWriterSpec match {
          case Some(spec) =>
            new MergeTreeDynamicPartitionDataConcurrentWriter(
              description,
              taskAttemptContext,
              committer,
              spec)
          case _ =>
            new MergeTreeDynamicPartitionDataSingleWriter(
              description,
              taskAttemptContext,
              committer)
        }
      }

    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        // Execute the task to write rows out and commit the task.
        dataWriter.writeWithIterator(iterator)
        dataWriter.commit()
      })(
        catchBlock = {
          // If there is an error, abort the task
          dataWriter.abort()
          logError(s"Job $jobId aborted.")
        },
        finallyBlock = {
          dataWriter.close()
        })
    } catch {
      case e: FetchFailedException =>
        throw e
      case f: FileAlreadyExistsException if SQLConf.get.fastFailFileFormatOutput =>
        // If any output file to write already exists, it does not make sense to re-run this task.
        // We throw the exception and let Executor throw ExceptionFailure to abort the job.
        throw new TaskOutputFileAlreadyExistException(f)
      case t: Throwable =>
        throw QueryExecutionErrors.taskFailedWhileWritingRowsError(t)
    }
  }
}
