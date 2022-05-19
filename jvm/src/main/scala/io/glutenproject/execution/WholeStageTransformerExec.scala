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

package io.glutenproject.execution

import scala.collection.mutable.ListBuffer

import com.google.common.collect.Lists
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression._
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.plan.{PlanBuilder, PlanNode}
import io.glutenproject.substrait.rel.RelNode
import io.glutenproject.vectorized._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.UserAddedJarUtils

case class TransformContext(
    inputAttributes: Seq[Attribute],
    outputAttributes: Seq[Attribute],
    root: RelNode)

case class WholestageTransformContext(
    inputAttributes: Seq[Attribute],
    outputAttributes: Seq[Attribute],
    root: PlanNode,
    substraitContext: SubstraitContext = null)

trait TransformSupport extends SparkPlan {

  /**
   * Validate whether this SparkPlan supports to be transformed into substrait node in Native Code.
   */
  def doValidate(): Boolean = false

  /**
   * Returns all the RDDs of ColumnarBatch which generates the input rows.
   *
   * @note Right now we support up to two RDDs
   */
  def columnarInputRDDs: Seq[RDD[ColumnarBatch]]

  def getBuildPlans: Seq[(SparkPlan, SparkPlan)]

  def getStreamedLeafPlan: SparkPlan

  def getChild: SparkPlan

  def doTransform(context: SubstraitContext): TransformContext = {
    throw new UnsupportedOperationException(
      s"This operator doesn't support doTransform with SubstraitContext.")
  }

  def dependentPlanCtx: TransformContext = null

  def updateMetrics(out_num_rows: Long, process_time: Long): Unit = {}
}

case class WholeStageTransformerExec(child: SparkPlan)(val transformStageId: Int)
    extends UnaryExecNode
    with TransformSupport {

  val sparkConf = sparkContext.getConf
  val numaBindingInfo = GlutenConfig.getConf.numaBindingInfo
  val enableColumnarSortMergeJoinLazyRead: Boolean =
    GlutenConfig.getConf.enableColumnarSortMergeJoinLazyRead

  var fakeArrowOutput = false

  def setFakeOutput(): Unit = {
    fakeArrowOutput = true
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_wholestagetransform"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build dependencies"),
    "pipelineTime" -> SQLMetrics.createTimingMetric(sparkContext, "duration"))

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def supportsColumnar: Boolean = GlutenConfig.getConf.enableColumnarIterator

  override def otherCopyArgs: Seq[AnyRef] = Seq(transformStageId.asInstanceOf[Integer])

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      indent: Int = 0): Unit = {
    val res = child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      if (printNodeId) "* " else s"*($transformStageId) ",
      false,
      maxFields,
      printNodeId,
      indent)
    res
  }

  override def nodeName: String = s"WholeStageCodegenTransformer (${transformStageId})"

  def uploadAndListJars(signature: String): Seq[String] =
    if (signature != "") {
      if (sparkContext.listJars.filter(path => path.contains(s"${signature}.jar")).isEmpty) {
        val tempDir = GlutenConfig.getRandomTempDir
        val jarFileName =
          s"${tempDir}/tmp/spark-columnar-plugin-codegen-precompile-${signature}.jar"
        sparkContext.addJar(jarFileName)
      }
      sparkContext.listJars.filter(path => path.contains(s"${signature}.jar"))
    } else {
      Seq()
    }

  def doWholestageTransform(): WholestageTransformContext = {
    val substraitContext = new SubstraitContext
    val childCtx = child
      .asInstanceOf[TransformSupport]
      .doTransform(substraitContext)
    if (childCtx == null) {
      throw new NullPointerException(
        s"ColumnarWholestageTransformer can't doTansform on ${child}")
    }
    val outNames = new java.util.ArrayList[String]()
    // Use the first item in output names to specify the output format of the WS computing.
    // When the next operator is ArrowColumnarToRow, fake Arrow output will be returned.
    // TODO: Use a more proper way to send some self-assigned parameters to native.
    if (GlutenConfig.getConf.isVeloxBackend) {
      if (fakeArrowOutput) {
        outNames.add("fake_arrow_output")
      } else {
        outNames.add("real_arrow_output")
      }
    }
    for (attr <- childCtx.outputAttributes) {
      // ============================================
      // outNames.add(attr.name)
      outNames.add(ConverterUtils.getShortAttributeName(attr) + "#" + attr.exprId.id)
    }
    val planNode =
      PlanBuilder.makePlan(substraitContext, Lists.newArrayList(childCtx.root), outNames)

    WholestageTransformContext(
      childCtx.inputAttributes,
      childCtx.outputAttributes,
      planNode,
      substraitContext)
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {
    child.asInstanceOf[TransformSupport].getBuildPlans
  }

  override def getStreamedLeafPlan: SparkPlan = {
    child.asInstanceOf[TransformSupport].getStreamedLeafPlan
  }

  override def getChild: SparkPlan = child

  override def updateMetrics(out_num_rows: Long, process_time: Long): Unit = {}

  var metricsUpdated: Boolean = false

  def updateMetrics(nativeIterator: GeneralOutIterator): Unit = {
    if (metricsUpdated) return
    try {
      val metrics = nativeIterator.getMetrics
      var curChild = child
      var idx = metrics.output_length_list.length - 1
      var child_process_time: Long = 0
      while (idx >= 0 && curChild.isInstanceOf[TransformSupport]) {
        curChild
          .asInstanceOf[TransformSupport]
          .updateMetrics(
            metrics.output_length_list(idx),
            metrics.process_time_list(idx) - child_process_time)
        child_process_time = metrics.process_time_list(idx)
        idx -= 1
        curChild = curChild.asInstanceOf[TransformSupport].getChild
      }
      metricsUpdated = true
    } catch {
      case e: NullPointerException =>
        logWarning(s"updateMetrics failed due to NullPointerException!")
    }
  }

  /**
   * Return built cpp library's signature
   */
  def doBuild(): String = {
    ""
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = child match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      throw new UnsupportedOperationException
  }

  def checkBatchScanExecTransformerChild(): Option[BasicScanExecTransformer] = {
    var current_op = child
    while (current_op.isInstanceOf[TransformSupport] &&
           !current_op.isInstanceOf[BasicScanExecTransformer] &&
           current_op.asInstanceOf[TransformSupport].getChild != null) {
      current_op = current_op.asInstanceOf[TransformSupport].getChild
    }
    if (current_op != null &&
        current_op.isInstanceOf[BasicScanExecTransformer]) {
      current_op match {
        case op: BatchScanExecTransformer =>
          op.partitions
          Some(current_op.asInstanceOf[BatchScanExecTransformer])
        case op: FileSourceScanExecTransformer =>
          Some(current_op.asInstanceOf[FileSourceScanExecTransformer])
      }
    } else {
      None
    }
  }

  override def doExecute(): RDD[InternalRow] = {
    val numOutputBatches = child.longMetric("numOutputBatches")
    val pipelineTime = longMetric("pipelineTime")

    // check if BatchScan exists
    val current_op = checkBatchScanExecTransformerChild()
    if (current_op.isDefined) {
      // If containing scan exec transformer, a new RDD is created.
      val fileScan = current_op.get
      val wsCxt = doWholestageTransform()

      val startTime = System.nanoTime()
      val substraitPlanPartition = fileScan.getPartitions.map(p =>
        BackendsApiManager.getIteratorApiInstance.genNativeFilePartition(p, wsCxt))
      logWarning(
        s"Generated substrait plan tooks: ${(System.nanoTime() - startTime) / 1000000} ms")

      val wsRDD = new NativeWholestageRowRDD(sparkContext, substraitPlanPartition, false)
      wsRDD
    } else {
      sparkContext.emptyRDD
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val signature = doBuild()
    val listJars = uploadAndListJars(signature)

    val numOutputBatches = child.metrics.get("numOutputBatches")
    val pipelineTime = longMetric("pipelineTime")

    // we should zip all dependent RDDs to current main RDD
    // TODO: Does it still need these parameters?
    val streamedSortPlan = getStreamedLeafPlan
    val dependentKernels: ListBuffer[ExpressionEvaluator] = ListBuffer()
    val dependentKernelIterators: ListBuffer[GeneralOutIterator] = ListBuffer()
    val buildRelationBatchHolder: ListBuffer[ColumnarBatch] = ListBuffer()
    // val serializableObjectHolder: ListBuffer[SerializableObject] = ListBuffer()
    // val relationHolder: ListBuffer[ColumnarHashedRelation] = ListBuffer()

    // check if BatchScan exists
    val current_op = checkBatchScanExecTransformerChild()
    if (current_op.isDefined) {
      // If containing scan exec transformer, a new RDD is created.
      // TODO: Remove ?
      val execTempDir = GlutenConfig.getTempFile
      val jarList = listJars.map(jarUrl => {
        logWarning(s"Get Codegened library Jar ${jarUrl}")
        UserAddedJarUtils.fetchJarFromSpark(
          jarUrl,
          execTempDir,
          s"spark-columnar-plugin-codegen-precompile-${signature}.jar",
          sparkConf)
        s"${execTempDir}/spark-columnar-plugin-codegen-precompile-${signature}.jar"
      })
      val fileScan = current_op.get
      val wsCxt = doWholestageTransform()

      val startTime = System.nanoTime()
      val substraitPlanPartition = fileScan.getPartitions.map(p =>
        BackendsApiManager.getIteratorApiInstance.genNativeFilePartition(p, wsCxt))
      logWarning(
        s"Generating the Substrait plan took: ${(System.nanoTime() - startTime) / 1000000} ms.")

      val wsRDD = new NativeWholeStageColumnarRDD(
        sparkContext,
        substraitPlanPartition,
        true,
        wsCxt.outputAttributes,
        jarList,
        dependentKernelIterators)
      numOutputBatches match {
        case Some(batches) =>
          wsRDD.map { iter =>
            batches += 1
            iter
          }
        case None => wsRDD
      }
    } else {
      val inputRDDs = columnarInputRDDs
      val resCtx = doWholestageTransform()
      logDebug(s"Generating substrait plan:\n${resCtx.root.toProtobuf.toString}")

      val genFinalStageIterator = (inputIterators: Seq[Iterator[ColumnarBatch]]) => {
        BackendsApiManager.getIteratorApiInstance
          .genFinalStageIterator(
            inputIterators,
            numaBindingInfo,
            listJars,
            signature,
            sparkConf,
            resCtx.outputAttributes,
            resCtx.root,
            streamedSortPlan,
            pipelineTime,
            buildRelationBatchHolder,
            dependentKernels,
            dependentKernelIterators)
      }
      new WholeStageZippedPartitionsRDD(sparkContext, inputRDDs, genFinalStageIterator)
    }
  }
  override protected def withNewChildInternal(newChild: SparkPlan): WholeStageTransformerExec =
    copy(child = newChild)(transformStageId)
}
