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

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.columnarbatch.ColumnarBatches
import io.glutenproject.exec.Runtimes
import io.glutenproject.extension.ValidationResult
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.memory.nmm.NativeMemoryManagers
import io.glutenproject.utils.{ArrowAbiUtil, Iterators}
import io.glutenproject.vectorized.VanillaColumnarToNativeColumnarJniWrapper

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.arrow.ArrowColumnarBatchConverter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.TaskResources

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}

case class VanillaColumnarToVeloxColumnarExec(child: SparkPlan)
  extends VanillaColumnarToNativeColumnarExecBase(child) {

  override protected def doValidateInternal(): ValidationResult = {
    BackendsApiManager.getValidatorApiInstance.doSchemaValidate(schema) match {
      case Some(reason) =>
        ValidationResult(
          isValid = false,
          Some(
            s"Input schema contains unsupported type when performing columnar" +
              s" to columnar for $schema " + s"due to $reason"))
      case None =>
        ValidationResult(isValid = true, None)
    }
  }

  override def doExecuteColumnarInternal(): RDD[ColumnarBatch] = {
    val numInputBatches = longMetric("numInputBatches")
    val numOutputBatches = longMetric("numOutputBatches")
    val convertTime = longMetric("convertTime")
    // This avoids calling `schema` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localSchema = schema
    child.executeColumnar().mapPartitions {
      rowIterator =>
        VanillaColumnarToVeloxColumnarExec.toColumnarBatchIterator(
          rowIterator,
          localSchema,
          numInputBatches,
          numOutputBatches,
          convertTime)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}

object VanillaColumnarToVeloxColumnarExec {

  def toColumnarBatchIterator(
      it: Iterator[ColumnarBatch],
      schema: StructType,
      numInputBatches: SQLMetric,
      numOutputBatches: SQLMetric,
      convertTime: SQLMetric): Iterator[ColumnarBatch] = {
    if (it.isEmpty) {
      return Iterator.empty
    }

    val arrowSchema =
      SparkArrowUtil.toArrowSchema(schema, SQLConf.get.sessionLocalTimeZone)
    val jniWrapper = VanillaColumnarToNativeColumnarJniWrapper.create()
    val allocator = ArrowBufferAllocators.contextInstance()
    val cSchema = ArrowSchema.allocateNew(allocator)
    ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
    val c2cHandle = jniWrapper.init(
      cSchema.memoryAddress(),
      NativeMemoryManagers
        .contextInstance("VanillaColumnarToNativeColumnar")
        .getNativeInstanceHandle)

    val arrowConverter = ArrowColumnarBatchConverter.create(arrowSchema, allocator)

    val res: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {

      var arrowArray: ArrowArray = null
      TaskResources.addRecycler("VanillaColumnarToNativeColumnar_arrowArray", 100) {
        releaseArrowBuf()
      }

      override def hasNext: Boolean = {
        releaseArrowBuf()
        it.hasNext
      }

      def nativeConvert(cb: ColumnarBatch): ColumnarBatch = {
        numInputBatches += 1
        arrowArray = ArrowArray.allocateNew(allocator)
        arrowConverter.write(cb)
        arrowConverter.finish()
        Data.exportVectorSchemaRoot(allocator, arrowConverter.root, null, arrowArray)
        val handle = jniWrapper.nativeConvert(c2cHandle, arrowArray.memoryAddress())
        ColumnarBatches.create(Runtimes.contextInstance(), handle)
      }

      override def next(): ColumnarBatch = {
        val currentBatch = it.next()
        val start = System.currentTimeMillis()
        val cb = nativeConvert(currentBatch)
        numOutputBatches += 1
        convertTime += System.currentTimeMillis() - start
        cb
      }

      private def releaseArrowBuf(): Unit = {
        if (arrowArray != null) {
          arrowArray.release()
          arrowArray.close()
          arrowConverter.reset()
          arrowArray = null
        }
      }
    }

    Iterators
      .wrap(res)
      .recycleIterator {
        jniWrapper.close(c2cHandle)
        arrowConverter.close()
        cSchema.close()
      }
      .recyclePayload(_.close())
      .create()
  }
}
