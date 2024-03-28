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
package org.apache.gluten.execution

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.ExpressionConverter
import org.apache.gluten.extension.columnar.TransformHints
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}

import java.util.ServiceLoader
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

object ScanTransformerFactory {

  private val scanTransformerMap = new ConcurrentHashMap[String, Class[_]]()

  def createFileSourceScanTransformer(
      scanExec: FileSourceScanExec,
      allPushDownFilters: Option[Seq[Expression]] = None,
      validation: Boolean = false): FileSourceScanExecTransformerBase = {
    // transform BroadcastExchangeExec to ColumnarBroadcastExchangeExec in partitionFilters
    val newPartitionFilters = if (validation) {
      scanExec.partitionFilters
    } else {
      ExpressionConverter.transformDynamicPruningExpr(scanExec.partitionFilters)
    }
    val fileFormat = scanExec.relation.fileFormat
    lookupDataSourceScanTransformer(fileFormat.getClass.getName) match {
      case Some(clz) =>
        clz
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[DataSourceScanTransformerRegister]
          .createDataSourceTransformer(scanExec, newPartitionFilters)
      case _ =>
        new FileSourceScanExecTransformer(
          scanExec.relation,
          scanExec.output,
          scanExec.requiredSchema,
          newPartitionFilters,
          scanExec.optionalBucketSet,
          scanExec.optionalNumCoalescedBuckets,
          allPushDownFilters.getOrElse(scanExec.dataFilters),
          scanExec.tableIdentifier,
          scanExec.disableBucketedScan
        )
    }
  }

  private def lookupBatchScanTransformer(
      batchScanExec: BatchScanExec,
      newPartitionFilters: Seq[Expression]): BatchScanExecTransformerBase = {
    val scan = batchScanExec.scan
    lookupDataSourceScanTransformer(scan.getClass.getName) match {
      case Some(clz) =>
        clz
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[DataSourceScanTransformerRegister]
          .createDataSourceV2Transformer(batchScanExec, newPartitionFilters)
      case _ =>
        scan match {
          case _: FileScan =>
            new BatchScanExecTransformer(
              batchScanExec.output,
              batchScanExec.scan,
              newPartitionFilters,
              table = SparkShimLoader.getSparkShims.getBatchScanExecTable(batchScanExec)
            )
          case _ =>
            throw new GlutenNotSupportException(s"Unsupported scan $scan")
        }
    }
  }

  def createBatchScanTransformer(
      batchScan: BatchScanExec,
      allPushDownFilters: Option[Seq[Expression]] = None,
      validation: Boolean = false): SparkPlan = {
    if (supportedBatchScan(batchScan.scan)) {
      val newPartitionFilters = if (validation) {
        // No transformation is needed for DynamicPruningExpressions
        // during the validation process.
        batchScan.runtimeFilters
      } else {
        ExpressionConverter.transformDynamicPruningExpr(batchScan.runtimeFilters)
      }
      val transformer = lookupBatchScanTransformer(batchScan, newPartitionFilters)
      if (!validation && allPushDownFilters.isDefined) {
        transformer.setPushDownFilters(allPushDownFilters.get)
        // Validate again if allPushDownFilters is defined.
        val validationResult = transformer.doValidate()
        if (validationResult.isValid) {
          transformer
        } else {
          val newSource = batchScan.copy(runtimeFilters = transformer.runtimeFilters)
          TransformHints.tagNotTransformable(newSource, validationResult.reason.get)
          newSource
        }
      } else {
        transformer
      }
    } else {
      if (validation) {
        throw new GlutenNotSupportException(s"Unsupported scan ${batchScan.scan}")
      }
      // If filter expressions aren't empty, we need to transform the inner operators,
      // and fallback the BatchScanExec itself.
      val newSource = batchScan.copy(runtimeFilters = ExpressionConverter
        .transformDynamicPruningExpr(batchScan.runtimeFilters))
      TransformHints.tagNotTransformable(newSource, "The scan in BatchScanExec is not supported.")
      newSource
    }
  }

  private def supportedBatchScan(scan: Scan): Boolean = scan match {
    case _: FileScan => true
    case _ => lookupDataSourceScanTransformer(scan.getClass.getName).nonEmpty
  }

  private def lookupDataSourceScanTransformer(scanClassName: String): Option[Class[_]] = {
    val clz = scanTransformerMap.computeIfAbsent(
      scanClassName,
      _ => {
        val loader = Option(Thread.currentThread().getContextClassLoader)
          .getOrElse(getClass.getClassLoader)
        val serviceLoader = ServiceLoader.load(classOf[DataSourceScanTransformerRegister], loader)
        serviceLoader.asScala
          .filter(_.scanClassName.equalsIgnoreCase(scanClassName))
          .toList match {
          case head :: Nil =>
            // there is exactly one registered alias
            head.getClass
          case _ => null
        }
      }
    )
    Option(clz)
  }

}
