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
package org.apache.spark.sql.execution.ui

import org.apache.gluten.events.{GlutenBuildInfoEvent, GlutenPlanFallbackEvent}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.status.{ElementTrackingStore, KVUtils}

import scala.collection.mutable

class GlutenSQLAppStatusListener(conf: SparkConf, kvstore: ElementTrackingStore)
  extends SparkListener
  with Logging {

  private val executionIdToDescription = new mutable.HashMap[Long, String]
  private val executionIdToFallbackEvent = new mutable.HashMap[Long, GlutenPlanFallbackEvent]

  kvstore.addTrigger(classOf[SQLExecutionUIData], conf.get[Int](UI_RETAINED_EXECUTIONS)) {
    count => cleanupExecutions(count)
  }

  private def onGlutenBuildInfo(event: GlutenBuildInfoEvent): Unit = {
    val uiData = new GlutenBuildInfoUIData(event.info.toSeq.sortBy(_._1))
    kvstore.write(uiData)
  }

  private def onGlutenPlanFallback(event: GlutenPlanFallbackEvent): Unit = {
    val description = executionIdToDescription.get(event.executionId)
    if (description.isDefined) {
      val uiData = new GlutenSQLExecutionUIData(
        event.executionId,
        description.get,
        event.numGlutenNodes,
        event.numFallbackNodes,
        event.physicalPlanDescription,
        event.fallbackNodeToReason.toSeq.sortBy(_._1)
      )
      kvstore.write(uiData)
    } else {
      // the first stage applies rule before post `SparkListenerSQLExecutionStart`,
      // so we should wait `SparkListenerSQLExecutionStart` then write to store.
      executionIdToFallbackEvent.put(event.executionId, event.copy())
    }
  }

  private def onSQLExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    val fallbackEvent = executionIdToFallbackEvent.get(event.executionId)
    if (fallbackEvent.isDefined) {
      val uiData = new GlutenSQLExecutionUIData(
        fallbackEvent.get.executionId,
        event.description,
        fallbackEvent.get.numGlutenNodes,
        fallbackEvent.get.numFallbackNodes,
        fallbackEvent.get.physicalPlanDescription,
        fallbackEvent.get.fallbackNodeToReason.toSeq.sortBy(_._1)
      )
      kvstore.write(uiData)
      executionIdToFallbackEvent.remove(event.executionId)
    }
    executionIdToDescription.put(event.executionId, event.description)
  }

  private def onSQLExtensionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    executionIdToDescription.remove(event.executionId)
    executionIdToFallbackEvent.remove(event.executionId)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart => onSQLExecutionStart(e)
    case e: SparkListenerSQLExecutionEnd => onSQLExtensionEnd(e)
    case e: GlutenBuildInfoEvent => onGlutenBuildInfo(e)
    case e: GlutenPlanFallbackEvent => onGlutenPlanFallback(e)
    case _ => // Ignore
  }

  private def cleanupExecutions(count: Long): Unit = {
    val countToDelete = count - conf.get(UI_RETAINED_EXECUTIONS)
    if (countToDelete <= 0) {
      return
    }

    val view = kvstore.view(classOf[GlutenSQLExecutionUIData]).first(0L)
    val toDelete = KVUtils.viewToSeq(view, countToDelete.toInt)(_ => true)
    toDelete.foreach(e => kvstore.delete(e.getClass(), e.executionId))
  }
}
