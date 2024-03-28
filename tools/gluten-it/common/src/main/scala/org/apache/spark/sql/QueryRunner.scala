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
package org.apache.spark.sql

import org.apache.spark.{SparkContext, Success, TaskKilled}
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorMetricsUpdate, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.KillTaskListener.INIT_WAIT_TIME_MS

import com.google.common.base.Preconditions
import org.apache.commons.lang3.RandomUtils

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

object QueryRunner {
  private val availableExecutorMetrics: Set[String] = Set(
    "JVMHeapMemory",
    "JVMOffHeapMemory",
    "OnHeapExecutionMemory",
    "OffHeapExecutionMemory",
    "OnHeapStorageMemory",
    "OffHeapStorageMemory",
    "OnHeapUnifiedMemory",
    "OffHeapUnifiedMemory",
    "DirectPoolMemory",
    "MappedPoolMemory",
    "ProcessTreeJVMVMemory",
    "ProcessTreeJVMRSSMemory",
    "ProcessTreePythonVMemory",
    "ProcessTreePythonRSSMemory",
    "ProcessTreeOtherVMemory",
    "ProcessTreeOtherRSSMemory"
  )

  def runTpcQuery(
      spark: SparkSession,
      desc: String,
      queryPath: String,
      explain: Boolean,
      metrics: Array[String],
      randomKillTasks: Boolean): RunResult = {
    val unrecognizableMetrics = metrics.filter(!availableExecutorMetrics.contains(_))
    if (unrecognizableMetrics.nonEmpty) {
      throw new IllegalArgumentException(
        "Unrecognizable metric names: " + unrecognizableMetrics.mkString("Array(", ", ", ")"))
    }
    val sc = spark.sparkContext
    sc.setJobDescription(desc)

    // metrics listener
    val em = new ExecutorMetrics()
    val metricsListener = new MetricsListener(em)
    sc.addSparkListener(metricsListener)

    // kill task listener
    val killTaskListener: Option[KillTaskListener] = if (randomKillTasks) {
      Some(new KillTaskListener(sc))
    } else {
      None
    }
    killTaskListener.foreach(sc.addSparkListener(_))

    println(s"Executing SQL query from resource path $queryPath...")
    try {
      val sql = resourceToString(queryPath)
      val prev = System.nanoTime()
      val df = spark.sql(sql)
      val rows = df.collect()
      if (explain) {
        df.explain(extended = true)
      }
      val millis = (System.nanoTime() - prev) / 1000000L
      val collectedMetrics = metrics.map(name => (name, em.getMetricValue(name))).toMap
      RunResult(rows, millis, collectedMetrics)
    } finally {
      sc.removeSparkListener(metricsListener)
      killTaskListener.foreach(
        l => {
          sc.removeSparkListener(l)
          println(s"Successful kill rate ${"%.2f%%".format(
              100 * l.successfulKillRate())} during execution of app: ${sc.applicationId}")
        })
      sc.setJobDescription(null)
    }
  }

  private def resourceToString(resource: String): String = {
    val inStream = QueryRunner.getClass.getResourceAsStream(resource)
    Preconditions.checkNotNull(inStream)
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while (reading) {
        inStream.read() match {
          case -1 => reading = false
          case c => outStream.write(c)
        }
      }
      outStream.flush()
    } finally {
      inStream.close()
    }
    new String(outStream.toByteArray, StandardCharsets.UTF_8)
  }

}

case class RunResult(rows: Seq[Row], executionTimeMillis: Long, metrics: Map[String, Long])

class MetricsListener(em: ExecutorMetrics) extends SparkListener {
  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    executorMetricsUpdate.executorUpdates.foreach {
      case (_, peakUpdates) =>
        em.compareAndUpdatePeakValues(peakUpdates)
    }
    super.onExecutorMetricsUpdate(executorMetricsUpdate)
  }
}

class KillTaskListener(val sc: SparkContext) extends SparkListener {
  private val taskCount = new AtomicInteger(0)
  private val killCount = new AtomicInteger(0)

  private val sync = new Object()
  private val stageKillWaitTimeLookup =
    new java.util.concurrent.ConcurrentHashMap[Int, Long]
  private val stageKillMaxWaitTimeLookup =
    new java.util.concurrent.ConcurrentHashMap[Int, Long]

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    taskCount.getAndIncrement()

    val killer = new Thread {
      override def run(): Unit = {

        def wait(): Long = {
          val startMs = System.currentTimeMillis()
          while (true) {
            sync.synchronized {
              val total = Math.min(
                stageKillMaxWaitTimeLookup.computeIfAbsent(taskStart.stageId, _ => Long.MaxValue),
                stageKillWaitTimeLookup.computeIfAbsent(taskStart.stageId, _ => INIT_WAIT_TIME_MS)
              )
              val elapsed = System.currentTimeMillis() - startMs
              val remaining = total - elapsed
              if (remaining <= 0L) {
                // 50ms, 100ms, 200ms, 400ms...
                stageKillWaitTimeLookup.put(taskStart.stageId, total * 2)
                sync.notifyAll()
                return elapsed
              }
              sync.wait(remaining)
            }
          }
          throw new IllegalStateException()
        }
        val elapsed = wait()

        // We have 50% chance to kill the task. FIXME make it configurable?
        if (RandomUtils.nextFloat(0.0f, 1.0f) < 0.5f) {
          if (sc.isStopped) {
            return
          }
          println(
            s"Killing task after $elapsed ms: [task ID:  ${taskStart.taskInfo.taskId}, stage ID: ${taskStart.stageId}, attempt number: ${taskStart.taskInfo.attemptNumber}]...")
          sc.killTaskAttempt(taskStart.taskInfo.taskId, interruptThread = true)
        }
      }
    }
    killer.setDaemon(true)
    killer.start()
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    taskEnd.reason match {
      case TaskKilled(_, _, _, _) =>
        killCount.getAndIncrement()
        println(
          s"Task successfully killed: ${taskEnd.taskInfo.taskId}, stage ID: ${taskEnd.stageId}, attempt number: ${taskEnd.taskInfo.attemptNumber}]")
      case Success =>
        // once one task from the stage ends, kill all the others immediately
        sync.synchronized {
          stageKillMaxWaitTimeLookup.put(
            taskEnd.stageId,
            (taskEnd.taskInfo.duration * 0.8d).asInstanceOf[Long])
          sync.notifyAll()
        }
        println(
          s"Task ended normally: ${taskEnd.taskInfo.taskId}, stage ID: ${taskEnd.stageId}, attempt number: ${taskEnd.taskInfo.attemptNumber}]")
      case _ =>
    }

  }

  def successfulKillRate(): Float = {
    killCount.get().asInstanceOf[Float] / taskCount.get()
  }
}

object KillTaskListener {
  private val INIT_WAIT_TIME_MS: Long = 50L
}
