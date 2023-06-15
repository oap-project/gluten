package io.glutenproject.integration.tpc

import io.glutenproject.integration.tpc.action.Action

import java.io.File
import java.util.Scanner
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.HistoryServerHelper
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.SparkSessionSwitcher
import org.apache.spark.sql.ConfUtils.ConfImplicits._

abstract class TpcSuite(
  private val masterUrl: String,
  private val actions: Array[Action],
  private val testConf: SparkConf,
  private val baselineConf: SparkConf,
  private val extraSparkConf: Map[String, String],
  private val logLevel: Level,
  private val errorOnMemLeak: Boolean,
  private val enableUi: Boolean,
  private val enableHsUi: Boolean,
  private val hsUiPort: Int,
  private val offHeapSize: String,
  private val disableAqe: Boolean,
  private val disableBhj: Boolean,
  private val disableWscg: Boolean,
  private val shufflePartitions: Int,
  private val minimumScanPartitions: Boolean) {

  resetLogLevel()

  private[tpc] val sessionSwitcher: SparkSessionSwitcher = new SparkSessionSwitcher(masterUrl, logLevel.toString)

  // define initial configs
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.sql.sources.useV1SourceList", "")
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.sql.shuffle.partitions", s"$shufflePartitions")
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.executor.heartbeatInterval", "1s") // for keeping metrics updated
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.executor.metrics.pollingInterval", "0")
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.network.timeout", "3601s")
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.sql.broadcastTimeout", "1800")
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.network.io.preferDirectBufs", "false")
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.unsafe.exceptionOnMemoryLeak", s"$errorOnMemLeak")
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.memory.offHeap.enabled", "true")
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.memory.offHeap.size", offHeapSize)

  if (!enableUi) {
    sessionSwitcher.defaultConf().setWarningOnOverriding("spark.ui.enabled", "false")
  }

  if (enableHsUi) {
    if (!new File(historyWritePath()).exists() && !new File(historyWritePath()).mkdirs()) {
      throw new RuntimeException("Unable to create history directory: " +
        historyWritePath())
    }
    sessionSwitcher.defaultConf().setWarningOnOverriding("spark.eventLog.enabled", "true")
    sessionSwitcher.defaultConf().setWarningOnOverriding("spark.eventLog.dir", historyWritePath())
  }

  if (disableAqe) {
    sessionSwitcher.defaultConf().setWarningOnOverriding("spark.sql.adaptive.enabled", "false")
  }

  if (disableBhj) {
    sessionSwitcher.defaultConf().setWarningOnOverriding("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  if (disableWscg) {
    sessionSwitcher.defaultConf().setWarningOnOverriding("spark.sql.codegen.wholeStage", "false")
  }

  if (minimumScanPartitions) {
    sessionSwitcher.defaultConf().setWarningOnOverriding("spark.sql.files.maxPartitionBytes", s"${ByteUnit.PiB.toBytes(1L)}")
    sessionSwitcher.defaultConf().setWarningOnOverriding("spark.sql.files.openCostInBytes", s"${ByteUnit.PiB.toBytes(1L)}")
    sessionSwitcher.defaultConf().setWarningOnOverriding("spark.default.parallelism", "1")
  }

  extraSparkConf.toStream.foreach { kv =>
    sessionSwitcher.defaultConf().setWarningOnOverriding(kv._1, kv._2)
  }

  // register sessions
  sessionSwitcher.registerSession("test", testConf)
  sessionSwitcher.registerSession("baseline", baselineConf)

  def startHistoryServer(): Unit = {
    val conf = new SparkConf(false)
    conf.setWarningOnOverriding("spark.history.ui.port", s"$hsUiPort")
    conf.setWarningOnOverriding("spark.history.fs.logDirectory", historyWritePath())
    HistoryServerHelper.startHistoryServer(conf)
  }

  // boot up history server
  if (enableHsUi) {
    startHistoryServer()
  }

  def run(): Boolean = {
    val succeed = actions.forall { action =>
      resetLogLevel() // to prevent log level from being set by unknown external codes
      action.execute(this)
    }
    succeed
  }

  def close(): Unit = {
    sessionSwitcher.close()
    // wait for input, if history server was started
    if (enableHsUi) {
      printf("History server was running at port %d. Press enter to exit... \n", hsUiPort)
      print("> ")
      new Scanner(System.in).nextLine
    }
  }

  private def resetLogLevel(): Unit = {
    LogManager.getRootLogger.setLevel(logLevel)
  }

  private[tpc] def getBaselineConf(): SparkConf = {
    baselineConf.clone()
  }

  private[tpc] def getTestConf(): SparkConf = {
    testConf.clone()
  }

  protected def historyWritePath(): String

  private[tpc] def dataWritePath(scale: Double): String

  private[tpc] def createDataGen(scale: Double, genPartitionedData: Boolean): DataGen

  private[tpc] def queryResource(): String

  protected def typeModifiers(): List[TypeModifier]

  private[tpc] def allQueryIds(): Array[String]

  private[tpc] def desc(): String

}

