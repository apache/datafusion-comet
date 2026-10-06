/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import org.apache.logging.log4j.Level
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerEvent, SparkListenerExecutorMetricsUpdate, SparkListenerExecutorRemoved}
import org.apache.spark.sql.{CometTestBase, SaveMode, SparkSession}
import org.apache.spark.sql.comet.CometPlan
import org.apache.spark.sql.comet.execution.shuffle.{CometShuffleExchangeExec, CometShuffleManager}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.util.{JsonProtocol, ManualClock, Utils}

import org.apache.comet.{COMET_VERSION, CometConf, CometExecIterator, CometExecutorMemoryUsage}

class CometPluginsSuite extends CometTestBase {
  override protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.driver.memory", "1G")
    conf.set("spark.executor.memory", "1G")
    conf.set("spark.executor.memoryOverhead", "2G")
    conf.set("spark.plugins", "org.apache.spark.CometPlugin")
    conf.set(
      "spark.shuffle.manager",
      "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.comet.exec.enabled", "true")
    conf.set("spark.comet.exec.onHeap.enabled", "true")
    conf.set("spark.comet.metrics.enabled", "true")
    conf
  }

  test("Register Comet extension") {
    // test common case where no extensions are previously registered
    {
      val conf = new SparkConf()
      CometDriverPlugin.registerCometSessionExtension(conf)
      assert(
        "org.apache.comet.CometSparkSessionExtensions" == conf.get(
          StaticSQLConf.SPARK_SESSION_EXTENSIONS.key))
    }
    // test case where Comet is already registered
    {
      val conf = new SparkConf()
      conf.set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        "org.apache.comet.CometSparkSessionExtensions")
      CometDriverPlugin.registerCometSessionExtension(conf)
      assert(
        "org.apache.comet.CometSparkSessionExtensions" == conf.get(
          StaticSQLConf.SPARK_SESSION_EXTENSIONS.key))
    }
    // test case where other extensions are already registered
    {
      val conf = new SparkConf()
      conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, "foo,bar")
      CometDriverPlugin.registerCometSessionExtension(conf)
      assert(
        "foo,bar,org.apache.comet.CometSparkSessionExtensions" == conf.get(
          StaticSQLConf.SPARK_SESSION_EXTENSIONS.key))
    }
    // test case where other extensions, including Comet, are already registered
    {
      val conf = new SparkConf()
      conf.set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        "foo,bar,org.apache.comet.CometSparkSessionExtensions")
      CometDriverPlugin.registerCometSessionExtension(conf)
      assert(
        "foo,bar,org.apache.comet.CometSparkSessionExtensions" == conf.get(
          StaticSQLConf.SPARK_SESSION_EXTENSIONS.key))
    }
  }

  test("Iceberg write report listener is registered only when a report directory is set") {
    val listenerKey = "spark.sql.queryExecutionListeners"
    val listenerClass = "org.apache.comet.iceberg.IcebergWriteReportListener"

    val unset = new SparkConf()
    CometDriverPlugin.registerIcebergWriteReport(unset)
    assert(!unset.contains(listenerKey))

    val set = new SparkConf()
      .set(CometConf.COMET_ICEBERG_WRITE_REPORT_DIR.key, "/tmp/report")
      .set(listenerKey, "foo")
    CometDriverPlugin.registerIcebergWriteReport(set)
    CometDriverPlugin.registerIcebergWriteReport(set)
    assert(set.get(listenerKey) == s"foo,$listenerClass")
  }

  test("Comet version is exposed as a Spark config") {
    // The driver plugin sets spark.comet.version, which is then visible both on the SparkContext
    // conf and through the session runtime config (SET / spark.conf.get).
    assert(spark.sparkContext.conf.get(CometDriverPlugin.COMET_VERSION_CONFIG) == COMET_VERSION)
    assert(spark.conf.get(CometDriverPlugin.COMET_VERSION_CONFIG) == COMET_VERSION)
  }

  test("CometSource metrics are recorded") {
    val nativeBefore = CometSource.NATIVE_OPERATORS.getCount
    val queriesBefore = CometSource.QUERIES_PLANNED.getCount

    withTempPath { dir =>
      val path = new File(dir, "test.parquet").toString
      spark.range(1000).toDF("id").write.mode(SaveMode.Overwrite).parquet(path)
      spark.read.parquet(path).filter("id > 500").collect()
    }
    spark.sparkContext.listenerBus.waitUntilEmpty()
    assert(
      CometSource.QUERIES_PLANNED.getCount > queriesBefore,
      "queries.planned should increment after query")
    assert(
      CometSource.NATIVE_OPERATORS.getCount > nativeBefore,
      "operators.native should increment for native execution")
  }

  test("metrics not double counted with AQE") {
    withSQLConf("spark.sql.adaptive.enabled" -> "true") {
      withTempPath { dir =>
        val path = new File(dir, "test.parquet").toString
        spark.range(10000).toDF("id").write.mode(SaveMode.Overwrite).parquet(path)

        spark.sparkContext.listenerBus.waitUntilEmpty()
        val queriesBefore = CometSource.QUERIES_PLANNED.getCount
        spark.read.parquet(path).filter("id > 100").collect()
        spark.read.parquet(path).filter("id > 200").collect()
        spark.sparkContext.listenerBus.waitUntilEmpty()
        val queriesAfter = CometSource.QUERIES_PLANNED.getCount
        assert(
          queriesAfter == queriesBefore + 2,
          s"Expected 2 queries, got ${queriesAfter - queriesBefore}")
      }
    }
  }

  test("executor memory overhead is left alone") {
    // Comet does not adjust spark.executor.memoryOverhead. A driver plugin runs too late to
    // influence the executor container on Spark 3.4, 3.5 and 4.0, so the value the application
    // set is the value that is used.
    val execMemOverhead1 = spark.conf.get("spark.executor.memoryOverhead")
    val execMemOverhead2 = spark.sessionState.conf.getConfString("spark.executor.memoryOverhead")
    val execMemOverhead3 = spark.sparkContext.getConf.get("spark.executor.memoryOverhead")
    val execMemOverhead4 = spark.sparkContext.conf.get("spark.executor.memoryOverhead")

    assert(execMemOverhead1 == "2G")
    assert(execMemOverhead2 == "2G")
    assert(execMemOverhead3 == "2G")
    assert(execMemOverhead4 == "2G")
  }

  test("the memory usage log sends the driver nothing without an event log") {
    // The suite runs the Comet plugin but writes no event log.
    assert(CometExecutorPlugin.eventLogContext.isEmpty)
  }
}

class CometPluginsDefaultSuite extends CometTestBase {
  override protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.driver.memory", "1G")
    conf.set("spark.executor.memory", "1G")
    conf.set("spark.executor.memoryOverheadFactor", "0.5")
    conf.set("spark.plugins", "org.apache.spark.CometPlugin")
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.comet.shuffle.enabled", "true")
    conf.set("spark.comet.exec.onHeap.enabled", "true")
    conf
  }

  test("unset executor memory overhead is left unset") {
    assert(!spark.sparkContext.conf.contains("spark.executor.memoryOverhead"))
  }
}

class CometPluginsMemoryOverheadWarningSuite extends CometTestBase {

  private val warning =
    "Neither spark.executor.memoryOverhead nor spark.executor.memoryOverheadFactor is set"

  private def warningsFor(conf: SparkConf): Seq[String] = {
    // Logging derives the logger name by stripping the object's trailing '$'
    val logger = CometDriverPlugin.getClass.getName.stripSuffix("$")
    val appender = new LogAppender("executor memory overhead warning")
    withLogAppender(appender, Seq(logger), Some(Level.WARN)) {
      CometDriverPlugin.warnIfExecutorMemoryOverheadUnset(conf)
    }
    appender.loggingEvents.map(_.getMessage.getFormattedMessage).toSeq
  }

  private val kubernetesMaster = "k8s://https://kubernetes.default.svc:443"

  private def cometConf(master: String = "yarn"): SparkConf =
    new SparkConf()
      .set("spark.master", master)
      .set("spark.comet.enabled", "true")
      .set("spark.comet.exec.enabled", "true")

  test("warns when executor memory overhead is unset and Comet is active") {
    assert(warningsFor(cometConf()).exists(_.contains(warning)))
  }

  test("does not warn when executor memory overhead is set") {
    val conf = cometConf().set("spark.executor.memoryOverhead", "2g")
    assert(!warningsFor(conf).exists(_.contains(warning)))
  }

  test("does not warn when the executor memory overhead factor is set") {
    val conf = cometConf().set("spark.executor.memoryOverheadFactor", "0.2")
    assert(!warningsFor(conf).exists(_.contains(warning)))
  }

  test("does not warn when the Kubernetes memory overhead factor is set") {
    // A factor other than the one spark-submit would have passed on, whatever the application type
    Seq(
      Map("spark.kubernetes.memoryOverheadFactor" -> "0.3"),
      Map(
        "spark.kubernetes.resource.type" -> "java",
        "spark.kubernetes.memoryOverheadFactor" -> "0.4"),
      Map(
        "spark.kubernetes.resource.type" -> "python",
        "spark.kubernetes.memoryOverheadFactor" -> "0.5")).foreach { settings =>
      val conf = cometConf(kubernetesMaster).setAll(settings)
      assert(!warningsFor(conf).exists(_.contains(warning)), settings)
    }
  }

  test("warns on Kubernetes when the memory overhead factor is the one spark-submit passed on") {
    // In cluster mode spark-submit sets spark.kubernetes.memoryOverheadFactor for the driver even
    // when the application did not: 0.4 for PySpark and SparkR applications, 0.1 for the rest
    Seq("java" -> "0.1", "python" -> "0.4", "r" -> "0.4").foreach { case (resourceType, factor) =>
      val conf = cometConf(kubernetesMaster)
        .set("spark.kubernetes.resource.type", resourceType)
        .set("spark.kubernetes.memoryOverheadFactor", factor)
      assert(warningsFor(conf).exists(_.contains(warning)), resourceType)
    }
  }

  test("does not fail on a Kubernetes memory overhead factor that does not parse") {
    // Spark rejects the value itself when it sizes the executor pods
    val conf = cometConf(kubernetesMaster).set("spark.kubernetes.memoryOverheadFactor", "lots")
    assert(!warningsFor(conf).exists(_.contains(warning)))
  }

  test("does not warn in local mode or on a standalone cluster") {
    Seq("local", "local[4]", "local-cluster[2,1,1024]", "spark://host:7077").foreach { master =>
      assert(!warningsFor(cometConf(master)).exists(_.contains(warning)), master)
    }
  }

  test("does not warn when Comet is not executing anything") {
    val conf = cometConf()
      .set("spark.comet.exec.enabled", "false")
      .set("spark.comet.shuffle.enabled", "false")
    assert(!warningsFor(conf).exists(_.contains(warning)))
  }
}

class CometPluginsMemoryPoolFractionWarningSuite extends CometTestBase {

  private val warning = "spark.comet.exec.memoryPool.fraction=0.8 is deprecated"

  private def warningsFor(conf: SparkConf): Seq[String] = {
    // Logging derives the logger name by stripping the object's trailing '$'
    val logger = CometDriverPlugin.getClass.getName.stripSuffix("$")
    val appender = new LogAppender("memory pool fraction warning")
    withLogAppender(appender, Seq(logger), Some(Level.WARN)) {
      CometDriverPlugin.warnIfMemoryPoolFractionSet(conf)
    }
    appender.loggingEvents.map(_.getMessage.getFormattedMessage).toSeq
  }

  test("warns when the memory pool fraction is set") {
    val conf = new SparkConf().set("spark.comet.exec.memoryPool.fraction", "0.8")
    assert(warningsFor(conf).exists(_.contains(warning)))
  }

  test("does not warn when the memory pool fraction is unset") {
    assert(!warningsFor(new SparkConf()).exists(_.contains("memoryPool.fraction")))
  }
}

class CometPluginsUnifiedModeSuite extends CometTestBase {
  override protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.driver.memory", "1G")
    conf.set("spark.executor.memory", "1G")
    conf.set("spark.executor.memoryOverhead", "1G")
    conf.set("spark.plugins", "org.apache.spark.CometPlugin")
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.memory.offHeap.enabled", "true")
    conf.set("spark.memory.offHeap.size", "2G")
    conf.set("spark.comet.shuffle.enabled", "true")
    conf.set("spark.comet.exec.enabled", "true")
    conf
  }

  test("executor memory overhead is left alone in off-heap mode") {
    val execMemOverhead1 = spark.conf.get("spark.executor.memoryOverhead")
    val execMemOverhead2 = spark.sessionState.conf.getConfString("spark.executor.memoryOverhead")
    val execMemOverhead3 = spark.sparkContext.getConf.get("spark.executor.memoryOverhead")
    val execMemOverhead4 = spark.sparkContext.conf.get("spark.executor.memoryOverhead")

    assert(execMemOverhead1 == "1G")
    assert(execMemOverhead2 == "1G")
    assert(execMemOverhead3 == "1G")
    assert(execMemOverhead4 == "1G")
  }
}

class CometPluginsExtensionOnlySuite extends CometTestBase {
  // No plugin and no off-heap memory. CometTestBase registers CometSparkSessionExtensions
  // directly, as an application can with spark.sql.extensions.
  override protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set(
      "spark.shuffle.manager",
      "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.comet.exec.enabled", "true")
    // Set explicitly, since ENABLE_COMET_ONHEAP in the environment changes the default.
    conf.set(CometConf.COMET_ONHEAP_ENABLED.key, "false")
    conf
  }

  private val query = "SELECT _1 FROM tbl WHERE _1 > 5"

  test("Comet is disabled when off-heap memory is disabled") {
    // Listen on the package logger, as CometExecRuleSuite does. For a logger with no config of its
    // own, withLogAppender creates one that outlives the test and does not pass events up, so
    // listening on CometSparkSessionExtensions directly would hide its warnings from later
    // appenders on org.apache.comet.
    val appender = new LogAppender("off-heap mode warning")
    withParquetTable((0 until 10).map(i => (i, i.toString)), "tbl") {
      withLogAppender(appender, Seq("org.apache.comet"), Some(Level.WARN)) {
        val (_, plan) = checkSparkAnswer(query)
        assert(collect(plan) { case op: CometPlan => op }.isEmpty, plan)
      }
    }
    assert(
      appender.loggingEvents
        .exists(_.getMessage.getFormattedMessage.contains("not running in off-heap mode")))
  }

  test("Comet stays disabled when only the session conf enables off-heap memory") {
    withParquetTable((0 until 10).map(i => (i, i.toString)), "tbl") {
      // The session already exists, so the builder only copies the setting into its SQLConf.
      // The SparkContext, whose conf the executors use, stays on-heap.
      val session =
        SparkSession.builder().config("spark.memory.offHeap.enabled", "true").getOrCreate()
      try {
        assert(session eq spark)
        assert(spark.sessionState.conf.getConfString("spark.memory.offHeap.enabled") == "true")
        val (_, plan) = checkSparkAnswer(query)
        assert(collect(plan) { case op: CometPlan => op }.isEmpty, plan)
      } finally {
        spark.sessionState.conf.unsetConf("spark.memory.offHeap.enabled")
      }
    }
  }

  test("spark.comet.exec.onHeap.enabled enables Comet without off-heap memory") {
    withParquetTable((0 until 10).map(i => (i, i.toString)), "tbl") {
      withSQLConf(CometConf.COMET_ONHEAP_ENABLED.key -> "true") {
        val (_, plan) = checkSparkAnswer(query)
        assert(collect(plan) { case op: CometPlan => op }.nonEmpty, plan)
      }
    }
  }
}

class CometPluginsSparkShuffleManagerSuite extends CometTestBase {
  // The application runs Spark's own shuffle manager.
  override protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.memory.offHeap.enabled", "true")
    conf.set("spark.memory.offHeap.size", "2g")
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.comet.exec.enabled", "true")
    conf
  }

  private val query = "SELECT _2, count(*) FROM tbl GROUP BY _2"

  test("Comet stays disabled when only the session conf names the Comet shuffle manager") {
    withParquetTable((0 until 100).map(i => (i, (i % 7).toString)), "tbl") {
      // The session already exists, so the builder only copies the setting into its SQLConf.
      // Spark's shuffle manager still runs the shuffle, and it cannot read a Comet shuffle.
      val manager = classOf[CometShuffleManager].getName
      val session = SparkSession.builder().config("spark.shuffle.manager", manager).getOrCreate()
      try {
        assert(session eq spark)
        assert(spark.sessionState.conf.getConfString("spark.shuffle.manager") == manager)
        val (_, plan) = checkSparkAnswer(query)
        assert(collect(plan) { case op: CometPlan => op }.isEmpty, plan)
      } finally {
        spark.sessionState.conf.unsetConf("spark.shuffle.manager")
      }
    }
  }

  test("Comet runs with Spark's shuffle when Comet shuffle is disabled") {
    withParquetTable((0 until 100).map(i => (i, (i % 7).toString)), "tbl") {
      withSQLConf(CometConf.COMET_SHUFFLE_ENABLED.key -> "false") {
        val (_, plan) = checkSparkAnswer(query)
        assert(collect(plan) { case op: CometPlan => op }.nonEmpty, plan)
        assert(collect(plan) { case op: CometShuffleExchangeExec => op }.isEmpty, plan)
      }
    }
  }
}

class CometPluginsEventLogSuite extends SparkFunSuite {

  import CometExecIterator.{memoryUsageEvent, JvmArrowMemory}

  /**
   * Runs `f` in an application that runs `plugin` and writes an event log, passing it the context
   * through which the executor sends memory usage samples, and returns the event log's events
   * once the application has stopped.
   */
  private def eventLog(plugin: Class[_ <: SparkPlugin] = classOf[CometPlugin])(
      f: (SparkContext, PluginContext) => Unit): Seq[SparkListenerEvent] = {
    val eventLogDir = Utils.createTempDir()
    try {
      val sc = new SparkContext(
        new SparkConf()
          .setMaster("local[1]")
          .setAppName(getClass.getSimpleName)
          .set("spark.plugins", plugin.getName)
          .set("spark.eventLog.enabled", "true")
          .set("spark.eventLog.dir", eventLogDir.toURI.toString)
          // One plain file, which the test reads directly. Spark 4 compresses and rolls it by
          // default.
          .set("spark.eventLog.compress", "false")
          .set("spark.eventLog.rolling.enabled", "false"))
      try {
        val pluginContext = CometExecutorPlugin.eventLogContext
        assert(pluginContext.isDefined, "The executor should send samples to the event log")
        f(sc, pluginContext.get)
      } finally {
        sc.stop()
      }
      eventLogDir
        .listFiles()
        .toSeq
        .filter(file => file.isFile && !file.getName.startsWith("."))
        .flatMap(file =>
          new String(Files.readAllBytes(file.toPath), StandardCharsets.UTF_8)
            .split("\n")
            .map(JsonProtocol.sparkEventFromJson))
    } finally {
      Utils.deleteRecursively(eventLogDir)
    }
  }

  private val marker =
    memoryUsageEvent("marker", 0L, Array(0L, 0L, 0L, 0L), JvmArrowMemory(0L, 0L))

  /**
   * Posts `event`, and a marker once every listener has handled it. The bus hands an event to one
   * queue at a time, so what the driver plugin's listener records on `event` can reach the event
   * log ahead of `event` itself. It always reaches it ahead of the marker.
   */
  private def postAndMark(sc: SparkContext, event: SparkListenerEvent): Unit = {
    sc.listenerBus.post(event)
    sc.listenerBus.waitUntilEmpty()
    sc.listenerBus.post(marker)
  }

  /**
   * Asserts that the event log records exactly one sample `isSample` accepts, ahead of the marker
   * and so not at the application's end.
   */
  private def assertRecordedOnceBeforeMarker(events: Seq[SparkListenerEvent])(
      isSample: SparkListenerEvent => Boolean): Unit = {
    val recorded = events.indices.filter(i => isSample(events(i)))
    assert(recorded.size == 1 && recorded(0) < events.indexOf(marker), events)
  }

  test("the driver records what the memory usage log sent it when the application ends") {
    // An allocation no real sample has, to tell this sample apart from any that the memory usage
    // log running in this JVM sends, and large enough to be the most untracked memory of its
    // summary.
    val usage = Array(123456789L * 1024, 0L, 1L, 1L)
    val jvmArrow = JvmArrowMemory(allocated = 3456789L, imported = 456789L)
    val events = eventLog() { (sc, pluginContext) =>
      CometExecIterator.sendToEventLog(usage, jvmArrow)
      // The driver plugin receives its messages in order, and `ask`, unlike the log's one-way
      // `send`, returns once it has received this one, so it has the sample above by then. It
      // replies to none of them.
      assert(
        pluginContext.ask(
          memoryUsageEvent("barrier", 0L, Array(0L, 0L, 0L, 0L), jvmArrow)) == null)
      // The end as the listener sees it, which alone records what the driver holds on Spark 3.4
      // and 3.5. From Spark 4.0 the plugin's shutdown would record it too, when the context stops.
      postAndMark(sc, SparkListenerApplicationEnd(System.currentTimeMillis()))
    }
    assertRecordedOnceBeforeMarker(events) {
      // Local mode runs the executor inside the driver.
      case sample: CometExecutorMemoryUsage =>
        sample == memoryUsageEvent(SparkContext.DRIVER_IDENTIFIER, sample.time, usage, jvmArrow)
      case _ => false
    }
  }

  test("the driver records what an executor sent since its last summary when it goes away") {
    val sample = memoryUsageEvent(
      "lost",
      System.currentTimeMillis(),
      Array(300L * 1024 * 1024, 100L * 1024 * 1024, 2L, 3L),
      JvmArrowMemory(0L, 0L))
    val events = eventLog() { (sc, pluginContext) =>
      pluginContext.ask(sample)
      postAndMark(sc, SparkListenerExecutorRemoved(sample.time, sample.executorId, "test"))
    }
    assertRecordedOnceBeforeMarker(events)(_ == sample)
  }

  test("the driver records an idle executor's samples at its first heartbeat a minute later") {
    val sample = memoryUsageEvent(
      "idle",
      System.currentTimeMillis(),
      Array(300L * 1024 * 1024, 100L * 1024 * 1024, 2L, 0L),
      JvmArrowMemory(0L, 0L))
    val events = eventLog(classOf[ManualClockCometPlugin]) { (sc, pluginContext) =>
      pluginContext.ask(sample)
      ManualClockCometPlugin.clock.advance(TimeUnit.MINUTES.toMillis(1))
      postAndMark(sc, SparkListenerExecutorMetricsUpdate(sample.executorId, Seq.empty))
    }
    assertRecordedOnceBeforeMarker(events)(_ == sample)
  }
}

/** The Comet plugin with a driver clock that `CometPluginsEventLogSuite` moves by hand. */
class ManualClockCometPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new CometDriverPlugin(ManualClockCometPlugin.clock)
  override def executorPlugin(): ExecutorPlugin = new CometExecutorPlugin
}

object ManualClockCometPlugin {
  val clock = new ManualClock()
}
