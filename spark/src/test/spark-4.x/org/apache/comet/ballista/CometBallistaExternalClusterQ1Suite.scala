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

package org.apache.comet.ballista

import java.io.File
import java.math.{BigDecimal => JBigDecimal}
import java.net.{InetSocketAddress, Socket}
import java.nio.file.Files
import java.sql.Date
import java.util.concurrent.atomic.AtomicInteger

import scala.io.Source

import org.apache.spark.CometListenerBusUtils
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart}
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.comet.CometNativeExec
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf

/**
 * The R1b/R2 external-cluster milestone from the FULL Spark-driver side: a live Spark driver
 * offloads TPC-H Q1's aggregate to a GENUINELY external, separately-spawned Ballista cluster
 * (`comet-scheduler` + `comet-executor` child processes) and gets results identical to Spark,
 * with ZERO Spark-executor tasks.
 *
 * How this differs from [[CometBallistaQ1Suite]] / [[CometBallistaDistributedSuite]]: those run
 * the same two-block Q1 aggregate against an IN-PROCESS standalone Ballista engine on the driver
 * (`scheduler.url` empty). Here `spark.comet.exec.ballista.scheduler.url` points at a real
 * external scheduler process, so the plan is shipped over gRPC/Flight to a SEPARATE, JVM-less
 * executor process, reconstructed there via the injected Comet codecs, and run. This is the first
 * time the full Q1 aggregate fragment (partial-agg NativeScan leaf -> hash shuffle -> final-agg
 * over a Scan leaf) runs on a separate executor PROCESS rather than in the test's own process.
 *
 * The `comet-scheduler` / `comet-executor` binaries and the `libcomet` this JVM loads must all be
 * built with `--features ballista` (`make core-ballista`), so the offload + URL path and the
 * Comet-flavored codecs exist. When the loaded `libcomet` lacks the feature, the suite
 * `assume`-skips (same guard as the other offload suites). When the feature binaries are missing
 * on disk, the suite fails with a build hint rather than silently passing.
 *
 * Spawns child processes and binds ports, so it mirrors the Rust harness
 * (`native/core/tests/ballista_external_cluster.rs`): libjvm on the loader path, wait for ports,
 * a short registration grace, and a teardown that kills the children even on failure.
 */
class CometBallistaExternalClusterQ1Suite extends CometTestBase with AdaptiveSparkPlanHelper {

  // Non-default ports so this suite does not collide with a real cluster on the usual
  // 50050/50051/50052, nor with the Rust harness on 51050-51052.
  private val schedulerPort = 51150
  private val executorFlightPort = 51151
  private val executorGrpcPort = 51152

  private var scheduler: Process = _
  private var executor: Process = _
  private var logDir: File = _
  private var schedulerLog: File = _
  private var executorLog: File = _

  /**
   * TPC-H `lineitem`, restricted to the columns Q1 touches, with the correct Spark types
   * (`decimal(12,2)` and a real `date`). Same fixture as [[CometBallistaQ1Suite]].
   */
  private val lineitemSchema: StructType = StructType(
    Seq(
      StructField("l_quantity", DecimalType(12, 2), nullable = false),
      StructField("l_extendedprice", DecimalType(12, 2), nullable = false),
      StructField("l_discount", DecimalType(12, 2), nullable = false),
      StructField("l_tax", DecimalType(12, 2), nullable = false),
      StructField("l_returnflag", StringType, nullable = false),
      StructField("l_linestatus", StringType, nullable = false),
      StructField("l_shipdate", DateType, nullable = false)))

  private def dec(v: String): JBigDecimal = new JBigDecimal(v).setScale(2)

  /** Same synthetic `lineitem` rows as [[CometBallistaQ1Suite]] (three surviving Q1 groups). */
  private def lineitemRows: Seq[Row] = Seq(
    Row(
      dec("17.00"),
      dec("21168.23"),
      dec("0.04"),
      dec("0.02"),
      "A",
      "F",
      Date.valueOf("1998-08-01")),
    Row(
      dec("36.00"),
      dec("45983.16"),
      dec("0.09"),
      dec("0.06"),
      "A",
      "F",
      Date.valueOf("1998-07-15")),
    Row(
      dec("8.00"),
      dec("13309.60"),
      dec("0.10"),
      dec("0.02"),
      "A",
      "F",
      Date.valueOf("1998-09-01")),
    Row(
      dec("28.00"),
      dec("28955.64"),
      dec("0.05"),
      dec("0.08"),
      "N",
      "O",
      Date.valueOf("1998-06-10")),
    Row(
      dec("24.00"),
      dec("32000.00"),
      dec("0.00"),
      dec("0.00"),
      "N",
      "O",
      Date.valueOf("1998-08-20")),
    Row(
      dec("2.00"),
      dec("2600.00"),
      dec("0.06"),
      dec("0.03"),
      "N",
      "O",
      Date.valueOf("1998-09-02")),
    Row(
      dec("32.00"),
      dec("41000.50"),
      dec("0.07"),
      dec("0.05"),
      "R",
      "F",
      Date.valueOf("1998-05-05")),
    Row(
      dec("45.00"),
      dec("60000.00"),
      dec("0.02"),
      dec("0.01"),
      "R",
      "F",
      Date.valueOf("1998-08-31")),
    // rows PAST the Q1 cutoff -- must be filtered out
    Row(
      dec("50.00"),
      dec("70000.00"),
      dec("0.03"),
      dec("0.04"),
      "N",
      "F",
      Date.valueOf("1998-09-03")),
    Row(
      dec("99.00"),
      dec("99999.99"),
      dec("0.05"),
      dec("0.05"),
      "N",
      "F",
      Date.valueOf("1998-12-01")))

  /**
   * TPC-H Q1's full aggregate (NO `ORDER BY`): `sum`x4, `avg`x3, `count`, grouped by the two keys
   * `(l_returnflag, l_linestatus)`. Same query as [[CometBallistaQ1Suite]]'s R2 test.
   */
  private val q1FullAggregate =
    """
      |SELECT l_returnflag, l_linestatus,
      |  sum(l_quantity) AS sum_qty,
      |  sum(l_extendedprice) AS sum_base_price,
      |  sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
      |  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
      |  avg(l_quantity) AS avg_qty,
      |  avg(l_extendedprice) AS avg_price,
      |  avg(l_discount) AS avg_disc,
      |  count(*) AS count_order
      |FROM lineitem
      |WHERE l_shipdate <= date '1998-12-01' - interval '90' day
      |GROUP BY l_returnflag, l_linestatus
      |""".stripMargin

  /** Runs `f`, counting Spark executor task starts during it (drains the bus around it). */
  private def countTaskStarts(f: => Unit): Int = {
    val taskStarts = new AtomicInteger(0)
    val listener = new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        taskStarts.incrementAndGet()
      }
    }
    CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    spark.sparkContext.addSparkListener(listener)
    try {
      f
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
    taskStarts.get()
  }

  /**
   * `$JAVA_HOME/lib/server` prepended to any inherited `DYLD_LIBRARY_PATH` (libjvm, not a JVM).
   */
  private def dyldPath(): Option[String] = {
    Option(System.getenv("JAVA_HOME")).map { javaHome =>
      val lib = new File(new File(javaHome, "lib"), "server").getAbsolutePath
      val existing = Option(System.getenv("DYLD_LIBRARY_PATH")).getOrElse("")
      if (existing.isEmpty) lib else s"$lib:$existing"
    }
  }

  /**
   * Locate the directory holding the feature-built `comet-scheduler` / `comet-executor` binaries.
   * Honors `COMET_BALLISTA_BIN_DIR`, else tries the usual debug/release target dirs relative to
   * the module (surefire's `user.dir` is the `spark/` module) and the repo root.
   */
  private def findBinDir(): Option[File] = {
    val candidates =
      Option(System.getenv("COMET_BALLISTA_BIN_DIR")).map(new File(_)).toSeq ++ Seq(
        "../native/target/debug",
        "../native/target/release",
        "native/target/debug",
        "native/target/release").map(p => new File(System.getProperty("user.dir"), p))
    candidates.find { d =>
      new File(d, "comet-scheduler").canExecute && new File(d, "comet-executor").canExecute
    }
  }

  /** Poll a TCP port until it accepts a connection or the deadline passes. */
  private def waitForPort(port: Int, what: String, timeoutMillis: Long): Unit = {
    val deadline = System.currentTimeMillis() + timeoutMillis
    var connected = false
    while (!connected && System.currentTimeMillis() < deadline) {
      val socket = new Socket()
      try {
        socket.connect(new InetSocketAddress("127.0.0.1", port), 200)
        connected = true
      } catch {
        case _: Throwable => Thread.sleep(150)
      } finally {
        try socket.close()
        catch { case _: Throwable => }
      }
    }
    if (!connected) {
      // Surface the child's log to make a startup failure diagnosable.
      throw new IllegalStateException(
        s"timed out waiting for $what on port $port\n${tailLog(what)}")
    }
    // scalastyle:off println
    println(s"[external-cluster] $what is listening on $port")
    // scalastyle:on println
  }

  private def tailLog(what: String): String = {
    val f = if (what.startsWith("scheduler")) schedulerLog else executorLog
    if (f != null && f.exists()) {
      val src = Source.fromFile(f)
      try s"--- $what log tail ---\n${src.getLines().toSeq.takeRight(40).mkString("\n")}"
      finally src.close()
    } else ""
  }

  /** Spawn the external `comet-scheduler` + `comet-executor` child processes. */
  private def startCluster(binDir: File): Unit = {
    logDir = Files.createTempDirectory("comet-external-cluster-").toFile
    schedulerLog = new File(logDir, "scheduler.log")
    executorLog = new File(logDir, "executor.log")
    val dyld = dyldPath()

    // --- scheduler ---
    val schedulerPb = new ProcessBuilder(new File(binDir, "comet-scheduler").getAbsolutePath)
    schedulerPb.redirectOutput(schedulerLog).redirectErrorStream(true)
    val schedEnv = schedulerPb.environment()
    schedEnv.put("COMET_BALLISTA_SCHEDULER_BIND_HOST", "127.0.0.1")
    schedEnv.put("COMET_BALLISTA_SCHEDULER_BIND_PORT", schedulerPort.toString)
    schedEnv.put("RUST_LOG", "info")
    dyld.foreach(schedEnv.put("DYLD_LIBRARY_PATH", _))
    scheduler = schedulerPb.start()
    waitForPort(schedulerPort, "scheduler", 30000)

    // --- executor (separate, JVM-less) ---
    val executorPb = new ProcessBuilder(new File(binDir, "comet-executor").getAbsolutePath)
    executorPb.redirectOutput(executorLog).redirectErrorStream(true)
    val exEnv = executorPb.environment()
    exEnv.put("COMET_BALLISTA_EXECUTOR_BIND_HOST", "127.0.0.1")
    exEnv.put("COMET_BALLISTA_EXECUTOR_PORT", executorFlightPort.toString)
    exEnv.put("COMET_BALLISTA_EXECUTOR_GRPC_PORT", executorGrpcPort.toString)
    exEnv.put("COMET_BALLISTA_SCHEDULER_HOST", "127.0.0.1")
    exEnv.put("COMET_BALLISTA_SCHEDULER_PORT", schedulerPort.toString)
    exEnv.put("COMET_BALLISTA_EXECUTOR_CONCURRENT_TASKS", "4")
    exEnv.put("RUST_LOG", "info")
    dyld.foreach(exEnv.put("DYLD_LIBRARY_PATH", _))
    executor = executorPb.start()
    waitForPort(executorFlightPort, "executor flight", 30000)
    waitForPort(executorGrpcPort, "executor grpc", 30000)
    // Grace for the executor to finish registering with the scheduler.
    Thread.sleep(3000)
  }

  private def stopCluster(): Unit = {
    Seq(("comet-executor", executor), ("comet-scheduler", scheduler)).foreach {
      case (name, proc) =>
        if (proc != null) {
          proc.destroyForcibly()
          proc.waitFor()
          // scalastyle:off println
          println(s"[external-cluster] stopped $name")
          // scalastyle:on println
        }
    }
  }

  override def afterAll(): Unit = {
    try stopCluster()
    finally super.afterAll()
  }

  test(
    "TPC-H Q1 full aggregate offloads to a LIVE external comet-scheduler/comet-executor cluster " +
      "with identical results and no executor tasks") {
    assume(
      NativeBallista.isAvailable,
      s"native ballista library not available (build with `make core-ballista`): " +
        s"${NativeBallista.loadFailure.map(_.getMessage)}")

    val binDir = findBinDir().getOrElse {
      fail(
        "could not find feature-built comet-scheduler/comet-executor binaries; " +
          "build them with `make core-ballista` (they require --features ballista), or set " +
          "COMET_BALLISTA_BIN_DIR")
    }
    // scalastyle:off println
    println(s"[external-cluster] using binaries from ${binDir.getAbsolutePath}")
    // scalastyle:on println

    startCluster(binDir)

    withTempPath { dir =>
      // Spread rows across several input files so same-group rows land in different partitions —
      // the hash shuffle must combine partial-aggregate states across partitions.
      spark
        .createDataFrame(spark.sparkContext.parallelize(lineitemRows), lineitemSchema)
        .repartition(3)
        .write
        .parquet(dir.getCanonicalPath)

      // AQE off (collect root carries the executeCollect override); direct-read off so block2's
      // input leaf serializes as a plain Scan fed by the Ballista shuffle; small shuffle-partition
      // count keeps the run fast. scheduler.url points at the LIVE external scheduler.
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "4",
        CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> "false",
        CometConf.COMET_EXEC_BALLISTA_SCHEDULER_URL.key -> s"http://127.0.0.1:$schedulerPort") {
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("lineitem")

        // Confirm the offloadable R2 shape BEFORE running: exactly one Comet hash exchange (two
        // stages) and exactly two serialized CometNativeExec blocks (partial + final aggregate).
        val executed = withSQLConf(CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "false") {
          spark.sql(q1FullAggregate).queryExecution.executedPlan
        }
        val exchanges = executed.collect { case e: CometShuffleExchangeExec => e }
        assert(
          exchanges.size == 1,
          s"expected exactly one Comet hash exchange (two stages), found ${exchanges.size}:\n" +
            s"$executed")
        val nativeBlocks = executed.collect {
          case n: CometNativeExec if n.serializedPlanOpt.isDefined => n
        }
        assert(
          nativeBlocks.size == 2,
          s"expected exactly two serialized CometNativeExec blocks, found ${nativeBlocks.size}:\n" +
            s"$executed")

        // Baseline oracle: Q1 via the Comet-on-executor native path (offload off). Positive control
        // for the listener (must launch executor tasks) and the row-for-row reference.
        var baseline: Seq[Seq[Any]] = null
        val baselineTaskStarts = countTaskStarts {
          baseline = withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "false") {
            spark.sql(q1FullAggregate).collect().map(_.toSeq.toIndexedSeq).toIndexedSeq
          }
        }
        assert(
          baselineTaskStarts > 0,
          "expected the Spark baseline collect to launch at least one Spark executor task " +
            s"(sanity check for the listener apparatus); got $baselineTaskStarts")

        // Ballista offload to the LIVE external cluster: same query, flag on, URL set.
        var offloaded: Seq[Seq[Any]] = null
        val offloadedTaskStarts = countTaskStarts {
          offloaded = withSQLConf(CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "true") {
            spark.sql(q1FullAggregate).collect().map(_.toSeq.toIndexedSeq).toIndexedSeq
          }
        }

        // Sort both sides by (returnflag, linestatus) on the driver (Q1's ORDER BY is not
        // offloaded). Compare full rows using the exact values Spark produced — decimals keep their
        // computed scale, so a wrong decimal scale from avg/sum composition fails the assertion.
        def sortKey(r: Seq[Any]): (String, String) = (s"${r.head}", s"${r(1)}")
        val baselineSorted = baseline.sortBy(sortKey)
        val offloadedSorted = offloaded.sortBy(sortKey)
        assert(
          offloadedSorted == baselineSorted,
          "external-cluster-offloaded Q1 aggregate rows do not match Spark's own Q1\n" +
            s"  spark:     $baselineSorted\n  offloaded: $offloadedSorted\n" +
            s"${tailLog("scheduler")}\n${tailLog("executor")}")

        // Sanity: three surviving groups after the Q1 date filter.
        assert(
          baselineSorted.map(r => (s"${r.head}", s"${r(1)}")) ==
            Seq(("A", "F"), ("N", "O"), ("R", "F")),
          s"unexpected Q1 groups: ${baselineSorted.map(r => (r.head, r(1)))}")

        // Crucially, NO Spark executor tasks ran for the offloaded collect — the external Ballista
        // cluster served it.
        assert(
          offloadedTaskStarts == 0,
          s"expected 0 Spark executor tasks for the external-cluster-offloaded collect, " +
            s"but $offloadedTaskStarts started")

        // scalastyle:off println
        println(s"[external-cluster] PASS: live Spark driver ran full Q1 on the external cluster")
        println(tailLog("scheduler"))
        println(tailLog("executor"))
        // scalastyle:on println
      }
    }
  }
}
