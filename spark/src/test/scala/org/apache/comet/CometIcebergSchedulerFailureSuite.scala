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

package org.apache.comet

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationLong
import scala.jdk.CollectionConverters._

import org.json4s.jackson.JsonMethods.parse
import org.scalatest.DoNotDiscover

import org.apache.spark.{CometListenerBusUtils, SparkConf}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.{CometIcebergWriteExec, IcebergCommitExec, IcebergSchedulerTestProbe}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

/**
 * Manual multi-host integration suite. Excluded from the ordinary single-host CI matrix. Requires
 * COMET_SCHEDULER_MASTER, SHARED_ROOT, EXECUTOR_CLASSPATH and KILL_COMMAND (all with the
 * COMET_SCHEDULER_ prefix). Missing infrastructure is a failure, never a skipped pass. See
 * docs/source/contributor-guide/iceberg-scheduler-failures.md for topology and commands.
 */
@DoNotDiscover
class CometIcebergSchedulerFailureSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometIcebergTestBase {
  import IcebergSchedulerFiles._

  private def required(name: String): String =
    sys.env.getOrElse(
      "COMET_SCHEDULER_" + name,
      throw new IllegalArgumentException(s"COMET_SCHEDULER_$name is required"))

  private var classpathDirectory: Option[Path] = None

  private def timeoutMillis: Long =
    sys.env.getOrElse("COMET_SCHEDULER_TIMEOUT_SECONDS", "180").toLong * 1000L

  override protected def sparkConf: SparkConf = {
    val master = required("MASTER")
    require(!master.startsWith("local"), "A genuinely multi-host Spark cluster is required")
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .setMaster(master)
      .set("spark.executor.extraClassPath", executorClasspath())
      .set("spark.executor.cores", "1")
      .set("spark.cores.max", "4")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.shuffle.service.enabled", "false")
      .set("spark.shuffle.push.enabled", "false")
      .set("spark.decommission.enabled", "false")
      .set("spark.task.maxFailures", "4")
      .set("spark.speculation", sys.env.getOrElse("COMET_SCHEDULER_SPECULATION_ENABLED", "true"))
      .set("spark.speculation.interval", "100ms")
      .set("spark.speculation.quantile", "0.5")
      .set("spark.speculation.multiplier", "1.0")
      .set("spark.speculation.minTaskRuntime", "500ms")
      .set("spark.speculation.efficiency.enabled", "false")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .set("spark.sql.files.maxPartitionBytes", "1048576")
      .set("spark.sql.files.openCostInBytes", "1048576")
      .set(CometConf.COMET_BATCH_SIZE.key, "1000")
      .set(CometConf.COMET_SHUFFLE_ENABLED.key, "false")
      .set(CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key, "true")
      .set(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key, "true")
      .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  }

  // Use a snapshot of the Maven test JVM's unshaded classpath on all hosts. This includes
  // the actual reactor outputs, test helpers, Iceberg runtime and native resource. Packaging
  // only the shaded Comet jar would give the unshaded task closures incompatible classes.
  private def executorClasspath(): String = {
    sys.env.get("COMET_SCHEDULER_EXECUTOR_CLASSPATH").getOrElse {
      val shared = Paths.get(required("SHARED_ROOT")).toAbsolutePath.normalize()
      require(Files.isDirectory(shared), s"Shared root does not exist: $shared")
      val destination = Files.createDirectory(
        shared.resolve("comet-scheduler-classpath-" + UUID.randomUUID().toString))
      classpathDirectory = Some(destination)
      val sources = (System
        .getProperty("java.class.path")
        .split(java.io.File.pathSeparator)
        .map(Paths.get(_))
        .toSeq ++ Seq(
        Paths.get(getClass.getProtectionDomain.getCodeSource.getLocation.toURI),
        Paths.get(
          classOf[CometIcebergWriteExec].getProtectionDomain.getCodeSource.getLocation.toURI)))
        .map(_.toAbsolutePath.normalize())
        .distinct
      require(sources.forall(Files.exists(_)), s"Missing classpath entries: $sources")
      sources.zipWithIndex
        .map { case (source, index) =>
          val target = destination.resolve(index.toString + "-" + source.getFileName.toString)
          if (Files.isDirectory(source)) {
            val tree = Files.walk(source)
            try
              tree.iterator().asScala.foreach { entry =>
                val copy = target.resolve(source.relativize(entry))
                if (Files.isDirectory(entry)) Files.createDirectories(copy)
                else Files.copy(entry, copy)
              }
            finally tree.close()
          } else Files.copy(source, target)
          target.toString
        }
        .mkString(java.io.File.pathSeparator)
    }
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally classpathDirectory.foreach(dir => deleteRecursively(dir.toFile))
  }

  test("speculative native attempts: original wins") {
    runScenario("speculation", speculativeWins = false)
  }

  test("speculative native attempts: speculative wins") {
    runScenario("speculation", speculativeWins = true)
  }

  test("executor process loss during a concurrent native write: task replacement") {
    runScenario("executor-loss", speculativeWins = false)
  }

  test("executor process loss with shuffle output loss: stage re-execution") {
    runScenario("stage-loss", speculativeWins = false)
  }

  private def preflight(root: Path): Unit = {
    val directory = root.resolve("preflight")
    Files.createDirectories(directory)
    val challenge = UUID.randomUUID().toString
    Files.write(directory.resolve("challenge"), challenge.getBytes(UTF_8))
    val shared = directory.toString
    val timeout = timeoutMillis
    val job = Future {
      spark.sparkContext
        .parallelize(0 until 16, 16)
        .mapPartitions { input =>
          val dir = Paths.get(shared)
          require(
            new String(Files.readAllBytes(dir.resolve("challenge")), UTF_8) == challenge,
            "Executors must see the driver's shared filesystem")
          val a = context()
          write(dir.resolve(s"worker-${a.attempt}.json"), a)
          await("preflight release", timeout)(Files.exists(dir.resolve("release")))(
            paths(dir).mkString("\n"))
          input
        }
        .collect()
    }
    try {
      await("two executor processes on distinct scheduler hosts", timeoutMillis)({
        job.value.foreach(_.get)
        val workers = paths(directory)
          .filter(p =>
            p.getFileName.toString.startsWith("worker-") &&
              p.getFileName.toString.endsWith(".json"))
          .map(read)
        workers.map(_.executor).distinct.size >= 2 && workers.map(_.host).distinct.size >= 2
      })(paths(directory).mkString("\n"))
    } finally {
      touch(directory.resolve("release"))
      Await.ready(job, timeoutMillis.millis)
    }
    assert(Await.result(job, timeoutMillis.millis).toSet == (0 until 16).toSet)
  }

  private def runScenario(mode: String, speculativeWins: Boolean): Unit = {
    assert(icebergAvailable, "A compatible Iceberg runtime must be on driver and executors")
    val shared = Paths.get(required("SHARED_ROOT")).toAbsolutePath.normalize()
    require(Files.isDirectory(shared), s"Shared POSIX root does not exist: $shared")
    val runId = UUID.randomUUID().toString.replace("-", "")
    val run = Files.createDirectory(shared.resolve(s"comet-scheduler-$runId"))
    val gates = Files.createDirectory(run.resolve("gates"))
    val warehouse = Files.createDirectory(run.resolve("warehouse"))
    val source = run.resolve("source")
    val catalog = "scheduler" + runId
    val table = s"$catalog.db.target"
    val group = "iceberg-scheduler-" + runId
    val listener = new IcebergSchedulerEvents(group)
    val probe = SharedIcebergSchedulerProbe(gates.toString, mode, timeoutMillis)
    var writeJob: Option[Future[Seq[SparkPlan]]] = None
    var passed = false
    var dataRoot: Option[Path] = None
    var tableCreated = false
    var speculativeWinner: Option[Long] = None
    spark.sparkContext.addSparkListener(listener)
    try {
      preflight(run)
      withSQLConf(
        s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
        s"spark.sql.catalog.$catalog.type" -> "hadoop",
        s"spark.sql.catalog.$catalog.warehouse" -> warehouse.toString) {
        spark.sql(s"CREATE NAMESPACE $catalog.db")
        spark.sql(
          s"CREATE TABLE $table (id INT) USING iceberg " +
            "TBLPROPERTIES ('write.distribution-mode'='none', " +
            "'write.target-file-size-bytes'='1')")
        tableCreated = true
        // Four files, disjoint ID ranges, many batches per writer; source creation is not
        // part of the monitored write job. Runtime events verify the actual partition count.
        spark
          .range(0L, 48000L, 1L, 4)
          .selectExpr("CAST(id AS INT) AS id")
          .write
          .parquet(source.toString)
        val input = spark.read.parquet(source.toString)
        val writeInput = if (mode == "stage-loss") {
          // Keep the ordinary Spark shuffle lineage, but expose it through an RDDScan so
          // Spark-to-Arrow can feed the native writer while Comet shuffle stays disabled.
          val shuffled = input.repartition(4)
          spark.createDataFrame(shuffled.rdd, shuffled.schema)
        } else input
        writeInput.createOrReplaceTempView("scheduler_source")
        val icebergTable = loadIcebergTable(spark, catalog, "db", "target")
        val location = icebergTable.getClass.getMethod("location").invoke(icebergTable).toString
        val root = localPath(location).resolve("data")
        dataRoot = Some(root)
        val before = spark.sql(s"SELECT count(*) FROM $table.snapshots").head().getLong(0)
        IcebergSchedulerTestProbe.withProbe(probe) {
          val job = Future {
            spark.sparkContext.setJobGroup(group, group, interruptOnCancel = true)
            try
              capturePlans(spark) {
                spark.sql(s"INSERT INTO $table SELECT id FROM scheduler_source")
              }
            finally spark.sparkContext.clearJobGroup()
          }
          writeJob = Some(job)
          def waitFor(description: String)(condition: => Boolean): Unit =
            await(description, timeoutMillis)({
              job.value.foreach(_.get)
              condition
            })(listener.diagnostic + "\nmarkers=" + paths(gates).mkString("\n"))

          if (mode == "speculation") {
            waitFor("both attempts to finish native handoff") {
              val attempts = IcebergSchedulerFiles
                .attempts(gates, "handoff")
                .filter(_.partition == 0)
              val events = listener.started.asScala.toVector
              attempts.map(_.attempt).distinct.size == 2 && attempts.exists(a =>
                events.exists(e => e.attempt == a.attempt && e.speculative))
            }
            val pair = attempts(gates, "handoff").filter(_.partition == 0)
            assert(pair.map(a => (a.stage, a.stageAttempt, a.partition)).distinct.size == 1)
            assert(pair.map(_.executor).distinct.size == 2)
            val events = listener.started.asScala.toVector
            assert(
              events
                .filter(e => pair.exists(_.attempt == e.attempt))
                .map(_.host)
                .distinct
                .size == 2)
            assert(pair.forall(_.files.nonEmpty))
            assert((pair.head.files.toSet intersect pair.last.files.toSet).isEmpty)
            val winner = pair
              .find(a =>
                events.exists(e => e.attempt == a.attempt && e.speculative == speculativeWins))
              .get
            speculativeWinner = Some(winner.attempt)
            // Release one only; Spark cancels the other after accepting the winner.
            touch(gates.resolve(s"attempt-${winner.attempt}").resolve("release-handoff"))
            waitFor("scheduler to accept the selected winner") {
              listener.ended.asScala.exists(e =>
                e.attempt == winner.attempt &&
                  e.reason == "Success")
            }
          } else {
            waitFor("multiple executors making native write progress") {
              val active = progress(gates)
              active.exists(_.partition == 0) && active.map(_.executor).distinct.size >= 2 &&
              active.map(_.partition).distinct.size >= 2
            }
            val active = progress(gates)
            val target = if (mode == "stage-loss") {
              active
                .find(a =>
                  listener.ended.asScala.exists(e =>
                    e.executor == a.executor &&
                      e.stage != a.stage && e.reason == "Success"))
                .getOrElse(fail("No active writer executor owns upstream shuffle output"))
            } else active.find(_.partition == 0).get
            assert(target.rows >= 1000L && target.files.nonEmpty)
            assert(
              !attempts(gates, "handoff").exists(_.attempt == target.attempt),
              "Loss must occur before native close and handoff")
            assert(!listener.ended.asScala.exists(_.attempt == target.attempt))
            // Paths come from the native tracking generator, never inferred from prefixes.
            assert(
              target.files.exists(p =>
                Files.exists(localPath(p)) &&
                  Files.size(localPath(p)) > 0L),
              "Native progress produced no physical bytes")
            if (mode == "stage-loss") {
              assert(
                listener.ended.asScala.exists(e =>
                  e.executor == target.executor &&
                    e.stage != target.stage && e.reason == "Success"),
                "The target must also own upstream shuffle output")
            }
            killExecutor(target, run)
            waitFor("executor removal and affected task failure") {
              listener.removedExecutor(target.executor) && listener.ended.asScala.exists(e =>
                e.attempt == target.attempt && e.reason.contains("ExecutorLostFailure"))
            }
            touch(gates.resolve("release-start"))
            // Remaining attempts can finish; killed-executor locations stay untouched for audit.
            active.filterNot(_.executor == target.executor).foreach { a =>
              touch(gates.resolve(s"attempt-${a.attempt}").resolve("release-native"))
            }
            // Also release tasks that reached a native gate just after the initial snapshot.
            touch(gates.resolve("release-all"))
            waitFor("replacement on a different executor") {
              val starts = attempts(gates, "start")
              starts.exists(a =>
                a.stage == target.stage && a.partition == target.partition &&
                  a.attempt != target.attempt && a.executor != target.executor &&
                  (a.number > target.number || a.stageAttempt > target.stageAttempt) &&
                  listener.ended.asScala.exists(e =>
                    e.attempt == a.attempt &&
                      e.reason == "Success" && !e.speculative))
            }
            if (mode == "stage-loss") {
              assert(
                listener.started.asScala.exists(e =>
                  e.stage == target.stage &&
                    e.stageAttempt > target.stageAttempt),
                "Task replacement alone does not satisfy stage re-execution")
              assert(
                listener.ended.asScala.exists(_.reason.contains("FetchFailed")),
                "Expected evidence that lost shuffle output caused a stage retry")
            }
          }
          val plans = Await.result(job, timeoutMillis.millis)
          Files.write(run.resolve("write-plans.txt"), plans.mkString("\n---\n").getBytes(UTF_8))
          // Drain known scheduler events and wait for live attempts' completion listeners.
          waitFor("all write attempts to become terminal") {
            val starts = listener.started.asScala.toVector
            val ends = listener.ended.asScala.map(_.attempt).toSet
            starts.nonEmpty && starts.forall(e => ends.contains(e.attempt)) &&
            attempts(gates, "start").forall(a =>
              listener.removedExecutor(a.executor) ||
                Files.exists(gates.resolve(s"attempt-${a.attempt}").resolve("complete.json")))
          }
          CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
          assert(
            plans.exists(p =>
              collectWithSubqueries(p) { case w: CometIcebergWriteExec =>
                w
              }.nonEmpty),
            s"Native writer did not execute: $plans")
          val commits = plans
            .flatMap(p =>
              collectWithSubqueries(p) { case c: IcebergCommitExec =>
                c
              })
            .distinct
          assert(commits.size == 1, s"Expected one commit operator: $commits")
          val writerStarts = attempts(gates, "start")
          assert(writerStarts.map(_.partition).distinct.size >= 3)
          assert(writerStarts.map(_.executor).distinct.size >= 2)
          val acceptedPaths = paths(gates)
            .filter(_.getFileName.toString.startsWith("accepted-"))
            .filter(_.getFileName.toString.endsWith(".json"))
          assert(acceptedPaths.size == writerStarts.map(_.partition).distinct.size)
          assert(commits.head.metrics("numCommittedMessages").value == acceptedPaths.size.toLong)
          assert(Files.exists(gates.resolve("committed")))
          assert(
            spark.sql(s"SELECT count(*) FROM $table.snapshots").head().getLong(0) -
              before == 1L)
          val counts = spark.sql(s"SELECT count(*), count(DISTINCT id) FROM $table").head()
          assert(counts.getLong(0) == 48000L && counts.getLong(1) == 48000L)
          val expected = spark.range(0L, 48000L).selectExpr("CAST(id AS INT) AS id")
          val actual = spark.table(table).select("id")
          assert(
            actual.exceptAll(expected).isEmpty && expected.exceptAll(actual).isEmpty,
            "Exact ID multiset mismatch")
          auditStorage(gates, root, table, mode, listener, speculativeWinner)
        }
        spark.catalog.dropTempView("scheduler_source")
        spark.sql(s"DROP TABLE $table")
        tableCreated = false
      }
      passed = true
    } finally {
      try {
        touch(gates.resolve("release-all"))
        touch(gates.resolve("release-start"))
        writeJob.foreach { job =>
          if (!job.isCompleted) spark.sparkContext.cancelJobGroup(group)
          try Await.ready(job, timeoutMillis.millis)
          catch { case _: java.util.concurrent.TimeoutException => () }
        }
        CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
        Files.write(run.resolve("scheduler-events.txt"), listener.diagnostic.getBytes(UTF_8))
        dataRoot.foreach { root =>
          if (!Files.exists(run.resolve("physical-files.txt"))) {
            Files.write(
              run.resolve("physical-files.txt"),
              physical(root).toSeq.sorted
                .mkString("\n")
                .getBytes(UTF_8))
          }
        }
        // Even an earlier scheduler assertion failure gets manifest-reference diagnostics.
        if (tableCreated) {
          try {
            val files = spark
              .sql(s"SELECT file_path FROM $table.files")
              .collect()
              .map(_.getString(0))
              .sorted
              .mkString("\n")
            Files.write(run.resolve("referenced-files.txt"), files.getBytes(UTF_8))
          } catch {
            case e: Exception =>
              Files.write(run.resolve("reference-audit-error.txt"), e.toString.getBytes(UTF_8))
          }
        }
      } finally spark.sparkContext.removeSparkListener(listener)
      // Copy diagnostics before deleting successful fixtures. Failed runs retain the warehouse.
      val artifacts = Paths
        .get(getClass.getProtectionDomain.getCodeSource.getLocation.toURI)
        .getParent
        .resolve("iceberg-scheduler-artifacts")
        .resolve(runId)
        .toAbsolutePath
      Files.createDirectories(artifacts)
      paths(run).filterNot(p => p.startsWith(warehouse) || p.startsWith(source)).foreach { p =>
        val dest = artifacts.resolve(run.relativize(p))
        Files.createDirectories(dest.getParent)
        Files.copy(p, dest)
      }
      info(s"Scheduler evidence: $artifacts; passed=$passed; shared run=$run")
      if (passed) deleteRecursively(run.toFile)
    }
  }

  private def killExecutor(target: SchedulerAttempt, run: Path): Unit = {
    val command = Paths.get(required("KILL_COMMAND")).toAbsolutePath
    require(Files.isExecutable(command), s"Kill harness is not executable: $command")
    write(run.resolve("kill-target.json"), target)
    val process = new ProcessBuilder(
      command.toString,
      target.host,
      target.pid.toString,
      target.executor,
      run.toString)
      .redirectErrorStream(true)
      .redirectOutput(run.resolve("kill-process.log").toFile)
      .start()
    if (!process.waitFor(30L, TimeUnit.SECONDS)) {
      process.destroyForcibly()
      fail("Executor termination harness timed out")
    }
    assert(
      process.exitValue() == 0,
      s"Executor termination failed; inspect ${run.resolve("kill-process.log")}")
  }

  private def auditStorage(
      gates: Path,
      root: Path,
      table: String,
      mode: String,
      listener: IcebergSchedulerEvents,
      speculativeWinner: Option[Long]): Unit = {
    val referenced = spark
      .sql(s"SELECT file_path FROM $table.files")
      .collect()
      .map(r => relative(root, r.getString(0)))
      .toSet
    val handoffs = attempts(gates, "handoff")
    val accepted = paths(gates)
      .filter(p =>
        p.getFileName.toString.startsWith("accepted-") &&
          p.getFileName.toString.endsWith(".json"))
      .map { p =>
        val partition = p.getFileName.toString.stripPrefix("accepted-").stripSuffix(".json").toInt
        val files = (parse(new String(Files.readAllBytes(p), UTF_8)) \ "files")
          .extract[Seq[String]]
          .toSet
        val owners = handoffs.filter(a => a.partition == partition && a.files.toSet == files)
        assert(
          files.nonEmpty && owners.size == 1,
          s"Ambiguous accepted message: $p, owners=$owners")
        owners.head
      }
    assert(
      accepted.forall(a =>
        listener.ended.asScala.exists(e => e.attempt == a.attempt && e.reason == "Success")),
      "Accepted files must belong to scheduler-successful attempts")
    speculativeWinner.foreach { winner =>
      assert(
        accepted.find(_.partition == 0).exists(_.attempt == winner),
        "Driver accepted a different attempt than the controlled speculative winner")
    }
    val winnerFiles = accepted.flatMap(_.files).map(relative(root, _)).toSet
    val losers = handoffs.filterNot(a => accepted.exists(_.attempt == a.attempt))
    val failed = progress(gates).filterNot(a => accepted.exists(_.attempt == a.attempt))
    val rejectedFiles = (losers ++ failed).flatMap(_.files).map(relative(root, _)).toSet
    var cleanupTimeout: Option[AssertionError] = None
    if (mode == "speculation") {
      try
        await("losing speculative attempt cleanup", timeoutMillis)(
          (physical(root) intersect rejectedFiles).isEmpty)(
          s"surviving rejected files=${physical(root) intersect rejectedFiles}")
      catch { case e: AssertionError => cleanupTimeout = Some(e) }
    }
    val disk = physical(root)
    Files.write(
      gates.getParent.resolve("physical-files.txt"),
      disk.toSeq.sorted
        .mkString("\n")
        .getBytes(UTF_8))
    Files.write(
      gates.getParent.resolve("referenced-files.txt"),
      referenced.toSeq.sorted
        .mkString("\n")
        .getBytes(UTF_8))
    val orphan = disk -- referenced
    val missing = referenced -- disk
    write(
      gates.resolve("storage-audit.json"),
      Map(
        "physical" -> disk.toSeq.sorted,
        "referenced" -> referenced.toSeq.sorted,
        "winnerFiles" -> winnerFiles.toSeq.sorted,
        "rejectedFiles" -> rejectedFiles.toSeq.sorted,
        "orphan" -> orphan.toSeq.sorted,
        "missing" -> missing.toSeq.sorted))
    info(s"Storage audit: missing=$missing; orphan=$orphan")
    cleanupTimeout.foreach(e => throw e)
    assert(referenced.nonEmpty && missing.isEmpty, s"Missing referenced files: $missing")
    assert(
      referenced == winnerFiles,
      "Manifest differs from accepted attempts' complete file sets")
    assert(
      (rejectedFiles intersect referenced).isEmpty,
      s"Rejected attempts are referenced: ${rejectedFiles intersect referenced}")
    // SIGKILL bypasses executor-side cleanup. Loss scenarios retain the orphan audit and
    // require every orphan to belong to a known rejected attempt, without requiring removal.
    // Speculation and any other scenario retain the strict storage cleanup assertion.
    val executorLoss = mode == "executor-loss" || mode == "stage-loss"
    if (executorLoss) {
      assert(
        orphan.subsetOf(rejectedFiles),
        s"Unexpected orphan files: ${orphan -- rejectedFiles}")
    } else {
      assert(orphan.isEmpty, s"Storage cleanup failed; orphans: $orphan")
    }
  }
}
