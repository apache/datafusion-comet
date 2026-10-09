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

package org.apache.spark.sql.benchmark

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.LockSupport

import scala.collection.mutable

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometExec, CometNativeColumnarToRowExec, CometNativeExec}
import org.apache.spark.sql.execution.{InputAdapter, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Measures how long a Comet reduce task keeps running after it is killed while it reads its
 * shuffle (review of apache/datafusion-comet#6805).
 *
 * One map task writes the whole shuffle as one block, and one reduce task reads it with a native
 * project on top. On the `aqe` path the project reads the shuffle directly in native code
 * (`ShuffleScan` over `CometShuffleBlockIterator`), which Comet does only behind an AQE query
 * stage. On the `noaqe` path it reads it through `NativeBatchDecoderIterator`.
 *
 * Each trial runs the reduce task alone over the same shuffle. A kill trial cancels the job
 * `KillAfterMs` after the task starts, with or without interrupting the task's thread, as
 * `spark.job.interruptOnCancel` selects, and measures how long the task runs after the cancel,
 * until its completion listeners run. A trial without a kill measures the whole task.
 *
 * With `variants=fix,head`, the default, the variants run in turn in one JVM. `head` sets the
 * local property `spark.comet.benchmark.skipKillCheck`, which only a temporary benchmark build
 * reads: it makes `readAsRawStream` skip its kill check, as #6805 did before the fix. Any other
 * variant name only labels the classes on the classpath. `paths=aqe,noaqe` selects the read
 * paths, `rows=N` the rows of the shuffle and `trials=N` the trials.
 */
object CometShuffleReadKillBenchmark extends CometBenchmarkBase {

  // `SqlBasedBenchmark` calls `getSparkSession` while it initializes, before the fields of this
  // object are assigned, so the field that method reads is a compile-time constant.
  private final val Cores = 4

  /** Rows of the shuffle. Its strings repeat, so it is small on disk but slow to decode. */
  private var rows = 16L * 1024 * 1024

  /** Trials per variant and mode, set by `trials=N`. */
  private var trials = 3

  private val KillAfterMs = 1000L

  private val SkipKillCheckKey = "spark.comet.benchmark.skipKillCheck"

  private case class Mode(name: String, kill: Boolean, interrupt: Boolean)

  private val Modes = Seq(
    Mode("not killed: task time", kill = false, interrupt = false),
    Mode("killed, thread interrupted: time after the kill", kill = true, interrupt = true),
    Mode("killed, thread not interrupted: time after the kill", kill = true, interrupt = false))

  /** Counts task ends and keeps the shuffle sizes and the end reason of the last one. */
  private object TaskRecorder extends SparkListener {
    val ends = new AtomicLong(0L)
    @volatile var lastReason = ""
    @volatile var shuffleWritten = 0L
    @volatile var shuffleRead = 0L

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      Option(taskEnd.taskMetrics).foreach { metrics =>
        if (metrics.shuffleWriteMetrics.bytesWritten > 0) {
          shuffleWritten = metrics.shuffleWriteMetrics.bytesWritten
        }
        if (metrics.shuffleReadMetrics.totalBytesRead > 0) {
          shuffleRead = metrics.shuffleReadMetrics.totalBytesRead
        }
      }
      lastReason = taskEnd.reason.toString.takeWhile(_ != '(')
      ends.incrementAndGet()
    }
  }

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometShuffleReadKillBenchmark")
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", s"local[$Cores]")
      .setIfMissing("spark.driver.memory", "3g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .setIfMissing("spark.memory.offHeap.size", "8g")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, "1")
    sparkSession.conf.set(SQLConf.COALESCE_PARTITIONS_ENABLED.key, "false")
    // An open cost as large as the largest split keeps the one file in one map task.
    val maxSplitBytes = (1024L * 1024 * 1024).toString
    sparkSession.conf.set(SQLConf.FILES_MAX_PARTITION_BYTES.key, maxSplitBytes)
    sparkSession.conf.set(SQLConf.FILES_OPEN_COST_IN_BYTES.key, maxSplitBytes)
    sparkSession
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    def listArg(name: String, default: Seq[String]): Seq[String] =
      mainArgs
        .collectFirst {
          case arg if arg.startsWith(s"$name=") => arg.stripPrefix(s"$name=").split(",").toSeq
        }
        .getOrElse(default)
    val variants = listArg("variants", Seq("fix", "head"))
    val paths = listArg("paths", Seq("aqe", "noaqe"))
    listArg("rows", Nil).headOption.foreach(value => rows = value.toLong)
    listArg("trials", Nil).headOption.foreach(value => trials = value.toInt)
    spark.sparkContext.addSparkListener(TaskRecorder)
    try {
      withTempPath { dir =>
        withTempTable("src") {
          writeSource(dir)
          runBenchmark("Killing a Comet reduce task while it reads its shuffle") {
            val conf = spark.sparkContext.getConf
            emit(
              s"Spark ${spark.version}, Java ${System.getProperty("java.version")}, " +
                s"master ${conf.get("spark.master")}")
            emit(
              s"src: $rows rows in 1 Parquet file, so 1 map task writes the shuffle as 1 block " +
                "for 1 reduce task")
            emit(
              s"variants: ${variants.mkString(", ")}. $trials trials per variant and mode, " +
                s"kills $KillAfterMs ms after the task starts. Times in ms, median (trials).")
            paths.foreach(path => measure(path, variants))
          }
        }
      }
    } catch {
      case e: Throwable =>
        // `BenchmarkBase.main` stops the session only after a run that returned.
        spark.stop()
        throw e
    }
  }

  /** Writes `src` with Comet disabled and checks that it scans as one map task. */
  private def writeSource(dir: File): Unit = {
    val path = new File(dir, "src").getCanonicalPath
    spark
      .range(0L, rows, 1L, 1)
      .selectExpr(
        "id",
        "XXHASH64(id) AS h",
        "REPEAT(CONCAT('k', CAST(PMOD(id, 97) AS STRING), '|'), 24) AS s1",
        "REPEAT(CONCAT('v', CAST(PMOD(id, 89) AS STRING), ';'), 24) AS s2")
      .write
      .option("compression", "snappy")
      .parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("src")
    val tasks = spark.table("src").queryExecution.toRdd.getNumPartitions
    require(tasks == 1, s"src scans as $tasks map tasks, expected 1")
  }

  /** Writes the shuffle of one read path, then runs and reports its trials. */
  private def measure(path: String, variants: Seq[String]): Unit = {
    val adaptive = path == "aqe"
    val results = mutable.LinkedHashMap.empty[(String, String), mutable.Buffer[Option[Double]]]
    val reasons = mutable.LinkedHashMap.empty[(String, String), mutable.Set[String]]
    var description = ""
    System.gc()
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      withConfsPropagated {
        val executed = spark
          .table("src")
          .repartition(1, col("id"))
          .selectExpr("id + 1 AS a", "h", "LENGTH(s1) + LENGTH(s2) AS l")
          .queryExecution
          .executedPlan
        // Under AQE, Comet's conversion to rows sits inside the adaptive plan. The query runs
        // once to materialize its shuffle stage, and the native part of the final plan then runs
        // alone. Without AQE, the native part runs alone from the start.
        val finalPlan = executed match {
          case adaptivePlan: AdaptiveSparkPlanExec =>
            adaptivePlan.execute().foreach(_ => ())
            adaptivePlan.executedPlan
          case plan => plan
        }
        val native = nativeChild(finalPlan)
        val direct = CometExec.findShuffleScanIndices(native.nativeOp).size
        require(direct == (if (adaptive) 1 else 0), s"$direct direct reads in:\n$finalPlan")
        val counts = native.executeColumnar().map(_.numRows().toLong)
        // Without AQE, this run also writes the shuffle.
        val counted = counts.fold(0L)(_ + _)
        require(counted == rows, s"counted $counted rows, expected $rows")
        spark.sparkContext.listenerBus.waitUntilEmpty()
        val mib = 1024.0 * 1024
        description =
          (if (adaptive) "AQE on, the project reads the shuffle directly (ShuffleScan)"
           else "AQE off, the project reads the shuffle through NativeBatchDecoderIterator") +
            f", shuffle ${TaskRecorder.shuffleWritten / mib}%.0f MiB written, " +
            f"${TaskRecorder.shuffleRead / mib}%.0f MiB read by the reduce task"
        emit("")
        emit(description)
        emit(native.treeString.linesIterator.map("  " + _).mkString("\n"))
        for (_ <- 1 to trials; mode <- Modes; variant <- variants) {
          val (result, reason) = trial(counts, variant, mode)
          results.getOrElseUpdate((variant, mode.name), mutable.Buffer.empty) += result
          reasons.getOrElseUpdate((variant, mode.name), mutable.Set.empty) += reason
        }
      }
    }
    val width = Modes.map(_.name.length).max
    emit("")
    emit(s"%-${width}s".format("") + variants.map(v => "%24s".format(v)).mkString)
    Modes.foreach { mode =>
      val cells = variants.map { variant =>
        val values = results((variant, mode.name)).toList
        val measured = values.flatten.sorted
        val median = measured.lift(measured.size / 2).fold("-")(ms => f"$ms%.0f")
        val each = values.map(_.fold("ended first")(ms => f"$ms%.0f")).mkString(", ")
        "%24s".format(s"$median ($each)")
      }
      emit(s"%-${width}s".format(mode.name) + cells.mkString)
    }
    emit("Task end reasons:")
    reasons.foreach { case ((variant, mode), seen) =>
      emit(s"  $variant, $mode: ${seen.mkString(", ")}")
    }
  }

  /**
   * The native plan under the Comet columnar-to-row conversion of `plan`. `collectFirst` of
   * `AdaptiveSparkPlanHelper` looks into query stages, such as AQE's final `ResultQueryStage`.
   * Under whole-stage codegen an `InputAdapter`, which tree strings do not show, sits between.
   */
  private def nativeChild(plan: SparkPlan): CometNativeExec =
    collectFirst(plan) {
      case conversion: CometColumnarToRowExec => conversion.child
      case conversion: CometNativeColumnarToRowExec => conversion.child
    }
      .map {
        case adapter: InputAdapter => adapter.child
        case child => child
      }
      .collect { case native: CometNativeExec => native }
      .getOrElse {
        throw new IllegalStateException(s"No native plan under a conversion to rows in:\n$plan")
      }

  /**
   * Runs the reduce task once. Returns how long it ran after the kill, or in total without one,
   * in ms, with the reason its task ended. The time is empty when the task ended before the kill.
   */
  private def trial(counts: RDD[Long], variant: String, mode: Mode): (Option[Double], String) = {
    val sc = spark.sparkContext
    // A collection now keeps one out of the time measured after a kill.
    System.gc()
    CometShuffleReadKillTaskClock.started.set(0L)
    CometShuffleReadKillTaskClock.ended.set(0L)
    val endsBefore = TaskRecorder.ends.get()
    val group = s"kill-benchmark-${System.nanoTime()}"
    val job = new Thread(
      () => {
        sc.setJobGroup(group, s"$variant, ${mode.name}", interruptOnCancel = mode.interrupt)
        sc.setLocalProperty(SkipKillCheckKey, if (variant == "head") "true" else null)
        try {
          sc.runJob(
            counts,
            (context: TaskContext, batches: Iterator[Long]) => {
              context.addTaskCompletionListener[Unit] { _ =>
                CometShuffleReadKillTaskClock.ended.set(System.nanoTime())
              }
              CometShuffleReadKillTaskClock.started.set(System.nanoTime())
              batches.sum
            },
            Seq(0))
        } catch {
          // A cancelled job fails here, as expected.
          case _: Throwable =>
        }
      },
      "kill-benchmark-job")
    job.setDaemon(true)
    job.start()
    val start = await(CometShuffleReadKillTaskClock.started)
    val result = if (mode.kill) {
      val killAt = start + KillAfterMs * 1000000L
      while (System.nanoTime() < killAt) {
        LockSupport.parkNanos(math.min(killAt - System.nanoTime(), 1000000L))
      }
      val killedAt = System.nanoTime()
      if (CometShuffleReadKillTaskClock.ended.get() != 0L) {
        None
      } else {
        sc.cancelJobGroup(group)
        Some((await(CometShuffleReadKillTaskClock.ended) - killedAt) / 1e6)
      }
    } else {
      Some((await(CometShuffleReadKillTaskClock.ended) - start) / 1e6)
    }
    job.join()
    // The task end event follows the executor's final status update, so the next trial does not
    // overlap the cleanup of this one.
    while (TaskRecorder.ends.get() == endsBefore) {
      LockSupport.parkNanos(1000000L)
    }
    (result, TaskRecorder.lastReason)
  }

  /** Waits until `clock` is set and returns it. */
  private def await(clock: AtomicLong): Long = {
    val deadline = System.nanoTime() + 300L * 1000000000L
    while (clock.get() == 0L) {
      require(System.nanoTime() < deadline, "the task did not start or end within 300 s")
      LockSupport.parkNanos(100000L)
    }
    clock.get()
  }

  /**
   * Runs `body` with the session's configs set as local properties of the jobs it submits, as
   * Spark does for a query it executes, so that tasks see them. The benchmark runs plans
   * directly, which skips that step.
   */
  private def withConfsPropagated[T](body: => T): T = {
    val sc = spark.sparkContext
    val confs = spark.conf.getAll.filter(_._1.startsWith("spark")).toList
    val previous = confs.map { case (key, _) => key -> sc.getLocalProperty(key) }
    confs.foreach { case (key, value) => sc.setLocalProperty(key, value) }
    try body
    finally previous.foreach { case (key, value) => sc.setLocalProperty(key, value) }
  }

  /** Writes a line to the console and, when results files are generated, to the results file. */
  private def emit(line: String): Unit = {
    // scalastyle:off println
    println(line)
    // scalastyle:on println
    output.foreach(_.write(s"$line\n".getBytes(StandardCharsets.UTF_8)))
  }
}

/**
 * When the task of a trial started, and when its completion listeners ran, in `nanoTime`. A
 * top-level object, so that the task's closure reads it without capturing the benchmark.
 */
private object CometShuffleReadKillTaskClock {
  val started = new AtomicLong(0L)
  val ended = new AtomicLong(0L)
}
