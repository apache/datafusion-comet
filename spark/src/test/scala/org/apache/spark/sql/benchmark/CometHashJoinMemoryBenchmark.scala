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

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.util.control.NonFatal

import org.apache.spark.{CometListenerBusUtils, SparkConf}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.comet.{CometHashJoinExec, CometSortMergeJoinExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Compares shuffled hash joins with sort-merge joins under a range of native memory budgets, to
 * show where a hash join stops completing and what each join costs on either side of that point.
 *
 * Every join shape runs Spark's sort-merge join once as the reference, then each Comet arm at
 * each budget: Comet's sort-merge join, its shuffled hash join with spilling disabled, and its
 * shuffled hash join, which spills when the build side does not fit. Each arm runs once untimed,
 * then `comet.bench.iterations` times timed. An arm that fails, such as a hash join whose build
 * side does not fit, is reported with its error and does not stop the other arms. Each row shows
 * the best and median wall time, and for the median run the process CPU time and the bytes
 * spilled to disk. Spills are split between the stages that read shuffle data, which run the
 * join, and the stages that scan and shuffle the tables, whose native shuffle writers also spill
 * under a small budget. Every arm's result is compared with Spark's.
 *
 * The budget is each task's native memory pool, set as a fraction of the off-heap size with
 * `spark.comet.exec.memoryPool.fraction`. Under the default `fair_unified` pool, every memory
 * consumer in the task's native plan is limited to an equal share of it, so a hash join's build
 * side gets one share and each sort of a sort-merge join gets one. Spark's own arm uses Spark's
 * off-heap memory and is not limited by the budget.
 *
 * To run:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometHashJoinMemoryBenchmark
 * }}}
 *
 * These system properties change the defaults: `comet.bench.buildRows` (rows of the build table),
 * `comet.bench.probeRows` (rows of the probe table), `comet.bench.budgetsMiB` (a comma separated
 * list), `comet.bench.iterations`, and `comet.bench.joins` (a comma separated subset of
 * `inner,left,right,full,anti,hotkey`).
 */
object CometHashJoinMemoryBenchmark extends CometBenchmarkBase {

  // Tasks run at once, which is also the shuffle partition count, so each join runs one wave.
  // These are defs because `getSparkSession` runs while the trait initializes, before any val of
  // this object is set.
  private def numTasks: Int = 4
  private def offHeapBytes: Long = 4L * 1024 * 1024 * 1024
  private def mib: Long = 1024L * 1024

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometHashJoinMemoryBenchmark")
      .set("spark.master", s"local[$numTasks]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      // Off-heap mode, because Comet's native memory is accounted, and can run out, only there.
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", offHeapBytes.toString)

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.ANSI_ENABLED.key, "false")
    // Keep every task's share of the join input fixed and stop AQE from splitting partitions or
    // turning a join into a broadcast join.
    sparkSession.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, numTasks.toString)
    sparkSession.conf.set(SQLConf.COALESCE_PARTITIONS_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.SKEW_JOIN_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    sparkSession.conf.set(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")

    sparkSession
  }

  private case class JoinShape(key: String, title: String, query: String)

  /**
   * One way to run the joins. `plannedJoin` returns None when the plan does not contain the join
   * the arm is named for, and otherwise a note on how the join was planned.
   */
  private case class Arm(
      name: String,
      confs: Seq[(String, String)],
      plannedJoin: SparkPlan => Option[String])

  private case class Outcome(
      result: String,
      wallMs: Seq[Double] = Nil,
      cpuMs: Seq[Double] = Nil,
      joinSpilledBytes: Seq[Long] = Nil,
      mapSpilledBytes: Seq[Long] = Nil)

  private val cometOn =
    Seq(CometConf.COMET_ENABLED.key -> "true", CometConf.COMET_EXEC_ENABLED.key -> "true")

  private val sparkArm = Arm(
    "Spark sort-merge join",
    Seq(CometConf.COMET_ENABLED.key -> "false"),
    plan => collect(plan) { case _: SortMergeJoinExec => "" }.headOption)

  private def hashJoinArm(name: String, spill: Boolean) = Arm(
    name,
    cometOn ++ Seq(
      CometConf.COMET_FORCE_SHJ.key -> "true",
      CometConf.COMET_FORCE_SHJ_MAX_BUILD_SIZE.key -> "-1",
      CometConf.COMET_EXEC_HASH_JOIN_SPILL_ENABLED.key -> spill.toString),
    plan => collect(plan) { case j: CometHashJoinExec => s" (${j.buildSide})" }.headOption)

  private val cometArms = Seq(
    Arm(
      "Comet sort-merge join",
      cometOn :+ (CometConf.COMET_FORCE_SHJ.key -> "false"),
      plan => collect(plan) { case _: CometSortMergeJoinExec => "" }.headOption),
    hashJoinArm("Comet hash, no spilling", spill = false),
    hashJoinArm("Comet hash join", spill = true))

  // The sink reads every column the join produces, so that column pruning leaves the build side's
  // payload in place and the join materializes all of its output.
  private val sums = "sum(p.v), sum(b.w), sum(length(b.s))"

  private val joinShapes = Seq(
    JoinShape("inner", "inner join", s"SELECT $sums FROM probe p JOIN build b ON p.k = b.k"),
    JoinShape(
      "left",
      "left outer join, outer side probed",
      s"SELECT $sums FROM probe p LEFT JOIN build b ON p.k = b.k"),
    JoinShape(
      "right",
      "right outer join, outer side built",
      s"SELECT $sums FROM probe p RIGHT JOIN build b ON p.k = b.k"),
    JoinShape(
      "full",
      "full outer join",
      s"SELECT $sums FROM probe p FULL JOIN build b ON p.k = b.k"),
    JoinShape(
      "anti",
      "left anti join",
      "SELECT sum(p.v), count(*) FROM probe p LEFT ANTI JOIN build b ON p.k = b.k"),
    JoinShape(
      "hotkey",
      "inner join, a quarter of the build rows share one key",
      s"SELECT $sums FROM probe_hot p JOIN build_hot b ON p.k = b.k"))

  // Spills of the stages that read shuffle data, which run the join, and of the stages that scan
  // and shuffle the tables, whose native shuffle writers spill under the same budget.
  private val joinSpilledBytes = new AtomicLong()
  private val mapSpilledBytes = new AtomicLong()
  private val shuffleReadingStages = ConcurrentHashMap.newKeySet[Int]()

  private object SpillListener extends SparkListener {
    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      val stage = stageSubmitted.stageInfo
      if (stage.parentIds.nonEmpty) {
        shuffleReadingStages.add(stage.stageId)
      }
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      Option(taskEnd.taskMetrics).foreach { metrics =>
        val spilled =
          if (shuffleReadingStages.contains(taskEnd.stageId)) joinSpilledBytes
          else mapSpilledBytes
        spilled.addAndGet(metrics.diskBytesSpilled)
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val buildRows = sys.props.get("comet.bench.buildRows").map(_.toLong).getOrElse(4000000L)
    val probeRows = sys.props.get("comet.bench.probeRows").map(_.toLong).getOrElse(5 * buildRows)
    val budgetsMiB = sys.props
      .get("comet.bench.budgetsMiB")
      .map(_.split(",").map(_.trim.toLong).toSeq)
      .getOrElse(Seq(1024L, 256L, 192L, 128L, 96L, 64L))
    val iterations = sys.props.get("comet.bench.iterations").map(_.toInt).getOrElse(3)
    val selected = sys.props
      .get("comet.bench.joins")
      .map(_.split(",").map(_.trim.toLowerCase(Locale.ROOT)).toSet)
      .getOrElse(joinShapes.map(_.key).toSet)
    val shapes = joinShapes.filter(shape => selected.contains(shape.key))

    spark.sparkContext.addSparkListener(SpillListener)
    try {
      withTempPath { dir =>
        withTempTable("build", "probe", "build_hot", "probe_hot") {
          writeTables(dir.getAbsolutePath, buildRows, probeRows)
          shapes.foreach { shape =>
            val title = s"${shape.title}: $buildRows build rows, $probeRows probe rows"
            // `runBenchmark` writes the title only to the results file.
            println(s"\n${"=" * 96}\n$title\n${"=" * 96}\n")
            runBenchmark(title) {
              runShape(shape, budgetsMiB, iterations)
            }
          }
        }
      }
    } finally {
      spark.sparkContext.removeSparkListener(SpillListener)
    }
  }

  /**
   * The build table has unique keys and a string payload, so it is the smaller side by row count
   * and the larger side per row. The probe table is five times as many rows of two longs, so it
   * is the larger side in total, and the hash join builds on the build table. About half of the
   * probe rows find a match. The hot-key tables give a quarter of the build rows key 0, which
   * four probe rows match, and keep every other key apart.
   */
  private def writeTables(path: String, buildRows: Long, probeRows: Long): Unit = {
    val payload = "concat('payload-', cast(id AS string), '-', repeat('x', 16)) AS s"
    spark
      .range(buildRows)
      .selectExpr("id AS k", "xxhash64(id) AS w", payload)
      .write
      .parquet(s"$path/build")
    spark
      .range(probeRows)
      .selectExpr(s"pmod(xxhash64(id, 42), ${2 * buildRows}) AS k", "id AS v")
      .write
      .parquet(s"$path/probe")
    spark
      .range(buildRows)
      .selectExpr("CASE WHEN id % 4 = 0 THEN 0 ELSE id END AS k", "xxhash64(id) AS w", payload)
      .write
      .parquet(s"$path/build_hot")
    spark
      .range(probeRows)
      .selectExpr(
        s"CASE WHEN id < 4 THEN 0 ELSE pmod(xxhash64(id, 42), ${2 * buildRows}) + 1 END AS k",
        "id AS v")
      .write
      .parquet(s"$path/probe_hot")
    Seq("build", "probe", "build_hot", "probe_hot").foreach { table =>
      spark.read.parquet(s"$path/$table").createOrReplaceTempView(table)
    }
  }

  private def runShape(shape: JoinShape, budgetsMiB: Seq[Long], iterations: Int): Unit = {
    val (reference, sparkOutcome) = runArm(sparkArm, shape.query, Nil, iterations, None)
    emit("Spark's arm is not limited by the native memory budget.")
    printHeader()
    printRow(sparkArm.name, sparkOutcome)
    budgetsMiB.foreach { budget =>
      val fraction = (budget * mib).toDouble / offHeapBytes
      val budgetConf = Seq(CometConf.COMET_OFFHEAP_MEMORY_POOL_FRACTION.key -> fraction.toString)
      emit("")
      emit(s"Native memory pool per task: $budget MiB")
      printHeader()
      cometArms.foreach { arm =>
        val (_, outcome) = runArm(arm, shape.query, budgetConf, iterations, reference)
        printRow(arm.name, outcome)
      }
    }
  }

  /**
   * Runs one arm untimed and then timed, and returns its result row with the outcome. The result
   * is None when the untimed run fails.
   */
  private def runArm(
      arm: Arm,
      query: String,
      extraConfs: Seq[(String, String)],
      iterations: Int,
      expected: Option[Row]): (Option[Row], Outcome) = {
    // `withSQLConf` returns Unit on Spark 3.x, so the result leaves through this variable.
    var result: (Option[Row], Outcome) = (None, Outcome("NOT RUN"))
    withSQLConf(arm.confs ++ extraConfs: _*) {
      result =
        try {
          val firstRun = runQuery(query)
          val (first, plan) = (firstRun.row, firstRun.plan)
          arm.plannedJoin(plan) match {
            case None =>
              (Some(first), Outcome("PLAN MISMATCH: " + joinNodes(plan).mkString(", ")))
            case Some(_) if expected.exists(_ != first) =>
              (Some(first), Outcome(s"WRONG RESULT: $first, Spark returned ${expected.get}"))
            case Some(note) =>
              val timed = (1 to iterations).map(_ => runQuery(query))
              val outcome = Outcome(
                "OK" + note,
                timed.map(_.wallMs),
                timed.map(_.cpuMs),
                timed.map(_.joinSpilledBytes),
                timed.map(_.mapSpilledBytes))
              (Some(first), outcome)
          }
        } catch {
          case NonFatal(e) => (None, Outcome("FAILED: " + failureReason(e)))
        }
    }
    result
  }

  private case class Run(
      row: Row,
      wallMs: Double,
      cpuMs: Double,
      joinSpilledBytes: Long,
      mapSpilledBytes: Long,
      plan: SparkPlan)

  /** Runs the query from a new DataFrame, so that every run plans and shuffles again. */
  private def runQuery(query: String): Run = {
    CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    joinSpilledBytes.set(0)
    mapSpilledBytes.set(0)
    val cpuStart = processCpuNanos()
    val start = System.nanoTime()
    val df = spark.sql(query)
    val row = df.collect().head
    val wallMs = (System.nanoTime() - start) / 1e6
    val cpuMs = (processCpuNanos() - cpuStart) / 1e6
    CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    Run(
      row,
      wallMs,
      cpuMs,
      joinSpilledBytes.get(),
      mapSpilledBytes.get(),
      df.queryExecution.executedPlan)
  }

  private def processCpuNanos(): Long = ManagementFactory.getOperatingSystemMXBean match {
    case os: com.sun.management.OperatingSystemMXBean => os.getProcessCpuTime
    case _ => 0L
  }

  private def joinNodes(plan: SparkPlan): Seq[String] =
    collect(plan) { case p if p.nodeName.contains("Join") => p.nodeName }

  /**
   * The memory consumer that a native pool refused, or else the first line of the deepest cause.
   */
  private def failureReason(e: Throwable): String = {
    val causes = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(20).toSeq
    val messages = causes.flatMap(c => Option(c.getMessage))
    val marker = "Additional allocation failed for "
    messages.find(_.contains(marker)) match {
      case Some(message) =>
        val rest = message.substring(message.indexOf(marker) + marker.length)
        s"native memory pool refused ${rest.takeWhile(_ != ' ')}"
      case None =>
        val line = messages.lastOption.getOrElse(e.getClass.getName).split("\n").head
        if (line.length > 120) line.take(117) + "..." else line
    }
  }

  private def printHeader(): Unit = {
    emit(
      f"${"Arm"}%-24s ${"Result"}%-36s ${"Best ms"}%8s ${"Median ms"}%10s " +
        f"${"CPU s"}%6s ${"Join spill MiB"}%15s ${"Map spill MiB"}%14s")
  }

  private def printRow(name: String, outcome: Outcome): Unit = {
    if (outcome.wallMs.isEmpty) {
      emit(f"$name%-24s ${outcome.result}%s")
    } else {
      val order = outcome.wallMs.indices.sortBy(outcome.wallMs)
      val median = order(order.length / 2)
      emit(
        f"$name%-24s ${outcome.result}%-36s ${outcome.wallMs.min}%8.0f " +
          f"${outcome.wallMs(median)}%10.0f ${outcome.cpuMs(median) / 1000}%6.1f " +
          f"${outcome.joinSpilledBytes(median).toDouble / mib}%15.1f " +
          f"${outcome.mapSpilledBytes(median).toDouble / mib}%14.1f")
    }
  }

  private def emit(line: String): Unit = {
    println(line)
    output.foreach(_.write((line + "\n").getBytes(StandardCharsets.UTF_8)))
  }
}
