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
import java.util.Locale

import scala.collection.mutable

import org.apache.spark.{SparkConf, Success}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.comet.{CometBroadcastHashJoinExec, CometColumnarToRowExec, CometExec, CometHashJoinExec, CometNativeColumnarToRowExec, CometNativeExec}
import org.apache.spark.sql.execution.{SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Compares Spark and Comet on the one reduce task of a skewed shuffled hash join that receives
 * 95% of the probe rows, each row carrying nested and wide columns
 * (apache/datafusion-comet#6528).
 *
 * The join builds `dim` and probes with `fact`, which puts 95% of its rows on one key. AQE is
 * off, so the hot partition is not split, and the one reduce task that receives it reads, joins
 * and emits about 95% of the rows. Every `fact` row carries a struct holding an array of structs,
 * an array of strings, a map, and `WideColumns` flat columns.
 *
 * Both engines run the same query over the same files. Spark's output rows are counted. Comet's
 * plan ends in a columnar-to-row conversion, which has no metric of its own, so the child of that
 * conversion runs instead and the rows of its batches are counted. Neither engine writes its
 * output anywhere.
 *
 * Task time is the executor run time of each task, read from task end events with the task's
 * shuffle and input metrics and its SQL metric updates. The comparison shows the run with the
 * median hot task time of each engine: the hot task, the map stage that scanned `fact` and wrote
 * its shuffle, and the whole run. Executor CPU time is shown for reference only: Comet runs part
 * of its native work on Tokio threads, which a task's CPU time does not count. After the
 * comparison, every metric of the hot task and of the map stage is listed by operator. Metrics of
 * one operator can contain each other, so they are listed but not added up.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometHashJoinTaskTimeBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometHashJoinTaskTimeBenchmark-**results.txt".
 *
 * With the argument `-- profile`, each engine then runs the hot task alone, reusing the shuffle
 * of one more run, for two windows of `ProfileSeconds` each. In the first, a thread samples the
 * stack of the hot task's thread every millisecond, and the samples are counted by what they were
 * doing. A native method on top of a stack stands for all the native code under it. The second
 * window is left for an external profiler of native code, such as `sample` on macOS, and starts
 * when a line beginning with "PROFILE" names the process id.
 *
 * With the argument `-- aqe`, AQE is on, with skew join handling and partition coalescing off, so
 * that the hot partition stays whole and the join keeps 32 tasks. Comet then reads the shuffle
 * directly in native code, which it does only behind AQE query stages. Its conversion to rows
 * sits inside the adaptive plan, so each Comet run executes the query once to materialize the
 * shuffle stages and then runs the native part of the final plan alone. The run reports the map
 * stages of the first execution and the join stage of the second.
 *
 * A `-Dspark.*` system property, for example in `JAVA_TOOL_OPTIONS`, sets a session config, which
 * is how to measure a feature flag of a pull request.
 */
object CometHashJoinTaskTimeBenchmark extends CometBenchmarkBase {

  // `SqlBasedBenchmark` calls `getSparkSession` while it initializes, before the fields of this
  // object are assigned. The fields that method reads are therefore `final` with literal values,
  // which makes them compile-time constants.

  /** Local cores. The hot task costs wall time only while the other cores sit idle. */
  private final val Cores = 8

  /** Reduce tasks of the join. The hot task gets the hot key and its share of the other keys. */
  private final val ShufflePartitions = 32

  /** Rows of `fact`, and so the rows the join emits. */
  private val FactRows = 2 * 1024 * 1024

  /** The share of `fact` rows on the hot key. */
  private val HotPercent = 95

  /** Parquet files, and so map tasks, of each table. */
  private val FactFiles = 32
  private val DimFiles = 8

  /** Rows of `dim`, one per key, so every `fact` row matches exactly one of them. */
  private val DimRows = 256 * 1024

  /** Flat columns of the payload, next to its three nested ones. */
  private val WideColumns = 100

  /** Measured runs per engine, set by `runs=N`. Odd, so that the median is one of the runs. */
  private var measuredRuns = 5

  /** Length of each profiling window. */
  private val ProfileSeconds = 25

  /**
   * Rows of `dim` in the broadcast mode, the largest build side of the earlier broadcast cases.
   */
  private val BroadcastDimRows = 1024 * 1024

  /** Keys of `fact` in the broadcast mode. Each is in `dim`, so every row matches once. */
  private val BroadcastKeys = 64 * 1024

  private def query: String =
    if (broadcast) {
      "SELECT /*+ BROADCAST(d) */ f.key, f.f_long, f.f_double, f.f_str, d.d_long, d.d_str " +
        "FROM fact f JOIN dim d ON f.key = d.k"
    } else {
      val payload = Seq("n_order", "n_tags", "n_attrs") ++ (0 until WideColumns).map(i => s"w_$i")
      val columns = ("f.key" +: payload.map(c => s"f.$c")) ++ Seq("d.d_long", "d.d_str")
      s"SELECT /*+ SHUFFLE_HASH(d) */ ${columns.mkString(", ")} " +
        "FROM fact f JOIN dim d ON f.key = d.k"
    }

  private case class Arm(
      name: String,
      comet: Boolean,
      extraConfigs: Seq[(String, String)] = Nil) {
    def configs: Seq[(String, String)] = Seq(
      CometConf.COMET_ENABLED.key -> comet.toString,
      CometConf.COMET_EXEC_ENABLED.key -> comet.toString) ++ extraConfigs
  }

  private val SparkArm = Arm("Spark", comet = false)
  private val CometArm = Arm("Comet", comet = true)

  /** The arms of the run: Spark and Comet, or one Comet arm per read buffer size of a sweep. */
  private var arms = Seq(SparkArm, CometArm)

  /**
   * One task of a run, with its task metrics and what it added to each accumulator, keyed by
   * accumulator id, which for a SQL metric is `SQLMetric.id`. `partition` is the task's index in
   * its stage.
   */
  private case class TaskRecord(
      stageId: Int,
      partition: Int,
      runTimeMs: Long,
      cpuTimeMs: Long,
      gcTimeMs: Long,
      inputBytes: Long,
      shuffleReadBytes: Long,
      shuffleReadRecords: Long,
      fetchWaitMs: Long,
      shuffleWriteBytes: Long,
      shuffleWriteRecords: Long,
      shuffleWriteNs: Long,
      failed: Boolean,
      metricUpdates: Map[Long, Long])

  /**
   * A SQL metric of the executed plan, which a task reports under the metric's accumulator id.
   * `column` is the operator's first output column, which tells the two sides of a join apart.
   */
  private case class PlanMetric(
      id: Long,
      nodeName: String,
      column: String,
      key: String,
      name: String,
      metricType: String)

  /**
   * One run of the query. `rows` is the row count the engine produced, `joinRowsId` the
   * accumulator of the join's output row count. A run keeps the plan's metrics rather than the
   * plan, because a plan holds its shuffle dependencies, and Spark deletes a shuffle's files only
   * once nothing references them.
   */
  private case class Run(
      tasks: Seq[TaskRecord],
      rows: Long,
      metrics: Seq[PlanMetric],
      joinRowsId: Option[Long],
      wholeStageJoin: Boolean) {
    private val lastStage = tasks.map(_.stageId).max

    /** The tasks of the last stage of the run, the result stage that runs the join. */
    def joinStage: Seq[TaskRecord] = tasks.filter(_.stageId == lastStage)

    /** The tasks of the map stage that wrote the most shuffle bytes, the one that read `fact`. */
    def mapStage: Seq[TaskRecord] =
      tasks
        .filter(_.stageId != lastStage)
        .groupBy(_.stageId)
        .values
        .maxBy(_.map(_.shuffleWriteBytes).sum)

    def joinRows(task: TaskRecord): Long =
      joinRowsId.flatMap(task.metricUpdates.get).getOrElse(0L)

    /** The join stage task that emitted the most rows, or the longest one without a count. */
    def hotTask: TaskRecord = joinStage.maxBy(task => (joinRows(task), task.runTimeMs))

    /**
     * What `of` reported for the metric `key` of the operators whose name starts with `nodeName`,
     * in ms for a timing, MiB for a size and as is otherwise. Empty when the plan has no such
     * metric.
     */
    def metric(of: Seq[TaskRecord], nodeName: String, key: String): Option[Double] = {
      val matching = metrics.filter(m => m.nodeName.startsWith(nodeName) && m.key == key)
      matching.headOption.map { first =>
        val total = of.map(task => matching.flatMap(m => task.metricUpdates.get(m.id)).sum).sum
        scaled(first.metricType, total)
      }
    }
  }

  /** A metric value in ms for a timing, MiB for a size and as is otherwise. */
  private def scaled(metricType: String, value: Long): Double = metricType match {
    case "nsTiming" => value / 1e6
    case "size" => value / (1024.0 * 1024)
    case _ => value.toDouble
  }

  /** Collects every task end. The listener bus delivers them on its own thread. */
  private object TaskRecorder extends SparkListener {
    private val records = mutable.ArrayBuffer.empty[TaskRecord]

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      def metric(f: TaskMetrics => Long): Long = Option(taskEnd.taskMetrics).map(f).getOrElse(0L)
      val updates = taskEnd.taskInfo.accumulables.flatMap { info =>
        info.update.collect { case value: Long => info.id -> value }
      }.toMap
      val record = TaskRecord(
        stageId = taskEnd.stageId,
        partition = taskEnd.taskInfo.index,
        runTimeMs = metric(_.executorRunTime),
        cpuTimeMs = metric(_.executorCpuTime) / 1000000,
        gcTimeMs = metric(_.jvmGCTime),
        inputBytes = metric(_.inputMetrics.bytesRead),
        shuffleReadBytes = metric(_.shuffleReadMetrics.totalBytesRead),
        shuffleReadRecords = metric(_.shuffleReadMetrics.recordsRead),
        fetchWaitMs = metric(_.shuffleReadMetrics.fetchWaitTime),
        shuffleWriteBytes = metric(_.shuffleWriteMetrics.bytesWritten),
        shuffleWriteRecords = metric(_.shuffleWriteMetrics.recordsWritten),
        shuffleWriteNs = metric(_.shuffleWriteMetrics.writeTime),
        failed = taskEnd.reason != Success,
        metricUpdates = updates)
      synchronized { records += record }
    }

    def drain(): List[TaskRecord] = synchronized {
      val drained = records.toList
      records.clear()
      drained
    }
  }

  // Keyed by arm name.
  private val runs = mutable.Map.empty[String, mutable.Buffer[Run]]

  /** Whether the run uses AQE, set by the `aqe` argument. */
  private var adaptive = false

  /** Whether the run joins by broadcast, set by the `bhj` argument. */
  private var broadcast = false

  /** Whether profiling follows a median small join task instead of the hot one. */
  private var profileSmallTask = false

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometHashJoinTaskTimeBenchmark")
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", s"local[$Cores]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      // Comet requires off-heap memory in production. Both engines then draw from the same pool.
      .set("spark.memory.offHeap.enabled", "true")
      .setIfMissing("spark.memory.offHeap.size", "8g")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.ANSI_ENABLED.key, "false")
    // Keeps the hot partition whole. AQE would split it, and Comet then leaves the join to Spark
    // (apache/datafusion-comet#6530).
    sparkSession.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, ShufflePartitions.toString)
    // The join carries a hint, so it cannot become a broadcast join.
    sparkSession.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    // An open cost as large as the largest split closes a scan partition after every file, so
    // each Parquet file is exactly one map task. `prepareTables` checks that it worked.
    val maxSplitBytes = (128L * 1024 * 1024).toString
    sparkSession.conf.set(SQLConf.FILES_MAX_PARTITION_BYTES.key, maxSplitBytes)
    sparkSession.conf.set(SQLConf.FILES_OPEN_COST_IN_BYTES.key, maxSplitBytes)

    sparkSession
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val args = mainArgs.map(_.toLowerCase(Locale.ROOT))
    val profiling = args.exists(_.startsWith("profile"))
    profileSmallTask = args.contains("profile=small")
    adaptive = args.contains("aqe")
    broadcast = args.contains("bhj")
    if (adaptive) {
      // Comet reads a shuffle directly in native code only behind an AQE query stage. Skew join
      // handling stays off, because splitting the hot partition makes Comet leave the join to
      // Spark (apache/datafusion-comet#6530), and so does coalescing, which keeps 32 join tasks.
      spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      spark.conf.set(SQLConf.SKEW_JOIN_ENABLED.key, "false")
      spark.conf.set(SQLConf.COALESCE_PARTITIONS_ENABLED.key, "false")
    }
    val readBufferSizes = mainArgs.collectFirst {
      case arg if arg.startsWith("readBufferSize=") =>
        arg.stripPrefix("readBufferSize=").split(",").toSeq
    }
    readBufferSizes.foreach { sizes =>
      arms = sizes.map { size =>
        Arm(
          s"Comet, $size",
          comet = true,
          Seq(CometConf.COMET_SHUFFLE_READ_BUFFER_SIZE.key -> size))
      }
    }
    // An A/B of the kill check in `readAsRawStream` (review of #6805). Only a temporary build
    // that reads this local property skips the check.
    if (args.contains("killcheck")) {
      arms = Seq(
        Arm("Comet, kill check", comet = true),
        Arm(
          "Comet, no kill check",
          comet = true,
          Seq("spark.comet.benchmark.skipKillCheck" -> "true")))
    }
    mainArgs
      .collectFirst { case arg if arg.startsWith("runs=") => arg.stripPrefix("runs=").toInt }
      .foreach(runs => measuredRuns = runs)
    val cometSweep = readBufferSizes.isDefined || args.contains("killcheck")
    spark.sparkContext.addSparkListener(TaskRecorder)
    try {
      withTempPath { dir =>
        withTempTable("fact", "dim") {
          prepareTables(dir)
          runBenchmark("Skewed hash join, Spark vs Comet: environment") {
            emitEnvironment()
          }
          runBenchmark("Skewed hash join, Spark vs Comet: runs") {
            emit("One untimed run of each engine, checked before the measured runs.")
            arms.foreach(inspect)
            for (_ <- 1 to measuredRuns; arm <- arms) {
              runs.getOrElseUpdate(arm.name, mutable.Buffer.empty) += execute(arm)._1
            }
            arms.foreach { arm =>
              val times = measured(arm).map(_.hotTask.runTimeMs).sorted
              emit(s"${arm.name} hot task, every measured run (ms): ${times.mkString(", ")}")
            }
          }
          if (cometSweep) {
            runBenchmark("Skewed hash join, Comet: one arm per setting") {
              emitReadBufferSweep()
            }
          } else if (broadcast) {
            runBenchmark("Broadcast hash join, Spark vs Comet: probe tasks") {
              emitProbeSummary()
            }
          } else {
            runBenchmark("Skewed hash join, Spark vs Comet: comparison") {
              emitComparison()
            }
            runBenchmark("Skewed hash join, Spark vs Comet: metrics by operator") {
              emitMetricListings()
            }
          }
          if (profiling && !cometSweep) {
            runBenchmark("Skewed hash join, Spark vs Comet: profiling") {
              arms.foreach(profile)
            }
          }
        }
      }
    } catch {
      case e: Throwable =>
        // `BenchmarkBase.main` stops the session only after a run that returned. Under
        // `exec:java` the classloader is closed before the JVM's shutdown hooks run, so Spark's
        // own cleanup hook fails too, and a failed run would leave its shuffle files on disk.
        spark.stop()
        throw e
    }
  }

  /** Writes both tables with Comet disabled and checks that each file is one map task. */
  private def prepareTables(dir: File): Unit = {
    // The hot rows are every row whose id falls in the first `HotPercent` of each hundred, so
    // every map task contributes to the hot partition. The other rows hash onto keys 1 to
    // `DimRows - 1`.
    val key = s"CAST(IF(PMOD(id, 100) < $HotPercent, 0, 1 + PMOD(HASH(id), ${DimRows - 1})) " +
      "AS BIGINT) AS key"
    val order = "NAMED_STRUCT(" +
      "'id', id, " +
      "'customer', NAMED_STRUCT(" +
      "'name', CONCAT('customer-', CAST(PMOD(id, 100000) AS STRING)), " +
      "'tier', CAST(PMOD(id, 5) AS INT)), " +
      "'items', TRANSFORM(SEQUENCE(0, CAST(PMOD(id, 3) AS INT)), " +
      "i -> NAMED_STRUCT('sku', id * 10 + i, 'qty', i + 1)))"
    val nested = Seq(
      s"$order AS n_order",
      "TRANSFORM(SEQUENCE(0, CAST(PMOD(id, 3) AS INT)), " +
        "i -> CONCAT('tag-', CAST(PMOD(id + i, 50) AS STRING))) AS n_tags",
      "MAP('views', PMOD(id, 1000), 'clicks', PMOD(id, 97)) AS n_attrs")
    // BIGINT, DOUBLE, STRING and INT in turn, with values hashed from the row, so that neither
    // Parquet nor the shuffle compresses them like a sequence.
    val wide = (0 until WideColumns).map { i =>
      val value = i % 4 match {
        case 0 => s"XXHASH64(id, $i)"
        case 1 => s"CAST(HASH(id, $i) AS DOUBLE) / 7"
        case 2 => s"CONCAT('v-', CAST(PMOD(HASH(id, $i), 1000000) AS STRING))"
        case _ => s"HASH(id, $i)"
      }
      s"$value AS w_$i"
    }
    val dim = Seq("id AS k", "id * 3 AS d_long", "CONCAT('dim-', CAST(id AS STRING)) AS d_str")

    val broadcastFact = Seq(
      s"CAST(PMOD(HASH(id), $BroadcastKeys) AS BIGINT) AS key",
      "id AS f_long",
      "CAST(id AS DOUBLE) / 7 AS f_double",
      "CONCAT('order-', CAST(id AS STRING)) AS f_str")
    val (factColumns, dimRows) =
      if (broadcast) (broadcastFact, BroadcastDimRows) else (key +: (nested ++ wide), DimRows)
    Seq(("fact", FactRows, FactFiles, factColumns), ("dim", dimRows, DimFiles, dim))
      .foreach { case (table, rows, files, columns) =>
        val path = new File(dir, table).getCanonicalPath
        spark
          .range(0L, rows.toLong, 1L, files)
          .selectExpr(columns: _*)
          .write
          .option("compression", "snappy")
          .parquet(path)
        spark.read.parquet(path).createOrReplaceTempView(table)
        val tasks = spark.table(table).queryExecution.toRdd.getNumPartitions
        require(
          tasks == files,
          s"$table scans as $tasks map tasks, expected one per file ($files)")
      }
  }

  /**
   * One row count per partition of Spark's output, or per batch of Comet's. Comet's plan ends in
   * a columnar-to-row conversion, whose child produces the batches.
   */
  private def rowCounts(plan: SparkPlan, arm: Arm): RDD[Long] =
    if (arm.comet) {
      val nativeChild = collectFirst(plan) {
        case conversion: CometColumnarToRowExec => conversion.child
        case conversion: CometNativeColumnarToRowExec => conversion.child
      }.getOrElse {
        throw new IllegalStateException(s"No Comet columnar-to-row conversion in:\n$plan")
      }
      nativeChild.executeColumnar().map(_.numRows().toLong)
    } else {
      plan.execute().mapPartitions(rows => Iterator(rows.size.toLong))
    }

  private def joinOf(plan: SparkPlan): Option[SparkPlan] = collectFirst(plan) {
    case join: ShuffledHashJoinExec => join
    case join: CometHashJoinExec => join
    case join: BroadcastHashJoinExec => join
    case join: CometBroadcastHashJoinExec => join
  }

  /**
   * Runs the query once under the arm and returns the tasks of the run with the executed plan.
   */
  private def execute(arm: Arm): (Run, SparkPlan) = {
    // `ContextCleaner` deletes a shuffle only once its dependency is garbage collected, which a
    // large heap can postpone until the shuffle files fill the disk. Collecting before each run
    // releases the shuffle of the run before, and no collection lands in a measured task.
    System.gc()
    spark.sparkContext.listenerBus.waitUntilEmpty()
    TaskRecorder.drain()
    var plan: Option[SparkPlan] = None
    var rows = 0L
    var materializing: Seq[TaskRecord] = Nil
    withSQLConf(arm.configs: _*) {
      withConfsPropagated {
        val executed = spark.sql(query).queryExecution.executedPlan
        val (counts, firstRun) = countsToRun(executed, arm)
        materializing = firstRun
        rows = counts.fold(0L)(_ + _)
        plan = Some(executed)
      }
    }
    spark.sparkContext.listenerBus.waitUntilEmpty()
    val executedPlan = plan.get
    val join = joinOf(executedPlan)
    val joinRowsId = join
      .flatMap(op => op.metrics.get("numOutputRows").orElse(op.metrics.get("output_rows")))
      .map(_.id)
    val wholeStageJoin = join.exists { op =>
      collect(executedPlan) { case stage: WholeStageCodegenExec => stage }
        .exists(_.find(_ eq op).isDefined)
    }
    val tasks = materializing ++ TaskRecorder.drain()
    val run = Run(tasks, rows, planMetrics(executedPlan), joinRowsId, wholeStageJoin)
    (run, executedPlan)
  }

  /**
   * The row counts to run for `executed`, after running whatever must come first. Under AQE,
   * Comet's conversion to rows sits inside the adaptive plan. The query then runs once, which
   * materializes its shuffle stages, and the counts come from the native part of the final plan,
   * which reads those stages again. The tasks of that first run are returned without its final
   * stage, so that a run has the map stages of the first run and the join stage of the second.
   */
  private def countsToRun(executed: SparkPlan, arm: Arm): (RDD[Long], Seq[TaskRecord]) =
    executed match {
      case adaptivePlan: AdaptiveSparkPlanExec if arm.comet =>
        adaptivePlan.execute().foreach(_ => ())
        spark.sparkContext.listenerBus.waitUntilEmpty()
        val firstRun = TaskRecorder.drain()
        val finalStage = firstRun.map(_.stageId).max
        (rowCounts(adaptivePlan.executedPlan, arm), firstRun.filter(_.stageId != finalStage))
      case _ =>
        (rowCounts(executed, arm), Nil)
    }

  /** How many shuffle inputs the native plan under Comet's conversion reads in native code. */
  private def directReadInputs(plan: SparkPlan): Int =
    collectFirst(plan) {
      case conversion: CometColumnarToRowExec => conversion.child
      case conversion: CometNativeColumnarToRowExec => conversion.child
    }.collect { case native: CometNativeExec =>
      CometExec.findShuffleScanIndices(native.nativeOp).size
    }.getOrElse(0)

  /**
   * The first operator that is not Comet native. `findFirstNonCometOperator` does not descend
   * into AQE wrappers or query stages, so the final plan and the plan of every stage are checked
   * one by one.
   */
  private def firstNonCometOperator(plan: SparkPlan): Option[SparkPlan] = {
    val fragments = plan +: collect(plan) {
      case adaptivePlan: AdaptiveSparkPlanExec => adaptivePlan.executedPlan
      case stage: QueryStageExec => stage.plan
    }
    fragments.iterator
      .flatMap(fragment =>
        findFirstNonCometOperator(
          fragment,
          classOf[AdaptiveSparkPlanExec],
          classOf[QueryStageExec],
          classOf[AQEShuffleReadExec]))
      .find(_ => true)
  }

  private def planMetrics(plan: SparkPlan): Seq[PlanMetric] =
    collect(plan) { case op => op }.flatMap { op =>
      val column = op.output.headOption.fold("")(_.name)
      op.metrics.toSeq.sortBy(_._1).map { case (key, metric) =>
        PlanMetric(
          metric.id,
          op.nodeName.trim,
          column,
          key,
          metric.name.getOrElse(key),
          metric.metricType)
      }
    }

  /**
   * Runs the query once, untimed, prints what it did, and warns when it does not measure the
   * case. The plan is dropped on return, so its shuffle is released before the measured runs.
   */
  private def inspect(arm: Arm): Unit = {
    val (run, plan) = execute(arm)
    val join = joinOf(plan)
    val hotRows = run.joinRows(run.hotTask)
    val share = 100.0 * hotRows / FactRows
    val directReads = if (arm.comet) directReadInputs(plan) else 0
    val codegen =
      if (arm.comet) s", $directReads shuffle inputs read directly in native code"
      else if (run.wholeStageJoin) ", join in whole-stage codegen"
      else ", join without whole-stage codegen"
    emit(
      f"${arm.name}: join ${join.fold("none")(_.nodeName)}, ${run.rows} rows, " +
        f"${run.joinStage.size} join stage tasks, the hot task emitted $hotRows rows " +
        f"($share%.1f%%)$codegen.")

    if (join.isEmpty) {
      warning(s"WARNING: '${arm.name}' ran no shuffled hash join.\n$plan")
    }
    if (run.rows != FactRows) {
      warning(s"WARNING: '${arm.name}' produced ${run.rows} rows, expected $FactRows.")
    }
    if (!broadcast && share < HotPercent) {
      warning(
        f"WARNING: the hot task of '${arm.name}' emitted $share%.1f%% of the rows, expected " +
          f"$HotPercent%d%%.")
    }
    if (adaptive && arm.comet && directReads == 0) {
      warning("WARNING: under AQE, Comet read no shuffle input directly in native code.")
    }
    if (arm.comet) {
      firstNonCometOperator(plan).foreach { op =>
        warning(
          s"WARNING: the Comet plan is not fully Comet native (first non-Comet operator: " +
            s"${op.nodeName}), so it partly measures Spark.\n$plan")
      }
    }
    if (run.joinStage.size != ShufflePartitions) {
      warning(
        s"WARNING: the join stage of '${arm.name}' ran ${run.joinStage.size} tasks, expected " +
          s"$ShufflePartitions.")
    }
    if (run.tasks.exists(_.failed)) {
      warning(s"WARNING: '${arm.name}' had failed tasks, so its task time is not comparable.")
    }
  }

  private def emitEnvironment(): Unit = {
    val conf = spark.sparkContext.getConf
    emit(
      s"Spark ${spark.version}, Java ${System.getProperty("java.version")}, " +
        s"master ${conf.get("spark.master")}")
    emit(
      if (adaptive) "AQE on, skew join handling and partition coalescing off"
      else "AQE off")
    emit(s"spark.memory.offHeap.size: ${conf.get("spark.memory.offHeap.size")}")
    emit(s"${SQLConf.SHUFFLE_PARTITIONS.key}: $ShufflePartitions")
    emit(
      s"spark.io.compression.codec (Spark shuffle): " +
        conf.get("spark.io.compression.codec", "lz4"))
    emit(
      s"${CometConf.COMET_SHUFFLE_COMPRESSION_CODEC.key} (Comet shuffle): " +
        CometConf.COMET_SHUFFLE_COMPRESSION_CODEC.get())
    emit(
      s"${SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key}: " +
        spark.conf.get(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key))
    emit(
      s"fact: $FactRows rows in $FactFiles files, $HotPercent% of them on one key, 3 nested " +
        s"and $WideColumns flat payload columns")
    emit(s"dim: $DimRows rows in $DimFiles files, one per key")
    emit(s"measured runs per arm: $measuredRuns, arms: ${arms.map(_.name).mkString("; ")}")
  }

  private def measured(arm: Arm): Seq[Run] =
    runs.get(arm.name).map(_.toList).getOrElse(Nil)

  /** The measured run with the median hot task time. */
  private def medianRun(arm: Arm): Option[Run] = {
    val all = measured(arm).sortBy(_.hotTask.runTimeMs)
    all.lift(all.length / 2)
  }

  /** A line of the comparison: how to read it from a run, and how many decimals to show. */
  private case class Line(label: String, decimals: Int, value: Run => Option[Double])

  private def emitComparison(): Unit = {
    val medians = arms.map(arm => arm -> medianRun(arm)).toMap
    def hot(run: Run): Seq[TaskRecord] = Seq(run.hotTask)
    def sum(tasks: Seq[TaskRecord])(f: TaskRecord => Long): Option[Double] =
      Some(tasks.map(f).sum.toDouble)
    val mib = 1024.0 * 1024
    val sections = Seq(
      "Hot task, the join stage task that emitted the most rows" -> Seq(
        Line("run time (ms)", 0, r => Some(r.hotTask.runTimeMs.toDouble)),
        Line("CPU time (ms)", 0, r => Some(r.hotTask.cpuTimeMs.toDouble)),
        Line("GC time (ms)", 0, r => Some(r.hotTask.gcTimeMs.toDouble)),
        Line("rows emitted", 0, r => Some(r.joinRows(r.hotTask).toDouble)),
        Line("shuffle bytes read (MiB)", 1, r => Some(r.hotTask.shuffleReadBytes / mib)),
        Line("shuffle records read", 0, r => Some(r.hotTask.shuffleReadRecords.toDouble)),
        Line("shuffle fetch wait (ms)", 0, r => Some(r.hotTask.fetchWaitMs.toDouble)),
        Line(
          "shuffle read operator (ms)",
          1,
          r => r.metric(hot(r), "CometExchange", "elapsed_compute")),
        Line(
          "  decode and decompress (ms)",
          1,
          r => r.metric(hot(r), "CometExchange", "decode_time")),
        Line(
          "join build (ms)",
          1,
          r =>
            r.metric(hot(r), "ShuffledHashJoin", "buildTime")
              .orElse(r.metric(hot(r), "CometHashJoin", "build_time"))),
        Line(
          "join probe and output (ms)",
          1,
          r => r.metric(hot(r), "CometHashJoin", "join_time")),
        Line(
          "whole-stage codegen pipeline (ms)",
          0,
          r => r.metric(hot(r), "WholeStageCodegen", "pipelineTime"))),
      "Map stage that scanned fact and wrote its shuffle" -> Seq(
        Line("tasks", 0, r => Some(r.mapStage.size.toDouble)),
        Line("task time, total (ms)", 0, r => sum(r.mapStage)(_.runTimeMs)),
        Line("longest task (ms)", 0, r => Some(r.mapStage.map(_.runTimeMs).max.toDouble)),
        Line("scan input (MiB)", 1, r => sum(r.mapStage)(_.inputBytes).map(_ / mib)),
        Line(
          "data size before compression (MiB)",
          1,
          r =>
            r.metric(r.mapStage, "Exchange", "dataSize")
              .orElse(r.metric(r.mapStage, "CometExchange", "dataSize"))),
        Line(
          "shuffle bytes written (MiB)",
          1,
          r => sum(r.mapStage)(_.shuffleWriteBytes).map(_ / mib)),
        Line("shuffle records written", 0, r => sum(r.mapStage)(_.shuffleWriteRecords)),
        Line(
          "shuffle bytes written per row",
          0,
          r => {
            val records = r.mapStage.map(_.shuffleWriteRecords).sum
            if (records == 0) None
            else Some(r.mapStage.map(_.shuffleWriteBytes).sum.toDouble / records)
          }),
        Line("shuffle write time (ms)", 0, r => sum(r.mapStage)(_.shuffleWriteNs).map(_ / 1e6)),
        Line(
          "native shuffle writer (ms)",
          0,
          r => r.metric(r.mapStage, "CometExchange", "elapsed_compute")),
        Line("  repartition (ms)", 0, r => r.metric(r.mapStage, "CometExchange", "repart_time")),
        Line(
          "  encode and compress (ms)",
          0,
          r => r.metric(r.mapStage, "CometExchange", "encode_time"))),
      "Whole run" -> Seq(
        Line("join stage task time, total (ms)", 0, r => sum(r.joinStage)(_.runTimeMs)),
        Line("all tasks, total (ms)", 0, r => sum(r.tasks)(_.runTimeMs))))

    emit("The run with the median hot task time of each engine. Ratio is Spark divided by Comet.")
    emit("A dash means the engine has no such metric.")
    val width = sections.flatMap(_._2).map(_.label.length).max + 2
    emit(s"%-${width}s %12s %12s %8s".format("", SparkArm.name, CometArm.name, "ratio"))
    sections.foreach { case (title, lines) =>
      emit("")
      emit(title)
      lines.foreach { line =>
        val values = arms.map(arm => medians(arm).flatMap(line.value))
        val cells = values.map(_.fold("-")(v => s"%.${line.decimals}f".format(v)))
        val ratio = values match {
          case Seq(Some(s), Some(c)) if c > 0 => f"${s / c}%.2f"
          case _ => ""
        }
        emit(s"  %-${width - 2}s %12s %12s %8s".format(line.label, cells.head, cells(1), ratio))
      }
    }
  }

  /** The broadcast mode's probe stage: one task per `fact` file, each probing the whole `dim`. */
  private def emitProbeSummary(): Unit = {
    emit(
      "The join stage probes, one task per fact file. Times in ms, from the run with the median")
    emit("join stage time of each engine. Other stages scan dim for the broadcast.")
    emit("")
    emit(
      f"${"arm"}%-6s ${"tasks"}%6s ${"stage total"}%12s ${"mean task"}%10s ${"median task"}%12s " +
        f"${"longest"}%8s ${"other stages"}%13s")
    arms.foreach { arm =>
      val all = measured(arm).sortBy(_.joinStage.map(_.runTimeMs).sum)
      all.lift(all.length / 2).foreach { run =>
        val probe = run.joinStage.map(_.runTimeMs).sorted
        val other = run.tasks.map(_.runTimeMs).sum - probe.sum
        emit(
          f"${arm.name}%-6s ${probe.size}%6d ${probe.sum}%12d ${probe.sum.toDouble / probe.size}%10.1f " +
            f"${probe(probe.size / 2)}%12d ${probe.last}%8d $other%13d")
      }
    }
    arms.foreach { arm =>
      val all = measured(arm).sortBy(_.joinStage.map(_.runTimeMs).sum)
      all.lift(all.length / 2).foreach { run =>
        val tasks = run.joinStage
        emit("")
        emit(s"${arm.name}, mean per probe task over ${tasks.size} tasks:")
        run.metrics.foreach { metric =>
          val values = tasks.flatMap(_.metricUpdates.get(metric.id))
          if (values.nonEmpty) {
            val value = scaled(metric.metricType, values.sum) / tasks.size
            val text = metric.metricType match {
              case "nsTiming" | "timing" => f"$value%.2f ms"
              case "size" => f"$value%.2f MiB"
              case _ => f"$value%.0f"
            }
            val operator = s"${metric.nodeName} [${metric.column}]"
            emit(f"  $text%14s  $operator%-28s ${metric.name}")
          }
        }
      }
    }
  }

  /** Every metric the hot task and the map stage of each engine's median run reported. */
  private def emitMetricListings(): Unit = {
    arms.foreach { arm =>
      medianRun(arm).foreach { run =>
        Seq("hot task" -> Seq(run.hotTask), "map stage that read fact" -> run.mapStage).foreach {
          case (what, tasks) =>
            emit("")
            emit(s"${arm.name}, $what, ${tasks.size} task(s):")
            run.metrics.foreach { metric =>
              val values = tasks.flatMap(_.metricUpdates.get(metric.id))
              if (values.nonEmpty) {
                val value = scaled(metric.metricType, values.sum)
                val text = metric.metricType match {
                  case "nsTiming" | "timing" => f"$value%.1f ms"
                  case "size" => f"$value%.1f MiB"
                  case _ => f"$value%.0f"
                }
                val operator = s"${metric.nodeName} [${metric.column}]"
                emit(f"  $text%14s  $operator%-28s ${metric.name}")
              }
            }
        }
      }
    }
  }

  /**
   * Runs the hot task of the arm alone, first while sampling the stack of its thread and then for
   * an external profiler. One more run of the query writes the shuffle the hot task reads.
   */
  private def profile(arm: Arm): Unit = {
    val run = medianRun(arm).getOrElse {
      throw new IllegalStateException(s"No measured run of '${arm.name}' to profile")
    }
    val partition = if (profileSmallTask) {
      val small =
        run.joinStage.filterNot(_.partition == run.hotTask.partition).sortBy(_.runTimeMs)
      small(small.size / 2).partition
    } else {
      run.hotTask.partition
    }
    val which = if (profileSmallTask) "a median small join task" else "the hot task"
    withSQLConf(arm.configs: _*)(withConfsPropagated {
      val (counts, _) = countsToRun(spark.sql(query).queryExecution.executedPlan, arm)
      counts.fold(0L)(_ + _)
      val sampler = new StackSampler
      sampler.start()
      val sampled = runHotTask(counts, partition)
      sampler.finish()
      emit("")
      emit(
        s"${arm.name}: ran $which (partition $partition) $sampled times, sampling the " +
          s"stack of its thread every millisecond, ${sampler.samples} samples.")
      sampler.emitSummary()
      emit(s"PROFILE ${arm.name}: pid ${ProcessHandle.current().pid()}, $ProfileSeconds s")
      val unsampled = runHotTask(counts, partition)
      emit(s"${arm.name}: ran $which $unsampled times for the external profiler")
    })
  }

  /**
   * Runs `body` with the session's configs set as local properties of the jobs it submits, as
   * Spark does for a query it executes, so that tasks see them. The benchmark runs plans
   * directly, which skips that step, and Comet reads some configs, such as the shuffle read
   * buffer size, in tasks.
   */
  private def withConfsPropagated[T](body: => T): T = {
    val sc = spark.sparkContext
    val confs = spark.conf.getAll.filter(_._1.startsWith("spark")).toList
    val previous = confs.map { case (key, _) => key -> sc.getLocalProperty(key) }
    confs.foreach { case (key, value) => sc.setLocalProperty(key, value) }
    try body
    finally previous.foreach { case (key, value) => sc.setLocalProperty(key, value) }
  }

  /** One line per Comet arm of a sweep over the shuffle read buffer size. */
  private def emitReadBufferSweep(): Unit = {
    def median(values: Seq[Double]): Option[Double] =
      if (values.isEmpty) None else Some(values.sorted.apply(values.length / 2))
    emit(s"Comet only, one arm per setting, run in turn. Medians over $measuredRuns runs, in ms.")
    emit(
      "Shuffle read is the hot task's shuffle read operator, which Comet reports with AQE off.")
    emit("")
    val width = arms.map(_.name.length).max
    emit(
      s"%-${width}s %9s %6s %6s %13s %11s %10s"
        .format("arm", "hot task", "min", "max", "shuffle read", "join stage", "all tasks"))
    arms.foreach { arm =>
      val all = measured(arm)
      if (all.nonEmpty) {
        val hot = all.map(_.hotTask.runTimeMs.toDouble)
        val read = median(
          all
            .flatMap(run => run.metric(Seq(run.hotTask), "CometExchange", "elapsed_compute"))
            .filter(_ > 0))
        emit(
          s"%-${width}s %9.0f %6.0f %6.0f %13s %11.0f %10.0f".format(
            arm.name,
            median(hot).get,
            hot.min,
            hot.max,
            read.fold("-")(ms => f"$ms%.0f"),
            median(all.map(_.joinStage.map(_.runTimeMs).sum.toDouble)).get,
            median(all.map(_.tasks.map(_.runTimeMs).sum.toDouble)).get))
      }
    }
  }

  /**
   * What a stack sample was doing: the first rule that a frame matches, from the top of the stack
   * down, so that generic frames such as a buffer copy take the category of their caller. A
   * native method on top is the time spent in native code under it.
   */
  private val StackRules: Seq[(String, StackTraceElement => Boolean)] = {
    def is(cls: String, method: String)(f: StackTraceElement) =
      f.getClassName == cls && f.getMethodName == method
    def under(prefix: String)(f: StackTraceElement) = f.getClassName.startsWith(prefix)
    Seq(
      "Comet native plan (join, batch import, project)" -> is(
        "org.apache.comet.Native",
        "executePlan"),
      "Comet native shuffle decode" -> (f =>
        f.getClassName == "org.apache.comet.Native" && f.getMethodName.startsWith(
          "decodeShuffle")),
      "Comet native, other" -> under("org.apache.comet.Native"),
      "Arrow C JNI (array release, export)" -> under("org.apache.arrow.c.jni."),
      "file read (read syscall)" -> (f =>
        f.isNativeMethod && Set("readBytes", "read0", "pread0").contains(f.getMethodName)),
      "FileInputStream.available (fstat, lseek)" -> (f => f.getMethodName == "available0"),
      "decompress (lz4-java)" -> under("net.jpountz."),
      "Channels.newChannel copy loop" -> under("java.nio.channels.Channels"),
      "Comet shuffle block read (direct read)" -> under(
        "org.apache.comet.CometShuffleBlockIterator"),
      "Comet shuffle block read" -> is(
        "org.apache.spark.sql.comet.execution.shuffle.NativeBatchDecoderIterator",
        "readNextBlock"),
      "Arrow C import into JVM" -> (f =>
        under("org.apache.arrow.c.")(f) && (f.getClassName + f.getMethodName).contains("mport")),
      "Arrow C export to native" -> (f =>
        under("org.apache.arrow.c.")(f) && (f.getClassName + f.getMethodName).contains("xport")),
      "Comet vectors" -> under("org.apache.comet.vector."),
      "Comet iterator glue" -> (f =>
        under("org.apache.comet.CometBatchIterator")(f) ||
          under("org.apache.comet.CometExecIterator")(f)),
      "row deserialization" -> (f =>
        f.getClassName.contains("UnsafeRowSerializer") || under("java.io.DataInputStream")(f)),
      "shuffle fetch and stream wrappers" -> (f =>
        under("org.apache.spark.storage.")(f) || under("org.apache.spark.network.")(f) ||
          under("org.apache.spark.shuffle.")(f)),
      "join hash lookup" -> under("org.apache.spark.sql.execution.joins."),
      "output projection (UnsafeRow writes)" -> (f =>
        f.getClassName.contains("SpecificUnsafeProjection") ||
          under("org.apache.spark.sql.catalyst.expressions.codegen.Unsafe")(f)),
      "UnsafeRow, array and map reads" -> under(
        "org.apache.spark.sql.catalyst.expressions.Unsafe"),
      "row count" -> under("org.apache.spark.sql.benchmark."),
      "broadcast decode: Arrow IPC read" -> (f =>
        under("org.apache.arrow.vector.ipc.")(f) || f.getClassName.endsWith(
          "ArrowReaderIterator") ||
          f.getClassName == "org.apache.comet.vector.StreamReader"),
      "broadcast decode, other" -> under("org.apache.spark.sql.comet.CometBatchRDD"),
      "whole-stage codegen (scan, probe, project)" -> (f =>
        f.getClassName.contains("GeneratedIteratorForCodegenStage")),
      "Parquet scan (JVM)" -> (f =>
        under("org.apache.spark.sql.execution.datasources.parquet.")(f) ||
          under("org.apache.parquet.")(f) || under("org.apache.spark.sql.execution.vectorized.")(
            f)))
  }

  private def stackCategory(frames: Array[StackTraceElement]): String =
    frames.iterator
      .flatMap(frame => StackRules.find(_._2(frame)).map(_._1))
      .find(_ => true)
      .getOrElse("other Java")

  /**
   * Samples the stack of every running executor thread every millisecond until `finish`, and
   * counts the samples by category and by top frame. `getStackTrace` waits for the next safepoint
   * of a thread running Java code, so Java frames carry safepoint bias. A thread in native code
   * is sampled where it is.
   */
  private class StackSampler extends Thread("hash-join-benchmark-stack-sampler") {
    setDaemon(true)
    @volatile private var running = true
    private val categories = mutable.Map.empty[String, Long].withDefaultValue(0L)
    private val tops = mutable.Map.empty[String, Long].withDefaultValue(0L)
    var samples = 0L

    override def run(): Unit = {
      var threads = Seq.empty[Thread]
      var refreshedAt = 0L
      while (running) {
        if (System.nanoTime() - refreshedAt > 200000000L) {
          threads = executorThreads()
          refreshedAt = System.nanoTime()
        }
        threads.filter(_.getState == Thread.State.RUNNABLE).foreach { thread =>
          val frames = thread.getStackTrace
          if (frames.nonEmpty) {
            samples += 1
            categories(stackCategory(frames)) += 1
            val top = frames.head
            val native = if (top.isNativeMethod) " (native)" else ""
            tops(s"${top.getClassName}.${top.getMethodName}$native") += 1
          }
        }
        Thread.sleep(1)
      }
    }

    def finish(): Unit = {
      running = false
      join()
    }

    def emitSummary(): Unit = {
      def share(n: Long): String = f"${100.0 * n / math.max(samples, 1L)}%5.1f%%"
      categories.toSeq.sortBy(-_._2).foreach { case (category, n) =>
        emit(s"  ${share(n)}  $category")
      }
      emit("  Top frames:")
      tops.toSeq.sortBy(-_._2).take(20).foreach { case (frame, n) =>
        emit(s"    ${share(n)}  $frame")
      }
    }
  }

  private def executorThreads(): Seq[Thread] = {
    val threads = Thread.getAllStackTraces.keySet.iterator()
    val found = mutable.Buffer.empty[Thread]
    while (threads.hasNext) {
      val thread = threads.next()
      if (thread.getName.startsWith("Executor task launch worker")) {
        found += thread
      }
    }
    found.toList
  }

  /** Runs the partition of `counts` alone, again and again for `ProfileSeconds`. */
  private def runHotTask(counts: RDD[Long], partition: Int): Int = {
    val end = System.nanoTime() + ProfileSeconds * 1000000000L
    var times = 0
    while (System.nanoTime() < end) {
      spark.sparkContext.runJob(counts, (rows: Iterator[Long]) => rows.sum, Seq(partition))
      times += 1
    }
    times
  }

  /** Sets a warning off from the lines around it, in the console and the results file. */
  private def warning(message: String): Unit = {
    val border = "=" * 80
    emit(s"\n$border\n$message\n$border")
  }

  /** Writes a line to the console and, when results files are generated, to the results file. */
  private def emit(line: String): Unit = {
    // scalastyle:off println
    println(line)
    // scalastyle:on println
    output.foreach(_.write(s"$line\n".getBytes(StandardCharsets.UTF_8)))
  }
}
