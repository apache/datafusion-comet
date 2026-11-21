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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.spark.CometListenerBusUtils
import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobStart, SparkListenerStageSubmitted}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions
import org.apache.comet.CometSparkSessionExtensions.{isSpark40Plus, isSpark42Plus}
import org.apache.comet.iceberg.compaction.CometRewriteDataFiles

/**
 * Benchmark of Iceberg's `CALL system.rewrite_data_files` with the bin-pack strategy, run by
 * Iceberg's procedure and by Comet's compaction (`spark.comet.iceberg.compaction.enabled`).
 *
 * Each table is compacted under three configurations:
 *
 *   1. `Spark`: stock Spark runs Iceberg's procedure. The baseline the `Relative` column is
 *      computed against.
 *   1. `Comet, Iceberg's procedure`: Iceberg's procedure still plans the file groups, runs a job
 *      for each and commits, but Comet reads each group with its native Iceberg scan and writes
 *      it with its native writer. This is what Comet does for a compaction while its own
 *      compaction is off, and for a call its compaction leaves to Iceberg.
 *   1. `Comet compaction, single job`: Iceberg's planner and commit, with all the file groups
 *      rewritten in one Spark job, one Spark task for each bin of about the target file size. The
 *      bins are the read tasks Iceberg's staged scan plans in the second case, so the two cases
 *      differ in the number of jobs.
 *
 * The second step is therefore what the native scan and writer give Iceberg's procedure, and the
 * last step is what one job gives over a job per group.
 *
 * Three tables of small files are compacted:
 *
 *   1. `many small partitions`: 200 partitions of four small files. The rewrite of a group is
 *      quick next to the cost of a Spark job, which is the case the single job is for.
 *   1. `few partitions, larger files`: 16 partitions of four larger files. Moving the data takes
 *      most of the time, so the number of jobs matters less and the native scan and writer show.
 *   1. `one group, several bins`: an unpartitioned table, compacted with a target file size well
 *      below its size so that its one file group is read in several bins. Every case runs one
 *      job, so the last two cases should be level.
 *
 * What the numbers do and do not cover:
 *   - The timed statement is the `CALL`, which includes planning the file groups and the commit.
 *     Every case pays for both.
 *   - A compaction is not idempotent, so every iteration compacts a table built afresh outside
 *     the timer. Stock Spark builds it, one file per task and with no shuffle, so every case
 *     rewrites the same files.
 *   - The master is `local[5]` unless `COMET_BENCHMARK_MASTER` says otherwise. Every group here
 *     is read in one task, except the one of the last table, so five groups at a time can keep
 *     the five task slots busy and the gap between the last two cases comes from the jobs
 *     themselves rather than from idle slots. On a cluster Iceberg's procedure still rewrites at
 *     most `max-concurrent-file-group-rewrites` groups at a time, which for groups of one task is
 *     that many tasks, while the single job can use every slot. The gap then grows with the
 *     cluster. A larger `COMET_BENCHMARK_MASTER`, such as `local[16]`, shows part of that.
 *   - The warehouse is a local temporary directory, so no object-store latency is included.
 *   - `Rate` and `Per Row` are computed against the rows of the table, all of which are
 *     rewritten.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometIcebergCompactionBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometIcebergCompactionBenchmark-**results.txt".
 */
object CometIcebergCompactionBenchmark extends CometBenchmarkBase {

  private val catalog = defaultIcebergCatalog
  private val targetTable = s"$catalog.db.compaction_target"

  /** One null in eight in every column, matching the corpus of the other Iceberg benchmarks. */
  private val nullStride = 8

  /** What a case is, which decides what [[verifyArm]] holds it to. */
  private sealed trait Kind
  private case object StockSpark extends Kind
  private case object IcebergProcedure extends Kind
  private case object SingleJob extends Kind

  /**
   * A configuration to time. Every flag that tells the cases apart is pinned in every case rather
   * than left to its default, because `new SparkConf()` inherits `-Dspark.*` system properties
   * and a caller's `BENCH_MAVEN_OPTS` could otherwise set one for the whole session.
   */
  private case class Arm(name: String, kind: Kind, confs: Seq[(String, String)])

  private val cometConfs = Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
    CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key -> "true")

  private val arms: Seq[Arm] = Seq(
    Arm(
      "Spark",
      StockSpark,
      Seq(
        CometConf.COMET_ENABLED.key -> "false",
        CometConf.COMET_ICEBERG_COMPACTION_ENABLED.key -> "false")),
    Arm(
      "Comet, Iceberg's procedure",
      IcebergProcedure,
      cometConfs :+ (CometConf.COMET_ICEBERG_COMPACTION_ENABLED.key -> "false")),
    Arm(
      "Comet compaction, single job",
      SingleJob,
      cometConfs :+ (CometConf.COMET_ICEBERG_COMPACTION_ENABLED.key -> "true")))

  /**
   * A table to compact, and what compacting it must do.
   *
   * @param partitions
   *   the number of partitions, or None for an unpartitioned table
   * @param commits
   *   the number of appends that build the table. Each adds one file to every partition, or
   *   `filesPerCommit` files to an unpartitioned table.
   * @param callOptions
   *   the `options` of the `CALL`
   * @param expectedGroups
   *   the number of file groups the call rewrites, which is also the number of jobs Iceberg's
   *   procedure runs
   * @param expectedFilesAfter
   *   the number of files the table holds afterwards, or None where it depends on the sizes of
   *   the files the writer produces
   */
  private case class Workload(
      title: String,
      partitions: Option[Int],
      commits: Int,
      filesPerCommit: Int,
      rowsPerFile: Int,
      callOptions: Seq[(String, String)],
      expectedGroups: Int,
      expectedFilesAfter: Option[Long]) {

    def files: Long = commits.toLong * filesPerCommit

    def rows: Long = files * rowsPerFile
  }

  /**
   * Iceberg leaves a group of fewer than five files alone unless it holds more than the target
   * file size, and each partition of the partitioned tables has four small files.
   */
  private val smallGroups = "min-input-files" -> "2"

  private val workloads: Seq[Workload] = Seq(
    Workload(
      "many small partitions",
      partitions = Some(200),
      commits = 4,
      filesPerCommit = 200,
      rowsPerFile = 500,
      callOptions = Seq(smallGroups),
      expectedGroups = 200,
      expectedFilesAfter = Some(200)),
    Workload(
      "few partitions, larger files",
      partitions = Some(16),
      commits = 4,
      filesPerCommit = 16,
      rowsPerFile = 25000,
      callOptions = Seq(smallGroups),
      expectedGroups = 16,
      expectedFilesAfter = Some(16)),
    Workload(
      "one group, several bins",
      partitions = None,
      commits = 8,
      filesPerCommit = 25,
      rowsPerFile = 2000,
      // Well below the size of the table, so that the group is read in several bins and written
      // as several files.
      callOptions = Seq("target-file-size-bytes" -> (4L * 1024 * 1024).toString),
      expectedGroups = 1,
      expectedFilesAfter = None))

  /**
   * The master is `local[5]` rather than the base class's `local[1]`, so that a job has several
   * task slots and Iceberg's five concurrent group rewrites have a slot each.
   *
   * Iceberg's SQL extensions are not installed. Spark 4 parses and runs `CALL` itself, and that
   * is where Comet's compaction takes over.
   */
  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometIcebergCompactionBenchmark")
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", sys.env.getOrElse("COMET_BENCHMARK_MASTER", "local[5]"))
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "10g")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    // Set default configs. Individual cases will change them if necessary.
    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    // Overriding `getSparkSession` skips the base class's defaults, and every other benchmark that
    // overrides it restates this one.
    sparkSession.conf.set(SQLConf.ANSI_ENABLED.key, "false")

    sparkSession
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    // The Spark 4.2 profile has no Iceberg runtime of its own, and the Iceberg suites skip it too.
    if (!icebergOnClasspath || !isSpark40Plus || isSpark42Plus) {
      // scalastyle:off println
      println("Comet's Iceberg compaction needs Spark 4.0 or 4.1 and Iceberg; skipping.")
      // scalastyle:on println
      return
    }

    try {
      withTempPath { warehouse =>
        configureIcebergHadoopCatalog(warehouse, catalog)
        try {
          workloads.foreach(runWorkload)
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS $targetTable")
        }
      }
    } catch {
      case e: Throwable =>
        // `BenchmarkBase.main` skips `afterAll`, and with it `spark.stop()`, when a benchmark
        // throws. Spark's shutdown hook then fails because exec:java has closed the class loader,
        // and the session's temporary directories stay behind.
        spark.stop()
        throw e
    }
  }

  private def runWorkload(workload: Workload): Unit = {
    // `warmupTime` is a deadline and `minTime` a total of the timed part, and every iteration also
    // builds a table. Both floors are below the time of one compaction and one build, so every
    // case warms up exactly once and runs exactly `minNumIters` measured compactions.
    val benchmark = new Benchmark(
      workload.title,
      workload.rows,
      minNumIters = 3,
      warmupTime = 500.millis,
      minTime = 500.millis,
      output = output)

    arms.foreach { arm =>
      val label = verifyArm(benchmark, arm, workload)
      benchmark.addTimerCase(label) { timer =>
        // Without a table built afresh, the next iteration would find the files already compacted
        // and have nothing to do.
        buildTable(workload)
        timer.startTiming()
        withSQLConf(arm.confs: _*) { compact(workload) }
        timer.stopTiming()
      }
    }

    benchmark.run()
  }

  /**
   * Compacts a freshly built table once under the arm's configuration and checks that what ran is
   * what the arm's name claims, before any timing happens.
   *
   * A Comet case that lost its native scan or writer, or a single job that fell back to a job per
   * group, is reported and renamed, because a recorded run that says what fell back is more
   * useful than no run at all. What makes the comparison meaningless throws: a table that is not
   * the one the workload describes, a compaction that changed the rows or did not rewrite every
   * file, or a case that ran another case's engine.
   *
   * @return
   *   the case name to time under.
   */
  private def verifyArm(benchmark: Benchmark, arm: Arm, workload: Workload): String = {
    buildTable(workload)
    val filesBefore = dataFileCount()
    val before = digest()
    var result: Row = null
    val trace = traced {
      withSQLConf(arm.confs: _*) { result = compact(workload) }
    }
    val filesAfter = dataFileCount()

    def fail(reason: String): Nothing =
      throw new IllegalStateException(s"${workload.title} / ${arm.name}: $reason")

    if (filesBefore != workload.files) {
      fail(s"the table was built with $filesBefore files, expected ${workload.files}")
    }
    if (result.getInt(0) != filesBefore) {
      fail(s"the call rewrote ${result.getInt(0)} of the $filesBefore files of the table")
    }
    if (digest() != before) {
      fail("the compacted table does not hold the rows it started with")
    }
    workload.expectedFilesAfter match {
      case Some(expected) if filesAfter != expected =>
        fail(s"the table holds $filesAfter files after the compaction, expected $expected")
      case None if filesAfter >= filesBefore =>
        fail(s"the table holds $filesAfter files after the compaction, from $filesBefore")
      case _ =>
    }

    val cometJobs = trace.cometJobs.size
    val icebergJobs = trace.icebergJobs.size
    benchmark.out.println(
      s"${workload.title} / ${arm.name}: $filesBefore files rewritten into $filesAfter by " +
        s"${cometJobs + icebergJobs} rewrite jobs, tasks in the largest stage: " +
        s"${trace.largestStage}")

    arm.kind match {
      case StockSpark | IcebergProcedure =>
        if (cometJobs > 0) {
          fail("Comet's compaction ran, although this case turns it off")
        }
        // Iceberg's own count of file groups, which checks that the workload's table is planned
        // into the groups the other cases are held to.
        if (icebergJobs != workload.expectedGroups) {
          fail(s"Iceberg rewrote $icebergJobs file groups, expected ${workload.expectedGroups}")
        }
      case SingleJob =>
        if (cometJobs == 0 || icebergJobs > 0) {
          fail(
            s"Iceberg's procedure ran the compaction ($icebergJobs Iceberg and $cometJobs " +
              "Comet jobs), so this case would time it under a Comet name. The driver log says " +
              "why Comet left the call to Iceberg.")
        }
    }

    // Comet's part of a case is its native scan and writer, which a case can lose without the
    // compaction failing.
    def nativeLabel: String = {
      val fellBack =
        Seq("CometIcebergNativeScan" -> "JVM scan", "CometIcebergWrite" -> "JVM writer")
          .collect { case (node, fallback) if !trace.nodeNames.contains(node) => fallback }
      if (fellBack.size == 2) {
        fail(
          "neither Comet's native Iceberg scan nor its native writer ran, so this case does " +
            "not measure Comet")
      }
      if (fellBack.isEmpty) {
        arm.name
      } else {
        warn(
          benchmark,
          s"${workload.title} / ${arm.name}: the plans have no " +
            s"${fellBack.mkString(" and ")}, so the numbers below do not describe Comet's " +
            "native Iceberg scan and writer.")
        s"${arm.name} (fell back to ${fellBack.mkString(" and ")})"
      }
    }

    arm.kind match {
      case StockSpark =>
        // The baseline is the divisor of every `Relative` figure. Comet's extensions are
        // installed in this session and only held off by `spark.comet.enabled`, so if that ever
        // stopped covering an operator the baseline would quietly become a partly Comet run.
        val comet = trace.nodeNames.filter(_.startsWith("Comet"))
        if (comet.nonEmpty) {
          fail(s"expected stock Spark but the plans contain ${comet.toSeq.sorted.mkString(", ")}")
        }
        arm.name
      case IcebergProcedure =>
        nativeLabel
      case SingleJob =>
        // Every workload fits one job, which only splits past CometRewriteDataFiles'
        // MaxBinsPerJob bins or with partial progress.
        if (cometJobs != 1) {
          fail(s"$cometJobs jobs ran for the ${workload.expectedGroups} file groups, not one")
        }
        nativeLabel
    }
  }

  // -- Tables ------------------------------------------------------------------------------

  /** The ten corpus columns, by name and the expression that derives each from `value`. */
  private val corpusColumns: Seq[(String, String)] = Seq(
    "c_bool" -> "PMOD(value, 2) = 0",
    "c_int" -> "CAST(PMOD(value, 2147483647) AS INT)",
    "c_long" -> "value",
    "c_double" -> "CAST(value AS DOUBLE) / 7",
    "c_dec" -> "CAST(PMOD(value, 100000000) AS DECIMAL(18,4))",
    "c_str_dict" -> "CAST(PMOD(value, 8) AS STRING)",
    "c_str" -> "REPEAT(CAST(value AS STRING), 3)",
    "c_bin" -> "CAST(CAST(value AS STRING) AS BINARY)",
    "c_date" -> "DATE_ADD(DATE '1970-01-01', CAST(PMOD(value, 20000) AS INT))",
    "c_ts" -> "TIMESTAMP_SECONDS(PMOD(value, 1600000000))")

  /**
   * Drops and recreates the target table and fills it with the workload's small files, with stock
   * Spark whichever case is about to be timed. The two writers roll files at different points, so
   * a table written by the case under test would hand each case different files to rewrite.
   *
   * Every append reads a range in `filesPerCommit` slices, and Iceberg is told not to shuffle, so
   * every task writes one file. In a partitioned table every slice holds the rows of one
   * partition. Without a shuffle there are also no shuffle files left on the disk from one
   * iteration to the next.
   *
   * `DROP TABLE` on the Hadoop catalog deletes the table directory, so the files of the previous
   * iteration go with it.
   */
  private def buildTable(workload: Workload): Unit =
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "false",
      "spark.sql.iceberg.distribution-mode" -> "none") {
      spark.sql(s"DROP TABLE IF EXISTS $targetTable")
      val partitioning = if (workload.partitions.isDefined) "PARTITIONED BY (part)" else ""
      spark.sql(s"""
        CREATE TABLE $targetTable
        USING iceberg
        $partitioning
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
        AS ${corpusQuery(workload, "range(0)")}
      """)
      val rowsPerCommit = workload.filesPerCommit.toLong * workload.rowsPerFile
      (0 until workload.commits).foreach { commit =>
        val source = s"range(${commit * rowsPerCommit}, ${(commit + 1) * rowsPerCommit}, 1, " +
          s"${workload.filesPerCommit})"
        spark.sql(s"INSERT INTO $targetTable ${corpusQuery(workload, source)}")
      }
    }

  /**
   * The corpus columns over the rows of `source`, a `range` whose `id` is the row's position, and
   * `part`, the partition column of the partitioned tables.
   *
   * `value` is a hash of the row's position, so that the columns hold values spread like real
   * data rather than sorted ones. Every column carries one null in [[nullStride]], staggered by
   * column position, so that the definition levels of a nullable column are part of what is
   * moved. `part` is the slice the row is in, modulo the number of partitions.
   */
  private def corpusQuery(workload: Workload, source: String): String = {
    val projections = corpusColumns.zipWithIndex.map { case ((name, expr), position) =>
      s"IF(PMOD(id + $position, $nullStride) = 0, NULL, $expr) AS $name"
    }
    val part = workload.partitions.map { partitions =>
      s"CAST(PMOD(id DIV ${workload.rowsPerFile}, $partitions) AS STRING) AS part"
    }
    s"SELECT ${(projections ++ part).mkString(", ")} " +
      s"FROM (SELECT id, XXHASH64(id) AS value FROM $source) AS source"
  }

  private def dataFileCount(): Long =
    spark.sql(s"SELECT COUNT(*) FROM $targetTable.data_files").head().getLong(0)

  /**
   * The row count and the sum of a hash of every row, which a lost, duplicated or changed row
   * changes. The sum is a decimal so that it cannot overflow.
   */
  private def digest(): Row = {
    val columns = corpusColumns.map(_._1).mkString(", ")
    spark
      .sql(s"SELECT COUNT(*), SUM(CAST(XXHASH64($columns) AS DECIMAL(38, 0))) FROM $targetTable")
      .head()
  }

  private def compact(workload: Workload): Row = {
    val options = workload.callOptions
      .map { case (key, value) => s"'$key', '$value'" }
      .mkString("map(", ", ", ")")
    spark
      .sql(
        s"CALL $catalog.system.rewrite_data_files(table => 'db.compaction_target', " +
          s"options => $options)")
      .head()
  }

  // -- What ran ----------------------------------------------------------------------------

  /**
   * What a compaction ran, read off the events of the listener bus. Only names and counts are
   * kept, because a retained plan keeps its shuffle files from being deleted.
   *
   * @param jobGroups
   *   the distinct job groups of the Spark jobs
   * @param largestStage
   *   the number of tasks of the largest stage
   * @param nodeNames
   *   the names of the operators in the plans of the queries
   */
  private case class Trace(jobGroups: Seq[String], largestStage: Int, nodeNames: Set[String]) {

    def cometJobs: Seq[String] =
      jobGroups.filter(_.startsWith(CometRewriteDataFiles.JobGroupPrefix))

    def icebergJobs: Seq[String] = jobGroups.filter(_.startsWith("REWRITE-DATA-FILES"))
  }

  /**
   * Runs `body` and reports the Spark jobs and the plans of the queries it ran. A compaction runs
   * its queries in a copy of the session. The events of the context's listener bus come from
   * every session, so they are read rather than the query listeners of one session.
   */
  private def traced(body: => Unit): Trace = {
    val jobGroups = ConcurrentHashMap.newKeySet[String]()
    val stageTasks = new ConcurrentLinkedQueue[Int]()
    val nodeNames = ConcurrentHashMap.newKeySet[String]()

    def addNodeNames(info: SparkPlanInfo): Unit = {
      nodeNames.add(info.nodeName)
      info.children.foreach(addNodeNames)
    }

    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit =
        Option(jobStart.properties)
          .flatMap(properties => Option(properties.getProperty("spark.jobGroup.id")))
          .foreach(jobGroups.add)

      override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
        stageTasks.add(stageSubmitted.stageInfo.numTasks)
      }

      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case start: SparkListenerSQLExecutionStart => addNodeNames(start.sparkPlanInfo)
        case update: SparkListenerSQLAdaptiveExecutionUpdate => addNodeNames(update.sparkPlanInfo)
        case _ =>
      }
    }
    CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    spark.sparkContext.addSparkListener(listener)
    try {
      body
      // The listener bus delivers asynchronously, so the events are not all in hand until it has
      // drained.
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
    Trace(
      jobGroups.asScala.toSeq.sorted,
      if (stageTasks.isEmpty) 0 else stageTasks.asScala.max,
      nodeNames.asScala.toSet)
  }
}
