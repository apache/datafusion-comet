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

import scala.collection.mutable
import scala.concurrent.duration._

import org.apache.spark.CometListenerBusUtils
import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.comet.CometIcebergWriteExec
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions
import org.apache.comet.iceberg.IcebergReflection

/**
 * Benchmark of writes into an Iceberg table with Comet's native (iceberg-rust) writer on and off.
 *
 * Four cases are measured, covering the writer implementations an Iceberg write can reach:
 *
 *   1. an unpartitioned `INSERT INTO ... SELECT`, which writes one file per task with no
 *      exchange;
 *   1. the same insert into a partitioned table with a declared sort order, so the write carries
 *      a required ordering and Iceberg selects the clustered writer, which keeps one file open;
 *   1. the same insert into a partitioned table configured for the fanout writer, which holds a
 *      file open per partition instead of requiring the exchange;
 *   1. a copy-on-write `DELETE`, where the write is a rewrite of every file that holds a matching
 *      row, and so is a read and a write of the whole table rather than of new rows.
 *
 * Each case is measured under three configurations, so that the writer's own contribution can be
 * read off the table the way the ad hoc measurements in
 * [[https://github.com/apache/datafusion-comet/pull/5361 #5361]] reported it:
 *
 *   1. `Spark` - stock Spark, the baseline the `Relative` column is computed against.
 *   1. `Comet scan` - Comet reads the Parquet source, iceberg-java still writes the data files.
 *   1. `Comet scan + native write` - the per-task write is delegated to iceberg-rust.
 *
 * The first-to-second step is therefore the scan speedup and the second-to-third step is the
 * writer speedup. The `Relative` column compares each case against stock Spark, so the writer's
 * own factor has to be divided out of the two Comet rows rather than read directly.
 *
 * What the numbers do and do not cover:
 *   - The timed statement includes the driver-side Iceberg commit, which all three configurations
 *     pay equally. It dilutes the writer's factor rather than inflating it.
 *   - The warehouse is a local temporary directory, so no object-store latency is included.
 *   - The two writers choose different file roll points (see the accepted divergences in
 *     `docs/source/user-guide/latest/iceberg-writes.md`), so the resulting file layouts are not
 *     expected to match. Only wall clock and row counts are compared.
 *   - `Rate` and `Per Row` are always computed against the corpus size, so for the `DELETE` case
 *     they describe the rows the statement passed over, not the far smaller number it removed.
 *   - The corpus below is this benchmark's own, not the one #5361 measured. The factors are the
 *     same comparison repeated on different data, not a continuation of that PR's numbers.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometIcebergWriteBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometIcebergWriteBenchmark-**results.txt".
 */
object CometIcebergWriteBenchmark extends CometBenchmarkBase {

  private val catalog = defaultIcebergCatalog
  private val namespace = "db"
  private val targetTable = s"$catalog.$namespace.write_target"

  /** One null in eight in every column, matching the corpus of the other Iceberg benchmarks. */
  private val nullStride = 8

  /**
   * Source file count, and therefore the number of write tasks an unpartitioned insert runs.
   * Pinned rather than left to `spark.sql.files.maxPartitionBytes` so that a change in corpus
   * size or in the machine's core count does not silently change the parallelism and make two
   * recorded runs incomparable.
   */
  private val sourceFiles = 8

  /**
   * The partition column of the two partitioned cases. Its eight values and its nulls give nine
   * partitions: few enough that the clustered writer holds one file open at a time, and more than
   * the source file count so that the fanout writer has something to fan out over.
   */
  private val partitionColumn = "c_str_dict"

  /**
   * The rows the copy-on-write case deletes. One row in a hundred, spread evenly, so that every
   * data file holds at least one of them and the rewrite covers the whole table - which is the
   * shape of copy-on-write that the writer's speed actually decides. A predicate matching a
   * contiguous range would instead measure how well the scan planner pruned files.
   */
  private val deletePredicate = "PMOD(c_long, 100) = 0"

  /**
   * How a case is configured and what its plan must contain. `expectComet` and
   * `expectNativeWrite` are what [[verifyArm]] checks before anything is timed: a configuration
   * that silently fell back measures a different engine than its name claims.
   */
  private case class Arm(
      name: String,
      confs: Seq[(String, String)],
      expectComet: Boolean,
      expectNativeWrite: Boolean)

  private val arms: Seq[Arm] = Seq(
    Arm(
      "Spark",
      Seq(CometConf.COMET_ENABLED.key -> "false"),
      expectComet = false,
      expectNativeWrite = false),
    Arm(
      "Comet scan",
      Seq(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        // Pin the native-write flags off rather than leaning on their defaults. `new SparkConf()`
        // inherits `-Dspark.*` system properties, so a caller's `BENCH_MAVEN_OPTS` could otherwise
        // turn native writes on for the whole session and make this arm a second copy of the native
        // one. `verifyArm` also rejects an unexpected `CometIcebergWriteExec`, so a leak throws
        // rather than being timed under the wrong label.
        CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key -> "false",
        CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key -> "false"),
      expectComet = true,
      expectNativeWrite = false),
    Arm(
      "Comet scan + native write",
      Seq(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        // The native writer requires the split-operator plan; enabling it alone is a no-op.
        CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key -> "true"),
      expectComet = true,
      expectNativeWrite = true))

  /**
   * One timed statement and the table it needs.
   *
   * @param orderBy
   *   the column to declare as the table's local write order, applied as an `ALTER TABLE ...
   *   WRITE` in [[resetTable]]. Set for the clustered case, whose writer Iceberg selects only
   *   when the write has a required ordering; `None` elsewhere.
   * @param prePopulate
   *   whether the table has to hold the corpus before the statement runs, as the copy-on-write
   *   case does and the inserts do not.
   * @param expectShuffle
   *   whether the statement's plan must contain an exchange. `None` where the case's identity
   *   does not rest on it.
   * @param expectSort
   *   whether the statement's plan must contain a sort. This, not the exchange, is what tells the
   *   clustered writer apart from the fanout one: Iceberg's `SparkWriteConf.useFanoutWriter` is
   *   `fanoutEnabled || !hasOrdering`, so the clustered writer is reached exactly when the write
   *   has the required ordering a sort node makes visible. Without this check a hash-distributed
   *   but unordered write would pass the exchange check while silently running the fanout writer
   *   under the clustered label, and the clustered writer would never be measured.
   */
  private case class Workload(
      title: String,
      partitionSpec: String,
      properties: Seq[String],
      orderBy: Option[String],
      prePopulate: Boolean,
      statement: String,
      expectedRowsAfter: Long,
      expectShuffle: Option[Boolean],
      expectSort: Option[Boolean])

  private def workloads(values: Int): Seq[Workload] = {
    val insert = s"INSERT INTO $targetTable SELECT * FROM parquetV1Table"
    val partitioned = s"PARTITIONED BY ($partitionColumn)"
    val deleted =
      spark.sql(s"SELECT count(*) FROM parquetV1Table WHERE $deletePredicate").head().getLong(0)

    Seq(
      Workload(
        "unpartitioned INSERT INTO ... SELECT",
        partitionSpec = "",
        properties = Nil,
        orderBy = None,
        prePopulate = false,
        statement = insert,
        expectedRowsAfter = values.toLong,
        expectShuffle = None,
        expectSort = None),
      Workload(
        "partitioned INSERT INTO ... SELECT, clustered writer",
        partitionSpec = partitioned,
        // Hash distribution alone does not reach the clustered writer. Iceberg picks the writer in
        // `SparkWriteConf.useFanoutWriter`, which is `fanoutEnabled || !hasOrdering`: with no sort
        // order the write has no required ordering, so it defaults to the fanout writer even under
        // hash distribution. The clustered (rolling) writer is reached only when an ordering is
        // required, and is only safe then, since it keeps one file open and errors if a partition
        // it already closed reappears. So this case pins fanout off and `resetTable` declares a
        // local sort, which turns `hasOrdering` true and routes both writers down the clustered
        // path. The `orderBy` sort is what `expectSort` checks; the exchange alone would not.
        properties = Seq("'write.spark.fanout.enabled'='false'"),
        orderBy = Some(partitionColumn),
        prePopulate = false,
        statement = insert,
        expectedRowsAfter = values.toLong,
        expectShuffle = Some(true),
        expectSort = Some(true)),
      Workload(
        "partitioned INSERT INTO ... SELECT, fanout writer",
        partitionSpec = partitioned,
        // Turn the fanout writer on and the distribution off, so the write has neither an exchange
        // nor a required ordering. With no ordering Iceberg would default to fanout anyway, but
        // pinning it on keeps the case immune to a change of default, and the absent sort is what
        // `expectSort = Some(false)` holds it to.
        properties =
          Seq("'write.spark.fanout.enabled'='true'", "'write.distribution-mode'='none'"),
        orderBy = None,
        prePopulate = false,
        statement = insert,
        expectedRowsAfter = values.toLong,
        expectShuffle = Some(false),
        expectSort = Some(false)),
      Workload(
        "copy-on-write DELETE",
        partitionSpec = "",
        properties = Seq("'write.delete.mode'='copy-on-write'"),
        orderBy = None,
        prePopulate = true,
        statement = s"DELETE FROM $targetTable WHERE $deletePredicate",
        expectedRowsAfter = values.toLong - deleted,
        expectShuffle = None,
        expectSort = None))
  }

  /**
   * `spark.sql.extensions` is static, so the Iceberg extensions have to be installed here rather
   * than through `withSQLConf`. Only the copy-on-write `DELETE` needs them, but running every
   * case under the same session configuration keeps the cases comparable.
   *
   * The master is `local[5]` rather than the base class's `local[1]`: a write's cost is spread
   * over its tasks, and the partitioned cases are about the exchange feeding the writer, which a
   * single thread cannot show.
   */
  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometIcebergWriteBenchmark")
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", "local[5]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
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
    // overrides it restates this one. ANSI is off so that the corpus is evaluated the same way here
    // as in the benchmarks this one is read alongside.
    sparkSession.conf.set(SQLConf.ANSI_ENABLED.key, "false")

    sparkSession
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    if (!icebergOnClasspath) {
      // scalastyle:off println
      println("Iceberg is not on the classpath; skipping. Build with an Iceberg-enabled profile.")
      // scalastyle:on println
      return
    }

    withTempPath { warehouse =>
      configureIcebergHadoopCatalog(warehouse, catalog)

      runBenchmarkWithTable("Iceberg write", 4 * 1024 * 1024) { values =>
        withTempPath { dir =>
          withTempTable("parquetV1Table") {
            saveAsParquetV1Table(
              spark.sql(corpusQuery).repartition(sourceFiles).write,
              dir.getCanonicalPath + "/parquetV1")
            try {
              workloads(values).foreach(runWorkload(_, values))
            } finally {
              spark.sql(s"DROP TABLE IF EXISTS $targetTable")
            }
          }
        }
      }
    }
  }

  private def runWorkload(workload: Workload, values: Int): Unit = {
    // `warmupTime` is a deadline and `minTime` a total across the measured iterations, so both are
    // floors on a case's total cost rather than on one write. The floors are set below the fastest
    // write the corpus produces - the native writer is around half a second here - so that every
    // arm warms up exactly once and runs exactly `minNumIters` measured writes. Raising them above
    // that would warm the fast arms up more times than the slow ones and hand the fast arms a
    // better-jitted measurement.
    val benchmark = new Benchmark(
      workload.title,
      values,
      // The iceberg-java baseline is the least repeatable of the three arms - it carries a fifth of
      // its own runtime as spread between iterations, against a few percent for the two Comet arms
      // - and it is also the divisor of every `Relative` figure. Five iterations rather than the
      // usual three are cheap here and steady the number the table is read for.
      minNumIters = 5,
      warmupTime = 500.millis,
      minTime = 500.millis,
      output = output)

    arms.foreach { arm =>
      val label = verifyArm(benchmark, arm, workload)
      benchmark.addTimerCase(label) { timer =>
        // Neither an insert nor a copy-on-write delete is idempotent. Without rebuilding the table
        // each iteration, an insert would append to the previous iteration's files and a delete
        // would find nothing left to remove, so the measurement would drift on its own.
        resetTable(workload)
        timer.startTiming()
        withSQLConf(arm.confs: _*) { spark.sql(workload.statement) }
        timer.stopTiming()
      }
    }

    benchmark.run()
  }

  /**
   * Runs the workload's statement once under the arm's configuration and checks that the plan and
   * the result are what the arm's name claims, before any timing happens.
   *
   * A missing native write is reported rather than thrown, and the case is renamed, because a
   * recorded run that says which configuration fell back is more useful than no run at all. The
   * conditions that make the whole comparison meaningless - Comet not engaging at all, the wrong
   * number of rows landing in the table, or a case not being the case it is named for - do throw.
   *
   * @return
   *   the case name to time under.
   */
  private def verifyArm(benchmark: Benchmark, arm: Arm, workload: Workload): String = {
    resetTable(workload)
    val plans = capturePlans {
      withSQLConf(arm.confs: _*) { spark.sql(workload.statement) }
    }

    val remaining = spark.sql(s"SELECT count(*) FROM $targetTable").head().getLong(0)
    if (remaining != workload.expectedRowsAfter) {
      throw new IllegalStateException(
        s"${arm.name}: table holds $remaining rows, expected ${workload.expectedRowsAfter}")
    }

    val cometOps = collectAcross(plans) { case op if isComet(op) => op }
    if (arm.expectComet && cometOps.isEmpty) {
      throw new IllegalStateException(
        s"${arm.name}: no Comet operator in the plan, so this case does not measure Comet. " +
          s"Plans:\n${plans.mkString("\n--\n")}")
    }
    // The baseline is the divisor of every `Relative` figure, so it is checked in the other
    // direction as well. Comet's extensions are installed in this session and only held off by
    // `spark.comet.enabled`; if that gating ever stopped covering an operator, the baseline would
    // quietly become a partly-Comet run and every speedup in the table would be understated.
    if (!arm.expectComet && cometOps.nonEmpty) {
      throw new IllegalStateException(
        s"${arm.name}: expected stock Spark but the plan contains " +
          s"${cometOps.map(_.nodeName).distinct.mkString(", ")}. " +
          s"Plans:\n${plans.mkString("\n--\n")}")
    }

    workload.expectShuffle.foreach { expected =>
      val shuffled = collectAcross(plans) { case op if op.nodeName.contains("Exchange") => op }
      if (shuffled.nonEmpty != expected) {
        val had =
          if (shuffled.isEmpty) "none" else shuffled.map(_.nodeName).distinct.mkString(", ")
        throw new IllegalStateException(
          s"${arm.name}: '${workload.title}' expected an exchange in the plan to be $expected " +
            s"but found $had, so this case is not the write it is named for. " +
            s"Plans:\n${plans.mkString("\n--\n")}")
      }
    }

    // A sort node is the plan-visible mark of the required ordering that makes Iceberg select the
    // clustered writer rather than the fanout one (see `Workload.expectSort`). Checking it, not
    // just the exchange, is what keeps the clustered case from silently measuring the fanout writer.
    workload.expectSort.foreach { expected =>
      val sorted = collectAcross(plans) { case op if op.nodeName.contains("Sort") => op }
      if (sorted.nonEmpty != expected) {
        val had = if (sorted.isEmpty) "none" else sorted.map(_.nodeName).distinct.mkString(", ")
        throw new IllegalStateException(
          s"${arm.name}: '${workload.title}' expected a sort in the plan to be $expected but " +
            s"found $had, so it would measure the wrong writer. Iceberg reaches the clustered " +
            "writer only when the write has a required ordering; without one it uses the fanout " +
            s"writer. Plans:\n${plans.mkString("\n--\n")}")
      }
    }

    val nativeWrites = collectAcross(plans) { case write: CometIcebergWriteExec => write }
    // Checked in both directions, like the baseline Comet check above. The two Comet arms differ
    // only in the writer, so if the JVM-writer arm ever grew a `CometIcebergWriteExec` - most
    // likely because the native-write flags leaked in through the session defaults - it would
    // become a second native run printed under a JVM-writer label.
    if (!arm.expectNativeWrite && nativeWrites.nonEmpty) {
      throw new IllegalStateException(
        s"${arm.name}: expected the iceberg-java writer but the plan contains " +
          "CometIcebergWriteExec, so this case would measure the native writer under a " +
          "JVM-writer label. This arm pins the native-write flags off, so they are leaking in " +
          "from the session defaults (e.g. `-Dspark.comet.iceberg.write.enabled=true` in " +
          s"BENCH_MAVEN_OPTS). Plans:\n${plans.mkString("\n--\n")}")
    }
    if (arm.expectNativeWrite && nativeWrites.isEmpty) {
      warn(
        benchmark,
        s"${workload.title} / ${arm.name}: no CometIcebergWriteExec in the plan. The write fell " +
          "back to iceberg-java, so this case measures the JVM writer and the numbers below do " +
          "not describe the native writer.")
      s"${arm.name} (fell back to JVM writer)"
    } else {
      arm.name
    }
  }

  /**
   * Drops and recreates the workload's target table, so that each measured statement starts from
   * the same state.
   *
   * `DROP TABLE` on the Hadoop catalog deletes the table directory rather than only unlinking the
   * metadata, so the data files of the previous iteration go with it. Were that not so, the
   * warehouse would grow by a full corpus on every iteration of every arm and the later cases
   * would be timed against a progressively fuller disk.
   */
  private def resetTable(workload: Workload): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $targetTable")
    val properties = Seq("'format-version'='2'", "'write.parquet.compression-codec'='snappy'") ++
      workload.properties
    // `WHERE false` takes the schema from the source without writing anything, which avoids
    // restating the corpus as DDL and keeps the two in step.
    spark.sql(s"""
      CREATE TABLE $targetTable
      USING iceberg
      ${workload.partitionSpec}
      TBLPROPERTIES (${properties.mkString(", ")})
      AS SELECT * FROM parquetV1Table WHERE false
    """)
    workload.orderBy.foreach { col =>
      // Give the write a required ordering so Iceberg selects the clustered writer instead of
      // defaulting to fanout. `DISTRIBUTED BY PARTITION` keeps the hash exchange so the case still
      // shuffles; `LOCALLY ORDERED BY` adds the per-task sort the clustered writer needs to keep a
      // single file open without erroring when a partition it already closed would reappear.
      spark.sql(
        s"ALTER TABLE $targetTable WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY $col")
    }
    if (workload.prePopulate) {
      // Always stock Spark, whichever arm is about to be timed. The two writers roll files at
      // different points, so a table filled by the arm under test would hand each arm a different
      // number of files to rewrite, and the three timings would no longer be of the same work.
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.sql(s"INSERT INTO $targetTable SELECT * FROM parquetV1Table")
      }
    }
  }

  /**
   * Ten columns covering the encodings an Iceberg write has to produce: fixed width, dictionary
   * encoded and unique strings, binary, and the decimal and temporal types that go through their
   * own Parquet conversions. `c_str_dict` holds eight distinct values so Parquet dictionary
   * encodes it, which is the shape a low-cardinality column normally arrives in; `c_str` is
   * distinct per row.
   *
   * Every column carries one null in [[nullStride]]. The definition levels a nullable column
   * writes are part of what is being measured, so a corpus without nulls understates the writer.
   * The nulls are staggered by column position rather than placed on the same rows in every
   * column, because a row that is null in all ten columns is one definition-level pattern
   * repeated ten times, which both writers would compress better than a real table allows.
   */
  private def corpusQuery: String = {
    val columns = Seq(
      "c_bool" -> "PMOD(value, 2) = 0",
      "c_int" -> "CAST(PMOD(value, 2147483647) AS INT)",
      "c_long" -> "value",
      "c_double" -> "CAST(value AS DOUBLE) / 7",
      "c_dec" -> "CAST(PMOD(value, 100000000) AS DECIMAL(18,4))",
      partitionColumn -> "CAST(PMOD(value, 8) AS STRING)",
      "c_str" -> "REPEAT(CAST(value AS STRING), 3)",
      "c_bin" -> "CAST(CAST(value AS STRING) AS BINARY)",
      "c_date" -> "DATE_ADD(DATE '1970-01-01', CAST(PMOD(value, 20000) AS INT))",
      "c_ts" -> "TIMESTAMP_SECONDS(PMOD(value, 1600000000))")
    val projections = columns.zipWithIndex.map { case ((name, expr), position) =>
      s"IF(PMOD(value + $position, $nullStride) = 0, NULL, $expr) AS $name"
    }
    s"SELECT ${projections.mkString(", ")} FROM $tbl"
  }

  /** The executed plan of every query that succeeded while `action` ran. */
  private def capturePlans(action: => Unit): Seq[SparkPlan] = {
    val captured = mutable.Buffer.empty[SparkPlan]
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        captured += qe.executedPlan
      override def onFailure(
          funcName: String,
          qe: QueryExecution,
          exception: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)
    try {
      action
      // The listener bus delivers asynchronously, so the plans are not all in hand until it has
      // drained.
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    } finally {
      spark.listenerManager.unregister(listener)
    }
    captured.toSeq
  }

  private def collectAcross[A](plans: Seq[SparkPlan])(f: PartialFunction[SparkPlan, A]): Seq[A] =
    plans.flatMap(plan => collectWithSubqueries(plan)(f))

  private def isComet(plan: SparkPlan): Boolean = plan.nodeName.startsWith("Comet")

  private def icebergOnClasspath: Boolean =
    try {
      IcebergReflection.loadClass("org.apache.iceberg.spark.SparkCatalog")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
}
