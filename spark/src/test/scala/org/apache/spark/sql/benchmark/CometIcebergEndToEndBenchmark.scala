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

import scala.concurrent.duration._

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.comet.{CometIcebergNativeScanExec, CometIcebergWriteExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions

/**
 * Benchmark of a whole Iceberg pipeline, with Comet on and off: read an Iceberg table, hash
 * repartition it on a key, and write the result to a second Iceberg table. Each case times one
 * `INSERT INTO target SELECT * FROM source` statement with a `REPARTITION(10, id)` hint, every
 * stage of which Comet can run natively: the iceberg-rust scan, the native shuffle, and the
 * iceberg-rust writer.
 *
 * The repartition hashes a primitive key, which Comet's native shuffle supports with complex
 * columns carried alongside it. A round-robin `REPARTITION(10)` would not measure a native
 * pipeline: Comet runs round-robin partitioning on its JVM shuffle unless
 * `spark.comet.shuffle.native.partitioning.roundrobin.enabled` is set.
 *
 * The statement is measured over five groups of column types, one results table each: primitives,
 * structs, arrays, maps, and types nested inside one another. The source table is written once
 * per group, by stock Spark, so every configuration reads the same files.
 *
 * Each group is measured under three configurations:
 *
 *   1. `Spark` - stock Spark, the baseline the `Relative` column is computed against.
 *   1. `Comet scan + shuffle` - Comet reads the source and shuffles natively, and iceberg-java
 *      writes the data files. This is what Comet does by default today.
 *   1. `Comet scan + shuffle + native write` - the write is delegated to iceberg-rust as well, so
 *      the whole statement runs natively apart from the driver-side commit.
 *
 * What the numbers do and do not cover:
 *   - The timed statement includes the driver-side Iceberg commit, which all three configurations
 *     pay equally.
 *   - The warehouse is a local temporary directory, so no object-store latency is included.
 *   - The two writers choose different file roll points (see the accepted divergences in
 *     `docs/source/user-guide/latest/iceberg-writes.md`), so the resulting file layouts are not
 *     expected to match. What is checked is that every configuration writes the same rows.
 *   - Every group has the same row count but not the same row width, so `Rate` and `Per Row`
 *     compare configurations within a group, not one group with another.
 *
 * [[CometIcebergWriteBenchmark]] measures the writer on its own, from a Parquet source.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometIcebergEndToEndBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometIcebergEndToEndBenchmark-**results.txt".
 */
object CometIcebergEndToEndBenchmark extends CometBenchmarkBase {

  private val catalog = defaultIcebergCatalog
  private val sourceTable = s"$catalog.db.e2e_source"
  private val targetTable = s"$catalog.db.e2e_target"

  /** One null in eight in every column, and in the elements and fields nested inside them. */
  private val nullStride = 8

  /**
   * The source's file count, and the partition count of the repartition. Both are pinned so that
   * neither the corpus size nor the machine's core count can change the parallelism, and both are
   * twice the session's five cores, so each stage runs two full waves of tasks.
   */
  private val sourceFiles = 10
  private val shufflePartitions = 10

  private val statement =
    s"INSERT INTO $targetTable SELECT /*+ REPARTITION($shufflePartitions, id) */ * " +
      s"FROM $sourceTable"

  private val tableProperties =
    Seq("'format-version'='2'", "'write.parquet.compression-codec'='snappy'")

  /**
   * How a case is configured and what its plan must contain, which [[verifyArm]] checks before
   * anything is timed.
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
      "Comet scan + shuffle",
      Seq(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        // Pinned off rather than left to their defaults, so that a `-Dspark.comet.write.*` in
        // BENCH_MAVEN_OPTS cannot turn this arm into a second copy of the native-write one.
        CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key -> "false",
        CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key -> "false"),
      expectComet = true,
      expectNativeWrite = false),
    Arm(
      "Comet scan + shuffle + native write",
      Seq(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        // The native writer requires the split-operator plan; enabling it alone is a no-op.
        CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key -> "true"),
      expectComet = true,
      expectNativeWrite = true))

  /** Columns that are measured together, as one results table. */
  private case class TypeGroup(name: String, columns: Seq[(String, String)])

  /** Entries in an array or a map: zero to nine, so one array or map in ten is empty. */
  private def entryCount(salt: String): String =
    s"CAST(PMOD(SHIFTRIGHT(value, 8) + $salt, 10) AS INT)"

  /**
   * An array of [[entryCount]] elements built by `element` from the element index `i`, with one
   * element in [[nullStride]] null. `x` and `i` name the lambda's variables, so that an array can
   * be built inside the lambda of another one, and `salt` varies the length between columns.
   */
  private def arrayOf(element: String, x: String = "x", i: String = "i", salt: String = "0") =
    s"TRANSFORM(ARRAY_REPEAT(0, ${entryCount(salt)}), ($x, $i) -> " +
      s"IF(PMOD(value + $i * 3, $nullStride) = 0, NULL, $element))"

  /**
   * A map of [[entryCount]] entries built from the entry index `i`. `key` must give each entry a
   * distinct key, since map keys are unique and non-null; one value in [[nullStride]] is null.
   */
  private def mapOf(key: String, entryValue: String) =
    s"MAP_FROM_ENTRIES(TRANSFORM(ARRAY_REPEAT(0, ${entryCount("1")}), (x, i) -> " +
      s"STRUCT($key, IF(PMOD(value + i * 3, $nullStride) = 0, NULL, $entryValue))))"

  /**
   * The five groups of column types. The primitives are the corpus of
   * [[CometIcebergWriteBenchmark]]; the other groups add the definition and repetition levels a
   * nested column writes. Nulls are placed at every level: on the column, on a struct's fields,
   * and on the elements of arrays and the values of maps.
   */
  private val typeGroups: Seq[TypeGroup] = Seq(
    TypeGroup(
      "primitives",
      Seq(
        "c_bool" -> "PMOD(value, 2) = 0",
        "c_int" -> "CAST(PMOD(value, 2147483647) AS INT)",
        "c_long" -> "value",
        "c_double" -> "CAST(value AS DOUBLE) / 7",
        "c_dec" -> "CAST(PMOD(value, 100000000) AS DECIMAL(18,4))",
        "c_str_dict" -> "CAST(PMOD(value, 8) AS STRING)",
        "c_str" -> "REPEAT(CAST(value AS STRING), 3)",
        "c_bin" -> "CAST(CAST(value AS STRING) AS BINARY)",
        "c_date" -> "DATE_ADD(DATE '1970-01-01', CAST(PMOD(value, 20000) AS INT))",
        "c_ts" -> "TIMESTAMP_SECONDS(PMOD(value, 1600000000))")),
    TypeGroup(
      "structs",
      Seq(
        "s_flat" -> ("NAMED_STRUCT(" +
          "'i', CAST(PMOD(value, 1000000) AS INT), " +
          "'l', value, " +
          "'d', CAST(value AS DOUBLE) / 7, " +
          s"'s', IF(PMOD(SHIFTRIGHT(value, 4), $nullStride) = 0, NULL, CAST(value AS STRING)), " +
          "'dt', DATE_ADD(DATE '1970-01-01', CAST(PMOD(value, 20000) AS INT)))"),
        "s_nested" -> ("NAMED_STRUCT(" +
          "'id', value, " +
          s"'inner', IF(PMOD(SHIFTRIGHT(value, 4), $nullStride) = 1, NULL, NAMED_STRUCT(" +
          "'x', CAST(PMOD(value, 1000) AS INT), " +
          "'y', CONCAT('y', CAST(PMOD(value, 100000) AS STRING)))), " +
          "'flag', PMOD(value, 3) = 0)"))),
    TypeGroup(
      "arrays",
      Seq(
        "a_int" -> arrayOf("CAST(PMOD(value, 1000000) + i AS INT)"),
        "a_double" -> arrayOf("CAST(PMOD(value, 1000000) + i AS DOUBLE) / 7", salt = "1"),
        "a_str" -> arrayOf("CONCAT('e', CAST(PMOD(value, 100000) + i AS STRING))", salt = "2"))),
    TypeGroup(
      "maps",
      Seq(
        "m_str_int" -> mapOf(
          "CONCAT('k', CAST(i AS STRING))",
          "CAST(PMOD(value, 1000) + i AS INT)"),
        "m_int_str" -> mapOf("i", "CONCAT('v', CAST(PMOD(value, 100000) + i AS STRING))"))),
    TypeGroup(
      "nested",
      Seq(
        "n_array_struct" -> arrayOf(
          "NAMED_STRUCT('k', CAST(PMOD(value, 1000) + i AS INT), " +
            "'v', CONCAT('v', CAST(PMOD(value, 100000) + i AS STRING)))"),
        "n_map_array" -> mapOf(
          "CONCAT('k', CAST(i AS STRING))",
          arrayOf("CAST(PMOD(value, 1000) + i * 10 + j AS INT)", x = "y", i = "j", salt = "i")),
        "n_struct_mix" -> {
          val tags = arrayOf("CONCAT('t', CAST(PMOD(value + i, 50) AS STRING))", salt = "3")
          val attrs =
            mapOf("CONCAT('a', CAST(i AS STRING))", "CAST(PMOD(value, 1000) + i AS INT)")
          s"NAMED_STRUCT('tags', $tags, 'attrs', $attrs)"
        })))

  /**
   * `spark.sql.extensions` is static, so the Iceberg extensions are installed here, as a
   * production Iceberg deployment installs them, rather than through `withSQLConf`. The master is
   * `local[5]`, as in [[CometIcebergWriteBenchmark]], so that each stage runs its tasks in
   * parallel as it would on a cluster, rather than one at a time.
   */
  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometIcebergEndToEndBenchmark")
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

      runBenchmarkWithTable("Iceberg read, repartition, and write", 2 * 1024 * 1024) { values =>
        try {
          typeGroups.foreach(runTypeGroup(_, values))
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS $targetTable")
          spark.sql(s"DROP TABLE IF EXISTS $sourceTable")
        }
      }
    }
  }

  private def runTypeGroup(group: TypeGroup, values: Int): Unit = {
    createSource(group)
    val expected = checksum(sourceTable)
    if (expected.rows != values) {
      throw new IllegalStateException(
        s"${group.name}: the source holds ${expected.rows} rows, expected $values")
    }

    // As in `CometIcebergWriteBenchmark`, the time floors are below a single statement, so every
    // arm warms up once and then runs exactly `minNumIters` measured statements.
    val benchmark = new Benchmark(
      s"Iceberg read, repartition, and write: ${group.name}",
      values.toLong,
      minNumIters = 5,
      warmupTime = 500.millis,
      minTime = 500.millis,
      output = output)

    arms.foreach { arm =>
      val label = verifyArm(benchmark, arm, group, expected)
      benchmark.addTimerCase(label) { timer =>
        // An insert is not idempotent, so each statement gets a fresh, empty target.
        resetTarget()
        timer.startTiming()
        withSQLConf(arm.confs: _*) { spark.sql(statement) }
        timer.stopTiming()
      }
    }

    benchmark.run()
  }

  /**
   * Writes the group's corpus to a fresh source table with stock Spark, so that every arm reads
   * the same [[sourceFiles]] files. Each column carries one null in [[nullStride]], staggered by
   * column position so that no row is null in every column. `id` is never null, so the
   * repartition spreads the rows evenly instead of sending every null key to one partition.
   */
  private def createSource(group: TypeGroup): Unit = {
    val projections = "value AS id" +: group.columns.zipWithIndex.map {
      case ((name, expr), position) =>
        s"IF(PMOD(value + $position, $nullStride) = 0, NULL, $expr) AS $name"
    }
    // Iceberg packs small files together into scan tasks, sized by the session's parallelism. An
    // open-file cost of the default 128 MiB split size gives every source file a task of its own
    // instead, so the scan runs `sourceFiles` tasks, for Spark and for Comet's native scan alike.
    val properties = tableProperties :+ "'read.split.open-file-cost'='134217728'"
    spark.sql(s"DROP TABLE IF EXISTS $sourceTable")
    spark.sql(s"""
      CREATE TABLE $sourceTable
      USING iceberg
      TBLPROPERTIES (${properties.mkString(", ")})
      AS SELECT /*+ REPARTITION($sourceFiles) */ ${projections.mkString(", ")} FROM $tbl
    """)
  }

  /**
   * Drops and recreates the target, so that each measured statement starts from an empty table.
   * `DROP TABLE` on the Hadoop catalog deletes the table directory, so the files of the previous
   * statement go with it.
   */
  private def resetTarget(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $targetTable")
    // `WHERE false` takes the schema from the source without writing anything.
    spark.sql(s"""
      CREATE TABLE $targetTable
      USING iceberg
      TBLPROPERTIES (${tableProperties.mkString(", ")})
      AS SELECT * FROM $sourceTable WHERE false
    """)
  }

  private case class Checksum(rows: Long, hash: java.math.BigDecimal)

  /**
   * The row count and an order-independent hash of every row's JSON form. Both are read with
   * Comet off, so the two writers' files are compared by iceberg-java's reader and not by either
   * one of the configurations under test.
   */
  private def checksum(table: String): Checksum = {
    val row = spark
      .sql(
        "SELECT COUNT(*), SUM(CAST(XXHASH64(TO_JSON(STRUCT(*))) AS DECIMAL(38, 0))) " +
          s"FROM $table")
      .head()
    Checksum(row.getLong(0), row.getDecimal(1))
  }

  /**
   * Runs the statement once under the arm's configuration and checks the plan and the written
   * rows before any timing happens.
   *
   * A stage of a Comet arm that does not run natively is reported rather than thrown, and the
   * case is renamed, because a recorded run that says what fell back is more useful than no run
   * at all. The conditions that make the comparison meaningless do throw: different rows in the
   * target, a plan that is not one repartition between the scan and the write, Comet in the Spark
   * arm or missing from a Comet arm, or the native writer in the JVM-writer arm.
   *
   * @return
   *   the case name to time under.
   */
  private def verifyArm(
      benchmark: Benchmark,
      arm: Arm,
      group: TypeGroup,
      expected: Checksum): String = {
    resetTarget()
    val plans = capturePlans {
      withSQLConf(arm.confs: _*) { spark.sql(statement) }
    }
    def fail(problem: String): Nothing =
      throw new IllegalStateException(
        s"${group.name} / ${arm.name}: $problem. Plans:\n${plans.mkString("\n--\n")}")

    val written = checksum(targetTable)
    if (written != expected) {
      fail(
        s"the target holds ${written.rows} rows with hash ${written.hash}, but the source holds " +
          s"${expected.rows} with hash ${expected.hash}")
    }

    val exchanges = collectAcross(plans) { case e: ShuffleExchangeLike => e }
    if (exchanges.size != 1) {
      fail(s"expected the repartition to be the plan's only exchange, found ${exchanges.size}")
    }

    val cometOps = collectAcross(plans) { case op if isComet(op) => op }
    if (!arm.expectComet) {
      if (cometOps.nonEmpty) {
        fail(
          s"expected stock Spark but found ${cometOps.map(_.nodeName).distinct.mkString(", ")}")
      }
      return arm.name
    }
    if (cometOps.isEmpty) {
      fail("no Comet operator in the plan, so this case does not measure Comet")
    }

    val nativeWrites = collectAcross(plans) { case w: CometIcebergWriteExec => w }
    if (!arm.expectNativeWrite && nativeWrites.nonEmpty) {
      fail(
        "expected the iceberg-java writer but found CometIcebergWriteExec. This arm pins the " +
          "native-write flags off, so they are leaking in from the session defaults")
    }

    val nativeScans = collectAcross(plans) { case s: CometIcebergNativeScanExec => s }
    val nativeShuffles = collectAcross(plans) {
      case s: CometShuffleExchangeExec if s.shuffleType == CometNativeShuffle => s
    }
    val notNative = Seq(
      "scan" -> nativeScans.isEmpty,
      "shuffle" -> nativeShuffles.isEmpty,
      "write" -> (arm.expectNativeWrite && nativeWrites.isEmpty)).filter(_._2).map(_._1)
    if (notNative.isEmpty) {
      arm.name
    } else {
      warn(
        benchmark,
        s"${group.name} / ${arm.name}: the ${notNative.mkString(", ")} did not run natively, so " +
          "the numbers below do not describe a fully native pipeline. " +
          s"Plans:\n${plans.mkString("\n--\n")}")
      s"${arm.name} (not native: ${notNative.mkString(", ")})"
    }
  }
}
