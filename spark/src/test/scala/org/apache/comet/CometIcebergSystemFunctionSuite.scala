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

import java.io.File
import java.math.{BigDecimal => JBigDecimal, BigInteger}
import java.nio.file.Files
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}

import scala.util.Random

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.comet.{CometIcebergWriteExec, CometSortExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.serde.{CometExpressionSerde, CometIcebergBucket, CometIcebergTruncate, CometStaticInvoke, Compatible, SupportLevel, Unsupported}

/**
 * Native support for Iceberg's system functions (`bucket`, `truncate`, `years`, `months`, `days`,
 * `hours`).
 *
 * Every comparison runs the same query with Comet on and off, so the reference values come from
 * Iceberg's own JVM implementations (`BucketFunction`, `TruncateFunction`, ...) evaluated by
 * Spark, over seeded random data plus the boundary values of each type. A native result that
 * disagreed with Iceberg would only fail loudly on the write path (the clustered writer rejects
 * out-of-order partitions); in a filter or projection it would be a silently wrong answer, which
 * is why the coverage is per type rather than a few hand-picked rows.
 */
class CometIcebergSystemFunctionSuite
    extends CometTestBase
    with CometIcebergTestBase
    with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key, "true")
      .set(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key, "true")
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      assume(icebergAvailable, "Iceberg not available in classpath")
      testFun
    }
  }

  private val catalog = "ice"
  private val source = "system_function_source"
  private val bucketColumns =
    Seq("i8", "i16", "i32", "i64", "dec18", "dec38", "str", "bin", "dt", "ts", "ts_ntz")
  private val truncateColumns = Seq("i8", "i16", "i32", "i64", "str", "bin")
  private val decimalColumns = Seq("dec18", "dec38")

  // The source data is written once per suite; every test reads the same parquet directory.
  private var sourceDir: File = _
  private def sourcePath: String = new File(sourceDir, "data").getAbsolutePath

  override def beforeAll(): Unit = {
    super.beforeAll()
    sourceDir = Files.createTempDirectory("comet-iceberg-system-functions").toFile
    // Three Spark 3.x defaults differ from Spark 4's in ways that block writing this corpus. All
    // are set to Spark 4's value so one corpus works on every profile, and none affects how a
    // result is compared, since every test reads the data back from parquet.
    //
    //   - `datetimeJava8ApiEnabled`: `sourceData` supplies java.time values for the date and
    //     timestamp columns. Spark 4 resolves those external types; on Spark 3.x the row encoder
    //     expects java.sql.Date / java.sql.Timestamp instead. The encoder is built here on the
    //     driver, so setting the flag around the write is enough.
    //   - `datetimeRebaseModeInWrite`: the timestamp corpus reaches back to 1843, and the corpus
    //     is deliberately pre-epoch in places, since the temporal transforms go negative before
    //     1970. Spark 3.x throws on writing a timestamp before 1900; Spark 4 defaults to
    //     CORRECTED, which writes the value as-is.
    //   - `outputTimestampType`: Spark 3.x defaults to INT96, which has its own separate ancient
    //     timestamp check. Spark 4 defaults to TIMESTAMP_MICROS, which is also what Iceberg
    //     itself writes.
    withSQLConf(
      SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true",
      SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> "CORRECTED",
      SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS") {
      sourceData().write.parquet(sourcePath)
    }
  }

  override def afterAll(): Unit = {
    try deleteRecursively(sourceDir)
    finally super.afterAll()
  }

  test("bucket matches Iceberg for every supported type") {
    withSourceTable {
      bucketColumns.foreach { column =>
        val buckets =
          Seq(1, 7, 16, Int.MaxValue).map(n => s"$catalog.system.bucket($n, $column)")
        checkSparkAnswerAndOperator(s"SELECT $column, ${buckets.mkString(", ")} FROM $source")
      }
    }
  }

  test("truncate matches Iceberg for every supported type") {
    withSourceTable {
      truncateColumns.foreach { column =>
        val truncated =
          Seq(1, 3, 10, 1000, Int.MaxValue).map(w => s"$catalog.system.truncate($w, $column)")
        checkSparkAnswerAndOperator(s"SELECT $column, ${truncated.mkString(", ")} FROM $source")
      }
    }
  }

  test("truncate on a decimal falls back to Spark") {
    withSourceTable {
      // Iceberg's `TruncateDecimal` can return a value wider than the column's precision, which
      // Spark nulls only on materialization and a `Decimal128(p, s)` array cannot represent at
      // all; the expression stays with Spark rather than null early. `bucket` on the same columns
      // is unaffected and is covered by the bucket test above.
      decimalColumns.foreach { column =>
        checkSparkAnswerAndFallbackReason(
          s"SELECT $catalog.system.truncate(10, $column) FROM $source",
          "Decimal128(precision, scale) array cannot carry that intermediate")
      }
    }
  }

  test("years, months, days, and hours match Iceberg regardless of session timezone") {
    withSourceTable {
      // Iceberg evaluates the temporal transforms in UTC; a shifted session timezone must not
      // leak into the native result either.
      for (timezone <- Seq("UTC", "America/Los_Angeles", "Asia/Kathmandu")) {
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timezone) {
          Seq("dt", "ts", "ts_ntz").foreach { column =>
            val functions = Seq("years", "months", "days") ++ (if (column == "dt") Nil
                                                               else Seq("hours"))
            val transformed = functions.map(f => s"$catalog.system.$f($column)")
            checkSparkAnswerAndOperator(
              s"SELECT $column, ${transformed.mkString(", ")} FROM $source")
          }
        }
      }
    }
  }

  test("system functions in filters stay native") {
    withSourceTable {
      checkSparkAnswerAndOperator(
        s"SELECT i32 FROM $source WHERE $catalog.system.bucket(8, i32) IN (0, 3)")
      checkSparkAnswerAndOperator(
        s"SELECT str FROM $source WHERE $catalog.system.truncate(1, str) = 'a'")
      checkSparkAnswerAndOperator(
        s"SELECT ts FROM $source WHERE $catalog.system.days(ts) >= DATE '2000-01-01'")
      checkSparkAnswerAndOperator(s"SELECT dt FROM $source WHERE $catalog.system.months(dt) < 0")
    }
  }

  test("sorting on system functions stays native") {
    withSourceTable {
      val df = sql(
        s"SELECT i32, str FROM $source " +
          s"ORDER BY $catalog.system.bucket(4, i32), $catalog.system.truncate(2, str), i32, str")
      checkSparkAnswerAndOperator(df)
      val sorts = collect(stripAQEPlan(df.queryExecution.executedPlan)) { case s: CometSortExec =>
        s
      }
      assert(sorts.nonEmpty, "expected a native sort")
    }
  }

  test("hash partitioning on system functions uses the native shuffle") {
    withSourceTable {
      val df = sql(
        s"SELECT i32, str, ts FROM $source " +
          s"DISTRIBUTE BY $catalog.system.bucket(8, i32), $catalog.system.truncate(2, str), " +
          s"$catalog.system.hours(ts)")
      checkSparkAnswerAndOperator(df)
      checkCometExchange(df, 1, native = true)
    }
  }

  test("partitioned Iceberg write with default distribution mode stays native end to end") {
    withSourceTable {
      val table = s"$catalog.db.hidden_partitioning"
      // No `write.distribution-mode`: Iceberg picks hash distribution for a partitioned table,
      // which plans a shuffle and a local sort on the partition transforms. The string column is
      // a partition source on purpose: its values include multi-byte characters and a surrogate
      // pair, which end up in partition directory names. Those names were written raw until
      // apache/iceberg-rust#2875, which #5651 picked up, so this also covers the URL escaping
      // iceberg-java's `PartitionSpec.partitionToPath` applies.
      sql(s"""
        CREATE TABLE $table (i32 INT, str STRING, ts TIMESTAMP, dt DATE)
        USING iceberg
        PARTITIONED BY (bucket(4, i32), truncate(2, str), days(ts), months(dt))""")
      val rows = s"SELECT i32, str, ts, dt FROM $source"
      try {
        val plans = capturePlans(spark) {
          sql(s"INSERT INTO $table $rows")
        }
        val writePlans = plans.filter(plan =>
          collectWithSubqueries(plan) { case w: CometIcebergWriteExec => w }.nonEmpty)
        assert(
          writePlans.nonEmpty,
          s"expected a native Iceberg write in the captured plans:\n${plans.mkString("\n--\n")}")
        writePlans.foreach { plan =>
          val cometShuffles = collectWithSubqueries(plan) { case s: CometShuffleExchangeExec =>
            s
          }
          assert(cometShuffles.nonEmpty, s"expected a Comet shuffle in $plan")
          cometShuffles.foreach(s => assert(s.shuffleType == CometNativeShuffle, s"$s"))
          assert(
            collectWithSubqueries(plan) { case s: ShuffleExchangeExec => s }.isEmpty,
            s"the distribution shuffle stayed on Spark:\n$plan")
        }

        checkAnswer(sql(s"SELECT i32, str, ts, dt FROM $table"), sql(rows).collect())

        // Iceberg's own view of the partitions must match what the JVM transforms compute over
        // the written rows: one partition per distinct transform tuple.
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          val expected = sql(s"""
            SELECT COUNT(*) FROM (
              SELECT DISTINCT $catalog.system.bucket(4, i32), $catalog.system.truncate(2, str),
                $catalog.system.days(ts), $catalog.system.months(dt)
              FROM $table)""").collect().head.getLong(0)
          val actual = sql(s"SELECT COUNT(*) FROM $table.partitions").collect().head.getLong(0)
          assert(actual == expected, s"expected $expected Iceberg partitions, found $actual")
        }
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("non-literal or non-positive parameters fall back to Spark") {
    withSourceTable {
      checkSparkAnswerAndFallbackReason(
        s"SELECT $catalog.system.bucket(pmod(i32, 100) + 1, i32) FROM $source " +
          "WHERE i32 IS NOT NULL",
        "numBuckets must be a positive integer literal")
      // Iceberg's Java implementation divides by the width, so a zero width has to stay with
      // Spark to raise the same error; only the planning decision can be checked here.
      val plan =
        sql(s"SELECT $catalog.system.truncate(0, str) FROM $source").queryExecution.executedPlan
      val reasons = new ExtendedExplainInfo().getFallbackReasons(plan)
      assert(
        reasons.exists(_.contains("width must be a positive integer literal")),
        s"unexpected fallback reasons: $reasons")
    }
  }

  test("support levels follow Iceberg's bind rules") {
    val value = AttributeReference("v", IntegerType)()
    val float = AttributeReference("f", FloatType)()
    val date = AttributeReference("d", DateType)()
    def level(
        serde: CometExpressionSerde[StaticInvoke],
        cls: Class[_],
        args: Expression*): SupportLevel =
      serde.getSupportLevel(StaticInvoke(cls, IntegerType, "invoke", args, propagateNull = false))

    val bucketInt = Class.forName("org.apache.iceberg.spark.functions.BucketFunction$BucketInt")
    assert(level(CometIcebergBucket, bucketInt, Literal(4), value) == Compatible())
    assert(level(CometIcebergBucket, bucketInt, Literal(4.toShort), value) == Compatible())
    assert(level(CometIcebergBucket, bucketInt, Literal(0), value).isInstanceOf[Unsupported])
    assert(level(CometIcebergBucket, bucketInt, Literal(-4), value).isInstanceOf[Unsupported])
    assert(level(CometIcebergBucket, bucketInt, value, value).isInstanceOf[Unsupported])
    assert(level(CometIcebergBucket, bucketInt, Literal(4), float).isInstanceOf[Unsupported])

    val truncateInt =
      Class.forName("org.apache.iceberg.spark.functions.TruncateFunction$TruncateInt")
    assert(level(CometIcebergTruncate, truncateInt, Literal(10), value) == Compatible())
    assert(level(CometIcebergTruncate, truncateInt, Literal(10), date).isInstanceOf[Unsupported])

    // A decimal value reports its own reason, but only once the width is valid: a zero width has
    // to keep reporting the width, since that is what decides whether Iceberg's own
    // ArithmeticException is raised.
    val decimal = AttributeReference("dec", DecimalType(18, 4))()
    val decimalLevel = level(CometIcebergTruncate, truncateInt, Literal(10), decimal)
    assert(
      decimalLevel == Unsupported(Some(CometIcebergTruncate.DecimalNote)),
      s"unexpected support level: $decimalLevel")
    val zeroWidth = level(CometIcebergTruncate, truncateInt, Literal(0), decimal)
    assert(
      zeroWidth.asInstanceOf[Unsupported].notes.exists(_.contains("width must be a positive")),
      s"unexpected support level: $zeroWidth")
    // `bucket` on a decimal stays native; only `truncate` has the precision problem.
    val bucketDecimal =
      Class.forName("org.apache.iceberg.spark.functions.BucketFunction$BucketDecimal")
    assert(level(CometIcebergBucket, bucketDecimal, Literal(4), decimal) == Compatible())

    // The reason also has to reach the generated compatibility guide, which only asks the serde
    // registered for `StaticInvoke`.
    assert(
      CometStaticInvoke
        .getUnsupportedReasons()
        .exists(_.contains(CometIcebergTruncate.DecimalNote)),
      "the decimal truncate note is missing from CometStaticInvoke.getUnsupportedReasons")

    // CometStaticInvoke dispatches on (functionName, class name), so the same expressions reach
    // the Iceberg handlers from there too.
    assert(level(CometStaticInvoke, bucketInt, Literal(4), value) == Compatible())
    assert(level(CometStaticInvoke, bucketInt, Literal(0), value).isInstanceOf[Unsupported])
    assert(
      level(CometStaticInvoke, truncateInt, Literal(10), decimal) ==
        Unsupported(Some(CometIcebergTruncate.DecimalNote)))
  }

  test("fallback reason for an unlisted static invoke names the declaring class") {
    val expr = StaticInvoke(
      classOf[java.lang.Math],
      IntegerType,
      "abs",
      Seq(AttributeReference("v", IntegerType)()),
      propagateNull = false)
    assert(CometStaticInvoke.convert(expr, Seq.empty, binding = false).isEmpty)
    val reasons = expr.getTagValue(CometExplainInfo.FALLBACK_REASONS).getOrElse(Set.empty)
    assert(
      reasons.exists(r =>
        r.contains("Static invoke expression: abs is not supported") &&
          r.contains("java.lang.Math")),
      s"unexpected fallback reasons: $reasons")
  }

  /** Runs `f` with the Iceberg catalog registered and the source parquet table in scope. */
  private def withSourceTable(f: => Unit): Unit = withTempIcebergDir { warehouseDir =>
    withSQLConf(
      s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$catalog.type" -> "hadoop",
      s"spark.sql.catalog.$catalog.warehouse" -> warehouseDir.getAbsolutePath) {
      withParquetTable(sourcePath, source)(f)
    }
  }

  private val sourceSchema = StructType(
    Seq(
      StructField("i8", ByteType),
      StructField("i16", ShortType),
      StructField("i32", IntegerType),
      StructField("i64", LongType),
      StructField("dec18", DecimalType(18, 4)),
      StructField("dec38", DecimalType(38, 10)),
      StructField("str", StringType),
      StructField("bin", BinaryType),
      StructField("dt", DateType),
      StructField("ts", TimestampType),
      StructField("ts_ntz", TimestampNTZType)))

  private def instant(micros: Long): Instant =
    Instant.ofEpochSecond(Math.floorDiv(micros, 1000000L), Math.floorMod(micros, 1000000L) * 1000)

  private def localDateTime(micros: Long): LocalDateTime =
    LocalDateTime.ofInstant(instant(micros), ZoneOffset.UTC)

  /**
   * Seeded random rows with nulls in every column, followed by the boundary values of each type
   * (numeric extremes, the epoch and the microsecond before it, empty and multi-byte strings).
   */
  private def sourceData(): DataFrame = {
    val random = new Random(42)
    // Code points rather than chars so that a surrogate pair is never split.
    val alphabet = Seq("a", "b", "c", " ", "é", "日", "本", "語", "😀")
    def maybeNull(value: => Any): Any = if (random.nextInt(8) == 0) null else value
    def randomString(): String =
      Seq.fill(random.nextInt(12))(alphabet(random.nextInt(alphabet.size))).mkString
    def randomBinary(): Array[Byte] = {
      val bytes = new Array[Byte](random.nextInt(10))
      random.nextBytes(bytes)
      bytes
    }
    def randomDecimal38(): JBigDecimal = {
      val unscaled = new BigInteger(126, random.self)
      new JBigDecimal(if (random.nextBoolean()) unscaled else unscaled.negate(), 10)
    }
    def randomMicros(): Long = random.nextLong() % 4000000000000000L

    val randomRows = (0 until 400).map { _ =>
      Row(
        maybeNull(random.nextInt().toByte),
        maybeNull(random.nextInt().toShort),
        maybeNull(random.nextInt()),
        maybeNull(random.nextLong()),
        maybeNull(JBigDecimal.valueOf(random.nextLong() % 100000000000000000L, 4)),
        maybeNull(randomDecimal38()),
        maybeNull(randomString()),
        maybeNull(randomBinary()),
        maybeNull(LocalDate.ofEpochDay(random.nextInt(40000) - 20000)),
        maybeNull(instant(randomMicros())),
        maybeNull(localDateTime(randomMicros())))
    }

    // One list of boundary values per column, in schema order; transposed into rows below so
    // each type's cases sit together (and a list of the wrong length fails loudly).
    val dec38Max = new JBigDecimal(BigInteger.TEN.pow(38).subtract(BigInteger.ONE), 10)
    val boundaryColumns: Seq[Seq[Any]] = Seq(
      Seq(Byte.MinValue, Byte.MaxValue, 0.toByte, (-1).toByte, null),
      Seq(Short.MinValue, Short.MaxValue, 0.toShort, (-1).toShort, null),
      Seq(Int.MinValue, Int.MaxValue, 0, -1, null),
      Seq(Long.MinValue, Long.MaxValue, 0L, -1L, null),
      Seq(
        new JBigDecimal("-99999999999999.9999"),
        new JBigDecimal("99999999999999.9999"),
        JBigDecimal.ZERO.setScale(4),
        new JBigDecimal("-0.0001"),
        null),
      Seq(
        dec38Max.negate(),
        dec38Max,
        JBigDecimal.ZERO.setScale(10),
        new JBigDecimal("-0.0000000001"),
        null),
      Seq("", "日本語😀", "a", "iceberg", null),
      Seq(
        Array.empty[Byte],
        Array[Byte](0, 1, 2, 3),
        Array[Byte](0),
        Array.fill[Byte](5)(-1),
        null),
      Seq(
        LocalDate.ofEpochDay(0),
        LocalDate.ofEpochDay(-1),
        LocalDate.ofEpochDay(-365),
        LocalDate.ofEpochDay(-366),
        null),
      Seq(Instant.EPOCH, instant(-1L), instant(-86400000000L), instant(-3600000000L - 1), null),
      Seq(
        LocalDateTime.of(1970, 1, 1, 0, 0),
        localDateTime(-1L),
        localDateTime(-86400000000L - 1),
        localDateTime(-3600000000L),
        null))
    val boundaryRows = boundaryColumns.transpose.map(Row.fromSeq)

    spark.createDataFrame(
      spark.sparkContext.parallelize(randomRows ++ boundaryRows, 3),
      sourceSchema)
  }
}
