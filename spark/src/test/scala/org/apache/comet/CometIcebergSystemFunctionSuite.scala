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

import java.math.{BigDecimal => JBigDecimal, BigInteger}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}

import scala.collection.mutable
import scala.util.Random

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.{CometListenerBusUtils, SparkConf}
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.comet.{CometIcebergWriteExec, CometSortExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.serde.{CometIcebergBucket, CometIcebergTruncate, CometStaticInvoke, Compatible, Unsupported}

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
  private val truncateColumns = Seq("i8", "i16", "i32", "i64", "dec18", "dec38", "str", "bin")

  test("bucket matches Iceberg for every supported type") {
    withSourceTable {
      for (column <- bucketColumns; numBuckets <- Seq(1, 7, 16, Int.MaxValue)) {
        checkSparkAnswerAndOperator(
          s"SELECT $column, $catalog.system.bucket($numBuckets, $column) FROM $source")
      }
    }
  }

  test("truncate matches Iceberg for every supported type") {
    withSourceTable {
      for (column <- truncateColumns; width <- Seq(1, 3, 10, 1000, Int.MaxValue)) {
        checkSparkAnswerAndOperator(
          s"SELECT $column, $catalog.system.truncate($width, $column) FROM $source")
      }
    }
  }

  test("years, months, days, and hours match Iceberg regardless of session timezone") {
    withSourceTable {
      // Iceberg evaluates the temporal transforms in UTC; a shifted session timezone must not
      // leak into the native result either.
      for (timezone <- Seq("UTC", "America/Los_Angeles", "Asia/Kathmandu")) {
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timezone) {
          for (column <- Seq("dt", "ts", "ts_ntz"); function <- Seq("years", "months", "days")) {
            checkSparkAnswerAndOperator(
              s"SELECT $column, $catalog.system.$function($column) FROM $source")
          }
          for (column <- Seq("ts", "ts_ntz")) {
            checkSparkAnswerAndOperator(
              s"SELECT $column, $catalog.system.hours($column) FROM $source")
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
      assert(
        collect(stripAQEPlan(df.queryExecution.executedPlan)) { case s: CometSortExec =>
          s
        }.nonEmpty,
        "expected a native sort")
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
      // which plans a shuffle and a local sort on the partition transforms.
      sql(s"""
        CREATE TABLE $table (i32 INT, str STRING, ts TIMESTAMP, dt DATE)
        USING iceberg
        PARTITIONED BY (bucket(4, i32), truncate(2, str), days(ts), months(dt))""")
      try {
        val plans = capturePlans {
          sql(s"INSERT INTO $table SELECT i32, str, ts, dt FROM $source")
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

        checkAnswer(
          sql(s"SELECT i32, str, ts, dt FROM $table"),
          sql(s"SELECT i32, str, ts, dt FROM $source").collect())

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
    def invoke(cls: Class[_], args: Seq[org.apache.spark.sql.catalyst.expressions.Expression]) =
      StaticInvoke(cls, IntegerType, "invoke", args, propagateNull = false)

    val bucketInt = Class.forName("org.apache.iceberg.spark.functions.BucketFunction$BucketInt")
    assert(
      CometIcebergBucket.getSupportLevel(
        invoke(bucketInt, Seq(Literal(4), value))) == Compatible())
    assert(
      CometIcebergBucket
        .getSupportLevel(invoke(bucketInt, Seq(Literal(4.toShort), value))) == Compatible())
    assert(
      CometIcebergBucket
        .getSupportLevel(invoke(bucketInt, Seq(Literal(0), value)))
        .isInstanceOf[Unsupported])
    assert(
      CometIcebergBucket
        .getSupportLevel(invoke(bucketInt, Seq(Literal(-4), value)))
        .isInstanceOf[Unsupported])
    assert(
      CometIcebergBucket
        .getSupportLevel(invoke(bucketInt, Seq(value, value)))
        .isInstanceOf[Unsupported])
    assert(
      CometIcebergBucket
        .getSupportLevel(invoke(bucketInt, Seq(Literal(4), AttributeReference("f", FloatType)())))
        .isInstanceOf[Unsupported])

    val truncateInt =
      Class.forName("org.apache.iceberg.spark.functions.TruncateFunction$TruncateInt")
    assert(
      CometIcebergTruncate
        .getSupportLevel(invoke(truncateInt, Seq(Literal(10), value))) == Compatible())
    assert(
      CometIcebergTruncate
        .getSupportLevel(
          invoke(truncateInt, Seq(Literal(10), AttributeReference("d", DateType)())))
        .isInstanceOf[Unsupported])

    // The dispatch in CometStaticInvoke goes by class name, so the same expressions resolve to
    // the Iceberg handlers there too.
    assert(
      CometStaticInvoke.getSupportLevel(
        invoke(bucketInt, Seq(Literal(4), value))) == Compatible())
    assert(
      CometStaticInvoke
        .getSupportLevel(invoke(bucketInt, Seq(Literal(0), value)))
        .isInstanceOf[Unsupported])
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
      withTempPath { dir =>
        sourceData().write.parquet(dir.getAbsolutePath)
        withParquetTable(dir.getAbsolutePath, source)(f)
      }
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
    def instant(micros: Long): Instant =
      Instant.ofEpochSecond(
        Math.floorDiv(micros, 1000000L),
        Math.floorMod(micros, 1000000L) * 1000)
    def localDateTime(micros: Long): LocalDateTime =
      LocalDateTime.ofEpochSecond(
        Math.floorDiv(micros, 1000000L),
        (Math.floorMod(micros, 1000000L) * 1000).toInt,
        ZoneOffset.UTC)

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
    val dec38Max = new JBigDecimal(BigInteger.TEN.pow(38).subtract(BigInteger.ONE), 10)
    val boundaryRows = Seq(
      Row(
        Byte.MinValue,
        Short.MinValue,
        Int.MinValue,
        Long.MinValue,
        new JBigDecimal("-99999999999999.9999"),
        dec38Max.negate(),
        "",
        Array.empty[Byte],
        LocalDate.ofEpochDay(0),
        Instant.EPOCH,
        LocalDateTime.of(1970, 1, 1, 0, 0)),
      Row(
        Byte.MaxValue,
        Short.MaxValue,
        Int.MaxValue,
        Long.MaxValue,
        new JBigDecimal("99999999999999.9999"),
        dec38Max,
        "日本語😀",
        Array[Byte](0, 1, 2, 3),
        LocalDate.ofEpochDay(-1),
        instant(-1L),
        localDateTime(-1L)),
      Row(
        0.toByte,
        0.toShort,
        0,
        0L,
        JBigDecimal.ZERO.setScale(4),
        JBigDecimal.ZERO.setScale(10),
        "a",
        Array[Byte](0),
        LocalDate.ofEpochDay(-365),
        instant(-86400000000L),
        localDateTime(-86400000000L - 1)),
      Row(
        (-1).toByte,
        (-1).toShort,
        -1,
        -1L,
        new JBigDecimal("-0.0001"),
        new JBigDecimal("-0.0000000001"),
        "iceberg",
        Array[Byte](-1, -1, -1, -1, -1),
        LocalDate.ofEpochDay(-366),
        instant(-3600000000L - 1),
        localDateTime(-3600000000L)),
      Row(null, null, null, null, null, null, null, null, null, null, null))
    spark.createDataFrame(
      spark.sparkContext.parallelize(randomRows ++ boundaryRows, 3),
      sourceSchema)
  }

  private def capturePlans(action: => Unit): Seq[SparkPlan] = {
    val captured = mutable.Buffer.empty[SparkPlan]
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        captured += qe.executedPlan
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
        ()
    }
    spark.listenerManager.register(listener)
    try {
      action
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    } finally {
      spark.listenerManager.unregister(listener)
    }
    captured.toSeq
  }
}
