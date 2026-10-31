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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.expressions.MakeDate
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{withDefaultTimeZone, LA}
import org.apache.spark.sql.comet.{CometBroadcastHashJoinExec, CometProjectExec}
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus

// spotless:off
/**
 * Benchmark to measure Comet execution performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometDatetimeExpressionBenchmark`
 * Append `-- --make-date-only` to run only the make_date operator fallback cases.
 * Results will be written to "spark/benchmarks/CometDatetimeExpressionBenchmark-**results.txt".
 */
// spotless:on
object CometDatetimeExpressionBenchmark extends CometBenchmarkBase {

  def makeDateOperatorFallbackBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("make_date_probe", "make_date_build") {
        val probePath = s"${dir.getCanonicalPath}/probe"
        val buildPath = s"${dir.getCanonicalPath}/build"
        spark
          .range(values.toLong)
          .selectExpr(
            "CAST(id % 1024 AS INT) AS k",
            "CAST(2000 + id % 25 AS INT) AS y",
            "CAST(1 + id % 12 AS INT) AS m",
            "CAST(1 + id % 28 AS INT) AS d")
          .write
          .parquet(probePath)
        spark.range(1024).selectExpr("CAST(id AS INT) AS k").write.parquet(buildPath)
        spark.read.parquet(probePath).createOrReplaceTempView("make_date_probe")
        spark.read.parquet(buildPath).createOrReplaceTempView("make_date_build")

        val queries = Seq(
          (
            "MakeDate - LIMIT 10",
            "SELECT make_date(y, m, d) AS dt FROM make_date_probe LIMIT 10",
            true),
          (
            "MakeDate - broadcast join probe projection",
            """SELECT /*+ BROADCAST(b) */ p.dt, b.k
              |FROM (SELECT k, make_date(y, m, d) AS dt FROM make_date_probe) p
              |JOIN make_date_build b ON p.k = b.k""".stripMargin,
            false))
        val modes = Seq(
          ("Spark", false, true),
          ("Comet (default)", true, true),
          ("Comet (operator fallback disabled)", true, false))
        queries.foreach { case (name, query, limited) =>
          val benchmark = new Benchmark(
            name,
            if (limited) 10L else values.toLong,
            minNumIters = 5,
            output = output)
          benchmark.out.println(
            s"Input: $values probe rows, 1024 build rows, ${spark.sparkContext.master}, AQE off")
          benchmark.out.println(query)
          modes.foreach { case (mode, comet, operatorFallback) =>
            val configs = Seq(
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
              SQLConf.SHUFFLE_PARTITIONS.key -> "1",
              CometConf.COMET_ENABLED.key -> comet.toString,
              CometConf.COMET_EXEC_ENABLED.key -> comet.toString,
              CometConf.COMET_EXEC_MAKE_DATE_OPERATOR_FALLBACK_ENABLED.key ->
                operatorFallback.toString)
            def consume(): Unit = {
              val df = spark.sql(query)
              // Collect only ten rows for LIMIT. The noop writer consumes the full join output,
              // preserving the date projection that count(*) would prune from the query.
              if (limited) {
                val _ = df.collect()
              } else {
                df.noop()
              }
            }
            withSQLConf(configs: _*) {
              // Capture the executed action, including the noop writer's actual query plan.
              val plans = capturePlans { consume() }
              require(plans.nonEmpty, s"No executed plan captured for $name / $mode")
              benchmark.out.println(s"$mode executed plans:")
              plans.foreach(plan => benchmark.out.println(plan.treeString))
              if (!limited) {
                val probes = collectAcross(plans) {
                  case join: BroadcastHashJoinExec =>
                    if (join.buildSide == BuildRight) join.left else join.right
                  case join: CometBroadcastHashJoinExec =>
                    if (join.buildSide == BuildRight) join.left else join.right
                }
                require(
                  probes.exists(_.exists {
                    case p: ProjectExec =>
                      p.projectList.exists(_.exists(_.isInstanceOf[MakeDate]))
                    case p: CometProjectExec =>
                      p.projectList.exists(_.exists(_.isInstanceOf[MakeDate]))
                    case _ => false
                  }),
                  s"make_date must remain in a projection below the broadcast join for $mode")
              }
            }
            benchmark.addCase(mode) { _ =>
              withSQLConf(configs: _*) {
                consume()
              }
            }
          }
          benchmark.run()
        }
      }
    }
  }

  def dateTruncExprBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            s"select cast(timestamp_micros(cast(value/100000 as integer)) as date) as dt FROM $tbl"))
        Seq("YEAR", "MONTH").foreach { level =>
          val name = s"Date Truncate - $level"
          val query = s"select trunc(dt, '$level') from parquetV1Table"
          runExpressionBenchmark(name, values.toLong, query)
        }
      }
    }
  }

  def timestampTruncExprBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(s"select timestamp_micros(cast(value/100000 as integer)) as ts FROM $tbl"))
        Seq(
          "YEAR",
          "QUARTER",
          "MONTH",
          "WEEK",
          "DAY",
          "HOUR",
          "MINUTE",
          "SECOND",
          "MILLISECOND",
          "MICROSECOND").foreach { level =>
          val name = s"Timestamp Truncate - $level"
          val query = s"select date_trunc('$level', ts) from parquetV1Table"
          runExpressionBenchmark(name, values.toLong, query)
        }
      }
    }
  }

  def unixTimestampBenchmark(values: Int, timeZone: String): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(s"select timestamp_micros(cast(value/100000 as integer)) as ts FROM $tbl"))
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
          val name = s"Unix Timestamp from Timestamp ($timeZone)"
          val query = "select unix_timestamp(ts) from parquetV1Table"
          runExpressionBenchmark(name, values.toLong, query)
        }
      }
    }
  }

  def unixTimestampFromDateBenchmark(values: Int, timeZone: String): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            s"select cast(timestamp_micros(cast(value/100000 as integer)) as date) as dt FROM $tbl"))
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
          val name = s"Unix Timestamp from Date ($timeZone)"
          val query = "select unix_timestamp(dt) from parquetV1Table"
          runExpressionBenchmark(name, values.toLong, query)
        }
      }
    }
  }

  def toTimeBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            s"select concat(cast(abs(value) % 24 as string), ':', lpad(cast(abs(value) % 60 as string), 2, '0'), ':', lpad(cast(abs(value) % 60 as string), 2, '0')) as s FROM $tbl"))
        val name = "to_time"
        val query = "select to_time(s) from parquetV1Table"
        runExpressionBenchmark(name, values.toLong, query)
      }
    }
  }

  def makeTimeBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            s"select cast(abs(value) % 24 as int) as h, cast(abs(value) % 60 as int) as m, cast(abs(value) % 60 as decimal(16,6)) as s FROM $tbl"))
        val name = "make_time"
        val query = "select make_time(h, m, s) from parquetV1Table"
        runExpressionBenchmark(name, values.toLong, query)
      }
    }
  }

  def makeIntervalBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(s"""SELECT
               |  CAST(ABS(value) % 10 AS INT) AS y,
               |  CAST(ABS(value) % 12 AS INT) AS mo,
               |  CAST(ABS(value) % 4 AS INT) AS w,
               |  CAST(ABS(value) % 28 AS INT) AS d,
               |  CAST(ABS(value) % 24 AS INT) AS h,
               |  CAST(ABS(value) % 60 AS INT) AS mi,
               |  CAST(ABS(value) % 60 AS DECIMAL(18, 6)) AS s
               |FROM $tbl""".stripMargin))

        val query = "SELECT make_interval(y, mo, w, d, h, mi, s) FROM parquetV1Table"
        def consumeIntervals(): Unit = {
          spark.sql(query).queryExecution.toRdd.foreachPartition(_.foreach(_.getInterval(0)))
        }
        val benchmark = new Benchmark("MakeInterval", values.toLong, output = output)
        val cometConfigs = Map(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          "spark.sql.optimizer.excludedRules" ->
            "org.apache.spark.sql.catalyst.optimizer.ConstantFolding")

        benchmark.addCase("Spark") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            consumeIntervals()
          }
        }
        benchmark.addCase("Comet (codegen dispatch)") { _ =>
          withSQLConf(cometConfigs.toSeq: _*) {
            consumeIntervals()
          }
        }
        benchmark.addCase("Comet (native)") { _ =>
          val configs =
            cometConfigs ++ Map(CometConf.getExprAllowIncompatConfigKey("MakeInterval") -> "true")
          withSQLConf(configs.toSeq: _*) {
            consumeIntervals()
          }
        }
        benchmark.run()
      }
    }
  }

  /**
   * `next_day` over a default-collation and, on Spark 4.0+, a collated `dayOfWeek`. The native
   * kernel reads the argument as raw bytes, so CometNextDay reports a collated argument as
   * Incompatible and CodegenDispatchFallback runs it through the JVM codegen dispatcher. The
   * collated case therefore measures the dispatcher rather than the native kernel. See
   * https://github.com/apache/datafusion-comet/issues/5591.
   */
  def nextDayExprBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(s"""
            SELECT
              date_from_unix_date(CAST(PMOD(value, 3650) AS INT)) AS dt,
              CASE CAST(PMOD(value, 7) AS INT)
                WHEN 0 THEN 'MON'
                WHEN 1 THEN 'TUE'
                WHEN 2 THEN 'WED'
                WHEN 3 THEN 'THU'
                WHEN 4 THEN 'FRI'
                WHEN 5 THEN 'SAT'
                ELSE 'SUN'
              END AS dow
            FROM $tbl
          """))
        runExpressionBenchmark(
          "NextDay",
          values.toLong,
          "select next_day(dt, dow) from parquetV1Table")
        if (isSpark40Plus) {
          runExpressionBenchmark(
            "NextDay - collated dayOfWeek",
            values.toLong,
            "select next_day(dt, dow collate utf8_lcase) from parquetV1Table")
        }
      }
    }
  }

  /** Dispatched interval arithmetic, bare and feeding a native filter. */
  def intervalArithmeticBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(s"""SELECT
               |  date_from_unix_date(CAST(PMOD(value, 3650) AS INT)) AS d1,
               |  date_from_unix_date(CAST(PMOD(value DIV 3650, 3650) AS INT)) AS d2,
               |  timestamp_micros(PMOD(value, 315360000000000)) AS ts1,
               |  timestamp_micros(PMOD(value DIV 7, 315360000000000)) AS ts2
               |FROM $tbl""".stripMargin))
        Seq(
          "Date - Date" -> "SELECT d1 - d2 FROM parquetV1Table",
          "Date - Date, filter" ->
            "SELECT d1 FROM parquetV1Table WHERE d1 - d2 > INTERVAL '30' DAY",
          "Timestamp - Timestamp" -> "SELECT ts1 - ts2 FROM parquetV1Table",
          "Timestamp - Timestamp, filter" ->
            ("SELECT ts1 FROM parquetV1Table " +
              "WHERE ts1 - ts2 > INTERVAL '1 00:00:00' DAY TO SECOND"),
          "Timestamp + Interval" ->
            "SELECT ts1 + INTERVAL '1 02:03:04' DAY TO SECOND FROM parquetV1Table",
          "Timestamp + Interval, filter" ->
            ("SELECT ts1 FROM parquetV1Table " +
              "WHERE ts1 + INTERVAL '1 02:03:04' DAY TO SECOND > ts2")).foreach {
          case (name, query) =>
            runExpressionBenchmark(name, values.toLong, query)
        }
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024;

    runBenchmark("MakeDate operator fallback") {
      makeDateOperatorFallbackBenchmark(values)
    }
    if (mainArgs.contains("--make-date-only")) return

    for (timeZone <- Seq("UTC", "America/Los_Angeles")) {
      withSQLConf("spark.sql.parquet.datetimeRebaseModeInWrite" -> "CORRECTED") {
        runBenchmarkWithTable(s"UnixTimestamp(timestamp) - $timeZone", values) { v =>
          unixTimestampBenchmark(v, timeZone)
        }
        runBenchmarkWithTable(s"UnixTimestamp(date) - $timeZone", values) { v =>
          unixTimestampFromDateBenchmark(v, timeZone)
        }
      }
    }

    withDefaultTimeZone(LA) {
      withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> LA.getId,
        "spark.sql.parquet.datetimeRebaseModeInWrite" -> "CORRECTED") {

        runBenchmarkWithTable("DateTrunc", values) { v =>
          dateTruncExprBenchmark(v)
        }
        runBenchmarkWithTable("TimestampTrunc", values) { v =>
          timestampTruncExprBenchmark(v)
        }
        runBenchmarkWithTable("IntervalArithmetic", values) { v =>
          intervalArithmeticBenchmark(v)
        }
      }
    }

    withSQLConf("spark.sql.timeType.enabled" -> "true") {
      runBenchmarkWithTable("ToTime", values) { v =>
        toTimeBenchmark(v)
      }
      runBenchmarkWithTable("MakeTime", values) { v =>
        makeTimeBenchmark(v)
      }
    }

    runBenchmarkWithTable("MakeInterval", values) { v =>
      makeIntervalBenchmark(v)
    }

    runBenchmarkWithTable("NextDay", values) { v =>
      nextDayExprBenchmark(v)
    }
  }

}
