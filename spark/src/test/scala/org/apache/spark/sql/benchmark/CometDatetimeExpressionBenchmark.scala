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
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{withDefaultTimeZone, LA}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus

// spotless:off
/**
 * Benchmark to measure Comet execution performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometDatetimeExpressionBenchmark`
 * Results will be written to "spark/benchmarks/CometDatetimeExpressionBenchmark-**results.txt".
 */
// spotless:on
object CometDatetimeExpressionBenchmark extends CometBenchmarkBase {

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
          runExpressionBenchmark(name, values, query)
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
          runExpressionBenchmark(name, values, query)
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
          runExpressionBenchmark(name, values, query)
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
          runExpressionBenchmark(name, values, query)
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
        runExpressionBenchmark(name, values, query)
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
        runExpressionBenchmark(name, values, query)
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
        val benchmark = new Benchmark("MakeInterval", values, output = output)
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
        runExpressionBenchmark("NextDay", values, "select next_day(dt, dow) from parquetV1Table")
        if (isSpark40Plus) {
          runExpressionBenchmark(
            "NextDay - collated dayOfWeek",
            values,
            "select next_day(dt, dow collate utf8_lcase) from parquetV1Table")
        }
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024;

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
