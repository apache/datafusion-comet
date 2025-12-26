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

import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{withDefaultTimeZone, LA}
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure Comet execution performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometDatetimeExpressionBenchmark` Results will be
 * written to "spark/benchmarks/CometDatetimeExpressionBenchmark-**results.txt".
 */
object CometDatetimeExpressionBenchmark extends CometBenchmarkBase {

  def dateTruncExprBenchmark(values: Int, useDictionary: Boolean): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            s"select cast(timestamp_micros(cast(value/100000 as integer)) as date) as dt FROM $tbl"))
        Seq("YEAR", "YYYY", "YY", "MON", "MONTH", "MM").foreach { level =>
          val isDictionary = if (useDictionary) "(Dictionary)" else ""
          val name = s"Date Truncate $isDictionary - $level"
          val query = s"select trunc(dt, '$level') from parquetV1Table"
          runExpressionBenchmark(name, values, query)
        }
      }
    }
  }

  def timestampTruncExprBenchmark(values: Int, useDictionary: Boolean): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(s"select timestamp_micros(cast(value/100000 as integer)) as ts FROM $tbl"))
        Seq(
          "YEAR",
          "YYYY",
          "YY",
          "MON",
          "MONTH",
          "MM",
          "DAY",
          "DD",
          "HOUR",
          "MINUTE",
          "SECOND",
          "WEEK",
          "QUARTER").foreach { level =>
          val isDictionary = if (useDictionary) "(Dictionary)" else ""
          val name = s"Timestamp Truncate $isDictionary - $level"
          val query = s"select date_trunc('$level', ts) from parquetV1Table"
          runExpressionBenchmark(name, values, query)
        }
      }
    }
  }

  def datePartExprBenchmark(values: Int, useDictionary: Boolean): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            s"select cast(timestamp_micros(cast(value/100000 as integer)) as date) as dt FROM $tbl"))
        Seq(
          ("year", "YEAR(dt)"),
          ("month", "MONTH(dt)"),
          ("day_of_month", "DAYOFMONTH(dt)"),
          ("day_of_week", "DAYOFWEEK(dt)"),
          ("weekday", "WEEKDAY(dt)"),
          ("day_of_year", "DAYOFYEAR(dt)"),
          ("week_of_year", "WEEKOFYEAR(dt)"),
          ("quarter", "QUARTER(dt)")).foreach { case (name, expr) =>
          val isDictionary = if (useDictionary) "(Dictionary)" else ""
          val benchName = s"Date Extract $isDictionary - $name"
          val query = s"select $expr from parquetV1Table"
          runExpressionBenchmark(benchName, values, query)
        }
      }
    }
  }

  def timestampPartExprBenchmark(values: Int, useDictionary: Boolean): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(s"select timestamp_micros(cast(value/100000 as integer)) as ts FROM $tbl"))
        Seq(
          ("year", "YEAR(ts)"),
          ("month", "MONTH(ts)"),
          ("day_of_month", "DAYOFMONTH(ts)"),
          ("hour", "HOUR(ts)"),
          ("minute", "MINUTE(ts)"),
          ("second", "SECOND(ts)"),
          ("quarter", "QUARTER(ts)")).foreach { case (name, expr) =>
          val isDictionary = if (useDictionary) "(Dictionary)" else ""
          val benchName = s"Timestamp Extract $isDictionary - $name"
          val query = s"select $expr from parquetV1Table"
          runExpressionBenchmark(benchName, values, query)
        }
      }
    }
  }

  def dateArithmeticExprBenchmark(values: Int, useDictionary: Boolean): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            s"select cast(timestamp_micros(cast(value/100000 as integer)) as date) as dt, cast(value % 365 as int) as days FROM $tbl"))
        Seq(("date_add", "DATE_ADD(dt, days)"), ("date_sub", "DATE_SUB(dt, days)")).foreach {
          case (name, expr) =>
            val isDictionary = if (useDictionary) "(Dictionary)" else ""
            val benchName = s"Date Arithmetic $isDictionary - $name"
            val query = s"select $expr from parquetV1Table"
            runExpressionBenchmark(benchName, values, query)
        }
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024;

    withDefaultTimeZone(LA) {
      withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> LA.getId,
        "spark.sql.parquet.datetimeRebaseModeInWrite" -> "CORRECTED") {

        runBenchmarkWithTable("DateTrunc", values) { v =>
          dateTruncExprBenchmark(v, useDictionary = false)
        }
        runBenchmarkWithTable("DateTrunc (Dictionary)", values, useDictionary = true) { v =>
          dateTruncExprBenchmark(v, useDictionary = true)
        }
        runBenchmarkWithTable("TimestampTrunc", values) { v =>
          timestampTruncExprBenchmark(v, useDictionary = false)
        }
        runBenchmarkWithTable("TimestampTrunc (Dictionary)", values, useDictionary = true) { v =>
          timestampTruncExprBenchmark(v, useDictionary = true)
        }
        runBenchmarkWithTable("DatePart", values) { v =>
          datePartExprBenchmark(v, useDictionary = false)
        }
        runBenchmarkWithTable("DatePart (Dictionary)", values, useDictionary = true) { v =>
          datePartExprBenchmark(v, useDictionary = true)
        }
        runBenchmarkWithTable("TimestampPart", values) { v =>
          timestampPartExprBenchmark(v, useDictionary = false)
        }
        runBenchmarkWithTable("TimestampPart (Dictionary)", values, useDictionary = true) { v =>
          timestampPartExprBenchmark(v, useDictionary = true)
        }
        runBenchmarkWithTable("DateArithmetic", values) { v =>
          dateArithmeticExprBenchmark(v, useDictionary = false)
        }
        runBenchmarkWithTable("DateArithmetic (Dictionary)", values, useDictionary = true) { v =>
          dateArithmeticExprBenchmark(v, useDictionary = true)
        }
      }
    }
  }

}
