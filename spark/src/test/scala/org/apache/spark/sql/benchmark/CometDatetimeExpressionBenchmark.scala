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
  }

}
