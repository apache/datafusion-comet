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
          runWithComet(s"Date Truncate $isDictionary - $level", values) {
            spark.sql(s"select trunc(dt, '$level') from parquetV1Table").noop()
          }
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
          runWithComet(s"Timestamp Truncate $isDictionary - $level", values) {
            spark.sql(s"select date_trunc('$level', ts) from parquetV1Table").noop()
          }
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
      }
    }
  }

}
