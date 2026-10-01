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

package org.apache.spark.sql

import org.apache.comet.{CometConf, ExtendedExplainInfo}

class CometCollationSuite extends CometTestBase {

  // Queries that group, sort, or shuffle on a non-default collated string must fall back to
  // Spark because Comet's shuffle/sort/aggregate compare raw bytes rather than collation-aware
  // keys. The shuffle-exchange rule is the primary line of defense (see #1947), so these tests
  // pin down the fallback reason it emits.
  private val hashShuffleCollationReason =
    "unsupported hash partitioning data type for columnar shuffle"
  private val rangeShuffleCollationReason =
    "unsupported range partitioning data type for columnar shuffle"

  test("listagg DISTINCT with utf8_lcase collation (issue #1947)") {
    checkSparkAnswerAndFallbackReason(
      "SELECT lower(listagg(DISTINCT c1 COLLATE utf8_lcase) " +
        "WITHIN GROUP (ORDER BY c1 COLLATE utf8_lcase)) " +
        "FROM (VALUES ('a'), ('B'), ('b'), ('A')) AS t(c1)",
      hashShuffleCollationReason)
  }

  test("DISTINCT on utf8_lcase collated string groups case-insensitively") {
    checkSparkAnswerAndFallbackReason(
      "SELECT DISTINCT c1 COLLATE utf8_lcase AS c " +
        "FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1) ORDER BY c",
      hashShuffleCollationReason)
  }

  test("GROUP BY utf8_lcase collated string groups case-insensitively") {
    checkSparkAnswerAndFallbackReason(
      "SELECT lower(c1 COLLATE utf8_lcase) AS k, count(*) " +
        "FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1) " +
        "GROUP BY c1 COLLATE utf8_lcase ORDER BY k",
      hashShuffleCollationReason)
  }

  test("ORDER BY utf8_lcase collated string sorts case-insensitively") {
    checkSparkAnswerAndFallbackReason(
      "SELECT c1 COLLATE utf8_lcase AS c " +
        "FROM (VALUES ('A'), ('b'), ('a'), ('B')) AS t(c1) ORDER BY c",
      rangeShuffleCollationReason)
  }

  test("default UTF8_BINARY string still runs through Comet") {
    // Sanity check that the collation fallback does not over-block the default string type.
    withParquetTable(Seq(("a", 1), ("b", 2), ("a", 3)), "tbl") {
      checkSparkAnswerAndOperator("SELECT DISTINCT _1 FROM tbl ORDER BY _1")
    }
  }

  // ---- datetime expression collation guards (issue #4646) --------------------------------
  //
  // Native datetime functions that interpret string arguments must account for their
  // collation. Expressions with CodegenDispatchFallback use Spark codegen for unsupported
  // cases when COMET_SCALA_UDF_CODEGEN_ENABLED is true, or fall back to Spark when it is false.
  // unix_timestamp uses codegen for string inputs. Date and timestamp inputs ignore the
  // format argument, including its collation, and stay native even with codegen disabled.

  private def withDatetimeCollationTable(f: => Unit): Unit = {
    withParquetTable(
      Seq(
        (
          "2024-01-01",
          "2024-01-01 00:00:00",
          "2024-06-15",
          "2024-06-15 10:00:00",
          "MON",
          "yyyy-MM-dd",
          "yyyy-MM-dd HH:mm:ss",
          "YEAR",
          "UTC",
          1718451045L)),
      "datetime_collation_tbl")(f)
  }

  private def checkDatetimeFallback(query: String, fallbackReason: String): Unit = {
    withDatetimeCollationTable {
      withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
        checkSparkAnswerAndFallbackReason(query, fallbackReason)
      }
    }
  }

  private def checkDatetimeDispatcher(query: String): Unit = {
    withDatetimeCollationTable {
      withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true") {
        checkSparkAnswerAndOperator(query)
      }
    }
  }

  test("next_day rejects non-UTF8_BINARY collated dayOfWeek (issue #4646)") {
    checkDatetimeFallback(
      "SELECT next_day(CAST(_1 AS DATE), _5 COLLATE utf8_lcase) " +
        "FROM datetime_collation_tbl",
      "next_day does not support non-UTF8_BINARY collations")
  }

  test("unix_timestamp stays native with a collated format for timestamp input (issue #4646)") {
    withDatetimeCollationTable {
      for (codegenEnabled <- Seq("false", "true")) {
        withSQLConf(
          CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> codegenEnabled,
          CometConf.getExprAllowIncompatConfigKey("UnixTimestamp") -> "false") {
          checkSparkAnswerAndImpl(
            "SELECT unix_timestamp(CAST(_2 AS TIMESTAMP), _7 COLLATE utf8_lcase) " +
              "FROM datetime_collation_tbl",
            native = Seq("unix_timestamp"),
            dispatched = Seq.empty)
        }
      }
    }
  }

  test("from_unixtime rejects non-UTF8_BINARY collated format (issue #4646)") {
    // A collation can only appear on the format argument, so a collated format is a non-default
    // format. from_unixtime has no native path for non-default formats, so it is reported as
    // Unsupported (the format reason) rather than Incompatible (the collation reason).
    checkDatetimeFallback(
      "SELECT from_unixtime(_10, _6 COLLATE utf8_lcase) FROM datetime_collation_tbl",
      "Only the default datetime format pattern `yyyy-MM-dd HH:mm:ss` is supported")
  }

  for ((exprName, functionName, arguments) <- Seq(
      ("MakeTimestamp", "make_timestamp", "2024, 6, 15, 10, 30, 45.0, _9 COLLATE utf8_lcase"),
      ("ToUnixTimestamp", "to_unix_timestamp", "_3, _6 COLLATE utf8_lcase"))) {
    for (codegenEnabled <- Seq("false", "true")) {
      test(
        s"$functionName with collated arguments has no native opt-in " +
          s"(codegen=$codegenEnabled, issue #6080)") {
        withDatetimeCollationTable {
          for (allowIncompatible <- Seq("false", "true")) {
            withSQLConf(
              CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> codegenEnabled,
              CometConf.getExprAllowIncompatConfigKey(exprName) -> allowIncompatible,
              "spark.sql.optimizer.excludedRules" ->
                "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
              val query = s"SELECT $functionName($arguments) FROM datetime_collation_tbl"
              val (_, cometPlan) = if (codegenEnabled == "true") {
                checkSparkAnswerAndImpl(query, native = Seq.empty, dispatched = Seq(functionName))
              } else {
                checkSparkAnswerAndFallbackReason(
                  query,
                  s"$functionName: ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false")
              }
              val explain = new ExtendedExplainInfo().generateExtendedInfo(cometPlan)
              assert(!explain.contains("allowIncompatible"), explain)
              assert(
                !explain.contains(s"A native implementation of $exprName is available"),
                explain)
              assert(!explain.contains("does not support non-UTF8_BINARY collations"), explain)
            }
          }
        }
      }
    }
  }

  test("convert_timezone rejects non-UTF8_BINARY collated timezone (issue #4646)") {
    checkDatetimeFallback(
      "SELECT convert_timezone(_9 COLLATE utf8_lcase, 'America/Los_Angeles', " +
        "TIMESTAMP_NTZ '2024-01-01 00:00:00') FROM datetime_collation_tbl",
      "convert_timezone does not support non-UTF8_BINARY collations")
  }

  test("trunc falls back with collated format when codegen is disabled (issue #4646)") {
    checkDatetimeFallback(
      "SELECT trunc(CAST(_3 AS DATE), _8 COLLATE utf8_lcase) FROM datetime_collation_tbl",
      "trunc does not support non-UTF8_BINARY collations")
  }

  test("date_trunc falls back with collated format when codegen is disabled (issue #4646)") {
    checkDatetimeFallback(
      "SELECT date_trunc(_8 COLLATE utf8_lcase, CAST(_4 AS TIMESTAMP)) " +
        "FROM datetime_collation_tbl",
      "date_trunc does not support non-UTF8_BINARY collations")
  }

  test("date_format falls back with collated format when codegen is disabled (issue #4646)") {
    checkDatetimeFallback(
      "SELECT date_format(CAST(_2 AS TIMESTAMP), _6 COLLATE utf8_lcase) " +
        "FROM datetime_collation_tbl",
      "date_format does not support non-UTF8_BINARY collations")
  }

  test("next_day uses native path with collated dayOfWeek when allowIncompatible is enabled") {
    // With allowIncompatible the collated case takes the native kernel instead of the dispatcher.
    // That kernel reads dayOfWeek as raw bytes, which matches Spark because
    // DateTimeUtils.getDayOfWeekFromString upper-cases with Locale.ROOT and matches a fixed
    // literal set, taking no collation. This test is what pins that claim, since every other
    // collated next_day test exercises the dispatcher rather than the native kernel.
    withDatetimeCollationTable {
      withSQLConf(
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false",
        "spark.comet.expression.NextDay.allowIncompatible" -> "true") {
        checkSparkAnswerAndOperator(
          "SELECT next_day(CAST(_1 AS DATE), 'MON' COLLATE utf8_lcase) " +
            "FROM datetime_collation_tbl")
      }
    }
  }

  test("date_format uses native path with collated format when allowIncompatible is enabled") {
    withDatetimeCollationTable {
      withSQLConf(
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false",
        "spark.comet.expression.DateFormatClass.allowIncompatible" -> "true") {
        checkSparkAnswerAndOperator(
          "SELECT date_format(CAST(_2 AS TIMESTAMP), 'yyyy-MM-dd' COLLATE utf8_lcase) " +
            "FROM datetime_collation_tbl")
      }
    }
  }

  test("trunc routes collated format through codegen dispatcher (issue #4646)") {
    checkDatetimeDispatcher(
      "SELECT trunc(CAST(_3 AS DATE), _8 COLLATE utf8_lcase) FROM datetime_collation_tbl")
  }

  test("date_trunc routes collated format through codegen dispatcher (issue #4646)") {
    checkDatetimeDispatcher(
      "SELECT date_trunc(_8 COLLATE utf8_lcase, CAST(_4 AS TIMESTAMP)) " +
        "FROM datetime_collation_tbl")
  }

  test("date_format routes collated format through codegen dispatcher (issue #4646)") {
    checkDatetimeDispatcher(
      "SELECT date_format(CAST(_2 AS TIMESTAMP), _6 COLLATE utf8_lcase) " +
        "FROM datetime_collation_tbl")
  }

  test("next_day routes collated dayOfWeek through codegen dispatcher (issue #5591)") {
    // Dispatching is both the compatible answer and the faster one: over 1M rows a collated
    // next_day measured 70ms dispatched against 89ms for Spark. Answer coverage lives in
    // sql-tests/expressions/datetime/next_day_collation.sql.
    checkDatetimeDispatcher(
      "SELECT next_day(CAST(_1 AS DATE), _5 COLLATE utf8_lcase) FROM datetime_collation_tbl")
  }

  test("datetime expressions still run with default UTF8_BINARY collation (issue #4646)") {
    withDatetimeCollationTable {
      checkSparkAnswerAndOperator(
        "SELECT next_day(CAST(_1 AS DATE), _5) FROM datetime_collation_tbl")
      checkSparkAnswerAndOperator(
        "SELECT trunc(CAST(_3 AS DATE), _8) FROM datetime_collation_tbl")
      checkSparkAnswerAndOperator(
        "SELECT unix_timestamp(CAST(_2 AS TIMESTAMP), _7) FROM datetime_collation_tbl")
    }
  }
}
