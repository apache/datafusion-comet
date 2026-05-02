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

import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

/**
 * Tests for spark.sql.legacy.timeParserPolicy compatibility.
 *
 * Spark's timeParserPolicy takes three values:
 *   - CORRECTED (default): uses java.time.format.DateTimeFormatter (strict ISO)
 *   - LEGACY: uses java.text.SimpleDateFormat (lenient)
 *   - EXCEPTION: throws when CORRECTED and LEGACY would disagree
 *
 * Data is written to Parquet and read back so that expressions actually execute (a plain
 * Seq(...).toDF is a LocalRelation and may be constant-folded by ConvertToLocalRelation before
 * Comet sees the plan).
 *
 * EXCEPTION policy is skipped here because Spark itself throws during its own parser-divergence
 * check, which is unrelated to whether Comet honors the policy.
 */
class CometTimeParserPolicySuite extends CometTestBase with AdaptiveSparkPlanHelper {

  import testImplicits._

  private val policies = Seq("CORRECTED", "LEGACY")

  /** Strings that LEGACY mode parses more permissively than CORRECTED. */
  private val looseDateStrings = Seq(
    "2020-01-01",
    "2020-1-1",
    "2020-1-01",
    "2020-01-1",
    "2020-01-01 10:00:00",
    "2020-01-01T10:00:00",
    "01/02/2020", // invalid ISO; LEGACY may parse with pattern
    null)

  private val looseTimestampStrings = Seq(
    "2020-01-01 10:00:00",
    "2020-01-01T10:00:00",
    "2020-1-1 1:2:3",
    "2020-01-01",
    "01/02/2020 10:00:00",
    null)

  private def withPolicy(policy: String)(f: => Unit): Unit = {
    withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> policy) {
      f
    }
  }

  /** Write to Parquet and return a scanned DataFrame so Comet actually runs. */
  private def parquetView(df: DataFrame, viewName: String)(block: => Unit): Unit = {
    withTempPath { dir =>
      df.coalesce(1).write.mode("overwrite").parquet(dir.getCanonicalPath)
      spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView(viewName)
      block
    }
  }

  test("CAST(string AS date) under timeParserPolicy") {
    parquetView(looseDateStrings.toDF("s"), "t_str_date") {
      for (policy <- policies) {
        withPolicy(policy) {
          checkSparkAnswer("select s, cast(s as date) from t_str_date")
        }
      }
    }
  }

  // Flip to `test` once Comet honors timeParserPolicy for string-to-timestamp casts.
  // Currently fails: Spark parses "2020-1-1 1:2:3" as 2020-01-01 01:02:03 under LEGACY,
  // but Comet's native ISO parser returns null.
  ignore("CAST(string AS timestamp) under timeParserPolicy") {
    parquetView(looseTimestampStrings.toDF("s"), "t_str_ts") {
      for (policy <- policies) {
        withPolicy(policy) {
          checkSparkAnswer("select s, cast(s as timestamp) from t_str_ts")
        }
      }
    }
  }

  test("to_date(s) without pattern under timeParserPolicy") {
    parquetView(looseDateStrings.toDF("s"), "t_to_date_no_pat") {
      for (policy <- policies) {
        withPolicy(policy) {
          checkSparkAnswer("select s, to_date(s) from t_to_date_no_pat")
        }
      }
    }
  }

  test("to_date(s, pattern) under timeParserPolicy") {
    parquetView(looseDateStrings.toDF("s"), "t_to_date_pat") {
      for (policy <- policies) {
        withPolicy(policy) {
          checkSparkAnswer("select s, to_date(s, 'yyyy-M-d') from t_to_date_pat")
          checkSparkAnswer("select s, to_date(s, 'MM/dd/yyyy') from t_to_date_pat")
        }
      }
    }
  }

  // Flip to `test` once Comet honors timeParserPolicy. to_timestamp(s) lowers to Cast,
  // so this fails for the same reason as the CAST(string AS timestamp) suite above.
  ignore("to_timestamp(s) without pattern under timeParserPolicy") {
    parquetView(looseTimestampStrings.toDF("s"), "t_to_ts_no_pat") {
      for (policy <- policies) {
        withPolicy(policy) {
          checkSparkAnswer("select s, to_timestamp(s) from t_to_ts_no_pat")
        }
      }
    }
  }

  test("to_timestamp(s, pattern) under timeParserPolicy") {
    parquetView(looseTimestampStrings.toDF("s"), "t_to_ts_pat") {
      for (policy <- policies) {
        withPolicy(policy) {
          checkSparkAnswer("select s, to_timestamp(s, 'yyyy-MM-dd HH:mm:ss') from t_to_ts_pat")
        }
      }
    }
  }

  test("unix_timestamp(string, pattern) under timeParserPolicy") {
    parquetView(looseTimestampStrings.toDF("s"), "t_unix_ts") {
      for (policy <- policies) {
        withPolicy(policy) {
          checkSparkAnswer("select s, unix_timestamp(s, 'yyyy-MM-dd HH:mm:ss') from t_unix_ts")
        }
      }
    }
  }

  test("from_unixtime(long, pattern) under timeParserPolicy") {
    parquetView(Seq[java.lang.Long](0L, 1000L, 1600000000L, null).toDF("ts"), "t_from_unix") {
      for (policy <- policies) {
        withPolicy(policy) {
          checkSparkAnswer("select ts, from_unixtime(ts, 'yyyy-MM-dd HH:mm:ss') from t_from_unix")
          // 'E' (day-of-week) behaves differently under SimpleDateFormat vs DateTimeFormatter.
          checkSparkAnswer("select ts, from_unixtime(ts, 'yyyy-MM-dd E') from t_from_unix")
        }
      }
    }
  }

  test("date_format(date, pattern) under timeParserPolicy") {
    parquetView(Seq("2020-01-01", "2020-12-31", "1900-06-15", null).toDF("s"), "t_date_fmt") {
      for (policy <- policies) {
        withPolicy(policy) {
          checkSparkAnswer("select s, date_format(cast(s as date), 'yyyy-MM-dd') from t_date_fmt")
          checkSparkAnswer(
            "select s, date_format(cast(s as date), 'yyyy-MM-dd E') from t_date_fmt")
        }
      }
    }
  }
}
