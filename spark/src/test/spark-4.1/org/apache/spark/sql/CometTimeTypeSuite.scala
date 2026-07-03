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

import org.apache.comet.CometConf

/**
 * Coverage for the Spark 4.1 `TIME` type and the SQL functions that operate on it. All function
 * calls that don't have a native lowering are routed through the JVM codegen dispatcher via the
 * `CometExprShim`. Casts to/from TIME follow the same route via `CometCast`.
 */
class CometTimeTypeSuite extends CometTestBase {

  override protected def sparkConf = super.sparkConf
    .set("spark.sql.timeType.enabled", "true")
    .set(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key, "true")

  test("literal TIME values round-trip through LocalTableScan") {
    val df = spark.sql(
      "SELECT * FROM VALUES (TIME '00:00:00'), (TIME '12:34:56.789012'), " +
        "(TIME '23:59:59.999999') AS t(c)")
    checkSparkAnswerAndOperator(df)
  }

  test("make_time(hour, minute, second) via native path") {
    val df = spark.sql(
      "SELECT make_time(h, m, s) FROM VALUES (0, 0, 0.0), (6, 30, 45.887), " +
        "(23, 59, 59.999999) AS t(h, m, s)")
    checkSparkAnswerAndOperator(df)
  }

  test("to_time(str) via native path") {
    val df = spark.sql(
      "SELECT to_time(s) FROM VALUES ('00:12:00'), ('12:34:56.789'), " +
        "('23:59:59.999999') AS t(s)")
    checkSparkAnswerAndOperator(df)
  }

  test("try_to_time(str) via native path returns NULL on malformed input") {
    val df = spark.sql("SELECT try_to_time(s) FROM VALUES ('00:12:00'), ('bad'), (NULL) AS t(s)")
    checkSparkAnswerAndOperator(df)
  }

  test("hour(TIME) via codegen dispatch") {
    val df = spark.sql(
      "SELECT hour(t) FROM VALUES (TIME '00:00:00'), (TIME '13:45:00'), " +
        "(TIME '23:59:59.999999') AS v(t)")
    checkSparkAnswerAndOperator(df)
  }

  test("minute(TIME) via codegen dispatch") {
    val df = spark.sql(
      "SELECT minute(t) FROM VALUES (TIME '00:00:00'), (TIME '13:45:00'), " +
        "(TIME '23:59:59.999999') AS v(t)")
    checkSparkAnswerAndOperator(df)
  }

  test("second(TIME) via codegen dispatch") {
    val df = spark.sql(
      "SELECT second(t) FROM VALUES (TIME '00:00:00'), (TIME '13:45:07'), " +
        "(TIME '23:59:59.999999') AS v(t)")
    checkSparkAnswerAndOperator(df)
  }

  test("EXTRACT SECOND FROM TIME returns decimal via codegen dispatch") {
    val df = spark.sql(
      "SELECT EXTRACT(SECOND FROM t) FROM VALUES (TIME '00:00:00'), " +
        "(TIME '13:45:07.123456'), (TIME '23:59:59.999999') AS v(t)")
    checkSparkAnswerAndOperator(df)
  }

  test("time + day-time interval via codegen dispatch") {
    val df = spark.sql(
      "SELECT t + INTERVAL '1' HOUR FROM VALUES (TIME '00:00:00'), (TIME '22:30:00') AS v(t)")
    checkSparkAnswerAndOperator(df)
  }

  test("time - time returns day-time interval via codegen dispatch") {
    // Comet does not yet support DayTimeIntervalType outputs at the projection level, so this
    // exercises the shim wiring but currently falls back to Spark. Verify results only.
    val df = spark.sql(
      "SELECT b - a FROM VALUES (TIME '01:00:00', TIME '02:15:30'), " +
        "(TIME '10:00:00', TIME '09:59:59.5') AS v(a, b)")
    checkSparkAnswer(df)
  }

  test("time_diff via codegen dispatch") {
    val df = spark.sql(
      "SELECT time_diff('HOUR', a, b), time_diff('MINUTE', a, b), time_diff('SECOND', a, b) " +
        "FROM VALUES (TIME '20:30:29', TIME '21:30:29'), (TIME '00:00:00', TIME '00:00:01') " +
        "AS v(a, b)")
    checkSparkAnswerAndOperator(df)
  }

  test("time_trunc via codegen dispatch") {
    val df = spark.sql(
      "SELECT time_trunc('HOUR', t), time_trunc('MINUTE', t), time_trunc('SECOND', t), " +
        "time_trunc('MILLISECOND', t) " +
        "FROM VALUES (TIME '09:32:05.359'), (TIME '23:59:59.999999') AS v(t)")
    checkSparkAnswerAndOperator(df)
  }

  test("cast string to TIME via codegen dispatch") {
    val df =
      spark.sql("SELECT CAST(s AS TIME) FROM VALUES ('00:12:00'), ('23:59:59.999999') AS v(s)")
    checkSparkAnswerAndOperator(df)
  }

  test("cast TIME to string via codegen dispatch") {
    val df = spark.sql(
      "SELECT CAST(t AS STRING) FROM VALUES (TIME '00:00:00'), " +
        "(TIME '13:45:07.123456') AS v(t)")
    checkSparkAnswerAndOperator(df)
  }

  test("cast TIME to LONG returns seconds via codegen dispatch") {
    val df = spark.sql(
      "SELECT CAST(t AS BIGINT) FROM VALUES (TIME '00:00:00'), (TIME '01:00:00'), " +
        "(TIME '23:59:59.999999') AS v(t)")
    checkSparkAnswerAndOperator(df)
  }
}
