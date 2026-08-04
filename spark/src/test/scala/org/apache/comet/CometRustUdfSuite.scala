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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.types.LongType

import org.apache.comet.udf.CometRustUDF

/**
 * End-to-end integration suite: register a Rust UDF, run a Spark query, verify the result.
 *
 * Requires the test cdylib at the path given by the system property `comet.test.udfs.lib`.
 *
 * Note that these tests are self-guarding on native execution: `CometRustUDF.register` installs a
 * catalog stub that throws if Spark ever evaluates the UDF itself, so a silent fallback to Spark
 * fails the test rather than passing.
 *
 * To run locally:
 * {{{
 *   cargo build -p comet-test-udfs --manifest-path native/Cargo.toml
 *   ./mvnw test -Dsuites="org.apache.comet.CometRustUdfSuite" -Dtest=none \
 *     -Dcomet.test.udfs.lib=$PWD/native/target/debug/libcomet_test_udfs.dylib
 * }}}
 */
class CometRustUdfSuite extends CometTestBase {

  private lazy val libPath: String = {
    val p = System.getProperty("comet.test.udfs.lib")
    if (p == null) {
      cancel("set -Dcomet.test.udfs.lib=<path to libcomet_test_udfs>; skipping without cdylib")
    }
    p
  }

  test("add_one_c returns id + 1 for a range") {
    CometRustUDF.register(spark, "add_one_c", libPath, Seq(LongType), LongType)
    val df = spark.range(0, 5).selectExpr("add_one_c(id) AS y")
    val out = df.collect().map(_.getLong(0)).sorted.toSeq
    assert(out == Seq(1L, 2L, 3L, 4L, 5L))
  }

  test("panic inside UDF invoke surfaces as a query error, not a crash") {
    CometRustUDF.register(spark, "panics_on_invoke", libPath, Seq(LongType), LongType)
    val e = intercept[Exception] {
      spark.range(0, 5).selectExpr("panics_on_invoke(id) AS y").collect()
    }
    assert(
      stackTraceContains(e, "deliberate panic from user UDF code"),
      s"panic message not propagated: $e")
  }

  test("panic inside UDF return_field surfaces as a query error, not a crash") {
    CometRustUDF.register(spark, "panics_on_return_field", libPath, Seq(LongType), LongType)
    val e = intercept[Exception] {
      spark.range(0, 5).selectExpr("panics_on_return_field(id) AS y").collect()
    }
    assert(
      stackTraceContains(e, "deliberate panic from user return_field"),
      s"panic message not propagated: $e")
  }

  /** True if `needle` appears anywhere in the exception's cause chain. */
  private def stackTraceContains(e: Throwable, needle: String): Boolean = {
    Iterator
      .iterate(e)(_.getCause)
      .takeWhile(_ != null)
      .exists(t => Option(t.getMessage).exists(_.contains(needle)))
  }
}
