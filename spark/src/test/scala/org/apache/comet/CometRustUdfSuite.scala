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
 * Exercises both ABI flavors that the test cdylib exports:
 *   - `add_one_c` via the pure C ABI (Arrow C Data Interface only)
 *   - `add_one_df` via datafusion-ffi (`FFI_ScalarUDF`)
 *
 * Requires the test cdylib at the path given by the system property `comet.test.udfs.lib`.
 *
 * To run locally:
 * {{{
 *   cargo build -p comet-test-udfs --manifest-path native/Cargo.toml
 *   ./mvnw test -pl spark -DwildcardSuites="CometRustUdfSuite" \
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

  test("C ABI: add_one_c returns id + 1 for a range") {
    CometRustUDF.register(spark, "add_one_c", libPath, Seq(LongType), LongType)
    val df = spark.range(0, 5).selectExpr("add_one_c(id) AS y")
    val out = df.collect().map(_.getLong(0)).sorted.toSeq
    assert(out == Seq(1L, 2L, 3L, 4L, 5L))
  }

  test("datafusion-ffi: add_one_df returns id + 1 for a range") {
    CometRustUDF.register(spark, "add_one_df", libPath, Seq(LongType), LongType)
    val df = spark.range(0, 5).selectExpr("add_one_df(id) AS y")
    val out = df.collect().map(_.getLong(0)).sorted.toSeq
    assert(out == Seq(1L, 2L, 3L, 4L, 5L))
  }
}
