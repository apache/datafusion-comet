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

package org.apache.comet.udf

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class CometRustUDFDriverSuite extends AnyFunSuite with BeforeAndAfterAll {

  // Build the test cdylib and resolve its path. The path is exposed by
  // core's build.rs via COMET_TEST_UDFS_LIB; for JVM tests we pass it
  // through a system property `comet.test.udfs.lib` (set by Maven
  // surefire — see common/pom.xml).
  lazy val libPath: String = {
    val p = System.getProperty("comet.test.udfs.lib")
    if (p == null) {
      cancel(
        "set -Dcomet.test.udfs.lib=<path to libcomet_test_udfs>; " +
          "skipping test on environments without the cdylib")
    }
    p
  }

  lazy val spark: SparkSession =
    SparkSession.builder().master("local[1]").appName("rust-udf-driver-test").getOrCreate()

  override def afterAll(): Unit = spark.stop()

  test("register validates and registers a known UDF") {
    CometRustUDF.register(spark, "add_one", libPath, Seq(LongType), LongType)
    assert(CometRustUdfRegistry.instance.get("add_one").isDefined)
    val parsed = spark.sessionState.sqlParser.parseExpression("add_one(1L)")
    assert(parsed != null)
  }

  test("register with wrong return type throws") {
    assertThrows[CometRustUdfSignatureException] {
      CometRustUDF.register(
        spark,
        "add_one",
        libPath,
        Seq(LongType),
        StructType(Seq(StructField("x", LongType))))
    }
  }

  test("register with missing path throws") {
    assertThrows[CometRustUdfLoadException] {
      CometRustUDF.register(spark, "add_one", "/no/such.so", Seq(LongType), LongType)
    }
  }

  test("registerAll registers all UDFs in the library") {
    val names = CometRustUDF.registerAll(spark, libPath)
    assert(names.toSet.contains("add_one"))
    assert(names.toSet.contains("struct_field_a"))
    assert(names.toSet.contains("always_err"))
  }
}
