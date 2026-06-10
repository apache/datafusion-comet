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
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus

/**
 * Structured-text functions (CSV / JSON / XPath) that have no native (rust) implementation. They
 * extend Spark's `CodegenFallback`, so they are wired into the codegen dispatcher via
 * [[org.apache.comet.serde.CometCsvToStructs]] and friends: a top-level projection stays native
 * (running Spark's own evaluation inside the Comet kernel) and matches Spark exactly. When the
 * dispatcher is disabled, they have no native path and fall back to Spark.
 */
class CometStructuredTextSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private def withStringTable(rows: String)(thunk: => Unit): Unit = {
    withTable("t") {
      sql("CREATE TABLE t (s STRING) USING parquet")
      sql(s"INSERT INTO t VALUES $rows")
      thunk
    }
  }

  private val csvRows = "('1,abc'), ('2,def'), (''), (null)"
  private val jsonRows = "('{\"a\":1,\"b\":2}'), ('{\"x\":true}'), ('{}'), (null)"
  private val xmlRows = "('<a><b>1</b><b>2</b></a>'), ('<a><b>9</b></a>'), ('<a/>'), (null)"

  // Disable constant folding so functions whose arguments are literals (schema_of_*) actually
  // execute inside Comet rather than being folded away at optimization time.
  private def withoutConstantFolding(thunk: => Unit): Unit = {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
      thunk
    }
  }

  test("from_csv") {
    withStringTable(csvRows) {
      checkSparkAnswerAndOperator("SELECT from_csv(s, 'a INT, b STRING') FROM t")
    }
  }

  test("schema_of_csv") {
    withoutConstantFolding {
      checkSparkAnswerAndOperator("SELECT schema_of_csv('1,abc')")
    }
  }

  test("schema_of_json") {
    withoutConstantFolding {
      checkSparkAnswerAndOperator("SELECT schema_of_json('{\"a\":1,\"b\":\"str\"}')")
    }
  }

  test("json_object_keys") {
    withStringTable(jsonRows) {
      checkSparkAnswerAndOperator("SELECT json_object_keys(s) FROM t")
    }
  }

  test("xpath_boolean") {
    withStringTable(xmlRows) {
      checkSparkAnswerAndOperator("SELECT xpath_boolean(s, 'a/b') FROM t")
    }
  }

  test("xpath_short / xpath_int / xpath_long") {
    withStringTable(xmlRows) {
      checkSparkAnswerAndOperator(
        "SELECT xpath_short(s, 'sum(a/b)'), xpath_int(s, 'sum(a/b)'), " +
          "xpath_long(s, 'sum(a/b)') FROM t")
    }
  }

  test("xpath_float / xpath_double") {
    withStringTable(xmlRows) {
      checkSparkAnswerAndOperator(
        "SELECT xpath_float(s, 'sum(a/b)'), xpath_double(s, 'sum(a/b)') FROM t")
    }
  }

  test("xpath_string") {
    withStringTable(xmlRows) {
      checkSparkAnswerAndOperator("SELECT xpath_string(s, 'a/b[1]') FROM t")
    }
  }

  test("xpath") {
    withStringTable(xmlRows) {
      checkSparkAnswerAndOperator("SELECT xpath(s, 'a/b/text()') FROM t")
    }
  }

  // XML SQL functions (from_xml / to_xml / schema_of_xml) were added in Spark 4.0, so these run
  // only on 4.0+.

  test("from_xml") {
    assume(isSpark40Plus)
    withStringTable(xmlRows) {
      checkSparkAnswerAndOperator("SELECT from_xml(s, 'b ARRAY<INT>') FROM t")
    }
  }

  test("to_xml") {
    assume(isSpark40Plus)
    withStringTable(csvRows) {
      checkSparkAnswerAndOperator("SELECT to_xml(named_struct('a', 1, 'b', s)) FROM t")
    }
  }

  test("schema_of_xml") {
    assume(isSpark40Plus)
    withoutConstantFolding {
      checkSparkAnswerAndOperator("SELECT schema_of_xml('<a><b>1</b></a>')")
    }
  }

  test("from_xml falls back to Spark when codegen dispatcher disabled") {
    assume(isSpark40Plus)
    withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
      withStringTable(xmlRows) {
        checkSparkAnswerAndFallbackReason(
          "SELECT from_xml(s, 'b ARRAY<INT>') FROM t",
          CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key)
      }
    }
  }

  test("from_csv falls back to Spark when codegen dispatcher disabled") {
    withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
      withStringTable(csvRows) {
        checkSparkAnswerAndFallbackReason(
          "SELECT from_csv(s, 'a INT, b STRING') FROM t",
          CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key)
      }
    }
  }

  test("xpath falls back to Spark when codegen dispatcher disabled") {
    withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
      withStringTable(xmlRows) {
        checkSparkAnswerAndFallbackReason(
          "SELECT xpath_int(s, 'sum(a/b)') FROM t",
          CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key)
      }
    }
  }
}
