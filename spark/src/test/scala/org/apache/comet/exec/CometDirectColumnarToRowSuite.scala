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

package org.apache.comet.exec

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometColumnarToRowExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * End-to-end tests for `spark.comet.exec.columnarToRow.direct.enabled`, which routes
 * `CometColumnarToRowExec`'s non-codegen conversion through `DirectColumnarToRowConverter`.
 * Whole-stage codegen is disabled so the operator's `doExecute` conversion path runs.
 */
class CometDirectColumnarToRowSuite extends CometTestBase {

  private def withDirectConverter(minBatchSize: Int = 0)(f: => Unit): Unit = {
    withSQLConf(
      CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false",
      CometConf.COMET_DIRECT_COLUMNAR_TO_ROW_ENABLED.key -> "true",
      CometConf.COMET_DIRECT_COLUMNAR_TO_ROW_MIN_BATCH_SIZE.key -> minBatchSize.toString,
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false")(f)
  }

  private def checkQueryUsesJvmColumnarToRow(query: String): Unit = {
    val df = sql(query)
    val c2r = df.queryExecution.executedPlan.collect { case c: CometColumnarToRowExec => c }
    assert(c2r.nonEmpty, s"expected CometColumnarToRowExec in plan:\n${df.queryExecution}")
    checkSparkAnswer(df)
  }

  test("direct converter: mixed types with nulls") {
    withDirectConverter() {
      withParquetTable((0 until 1000).map(i => (i.toLong, i.toString, i % 5)), "tbl") {
        checkQueryUsesJvmColumnarToRow("""
            | SELECT
            |   _1,
            |   _2,
            |   CAST(_1 AS decimal(12,2)) AS dec_compact,
            |   CAST(_1 AS decimal(38,10)) AS dec_wide,
            |   DATE_ADD(DATE'2020-01-01', _3) AS dt,
            |   CAST(_1 AS double) AS dbl,
            |   CASE WHEN _3 = 0 THEN NULL ELSE _1 END AS maybe_null
            | FROM tbl
            |""".stripMargin)
      }
    }
  }

  test("direct converter: all-fixed-width schema takes the columnar fast path") {
    withDirectConverter() {
      withParquetTable((0 until 1000).map(i => (i.toLong, i, i.toDouble)), "tbl") {
        checkQueryUsesJvmColumnarToRow("""
            | SELECT
            |   _1,
            |   _2,
            |   _3,
            |   CAST(_1 AS decimal(10,2)) AS dec_compact,
            |   CASE WHEN _2 % 3 = 0 THEN NULL ELSE _2 END AS maybe_null
            | FROM tbl
            |""".stripMargin)
      }
    }
  }

  test("batches below minBatchSize fall back to default conversion") {
    withDirectConverter(minBatchSize = Int.MaxValue) {
      withParquetTable((0 until 1000).map(i => (i.toLong, i.toString)), "tbl") {
        // Every batch is below the threshold, so this exercises the per-batch fallback while
        // the direct converter is enabled.
        checkQueryUsesJvmColumnarToRow("SELECT _1, _2, CAST(_1 AS decimal(12,2)) AS dec FROM tbl")
      }
    }
  }

  test("unsupported schema falls back to default conversion") {
    withDirectConverter() {
      withParquetTable((0 until 100).map(i => (i, i.toString)), "tbl") {
        // BinaryType is not supported by DirectColumnarToRowConverter, so this exercises the
        // per-plan fallback to the UnsafeProjection path.
        checkQueryUsesJvmColumnarToRow("SELECT _1, CAST(_2 AS binary) FROM tbl")
      }
    }
  }
}
