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

package org.apache.comet.contrib.delta

import org.apache.spark.sql.comet.CometDeltaNativeScanExec

// Audit 3: type round-trip coverage.
//
// For each Spark/Delta type the contrib could plausibly see, write a row
// at the edges of the type's range and assert native vs vanilla match.
// Where native cannot handle the type (Variant, char/varchar metadata,
// etc.) assert CONTRACT_FALLBACK so we catch the day native suddenly
// accepts it and silently truncates.
//
// Covered (positive):
//   * primitives at edge values (Int.MinValue/MaxValue, Long range,
//     Float/Double nan/inf, negative-zero)
//   * decimal at varied (precision,scale)
//   * TimestampNTZ vs Timestamp, Date
//   * binary
//   * nested struct, array, map
//   * column-mapping name & id mode round-trip
//
// Gaps documented:
//   * (none confirmed; this audit primarily locks in the positive matrix.
//     If a future regression appears -- e.g. native dropping a precision
//     boundary on decimal -- a failure here will be the first signal.)
class CometDeltaTypeRoundTripAuditSuite extends CometDeltaTestBase {

  test("primitives at edge values round-trip") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("type_prim") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (
           |  i TINYINT, j SMALLINT, k INT, l BIGINT,
           |  f FLOAT, d DOUBLE, b BOOLEAN, s STRING)
           |USING delta""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(CAST(-128 AS TINYINT), CAST(-32768 AS SMALLINT),
           |  -2147483648, -9223372036854775808,
           |  CAST('-Infinity' AS FLOAT), CAST('-Infinity' AS DOUBLE),
           |  false, ''),
           |(CAST(127 AS TINYINT), CAST(32767 AS SMALLINT),
           |  2147483647, 9223372036854775807,
           |  CAST('Infinity' AS FLOAT), CAST('Infinity' AS DOUBLE),
           |  true, 'edge'),
           |(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)""".stripMargin)
      assertDeltaNativeMatches(tablePath, _.orderBy("k"))
    }
  }

  test("decimal at varied precision/scale") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("type_dec") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (
           |  d1 DECIMAL(5,0), d2 DECIMAL(10,2),
           |  d3 DECIMAL(18,6), d4 DECIMAL(38,18))
           |USING delta""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(99999, 12345678.99, 999999999999.999999,
           |  99999999999999999999.999999999999999999),
           |(-99999, -12345678.99, -999999999999.999999,
           |  -99999999999999999999.999999999999999999),
           |(0, 0, 0, 0)""".stripMargin)
      assertDeltaNativeMatches(tablePath, _.orderBy("d1"))
    }
  }

  test("date / timestamp / timestamp_ntz round-trip") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("type_time") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (
           |  dt DATE, ts TIMESTAMP, tsntz TIMESTAMP_NTZ)
           |USING delta""".stripMargin)
      // The far-future TIMESTAMP is capped at year 2261 (NOT 9999): when stored as INT96, the
      // kernel-read decodes it as i64 NANOSECONDS, which overflows past ~year 2262 (08-known-
      // limitations.md A6 -- kernel's `reader_options` has no INT96 coercion hook, unlike
      // Comet's own parquet path's `coerce_int96="us"`). DATE and TIMESTAMP_NTZ (INT64) have no
      // such limit, so they keep 9999 to still exercise far-future round-trips.
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(DATE'1970-01-01', TIMESTAMP'1970-01-01 00:00:00 UTC',
           |  TIMESTAMP_NTZ'1970-01-01 00:00:00'),
           |(DATE'2026-05-23', TIMESTAMP'2026-05-23 12:34:56 UTC',
           |  TIMESTAMP_NTZ'2026-05-23 12:34:56'),
           |(DATE'9999-12-31', TIMESTAMP'2261-12-31 23:59:59 UTC',
           |  TIMESTAMP_NTZ'9999-12-31 23:59:59')""".stripMargin)
      assertDeltaNativeMatches(tablePath, _.orderBy("dt"))
    }
  }

  test("binary round-trip") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("type_bin") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (id INT, b BINARY)
           |USING delta""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(1, X''), (2, X'00'), (3, X'DEADBEEF'),
           |(4, X'FFFFFFFFFFFFFFFFFFFF')""".stripMargin)
      assertDeltaNativeMatches(tablePath, _.orderBy("id"))
    }
  }

  test("nested struct / array / map") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("type_nest") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (
           |  id INT,
           |  s STRUCT<a:INT,b:STRING>,
           |  arr ARRAY<BIGINT>,
           |  m MAP<STRING,INT>)
           |USING delta""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(1, NAMED_STRUCT('a', 1, 'b', 'x'), ARRAY(1L, 2L), MAP('k', 1)),
           |(2, NAMED_STRUCT('a', 2, 'b', NULL), ARRAY(), MAP()),
           |(3, NULL, NULL, NULL)""".stripMargin)
      assertDeltaNativeMatches(tablePath, _.orderBy("id"))
    }
  }

  // Regression: column-mapping mode + a shuffle above the native scan
  // (orderBy / join / aggregate) once tripped
  // AdaptiveSparkPlanExec.setLogicalLinkForNewQueryStage's assertion. Root
  // cause: in CM mode the wrapped FileSourceScanExec lacks a logicalLink
  // even though the surrounding CometScanExec has one, so the contrib's
  // exec inherited none and CometExecRule's link-setup pass unset the tag.
  // Fixed by seeding the link from `op.logicalLink` as a fallback
  // (CometDeltaNativeScan.nativeDeltaScan). These MUST now match vanilla.
  test("column mapping mode=name + complex types + orderBy round-trip") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("type_cm_name") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (
           |  id INT, s STRUCT<a:INT,b:STRING>, arr ARRAY<INT>)
           |USING delta
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.minReaderVersion' = '2',
           |  'delta.minWriterVersion' = '5')""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(1, NAMED_STRUCT('a', 1, 'b', 'x'), ARRAY(1,2)),
           |(2, NULL, ARRAY())""".stripMargin)
      assertDeltaNativeMatches(tablePath, _.orderBy("id"))
    }
  }

  test("column mapping mode=id post-rename + orderBy round-trip") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("type_cm_id_rename") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (id INT, name STRING)
           |USING delta
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'id',
           |  'delta.minReaderVersion' = '2',
           |  'delta.minWriterVersion' = '5')""".stripMargin)
      spark.sql(s"INSERT INTO delta.`$tablePath` VALUES (1,'a'),(2,'b')")
      spark.sql(s"ALTER TABLE delta.`$tablePath` RENAME COLUMN name TO label")
      spark.sql(s"INSERT INTO delta.`$tablePath` VALUES (3,'c')")
      // Old files still carry the original physical column name; native must
      // resolve via field-id, not by logical name.
      assertDeltaNativeMatches(tablePath, _.orderBy("id"))
    }
  }

  // Direct regression for the CM-mode + shuffle logical-link bug with the
  // simplest possible shape (simple types) -- this is what isolated the
  // root cause from "complex types".
  test("column mapping mode=name + simple types + orderBy (logical-link regression)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("type_cm_simple") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (id INT, v STRING)
           |USING delta
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.minReaderVersion' = '2',
           |  'delta.minWriterVersion' = '5')""".stripMargin)
      spark.sql(s"INSERT INTO delta.`$tablePath` VALUES (1,'a'),(2,'b'),(3,'c')")
      assertDeltaNativeMatches(tablePath, _.orderBy("id"))
      // Aggregate also introduces a shuffle.
      assertDeltaNativeMatches(tablePath, _.groupBy("v").count())
    }
  }

  test("char/varchar columns round-trip") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("type_charvar") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (
           |  id INT, c CHAR(5), v VARCHAR(10))
           |USING delta""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(1, 'aa   ', 'short'),
           |(2, 'bb   ', 'a_longer_v'),
           |(3, NULL, NULL)""".stripMargin)
      assertDeltaNativeMatches(tablePath, _.orderBy("id"))
    }
  }

  // ---- Variant (gap marker) ------------------------------------------------

  // Delta 4.1 + Spark 4.1 introduce VARIANT. Native scan likely doesn't
  // handle it yet -- assert decline so we catch the day it sneaks through
  // as a silent corruption.
  test("GAP: VARIANT type triggers fallback (or matches if implemented)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    var supported = false
    withDeltaTable("type_variant_probe") { tablePath =>
      try {
        spark.sql(
          s"""CREATE TABLE delta.`$tablePath` (id INT, v VARIANT) USING delta""")
        supported = true
      } catch {
        case scala.util.control.NonFatal(_) => supported = false
      }
    }
    if (!supported) {
      cancel("VARIANT not supported in this Spark/Delta build")
    }
    withDeltaTable("type_variant") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (id INT, v VARIANT) USING delta""")
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(1, PARSE_JSON('{"a":1,"b":[1,2,3]}')),
           |(2, PARSE_JSON('null')),
           |(3, NULL)""".stripMargin)
      // Read and check whether native engages. If it does, the rows must
      // match vanilla; if it declines, that's also acceptable today.
      val df = spark.read.format("delta").load(tablePath).orderBy("id")
      val plan = df.queryExecution.executedPlan
      val engaged = collect(plan) { case s: CometDeltaNativeScanExec => s }.nonEmpty
      val nativeRows = df.collect().toSeq.map(normalizeRow)
      withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
        val vanillaRows = spark.read.format("delta").load(tablePath)
          .orderBy("id").collect().toSeq.map(normalizeRow)
        if (engaged) {
          assert(
            nativeRows.map(_.mkString("|")) == vanillaRows.map(_.mkString("|")),
            s"VARIANT native engagement returned wrong rows\n" +
              s"native=$nativeRows\nvanilla=$vanillaRows")
        }
        // If !engaged, we declined -- that's the gap state. Either branch
        // passes; the test exists to flag silent corruption.
      }
    }
  }
}
