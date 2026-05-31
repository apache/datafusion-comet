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

import org.apache.spark.sql.functions._

// Regression guards for two Delta 4.1 own-suite edge cases that surfaced
// CORE Comet bugs (both now fixed). Each test mirrors the failing Delta test
// and asserts the corrected behavior:
//   F4 -- deeply-nested boolean predicate overflowed protobuf's recursion limit
//         (fixed by balancing And/Or chains in QueryPlanSerde).
//   F6 -- a corrupted/0-byte file produced a non-Spark error (fixed by mapping
//         object-store read errors to FAILED_READ_FILE in CometExecIterator).
class CometDeltaEdgeCaseRegressionSuite extends CometDeltaTestBase {

  // === F4: deeply-nested data-skipping expression -> protobuf recursion
  //
  // Mirrors DataSkippingDeltaTests "remove redundant stats column references in
  // data skipping expression". A WHERE with ~101 AND'd conditions builds a very
  // deep boolean expression; serializing it left-deep made the plan proto exceed
  // protobuf's default recursion limit (100) when re-parsed
  // (CometNativeExec.findShuffleScanIndices), throwing "Protocol message had too
  // many levels of nesting". Fixed by balancing And/Or chains in base Comet's
  // serializer (QueryPlanSerde.createBalancedBinaryExpr). Kept as a guard.

  test("F4: deeply-nested data-skipping filter does not overflow protobuf nesting") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    val tbl = "f4_deep_filter"
    withTable(tbl) {
      val colNames = (0 to 100).map(i => s"col_$i")
      spark.sql(
        s"CREATE TABLE $tbl (${colNames.map(_ + " INT").mkString(", ")}) USING delta")
      spark.sql(
        s"INSERT INTO $tbl VALUES (${colNames.map(_ => "0").mkString(", ")})")
      val whereClause = colNames.map(c => s"$c != 1").mkString(" AND ")
      // Must not throw a protobuf recursion error.
      val rows = spark.sql(s"SELECT col_0 FROM $tbl WHERE $whereClause").collect()
      assert(rows.length == 1, s"expected the single all-zero row, got ${rows.length}")
    }
  }

  // === F6: corrupted / empty file (SC-8810) =================================
  //
  // Mirrors DeltaSuite "SC-8810: skipping deleted file still throws on corrupted
  // file". With one data file truncated to 0 bytes, vanilla Spark+Delta throws a
  // `[FAILED_READ_FILE.NO_HINT]` SparkException. Comet's native reader instead
  // throws `CometNativeException: ... Requested range was invalid`. Expected:
  // the error is the Spark-compatible one (so user-facing error handling and the
  // Delta test pass). Repro asserts the version-stable Spark wording (the
  // FAILED_READ_FILE class tag is Spark-4.x-only; see assertion below).

  test("F6: reading a corrupted file surfaces a Spark-compatible error (SC-8810)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("f6_corrupt") { tablePath =>
      val ss = spark
      import ss.implicits._
      Seq(1).toDF().write.format("delta").mode("append").save(tablePath)
      Seq(2, 2).toDF().write.format("delta").mode("append").save(tablePath)
      Seq(4).toDF().write.format("delta").mode("append").save(tablePath)

      // Truncate one data file to 0 bytes to simulate corruption.
      val dir = new java.io.File(tablePath)
      val parquet = dir.listFiles()
        .filter(f => !f.getName.startsWith("_") && f.getName.endsWith(".parquet"))
        .sortBy(_.getName)
        .head
      val ch = new java.io.FileOutputStream(parquet)
      try ch.getChannel.truncate(0) finally ch.close()

      val ex = intercept[Exception] {
        spark.read.format("delta").load(tablePath).collect()
      }
      val msg = Option(ex.getMessage).getOrElse("") +
        Option(ex.getCause).map(c => Option(c.getMessage).getOrElse("")).getOrElse("")
      // Assert on the version-stable wording, not the `FAILED_READ_FILE` error-class
      // literal: Spark's `cannotReadFilesError` only prepends `[FAILED_READ_FILE.NO_HINT]`
      // to getMessage on Spark 4.x; on Spark 3.4/3.5 the message is just
      // "Encountered error while reading file ...". (Same version-stability fix applied
      // to SparkErrorConverterSuite / CometExecSuite in #4536.)
      assert(
        msg.contains("Encountered error while reading file"),
        s"expected a Spark-compatible cannotReadFilesError, got: $msg")
    }
  }
}
