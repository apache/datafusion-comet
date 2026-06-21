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

// Reproducers for Delta Change Data Feed (CDF / CDC) reads.
//
// In the v4.1.0 regression run the entire DeltaCDCSuite family fails with:
//   org.apache.comet.CometNativeException:
//     Comet Internal Error: Output column count mismatch: expected 14, got 13
//
// Root cause hypothesis: a CDC read (option readChangeFeed=true) augments
// the scan output with _change_type, _commit_version, and _commit_timestamp
// metadata columns. The native Delta scan in contrib-delta isn't aware of
// these and emits the base table's column count; the operator above
// expects N+3.
//
// The fix is expected to be a fall-back gate in DeltaScanRule: when the
// scan output exposes CDC metadata columns, decline the native path and
// let Spark/Delta handle it.
class CometDeltaCdcSuite extends CometDeltaTestBase {

  test("CDC read across versions returns _change_type/_commit_version/_commit_timestamp columns") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cdc_basic") { tablePath =>
      spark.sql(
        s"""CREATE OR REPLACE TABLE delta.`$tablePath`(id INT, v STRING)
            USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
      spark.sql(s"INSERT INTO delta.`$tablePath` VALUES (1, 'a'), (2, 'b')")
      spark.sql(s"UPDATE delta.`$tablePath` SET v = 'A' WHERE id = 1")
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id = 2")

      val df = spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "0")
        .load(tablePath)

      val rows = df.collect()
      assert(rows.nonEmpty, "CDC read returned no rows")
      val schema = df.schema.fieldNames.toSet
      assert(schema.contains("_change_type"),
        s"CDC schema missing _change_type: ${df.schema.fieldNames.mkString(",")}")
      assert(schema.contains("_commit_version"),
        s"CDC schema missing _commit_version: ${df.schema.fieldNames.mkString(",")}")
      assert(schema.contains("_commit_timestamp"),
        s"CDC schema missing _commit_timestamp: ${df.schema.fieldNames.mkString(",")}")

      // CDC now reads natively via delta-kernel's TableChanges: CometExecRule intercepts the
      // RowDataSourceScanExec over DeltaCDFRelation and replaces it with a CometDeltaCdfScanExec,
      // which emits the data columns + _change_type / _commit_version / _commit_timestamp. Inspect
      // the plan only after collect() so AQE has finalized it.
      val cdfScans = df.queryExecution.executedPlan.collect {
        case s: org.apache.spark.sql.comet.CometDeltaCdfScanExec => s
      }
      assert(cdfScans.nonEmpty,
        s"CDC read should engage the native kernel TableChanges path (CometDeltaCdfScanExec):\n" +
          s"${df.queryExecution.executedPlan}")

      // Correctness: the result with the native config ON must match vanilla Spark.
      // _commit_timestamp is wall-clock and nondeterministic, so drop it first.
      val cdcRead = () => spark.read.format("delta")
        .option("readChangeFeed", "true").option("startingVersion", "0")
        .load(tablePath).drop("_commit_timestamp")
      val nativeRows = cdcRead().collect().toSeq.map(normalizeRow)
      withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
        val vanillaRows = cdcRead().collect().toSeq.map(normalizeRow)
        assert(
          nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
          s"CDC native=$nativeRows vanilla=$vanillaRows")
      }
    }
  }

  test("CDC read with orderBy + unix_timestamp triggers the 14-vs-13 column mismatch") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    import org.apache.spark.sql.functions.{col, unix_timestamp}
    // Use catalog-named table to mirror DeltaCDCSuite's `withTable(tblName)`
    // shape, which goes through SessionCatalog -> different planning path than
    // `delta.`path``.
    val tblName = s"cdc_tz_${System.nanoTime()}"
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tblName")
      spark.sql(s"CREATE TABLE $tblName(id INT, name STRING, age INT) " +
        s"USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)")
      spark.sql(s"INSERT INTO $tblName(id, name, age) VALUES (1,'abc',20)")
      spark.sql(s"INSERT INTO $tblName(id, name, age) VALUES (2,'def',21)")
      spark.sql(s"UPDATE $tblName SET age = 19 WHERE id = 1")
      spark.sql(s"INSERT INTO $tblName(id, name, age) VALUES (3,'ghi',15)")
      spark.sql(s"DELETE FROM $tblName WHERE id = 3")

      val df = spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "0")
        .option("endingVersion", "10")
        .table(tblName)
        .orderBy("_commit_version", "_change_type")
        .select(col("_commit_version"), col("_change_type"),
          unix_timestamp(col("_commit_timestamp")))
      val rows = df.collect()
      assert(rows.nonEmpty)

      // CDC engages the native kernel TableChanges path even with an orderBy + projection on the
      // metadata columns. The orderBy makes this plan AQE-wrapped, so use the AdaptiveSparkPlanHelper
      // `collect` (strips AQE) rather than a plain TreeNode collect, which can't see inside the
      // finalized AQE subtree. Inspect after collect() so AQE has finalized the plan.
      val plan = df.queryExecution.executedPlan
      assert(
        collect(plan) {
          case s: org.apache.spark.sql.comet.CometDeltaCdfScanExec => s
        }.nonEmpty,
        s"CDC read should engage CometDeltaCdfScanExec:\n$plan")

      // Compare the deterministic CDC columns native-on vs vanilla (the
      // unix_timestamp(_commit_timestamp) projection above is wall-clock-derived
      // and nondeterministic, so it's excluded from the comparison).
      val detRead = () => spark.read.format("delta")
        .option("readChangeFeed", "true").option("startingVersion", "0")
        .option("endingVersion", "10").table(tblName)
        .orderBy("_commit_version", "_change_type")
        .select(col("_commit_version"), col("_change_type"))
      val nativeRows = detRead().collect().toSeq.map(normalizeRow)
      withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
        val vanillaRows = detRead().collect().toSeq.map(normalizeRow)
        assert(
          nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
          s"CDC native=$nativeRows vanilla=$vanillaRows")
      }
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tblName")
    }
  }

  test("user-defined column literally named _change_type round-trips through CDC scan") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cdc_userdef") { tablePath =>
      // User table happens to have a column named `_change_type` -- nothing
      // to do with CDF. The native scan must NOT mistake this for the
      // synthetic CDC metadata column.
      spark.sql(
        s"""CREATE OR REPLACE TABLE delta.`$tablePath`(id INT, _change_type STRING)
            USING DELTA""")
      spark.sql(s"INSERT INTO delta.`$tablePath` VALUES (1, 'user_value')")
      val rows = spark.read.format("delta").load(tablePath).collect()
      assert(rows.length === 1)
      assert(rows.head.getString(1) === "user_value")
    }
  }
}
