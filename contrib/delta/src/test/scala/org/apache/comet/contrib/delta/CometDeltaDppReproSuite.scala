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

import org.apache.spark.sql.Row
import org.apache.spark.sql.comet.CometDeltaNativeScanExec

// Repro for regression family F1 (DPP):
//   MergeIntoSuite "...isPartitioned: true" fails with
//   "CometSubqueryAdaptiveBroadcastExec (should have been converted by
//   CometPlanAdaptiveDynamicPruningFilters) does not support the execute()
//   code path."
//
// Trigger: a broadcast hash join where a partitioned Delta table is the probe
// side and the join key is the partition column, so AQE+DPP inserts a dynamic
// partition-pruning InSubquery over the scan. CometExecRule wraps it as
// CometSubqueryAdaptiveBroadcastExec; CometPlanAdaptiveDynamicPruningFilters
// must convert it but does not for CometDeltaNativeScanExec.
//
// useStats=false forces DPP insertion regardless of the cost-benefit estimate
// (small test tables otherwise skip DPP).
class CometDeltaDppReproSuite extends CometDeltaTestBase {

  test("DPP broadcast join over partitioned Delta scan") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withSQLConf(
      "spark.sql.optimizer.dynamicPartitionPruning.enabled" -> "true",
      "spark.sql.optimizer.dynamicPartitionPruning.useStats" -> "false",
      "spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly" -> "true",
      "spark.sql.exchange.reuse" -> "true",
      "spark.sql.autoBroadcastJoinThreshold" -> "10485760") {
      withDeltaTable("dpp_join") { tablePath =>
        val ss = spark
        import ss.implicits._
        (0 until 2000)
          .map(i => (i.toLong, (i % 50).toLong, s"v_$i"))
          .toDF("id", "pkey", "v")
          .write
          .format("delta")
          .partitionBy("pkey")
          .save(tablePath)
        spark.read.format("delta").load(tablePath).createOrReplaceTempView("fact")

        withDeltaTable("dpp_dim") { dimPath =>
          (0 until 50)
            .map(i => (i.toLong, if (Set(3, 7, 11).contains(i)) "keep" else "drop"))
            .toDF("dimkey", "flag")
            .write.format("delta").save(dimPath)
          spark.read.format("delta").load(dimPath).createOrReplaceTempView("dim")

          val df = spark.sql(
            """SELECT f.id, f.pkey, f.v
              |FROM fact f JOIN dim d ON f.pkey = d.dimkey
              |WHERE d.flag = 'keep'""".stripMargin)
          val rows = df.collect()
          val plan = df.queryExecution.executedPlan
          val scans = collect(plan) { case s: CometDeltaNativeScanExec => s }
          // Native scan must engage (not fall back to Spark's Delta reader) and
          // must not crash on the DPP subquery (the original F1 regression).
          assert(scans.nonEmpty, s"expected CometDeltaNativeScanExec in plan:\n$plan")
          // Correctness: result equals all fact rows whose pkey is a kept dim key.
          // (Holds whether or not DPP pruning fires -- if it doesn't, the join
          // still filters; the scan just reads more partitions.)
          val expected = (0 until 2000).count(i => Set(3, 7, 11).contains(i % 50))
          assert(rows.length == expected, s"got ${rows.length} want $expected")
          // DPP pruning: the partitioned fact scan must read only the 3 matching
          // partitions (~120 rows), not all 2000.
          val factScanRows = scans
            .map(_.metrics.get("numOutputRows").map(_.value).getOrElse(0L)).max
          // 3 of 50 partitions kept => ~120 rows (40 per partition); allow slack
          // but require well under the full 2000 to prove real pruning.
          assert(
            factScanRows <= 200,
            s"DPP pruning did not apply: fact scan read $factScanRows rows " +
              "(expected ~120 for 3 of 50 partitions)")
        }
      }
    }
  }

  // Mirrors MergeIntoSuiteBase "basic case - local predicates - ... isPartitioned:
  // true" -- the actual failing Delta own-suite test. MERGE into a table partitioned
  // by the join key, with cross-join enabled (Delta's local-predicate path) so AQE+DPP
  // inserts a dynamic-partition-pruning InSubquery over the target read. The subquery
  // resolution here goes through the standard `child.executeColumnar() ->
  // waitForSubqueries()` lifecycle on the native block (not the scan's own
  // `ensureSubqueriesResolved`), so the CometSubqueryAdaptiveBroadcastExec must be
  // CONVERTED, not merely skipped.
  test("MERGE into partitioned table with local predicate (DPP)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withSQLConf(
      "spark.sql.optimizer.dynamicPartitionPruning.enabled" -> "true",
      "spark.sql.optimizer.dynamicPartitionPruning.useStats" -> "false",
      "spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly" -> "true",
      "spark.sql.exchange.reuse" -> "true",
      "spark.sql.crossJoin.enabled" -> "true",
      "spark.sql.autoBroadcastJoinThreshold" -> "10485760") {
      withDeltaTable("merge_dpp") { tablePath =>
        val ss = spark
        import ss.implicits._
        Seq(("1", "2", "noop"), ("1", "4", "noop"), ("3", "2", "noop"), ("4", "4", "noop"))
          .toDF("key2", "value", "op")
          .repartition(2)
          .write.format("delta").partitionBy("key2").save(tablePath)
        Seq(("1", "8"), ("0", "3")).toDF("key1", "value")
          .createOrReplaceTempView("merge_dpp_src")
        spark.sql(
          s"""MERGE INTO delta.`$tablePath` trg
             |USING merge_dpp_src src
             |ON src.key1 = trg.key2 AND trg.key2 < '3'
             |WHEN MATCHED THEN UPDATE SET
             |  key2 = src.key1, value = src.value, op = 'update'
             |WHEN NOT MATCHED THEN INSERT
             |  (key2, value, op) VALUES (src.key1, src.value, 'insert')""".stripMargin)
        val rows = spark.read.format("delta").load(tablePath).collect().toSet
        val expected = Set(
          Row("3", "2", "noop"), Row("4", "4", "noop"),
          Row("1", "8", "update"), Row("1", "8", "update"), Row("0", "3", "insert"))
        assert(rows == expected, s"MERGE result mismatch\n got=$rows\n want=$expected")
      }
    }
  }
}
