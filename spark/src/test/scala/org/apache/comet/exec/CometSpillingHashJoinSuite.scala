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

import org.apache.spark.SparkException
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.comet.{CometBroadcastHashJoinExec, CometHashJoinExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/** The native shuffled hash join that spills its build side when it does not fit in memory. */
class CometSpillingHashJoinSuite extends CometTestBase {

  // Plan shuffled hash joins where the hints ask for them, and never broadcast.
  private val shuffledHashJoins = Seq(
    SQLConf.PREFER_SORTMERGEJOIN.key -> "false",
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1")

  private def hashJoins(plan: SparkPlan): Seq[CometHashJoinExec] =
    collect(plan) { case join: CometHashJoinExec => join }

  /** A metric of the hash joins in `plan`, which must have run. */
  private def metric(plan: SparkPlan, name: String): Long =
    hashJoins(plan).map(_.metrics(name).value).sum

  /** Builds of 200,000 rows and probes of 300,000, with null keys on both sides. */
  private def withJoinTables(f: => Unit): Unit = {
    withParquetTable(
      (0 until 200000).map(i =>
        (if (i % 97 == 0) None else Some(i % 50000), i, s"build-payload-$i")),
      "build") {
      withParquetTable(
        (0 until 300000).map(i => (if (i % 89 == 0) None else Some(i % 90000), i)),
        "probe") {
        f
      }
    }
  }

  test("a shuffled hash join spills instead of failing when its build side does not fit") {
    withJoinTables {
      val query = "SELECT /*+ SHUFFLE_HASH(build) */ probe._1, probe._2, build._2, build._3 " +
        "FROM probe JOIN build ON probe._1 = build._1"
      // About 1 MB of native memory per task, far less than a task's build side.
      val smallPool = CometConf.COMET_OFFHEAP_MEMORY_POOL_FRACTION.key -> (1.0 / 2048).toString
      withSQLConf(shuffledHashJoins :+ smallPool: _*) {
        withSQLConf(CometConf.COMET_EXEC_HASH_JOIN_SPILL_ENABLED.key -> "false") {
          val error = intercept[SparkException](sql(query).collect())
          assert(
            causeChain(error).exists(e =>
              Option(e.getMessage).exists(_.contains("Additional allocation failed for"))),
            error)
        }
        val (_, comet) = checkSparkAnswerAndOperator(sql(query))
        assert(metric(comet, "spilled_buckets") > 0)
        assert(metric(comet, "spilled_bytes") > 0)
        assert(metric(comet, "budget_overflow_bytes") == 0)
      }
    }
  }

  test("a shuffled hash join whose build side fits does not spill") {
    withJoinTables {
      withSQLConf(shuffledHashJoins: _*) {
        val df = sql(
          "SELECT /*+ SHUFFLE_HASH(build) */ * FROM probe LEFT JOIN build " +
            "ON probe._1 = build._1")
        val (_, comet) = checkSparkAnswerAndOperator(df)
        assert(hashJoins(comet).nonEmpty)
        assert(metric(comet, "spilled_buckets") == 0)
      }
    }
  }

  test("every join type matches Spark when the build side spills") {
    withJoinTables {
      // Each join builds on a side that Spark 3.4 can build a shuffled hash join on.
      val joins = Seq(
        "SELECT /*+ SHUFFLE_HASH(build) */ * FROM probe JOIN build ON probe._1 = build._1",
        "SELECT /*+ SHUFFLE_HASH(build) */ * FROM probe LEFT JOIN build ON probe._1 = build._1",
        "SELECT /*+ SHUFFLE_HASH(build) */ * FROM build RIGHT JOIN probe ON probe._1 = build._1",
        "SELECT /*+ SHUFFLE_HASH(build) */ * FROM probe FULL JOIN build ON probe._1 = build._1",
        "SELECT /*+ SHUFFLE_HASH(build) */ * FROM probe LEFT SEMI JOIN build " +
          "ON probe._1 = build._1",
        "SELECT /*+ SHUFFLE_HASH(build) */ * FROM probe LEFT ANTI JOIN build " +
          "ON probe._1 = build._1")
      withSQLConf(
        shuffledHashJoins :+
          (CometConf.COMET_EXEC_HASH_JOIN_SPILL_MAX_BUILD_BUFFER_BYTES.key -> "262144"): _*) {
        for (join <- joins; condition <- Seq("", " AND build._2 % 7 < probe._2 % 5")) {
          val (_, comet) = checkSparkAnswerAndOperator(sql(join + condition))
          assert(metric(comet, "spilled_buckets") > 0, join + condition)
          assert(metric(comet, "budget_overflow_bytes") == 0, join + condition)
        }
      }
    }
  }

  test("an existence join matches Spark when the build side spills") {
    withJoinTables {
      withSQLConf(
        shuffledHashJoins ++ Seq(
          "spark.sql.join.forceApplyShuffledHashJoin" -> "true",
          CometConf.COMET_EXEC_HASH_JOIN_SPILL_MAX_BUILD_BUFFER_BYTES.key -> "262144"): _*) {
        val df =
          sql("SELECT * FROM probe WHERE probe._1 IN (SELECT _1 FROM build) OR probe._2 % 10 = 3")
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("only shuffled hash joins whose output order nothing relies on spill") {
    def spillEnabled(df: DataFrame): Seq[Boolean] =
      collect(df.queryExecution.executedPlan) {
        case join: CometHashJoinExec => join.nativeOp.getHashJoin.getSpillEnabled
        case join: CometBroadcastHashJoinExec => join.nativeOp.getHashJoin.getSpillEnabled
      }
    withParquetTable((0 until 1000).map(i => (i % 100, i)), "a") {
      withParquetTable((0 until 1000).map(i => (i % 100, i)), "b") {
        withParquetTable((0 until 1000).map(i => (i % 100, i)), "c") {
          withSQLConf(
            shuffledHashJoins :+ (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"): _*) {
            val shuffled = "SELECT /*+ SHUFFLE_HASH(b) */ * FROM a JOIN b ON a._1 = b._1"
            val df = sql(shuffled)
            df.collect()
            assert(spillEnabled(df) == Seq(true))

            withSQLConf(CometConf.COMET_EXEC_HASH_JOIN_SPILL_ENABLED.key -> "false") {
              val disabled = sql(shuffled)
              disabled.collect()
              assert(spillEnabled(disabled) == Seq(false))
            }

            withSQLConf(CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ENABLED.key -> "true") {
              val filtered = sql(shuffled)
              filtered.collect()
              assert(spillEnabled(filtered) == Seq(false))
            }

            val broadcast = sql("SELECT /*+ BROADCAST(b) */ * FROM a JOIN b ON a._1 = b._1")
            broadcast.collect()
            assert(spillEnabled(broadcast) == Seq(false))

            // The sort-merge join below needs no exchange above it, so the hash join probes its
            // sorted output and Spark reports that order for the hash join too. The query's
            // output keeps that order.
            val ordered = sql(
              "SELECT /*+ MERGE(a, b), SHUFFLE_HASH(c) */ * FROM a JOIN b ON a._1 = b._1 " +
                "JOIN c ON a._1 = c._1")
            checkSparkAnswerAndOperator(ordered)
            assert(spillEnabled(ordered) == Seq(false))

            // The same hash join under an aggregate: Spark reports the order, but the aggregate
            // does not use it, so the join spills.
            withSQLConf(
              CometConf.COMET_EXEC_HASH_JOIN_SPILL_MAX_BUILD_BUFFER_BYTES.key -> "1024") {
              val aggregated = sql(
                "SELECT /*+ MERGE(a, b), SHUFFLE_HASH(c) */ a._1, count(*), sum(c._2) FROM a " +
                  "JOIN b ON a._1 = b._1 JOIN c ON a._1 = c._1 GROUP BY a._1")
              val (_, comet) = checkSparkAnswerAndOperator(aggregated)
              assert(spillEnabled(aggregated) == Seq(true))
              val joins = collect(comet) { case join: CometHashJoinExec => join }
              assert(joins.forall(_.outputOrdering.isEmpty))
              assert(metric(comet, "spilled_buckets") > 0)
            }
          }
        }
      }
    }
  }
}
