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

package org.apache.comet.ballista

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.serde.OperatorOuterClass.CometBallistaOffloadPlan

/**
 * Unit tests for [[BallistaOffloadPlanner]]: drives real Comet plans via SQL and asserts the
 * `CometBallistaOffloadPlan` descriptor the walker emits, without requiring the native `ballista`
 * feature to be built (no execution, only plan decomposition + serialization).
 */
class BallistaOffloadPlannerSuite extends CometTestBase {

  test("two-stage GROUP BY builds a 2-fragment linear descriptor with a hash edge on the key") {
    withParquetTable((0 until 100).map(i => (i % 5, i)), "t") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "4",
        CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> "false",
        CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "false") {
        val plan = sql("SELECT _1, count(*) FROM t GROUP BY _1").queryExecution.executedPlan
        val bytes = BallistaOffloadPlanner.buildOffloadPlan(plan, numPartitions = 4)
        val desc = CometBallistaOffloadPlan.parseFrom(bytes)
        assert(desc.getFragmentsCount == 2)
        assert(desc.getNumPartitions == 4)
        // fragment 1 (root) has one input from fragment 0, hashed on ordinal 0 (the group key)
        val rootInputs = desc.getFragments(1).getInputsList
        assert(rootInputs.size == 1)
        assert(rootInputs.get(0).getProducer == 0)
        assert(rootInputs.get(0).getHashKeyOrdinalsList.contains(0))
      }
    }
  }

  test("single native block builds a 1-fragment descriptor with no inputs") {
    withParquetTable((0 until 10).map(i => (i, i)), "t") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "false") {
        val plan = sql("SELECT _1 + 1 FROM t").queryExecution.executedPlan
        val desc = CometBallistaOffloadPlan.parseFrom(
          BallistaOffloadPlanner.buildOffloadPlan(plan, numPartitions = 4))
        assert(desc.getFragmentsCount == 1)
        assert(desc.getFragments(0).getInputsCount == 0)
      }
    }
  }

  test("shuffle-hash join builds a join fragment with two hash inputs on the join keys") {
    withParquetTable((0 until 50).map(i => (i, i * 10)), "l") {
      withParquetTable((0 until 50).map(i => (i, i * 100)), "r") {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.SHUFFLE_PARTITIONS.key -> "4",
          CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "false") {
          val plan =
            sql("SELECT l._2, r._2 FROM l JOIN r ON l._1 = r._1").queryExecution.executedPlan
          val desc = CometBallistaOffloadPlan.parseFrom(
            BallistaOffloadPlanner.buildOffloadPlan(plan, numPartitions = 4))
          // root fragment = the join; two inputs (left, right) each on one key ordinal
          val join = desc.getFragments(desc.getFragmentsCount - 1)
          assert(join.getInputsCount == 2, s"expected 2 join inputs, got:\n$desc")
          assert(join.getInputs(0).getHashKeyOrdinalsList.size == 1)
          assert(join.getInputs(1).getHashKeyOrdinalsList.size == 1)
        }
      }
    }
  }

  test("range/ORDER BY exchange is rejected with a clear message") {
    withParquetTable((0 until 20).map(i => (i % 3, i)), "t") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "false") {
        val plan =
          sql("SELECT _1, count(*) c FROM t GROUP BY _1 ORDER BY _1").queryExecution.executedPlan
        val e = intercept[UnsupportedOperationException] {
          BallistaOffloadPlanner.buildOffloadPlan(plan, numPartitions = 4)
        }
        assert(e.getMessage.contains("HashPartitioning") || e.getMessage.contains("range"))
      }
    }
  }

  test("broadcast join is rejected with a clear message") {
    // Do NOT disable auto-broadcast (no AUTO_BROADCASTJOIN_THRESHOLD=-1 override): `r` is tiny
    // so Spark plans a broadcast join, giving a `CometBroadcastHashJoinExec` fed by a
    // `CometBroadcastExchangeExec` build side -- the out-of-scope shape the walker must reject.
    withParquetTable((0 until 50).map(i => (i, i * 10)), "l") {
      withParquetTable((0 until 5).map(i => (i, i * 100)), "r") {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.SHUFFLE_PARTITIONS.key -> "4",
          CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> "false",
          CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "false") {
          val plan =
            sql("SELECT l._2, r._2 FROM l JOIN r ON l._1 = r._1").queryExecution.executedPlan
          val e = intercept[UnsupportedOperationException] {
            BallistaOffloadPlanner.buildOffloadPlan(plan, numPartitions = 4)
          }
          assert(
            e.getMessage.toLowerCase.contains("broadcast"),
            s"expected a message mentioning broadcast, got: ${e.getMessage}")
        }
      }
    }
  }
}
