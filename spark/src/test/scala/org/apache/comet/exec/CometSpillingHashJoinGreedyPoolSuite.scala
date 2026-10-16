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

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometHashJoinExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * The spilling shuffled hash join under the `greedy_unified` memory pool, which has no share of
 * its own: it takes memory from Spark's off-heap pool until Spark refuses. The off-heap pool is
 * kept small, so that Spark refuses before a task's build side fits.
 */
class CometSpillingHashJoinGreedyPoolSuite extends CometTestBase {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.memory.offHeap.size", "32m")
      .set(CometConf.COMET_OFFHEAP_MEMORY_POOL_TYPE.key, "greedy_unified")

  test("a shuffled hash join spills when Spark refuses it memory") {
    // The join builds on the smaller table. Each of its two tasks builds on about 20 MB of
    // rows, and holding them, their concatenation and the table takes more than the whole
    // off-heap pool.
    val padding = "x" * 40
    withParquetTable(
      (0 until 600000).map(i => (i % 150000, i, s"build-payload-$i-$padding")),
      "build") {
      withParquetTable(
        (0 until 900000).map(i => (i % 200000, i, s"probe-payload-$i-$padding")),
        "probe") {
        // Comet forces the hash join, while Spark's own run of the query, which gives the
        // expected answer, keeps the sort-merge join that it can spill.
        withSQLConf(
          CometConf.COMET_FORCE_SHJ.key -> "true",
          CometConf.COMET_FORCE_SHJ_MAX_BUILD_SIZE.key -> "-1",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.SHUFFLE_PARTITIONS.key -> "2",
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
          // Both strings are read, so that the probe side stays the larger one.
          val df = sql(
            "SELECT count(*), sum(probe._2), sum(length(probe._3)), sum(build._2), " +
              "sum(length(build._3)) FROM probe JOIN build ON probe._1 = build._1")
          val (_, comet) = checkSparkAnswerAndOperator(df)
          val joins = collect(comet) { case join: CometHashJoinExec => join }
          assert(joins.nonEmpty)
          assert(joins.map(_.metrics("spilled_buckets").value).sum > 0)
          assert(joins.map(_.metrics("budget_overflow_bytes").value).sum == 0)
        }
      }
    }
  }
}
