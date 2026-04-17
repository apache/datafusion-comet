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

package org.apache.spark.sql.comet

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Pins the fix for the DPP fallback decision being stable across the initial-plan vs.
 * AQE-stage-prep passes.
 *
 * Background: Comet's `stageContainsDPPScan` (used by `columnarShuffleSupported`) walks the
 * shuffle's child tree looking for a `FileSourceScanExec` with a `PlanExpression` partition
 * filter. Under AQE, once a child stage materializes, the subtree is replaced by a
 * `ShuffleQueryStageExec` (a `LeafExecNode`, so `children == Seq.empty`). A plain `.exists` walk
 * cannot descend into it, so the DPP scan becomes invisible and the same shuffle flips from Spark
 * (initial plan) to Comet (stage prep) — producing plan-shape inconsistencies across the two
 * passes.
 *
 * The fix descends into `QueryStageExec.plan` explicitly. This suite verifies the fix.
 */
class CometDppFallbackConsistencySuite extends CometTestBase {

  private def buildDppTables(dir: java.io.File): Unit = {
    val factPath = s"${dir.getAbsolutePath}/fact.parquet"
    val dimPath = s"${dir.getAbsolutePath}/dim.parquet"
    withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
      val sess = spark
      import sess.implicits._
      val oneDay = 24L * 60L * 60000L
      val now = System.currentTimeMillis()
      (0 until 400)
        .map(i => (i, new java.sql.Date(now + (i % 40) * oneDay), i.toString))
        .toDF("fact_id", "fact_date", "fact_str")
        .write
        .partitionBy("fact_date")
        .parquet(factPath)
      (0 until 40)
        .map(i => (i, new java.sql.Date(now + i * oneDay), i.toString))
        .toDF("dim_id", "dim_date", "dim_str")
        .write
        .parquet(dimPath)
    }
    spark.read.parquet(factPath).createOrReplaceTempView("dpp_consistency_fact")
    spark.read.parquet(dimPath).createOrReplaceTempView("dpp_consistency_dim")
  }

  private def unwrapAqe(plan: SparkPlan): SparkPlan = plan match {
    case a: AdaptiveSparkPlanExec => a.initialPlan
    case other => other
  }

  private def findFirstShuffle(plan: SparkPlan): Option[ShuffleExchangeExec] = {
    var found: Option[ShuffleExchangeExec] = None
    plan.foreach {
      case s: ShuffleExchangeExec if found.isEmpty => found = Some(s)
      case _ =>
    }
    found
  }

  test("columnarShuffleSupported detects DPP through a materialized ShuffleQueryStageExec") {
    withTempDir { dir =>
      buildDppTables(dir)
      withSQLConf(
        CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "true",
        // Force SMJ so we get a shuffle above the DPP scan.
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {

        val df = spark.sql(
          "select f.fact_date, count(*) c " +
            "from dpp_consistency_fact f " +
            "join dpp_consistency_dim d on f.fact_date = d.dim_date " +
            "where d.dim_id > 35 " +
            "group by f.fact_date")
        val initialPlan = unwrapAqe(df.queryExecution.executedPlan)

        val dppShuffle = findFirstShuffle(initialPlan).getOrElse {
          fail(s"No ShuffleExchangeExec found in initial plan:\n${initialPlan.treeString}")
        }

        // (1) Direct call — the child subtree is walkable, DPP is visible.
        val initialDecision = CometShuffleExchangeExec.columnarShuffleSupported(dppShuffle)

        // (2) Simulate AQE having materialized the DPP stage: wrap the real DPP shuffle in a
        // ShuffleQueryStageExec (the exact wrapper AQE places around a completed shuffle), then
        // parent it under a new outer shuffle. The outer shuffle's child is now an opaque stage
        // from the perspective of a naive `.exists` walk, but the DPP scan still lives in
        // `stage.plan`.
        val materializedStage = ShuffleQueryStageExec(
          id = 0,
          plan = dppShuffle,
          _canonicalized = dppShuffle.canonicalized)
        val outerShuffle = ShuffleExchangeExec(SinglePartition, materializedStage)
        val postAqeDecision = CometShuffleExchangeExec.columnarShuffleSupported(outerShuffle)

        // The fix descends into `QueryStageExec.plan`, so the outer shuffle must also see the
        // DPP scan through the materialized stage and fall back to Spark. Without the fix
        // this would return true (Comet shuffle), producing plan-shape inconsistencies across
        // the two planning passes.
        assert(
          !initialDecision,
          s"expected Spark fallback for the DPP shuffle, got $initialDecision")
        assert(
          !postAqeDecision,
          "expected Spark fallback for a shuffle whose child tree contains a materialized " +
            "ShuffleQueryStageExec wrapping a DPP scan, but got Comet conversion")
      }
    }
  }
}
