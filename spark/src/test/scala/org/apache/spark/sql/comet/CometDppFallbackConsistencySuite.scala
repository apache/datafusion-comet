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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Demonstrates the suspected root cause of issue #3949. Comet's DPP fallback decision
 * (`columnarShuffleSupported` → `stageContainsDPPScan`) walks `s.child.exists(...)` to look for a
 * `FileSourceScanExec` with a `PlanExpression` partition filter. That walk is not stable across
 * the two planning passes:
 *
 *   - initial planning: the shuffle's child subtree includes the DPP scan, so `.exists` finds it
 *     and Comet falls back (keeps the shuffle as Spark).
 *   - AQE stage-prep (after the inner stage materializes): the DPP subtree is replaced by an
 *     opaque wrapper (`ShuffleQueryStageExec`, whose `children == Seq.empty`). `.exists` can no
 *     longer see the scan, `stageContainsDPPScan` returns false, and the same shuffle is
 *     converted to Comet.
 *
 * That decision flip changes the plan shape between passes — the suspected trigger for the
 * canonicalization assertion in #3949. A fix needs `stageContainsDPPScan` to descend into
 * `QueryStageExec.plan` when walking for DPP filters.
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

  test("columnarShuffleSupported decision flips when child is wrapped in ShuffleQueryStageExec") {
    withTempDir { dir =>
      buildDppTables(dir)
      withSQLConf(
        CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "true",
        // Force SMJ so we get a shuffle above the DPP scan.
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {

        // Aggregation guarantees a ShuffleExchangeExec; the join's DPP filter attaches below.
        val df = spark.sql(
          "select f.fact_date, count(*) c " +
            "from dpp_consistency_fact f " +
            "join dpp_consistency_dim d on f.fact_date = d.dim_date " +
            "where d.dim_id > 35 " +
            "group by f.fact_date")
        val initialPlan = unwrapAqe(df.queryExecution.executedPlan)

        val shuffle = findFirstShuffle(initialPlan).getOrElse {
          fail(s"No ShuffleExchangeExec found in initial plan:\n${initialPlan.treeString}")
        }

        // (1) initial-plan decision
        val initialDecision = CometShuffleExchangeExec.columnarShuffleSupported(shuffle)

        // Prove the DPP scan is visible in the initial child subtree.
        val initialDppVisible = shuffle.child.exists {
          case scan: FileSourceScanExec =>
            scan.partitionFilters.exists(e =>
              e.exists(
                _.isInstanceOf[org.apache.spark.sql.catalyst.expressions.PlanExpression[_]]))
          case _ => false
        }

        // (2) simulate AQE stage-prep: swap the child for a LeafExecNode, matching the
        // tree-walking behavior of ShuffleQueryStageExec (whose children is Seq.empty from the
        // perspective of `.exists`). This models what the shuffle "sees" after its child stage
        // has materialized and been replaced by an opaque stage wrapper.
        val hiddenChild = OpaqueStageStub(shuffle.child.output)
        val postAqeShuffle =
          shuffle.withNewChildren(Seq(hiddenChild)).asInstanceOf[ShuffleExchangeExec]
        val postAqeDecision = CometShuffleExchangeExec.columnarShuffleSupported(postAqeShuffle)

        val postAqeDppVisible = postAqeShuffle.child.exists {
          case scan: FileSourceScanExec =>
            scan.partitionFilters.exists(e =>
              e.exists(
                _.isInstanceOf[org.apache.spark.sql.catalyst.expressions.PlanExpression[_]]))
          case _ => false
        }

        // scalastyle:off println
        println(s"=== DPP consistency check ===")
        println(s"initial shuffle.child:\n${shuffle.child.treeString}")
        println(s"initialDppVisible=$initialDppVisible, initialDecision=$initialDecision")
        println(s"postAqeDppVisible=$postAqeDppVisible, postAqeDecision=$postAqeDecision")
        // scalastyle:on println

        // The DPP scan is only visible while the child subtree is walkable; hiding it behind
        // an opaque stage wrapper removes it from `.exists`. This is the mechanism.
        assert(initialDppVisible, "sanity: initial child tree should expose DPP scan")
        assert(!postAqeDppVisible, "sanity: stage-wrapped child should hide DPP scan")

        // The bug: Comet decides to fall back at initial planning but to convert to Comet
        // once the stage has materialized. That is the same shuffle getting two different
        // treatments across the two passes.
        //
        // TODO(#3949): once `stageContainsDPPScan` descends into `QueryStageExec.plan`, flip
        // this assertion to `initialDecision == postAqeDecision`.
        assert(
          initialDecision == false,
          s"expected Spark fallback initially, got $initialDecision")
        assert(
          postAqeDecision == true,
          s"expected Comet conversion after stage wrap, got $postAqeDecision " +
            "(if this now returns false, the bug has been fixed — invert these assertions)")
      }
    }
  }
}

/**
 * LeafExecNode stub that mimics how `ShuffleQueryStageExec` presents itself to tree walks:
 * `children == Seq.empty`, so `.exists(...)` cannot descend. Used to model what the parent
 * shuffle's `.child` looks like after the inner stage has materialized.
 */
private case class OpaqueStageStub(output: Seq[Attribute]) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("stub")
}
