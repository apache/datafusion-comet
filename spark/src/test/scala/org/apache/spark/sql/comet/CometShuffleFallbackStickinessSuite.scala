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
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.{hasExplainInfo, withInfo}

/**
 * Pins the sticky-fallback invariant for Comet shuffle decisions: `shuffleSupported` must return
 * `None` whenever the shuffle already carries explain info from a prior rule pass.
 *
 * Without this behavior, AQE's stage-prep rule re-evaluation can flip the decision - e.g.,
 * `stageContainsDPPScan` walks the shuffle's child tree with `.exists`, but a materialized child
 * stage is wrapped in `ShuffleQueryStageExec` (a `LeafExecNode`) so `.exists` stops at the
 * wrapper and the DPP scan becomes invisible. That causes the same shuffle to fall back to Spark
 * at initial planning and then convert to Comet at stage prep, producing plan-shape
 * inconsistencies across the two passes (suspected mechanism behind #3949).
 *
 * The coordinator tags the node with `withInfos` only on total fallback and short-circuits via
 * `hasExplainInfo` on subsequent passes.
 */
class CometShuffleFallbackStickinessSuite extends CometTestBase {

  test("shuffleSupported returns None when the shuffle already carries explain info") {
    val shuffle = ShuffleExchangeExec(SinglePartition, SyntheticLeaf(Nil))
    withInfo(shuffle, "pretend prior pass decided Spark fallback")

    assert(
      CometShuffleExchangeExec.shuffleSupported(shuffle).isEmpty,
      "marked shuffle must preserve its prior-pass fallback decision")
  }

  test(
    "DPP fallback decision is sticky across two invocations even when the child tree changes") {
    withTempDir { dir =>
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
      spark.read.parquet(factPath).createOrReplaceTempView("t_sticky_fact")
      spark.read.parquet(dimPath).createOrReplaceTempView("t_sticky_dim")

      // Disable native scan so the scan stays as FileSourceScanExec with DPP,
      // producing the mixed state (Spark shuffle wrapping Spark DPP scan) that
      // stageContainsDPPScan is designed to catch. With native scan enabled, AQE DPP
      // scans convert to CometNativeScanExec and the shuffle goes native.
      withSQLConf(
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {

        val df = spark.sql(
          "select f.fact_date, count(*) c from t_sticky_fact f " +
            "join t_sticky_dim d on f.fact_date = d.dim_date " +
            "where d.dim_id > 35 group by f.fact_date")
        val initial = df.queryExecution.executedPlan match {
          case a: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec => a.initialPlan
          case other => other
        }
        val shuffle =
          initial
            .collectFirst { case s: ShuffleExchangeExec => s }
            .getOrElse(fail(s"no shuffle found:\n${initial.treeString}"))

        // Pass 1: real DPP subtree visible. Returns None AND tags the shuffle.
        val first = CometShuffleExchangeExec.shuffleSupported(shuffle)
        assert(first.isEmpty, "initial pass must fall back (DPP visible)")
        assert(hasExplainInfo(shuffle), "fallback reason must be tagged on the shuffle")

        // Pass 2 simulates AQE stage-prep: replace the child with an opaque leaf that hides
        // the DPP subtree from tree walks. A naive `.exists`-based check would flip to "convert"
        // here; the sticky tag must keep the decision stable.
        val reshapedShuffle =
          shuffle
            .withNewChildren(Seq(SyntheticLeaf(shuffle.child.output)))
            .asInstanceOf[ShuffleExchangeExec]
        val second = CometShuffleExchangeExec.shuffleSupported(reshapedShuffle)
        assert(
          second.isEmpty,
          "second pass must still fall back even though the DPP subtree is now hidden")
      }
    }
  }
}

private case class SyntheticLeaf(output: Seq[Attribute]) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("stub")
}
