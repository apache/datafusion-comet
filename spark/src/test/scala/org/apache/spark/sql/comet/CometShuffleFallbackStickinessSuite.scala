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

import org.apache.comet.{CometConf, CometFallback}

/**
 * Pins the sticky-fallback invariant for Comet shuffle decisions: `nativeShuffleSupported` /
 * `columnarShuffleSupported` must return `false` whenever the shuffle already carries a
 * `CometFallback` marker from a prior rule pass.
 *
 * Without this behavior, AQE's stage-prep rule re-evaluation can flip the decision — e.g.,
 * `stageContainsDPPScan` walks the shuffle's child tree with `.exists`, but a materialized child
 * stage is wrapped in `ShuffleQueryStageExec` (a `LeafExecNode`) so `.exists` stops at the
 * wrapper and the DPP scan becomes invisible. That causes the same shuffle to fall back to Spark
 * at initial planning and then convert to Comet at stage prep, producing plan-shape
 * inconsistencies across the two passes (suspected mechanism behind #3949).
 *
 * Fallback decisions that must survive AQE replanning use `CometFallback.markForFallback`. The
 * shuffle-support predicates check `isMarkedForFallback` at the top and short-circuit.
 */
class CometShuffleFallbackStickinessSuite extends CometTestBase {

  test("both support predicates fall back when the shuffle carries a CometFallback marker") {
    val shuffle = ShuffleExchangeExec(SinglePartition, SyntheticLeaf(Nil))
    CometFallback.markForFallback(shuffle, "pretend prior pass decided Spark fallback")

    withSQLConf(CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "true") {
      assert(
        !CometShuffleExchangeExec.columnarShuffleSupported(shuffle),
        "marked shuffle must preserve its prior-pass fallback decision (columnar path)")
      assert(
        !CometShuffleExchangeExec.nativeShuffleSupported(shuffle),
        "marked shuffle must preserve its prior-pass fallback decision (native path)")
    }
  }

  test("informational explain-info alone does NOT force fallback") {
    // A shuffle can accumulate explain info (e.g. 'Comet native shuffle not enabled') as
    // informational output from earlier checks without being a full-fallback signal. That
    // info must not cause the columnar path to decline.
    val shuffle = ShuffleExchangeExec(SinglePartition, SyntheticLeaf(Nil))
    // Note: withInfo, not markForFallback.
    org.apache.comet.CometSparkSessionExtensions
      .withInfo(shuffle, "Comet native shuffle not enabled")
    assert(
      !CometFallback.isMarkedForFallback(shuffle),
      "explain info alone must not imply a sticky fallback marker")
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

      withSQLConf(
        CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "true",
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

        // Pass 1: real DPP subtree visible. Returns false AND marks the shuffle.
        val first = CometShuffleExchangeExec.columnarShuffleSupported(shuffle)
        assert(!first, "initial pass must fall back (DPP visible)")
        assert(
          CometFallback.isMarkedForFallback(shuffle),
          "fallback marker must be placed on the shuffle")

        // Pass 2 simulates AQE stage-prep: replace the child with an opaque leaf that hides
        // the DPP subtree from tree walks. A naive `.exists`-based check would flip to true
        // here; the sticky marker must keep the decision stable.
        val reshapedShuffle =
          shuffle
            .withNewChildren(Seq(SyntheticLeaf(shuffle.child.output)))
            .asInstanceOf[ShuffleExchangeExec]
        val second = CometShuffleExchangeExec.columnarShuffleSupported(reshapedShuffle)
        assert(
          !second,
          "second pass must still fall back even though the DPP subtree is now hidden")
      }
    }
  }
}

private case class SyntheticLeaf(output: Seq[Attribute]) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("stub")
}
