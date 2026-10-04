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

package org.apache.comet.rules

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.{CometExec, CometFilterExec, CometHashAggregateExec, CometNativeScanExec, CometSparkToColumnarExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.cost.{CometCostEstimate, CometCostModel, DefaultCometCostModel}

/** The default model, recording what it was asked and what it answered. */
class RecordingCostModel extends DefaultCometCostModel {
  override def estimate(cometPlan: SparkPlan, sparkPlan: SparkPlan): CometCostEstimate = {
    val estimate = super.estimate(cometPlan, sparkPlan)
    RecordingCostModel.calls.synchronized {
      RecordingCostModel.calls += ((cometPlan, sparkPlan, estimate))
    }
    estimate
  }
}

object RecordingCostModel {
  val calls: ArrayBuffer[(SparkPlan, SparkPlan, CometCostEstimate)] = ArrayBuffer.empty
}

class AlwaysRevertCostModel extends CometCostModel {
  override def estimate(cometPlan: SparkPlan, sparkPlan: SparkPlan): CometCostEstimate =
    CometCostEstimate(cometCost = 2.0, sparkCost = 1.0)
}

class FailingCostModel extends CometCostModel {
  override def estimate(cometPlan: SparkPlan, sparkPlan: SparkPlan): CometCostEstimate =
    throw new IllegalStateException("no estimate")
}

class CometCostModelSuite extends CometTestBase {

  // The scan is native, and Spark's projection of every scanned row follows a transition.
  private val scanThenSparkProject = "SELECT _1 + 1 FROM tbl"

  // As above, but a native filter removes most rows before the transition.
  private val filterThenSparkProject = "SELECT _1 + 1 FROM tbl WHERE _2 = 5"

  private val groupingAggregate = "SELECT _2, sum(_1) FROM tbl GROUP BY _2"

  private def withTestData(f: => Unit): Unit =
    withParquetTable((0 until 1000).map(i => (i, i % 10)), "tbl")(f)

  /** Leaves the projection to Spark, so the stage needs a transition below it. */
  private def withSparkProject(confs: (String, String)*)(f: => Unit): Unit =
    withSQLConf((CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false") +: confs: _*)(f)

  private def costModel(className: String): Seq[(String, String)] = Seq(
    CometConf.COMET_EXEC_COST_MODEL_ENABLED.key -> "true",
    CometConf.COMET_EXEC_COST_MODEL_CLASS.key -> className)

  private val defaultModel = costModel(classOf[DefaultCometCostModel].getName)

  private def cometOperators(plan: SparkPlan): Seq[CometExec] =
    collect(plan) { case op: CometExec => op }

  /** Runs `query` with the recording model and returns the estimate for each stage. */
  private def estimates(query: String): Seq[(SparkPlan, SparkPlan, CometCostEstimate)] = {
    RecordingCostModel.calls.clear()
    withSQLConf(costModel(classOf[RecordingCostModel].getName): _*) {
      checkSparkAnswer(query)
    }
    RecordingCostModel.calls.toSeq
  }

  test("the cost model is disabled by default") {
    assert(!CometConf.COMET_EXEC_COST_MODEL_ENABLED.get())
    withTestData {
      withSparkProject() {
        val (_, plan) = checkSparkAnswer(scanThenSparkProject)
        assert(collect(plan) { case scan: CometNativeScanExec => scan }.size == 1, s"$plan")
        assert(collect(plan) { case project: ProjectExec => project }.size == 1, s"$plan")
      }
    }
  }

  for (adaptive <- Seq(true, false)) {
    test(s"reverts a native scan that only feeds a Spark projection: AQE=$adaptive") {
      withTestData {
        withSparkProject(
          (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) +: defaultModel: _*) {
          val (_, plan) = checkSparkAnswerAndFallbackReason(
            scanThenSparkProject,
            "Stage reverted: estimated speedup from Comet of")
          assert(cometOperators(plan).isEmpty, s"$plan")
        }
      }
    }

    test(s"keeps a native filter that shrinks the input of a Spark projection: AQE=$adaptive") {
      withTestData {
        withSparkProject(
          (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) +: defaultModel: _*) {
          val (_, plan) = checkSparkAnswer(filterThenSparkProject)
          assert(collect(plan) { case filter: CometFilterExec => filter }.size == 1, s"$plan")
          assert(collect(plan) { case project: ProjectExec => project }.size == 1, s"$plan")
        }
      }
    }

    test(s"keeps both stages of a native aggregate: AQE=$adaptive") {
      withTestData {
        withSQLConf(
          (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) +: defaultModel: _*) {
          val (_, plan) = checkSparkAnswer(groupingAggregate)
          assert(
            collect(plan) { case aggregate: CometHashAggregateExec => aggregate }.size == 2,
            s"$plan")
        }
      }
    }
  }

  for (adaptive <- Seq(true, false)) {
    test(s"reverts a stage that is only a native scan: AQE=$adaptive") {
      withTestData {
        withSQLConf(
          (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) +: defaultModel: _*) {
          for (query <- Seq("SELECT * FROM tbl", "SELECT _1 + 1 FROM tbl")) {
            val (_, plan) = checkSparkAnswer(query)
            assert(cometOperators(plan).isEmpty, s"$query: $plan")
          }
        }
      }
    }
  }

  test("the model is given each stage as Comet and as Spark would run it") {
    withTestData {
      withSparkProject() {
        val calls = estimates(scanThenSparkProject)
        assert(calls.size == 1)
        val (cometPlan, sparkPlan, _) = calls.head
        assert(cometPlan.collect { case op: CometExec => op }.nonEmpty, s"$cometPlan")
        assert(sparkPlan.collect { case op: CometExec => op }.isEmpty, s"$sparkPlan")
        assert(cometPlan.output == sparkPlan.output)
      }
    }
  }

  test("default model estimates") {
    withTestData {
      withSparkProject() {
        val scanOnly = estimates(scanThenSparkProject).map(_._3)
        assert(scanOnly.size == 1 && scanOnly.head.speedup < 1.0, s"$scanOnly")

        val filtered = estimates(filterThenSparkProject).map(_._3)
        assert(filtered.size == 1 && filtered.head.speedup > 1.0, s"$filtered")
        assert(filtered.head.speedup > scanOnly.head.speedup)

        // A cheaper transition makes the native scan worth keeping. A faster scan helps, but
        // on its own does not pay for converting every row at the default transition cost.
        withSQLConf(CometConf.COMET_EXEC_COST_MODEL_TRANSITION_COST_FACTOR.key -> "1.0") {
          assert(estimates(scanThenSparkProject).head._3.speedup > 1.0)
        }
        withSQLConf(CometConf.COMET_EXEC_COST_MODEL_NATIVE_SPEEDUP.key -> "100.0") {
          val fasterScan = estimates(scanThenSparkProject).head._3.speedup
          assert(fasterScan > scanOnly.head.speedup && fasterScan < 1.0)
        }
      }
      // With no fallback, every stage of the aggregate is faster with Comet.
      val aggregate = estimates(groupingAggregate).map(_._3)
      assert(aggregate.size == 2 && aggregate.forall(_.speedup > 1.0), s"$aggregate")
    }
  }

  test("minSpeedup sets how much faster Comet has to be") {
    withTestData {
      withSparkProject(defaultModel: _*) {
        withSQLConf(CometConf.COMET_EXEC_COST_MODEL_MIN_SPEEDUP.key -> "0.0") {
          val (_, plan) = checkSparkAnswer(scanThenSparkProject)
          assert(cometOperators(plan).nonEmpty, s"$plan")
        }
        withSQLConf(CometConf.COMET_EXEC_COST_MODEL_MIN_SPEEDUP.key -> "1000.0") {
          val (_, plan) = checkSparkAnswer(filterThenSparkProject)
          assert(cometOperators(plan).isEmpty, s"$plan")
        }
      }
    }
  }

  test("a custom cost model decides which stages revert") {
    withTestData {
      withSQLConf(
        (CometConf.COMET_SHUFFLE_MODE.key -> "native") +:
          costModel(classOf[AlwaysRevertCostModel].getName): _*) {
        val (_, plan) = checkSparkAnswer("SELECT _2, sum(_1) FROM tbl WHERE _1 > 10 GROUP BY _2")
        assert(cometOperators(plan).isEmpty, s"$plan")
        // The native shuffle stays, and reads the reverted stage through an Arrow conversion.
        val shuffles = collect(plan) { case shuffle: CometShuffleExchangeExec => shuffle }
        assert(shuffles.size == 1 && shuffles.head.shuffleType == CometNativeShuffle, s"$plan")
        assert(shuffles.head.child.isInstanceOf[CometSparkToColumnarExec], s"$plan")
      }
    }
  }

  for (shuffleMode <- Seq("native", "jvm"); adaptive <- Seq(true, false)) {
    test(s"reverts a scan that feeds a Comet shuffle: mode=$shuffleMode AQE=$adaptive") {
      withTestData {
        withSQLConf(
          Seq(
            CometConf.COMET_SHUFFLE_MODE.key -> shuffleMode,
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) ++
            costModel(classOf[AlwaysRevertCostModel].getName): _*) {
          val (_, plan) = checkSparkAnswer(sql("SELECT * FROM tbl").repartition(4, col("_2")))
          assert(cometOperators(plan).isEmpty, s"$plan")
          assert(
            collect(plan) { case shuffle: CometShuffleExchangeExec => shuffle }.size == 1,
            s"$plan")
        }
      }
    }
  }

  test("a cost model that fails or cannot be loaded leaves the Comet plan in place") {
    withTestData {
      for (model <- Seq(classOf[FailingCostModel].getName, "org.example.NoSuchCostModel")) {
        withSparkProject(costModel(model): _*) {
          val (_, plan) = checkSparkAnswer(scanThenSparkProject)
          assert(cometOperators(plan).nonEmpty, s"$model: $plan")
        }
      }
    }
  }

  test("the cost model does not split a native aggregate that Spark cannot share") {
    withSQLConf(
      Seq(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "native") ++
        costModel(classOf[AlwaysRevertCostModel].getName): _*) {
      withParquetTable((0 until 256).map(i => (i % 4, i)), "counted") {
        val (_, plan) = checkSparkAnswer("SELECT _1, count(*) FROM counted GROUP BY _1")
        assert(
          collect(plan) { case aggregate: CometHashAggregateExec => aggregate }.size == 2,
          s"$plan")
      }
    }
  }
}
