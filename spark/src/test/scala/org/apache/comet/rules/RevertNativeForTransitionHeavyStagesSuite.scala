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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.{CometTestBase, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, Partial}
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus
import org.apache.comet.serde.OperatorOuterClass.Operator

private case class AliasingFallbackCometExec(
    override val originalPlan: SparkPlan,
    child: SparkPlan)
    extends CometExec
    with UnaryExecNode {
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

class RevertNativeForTransitionHeavyStagesSuite extends CometTestBase {

  private def cometIcebergWrite(child: SparkPlan): CometIcebergWriteExec = {
    val output = Seq(
      AttributeReference(IcebergWriteExec.CommitMessageColumn, BinaryType, nullable = false)())
    val originalPlan = IcebergWriteExec(null, output, child)
    CometIcebergWriteExec(
      Operator.newBuilder().build(),
      originalPlan,
      child,
      output,
      batchWrite = null,
      table = null,
      partitionSpecId = 0)
  }

  private def cometFilter(child: SparkPlan): CometFilterExec = {
    val condition = Literal.TrueLiteral
    val sparkFilter = FilterExec(condition, child)
    CometFilterExec(
      Operator.newBuilder().build(),
      sparkFilter,
      sparkFilter.output,
      condition,
      child,
      SerializedPlan(None))
  }

  private def captureDataWritingCommand(path: String): DataWritingCommandExec = {
    val captured = new AtomicReference[SparkPlan]()
    val callbackCompleted = new CountDownLatch(1)
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        if (funcName == "save" || funcName.contains("command")) {
          captured.set(qe.executedPlan)
          callbackCompleted.countDown()
        }
      }
      override def onFailure(
          funcName: String,
          qe: QueryExecution,
          exception: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)
    try {
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.range(1).toDF("id").write.mode("overwrite").parquet(path)
      }
      assert(
        callbackCompleted.await(10, TimeUnit.SECONDS),
        "timed out waiting to capture the parquet write plan")
    } finally {
      spark.listenerManager.unregister(listener)
    }
    val plan = stripAQEPlan(
      Option(captured.get()).getOrElse(fail("expected a captured parquet write plan")))
    plan
      .collectFirst { case command: DataWritingCommandExec => command }
      .getOrElse(fail(s"expected DataWritingCommandExec:\n$plan"))
  }

  private def cometNativeWrite(
      child: SparkPlan,
      command: DataWritingCommandExec): CometNativeWriteExec = {
    CometNativeWriteExec(
      Operator.newBuilder().build(),
      command,
      child,
      outputPath = "/tmp/unused-native-write",
      mode = SaveMode.Overwrite)
  }

  private def assertRestoredParquetWrite(reverted: SparkPlan): WriteFilesExec = {
    val command = reverted match {
      case node: DataWritingCommandExec => node
      case other => fail(s"expected DataWritingCommandExec, got:\n$other")
    }
    val writeFiles = command.child match {
      case node: WriteFilesExec => node
      case other => fail(s"expected WriteFilesExec under DataWritingCommandExec, got:\n$other")
    }
    assert(
      command.collect { case _: CometNativeWriteExec => true }.isEmpty,
      s"native parquet write should be restored, not erased:\n$reverted")
    writeFiles
  }

  private def createSparkPlan(sql: String): SparkPlan = {
    var plan: SparkPlan = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      plan = spark.sql(sql).queryExecution.executedPlan
    }
    stripAQEPlan(plan)
  }

  private def applyCometExecRule(plan: SparkPlan): SparkPlan = {
    CometExecRule(spark).apply(plan)
  }

  private def applyFullColumnarPipeline(plan: SparkPlan): SparkPlan = {
    val cometPlan = CometRule(spark).apply(plan)
    val withTransitions =
      ApplyColumnarRulesAndInsertTransitions(Seq.empty, false).apply(cometPlan)
    EliminateRedundantTransitions(spark).apply(withTransitions)
  }

  private def countCometExecs(plan: SparkPlan): Int = {
    plan.collect { case _: CometExec => true }.size
  }

  private def countC2RNodes(plan: SparkPlan): Int = {
    plan.collect { case _: ColumnarToRowTransition => true }.size
  }

  private def unwrapCodegen(plan: SparkPlan): SparkPlan = plan match {
    case wholeStage: WholeStageCodegenExec => unwrapCodegen(wholeStage.child)
    case inputAdapter: InputAdapter => unwrapCodegen(inputAdapter.child)
    case other => other
  }

  private def collectCometAggregates(plan: SparkPlan): Seq[CometHashAggregateExec] = {
    val current = plan match {
      case aggregate: CometHashAggregateExec => Seq(aggregate)
      case _ => Seq.empty
    }
    val descendants = plan match {
      case stage: QueryStageExec => collectCometAggregates(stage.plan)
      case _ => plan.children.flatMap(collectCometAggregates)
    }
    current ++ descendants
  }

  /**
   * Returns every node that produces a columnar output but consumes a row-based child without a
   * RowToColumnar transition. Such a node is an invalid columnar/row boundary: a columnar parent
   * (e.g. a native CometShuffleExchangeExec) requires columnar input. RowToColumnarExec and
   * CometSparkToColumnarExec are the legitimate row->columnar bridges and are excluded.
   */
  private def invalidColumnarBoundaries(plan: SparkPlan): Seq[SparkPlan] = {
    plan.collect {
      case n
          if n.supportsColumnar && !n.isInstanceOf[RowToColumnarTransition] &&
            n.children.exists(c => !c.supportsColumnar) =>
        n
    }
  }

  test("rule is a no-op when disabled") {
    withSQLConf(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan = createSparkPlan("SELECT id, id * 2 FROM test_data WHERE id > 5")
        val cometPlan = applyCometExecRule(sparkPlan)
        assert(countCometExecs(cometPlan) > 0, "Plan should have CometExec nodes")

        val rule = RevertNativeForTransitionHeavyStages(spark)
        val result = rule.apply(cometPlan)
        assert(result eq cometPlan, "Rule should be a no-op when disabled")
      }
    }
  }

  test("rule does not revert plan below threshold") {
    withSQLConf(
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "10",
      "spark.comet.exec.project.enabled" -> "false") {
      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan =
          createSparkPlan("SELECT id, id * 2 as doubled FROM test_data WHERE id > 5")
        val cometPlan = applyFullColumnarPipeline(sparkPlan)

        val rule = RevertNativeForTransitionHeavyStages(spark)
        val transitions = rule.countTransitions(cometPlan)
        assert(transitions > 0, s"Plan should have transitions, got $transitions")
        assert(transitions <= 10, "Transitions should be below threshold")

        val result = rule.apply(cometPlan)
        assert(result eq cometPlan, "Plan should be unchanged when below threshold")
      }
    }
  }

  test("revertToSpark preserves plan structure") {
    withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {

      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan =
          createSparkPlan("SELECT id, id * 2 as doubled FROM test_data WHERE id > 5")
        val cometPlan = applyCometExecRule(sparkPlan)
        val rule = RevertNativeForTransitionHeavyStages(spark)
        val reverted = rule.revertToSpark(cometPlan)

        // Reverted plan should have same output schema
        assert(
          reverted.output.map(_.name) == cometPlan.output.map(_.name),
          "Output schema should be preserved after revert")
      }
    }
  }

  test("revertToSpark preserves an Iceberg write with a leaf child") {
    val sparkPlan = createSparkPlan("SELECT id FROM VALUES (1) AS t(id)")
    val leaf = sparkPlan.collectFirst { case node: LeafExecNode => node }.getOrElse {
      fail(s"expected a leaf node in test plan:\n$sparkPlan")
    }
    val write = cometIcebergWrite(leaf)

    val reverted = RevertNativeForTransitionHeavyStages(spark).revertToSpark(write)
    val icebergWrite = reverted match {
      case node: IcebergWriteExec => node
      case other => fail(s"expected IcebergWriteExec, got:\n$other")
    }

    assert(icebergWrite.child eq leaf)
    assert(icebergWrite.output.map(_.name) == Seq(IcebergWriteExec.CommitMessageColumn))
    assert(icebergWrite.output.map(_.dataType) == Seq(BinaryType))
  }

  test("revertToSpark preserves an Iceberg write over SparkToColumnar of a row source") {
    val sparkPlan = createSparkPlan("SELECT id FROM VALUES (1) AS t(id)")
    val leaf = sparkPlan.collectFirst { case node: LeafExecNode => node }.getOrElse {
      fail(s"expected a leaf node in test plan:\n$sparkPlan")
    }
    val write = cometIcebergWrite(CometSparkToColumnarExec(leaf))

    val reverted = RevertNativeForTransitionHeavyStages(spark).revertToSpark(write)
    val icebergWrite = reverted match {
      case node: IcebergWriteExec => node
      case other => fail(s"expected IcebergWriteExec, got:\n$other")
    }

    assert(
      icebergWrite.child eq leaf,
      s"SparkToColumnar should unwrap to the row source:\n$reverted")
    assert(icebergWrite.output.map(_.name) == Seq(IcebergWriteExec.CommitMessageColumn))
    assert(icebergWrite.output.map(_.dataType) == Seq(BinaryType))
    assert(
      reverted.collect { case _: CometSparkToColumnarExec => true }.isEmpty,
      s"SparkToColumnar should be fully unwrapped:\n$reverted")
  }

  test("revertToSpark preserves an Iceberg write without duplicating its unary child") {
    val sparkPlan = createSparkPlan("SELECT id FROM VALUES (1), (2) AS t(id)")
    val leaf = sparkPlan.collectFirst { case node: LeafExecNode => node }.getOrElse {
      fail(s"expected a leaf node in test plan:\n$sparkPlan")
    }
    val write = cometIcebergWrite(cometFilter(leaf))

    val reverted = RevertNativeForTransitionHeavyStages(spark).revertToSpark(write)

    assert(reverted.isInstanceOf[IcebergWriteExec], s"expected IcebergWriteExec:\n$reverted")
    assert(
      reverted.collect { case _: FilterExec => true }.size == 1,
      s"expected exactly one Spark FilterExec:\n$reverted")
    assert(countCometExecs(reverted) == 0, s"expected no Comet operators:\n$reverted")
  }

  test("revertToSpark unwraps stacked SparkToColumnar(C2R) under a native write") {
    val sparkPlan = createSparkPlan("SELECT id FROM VALUES (1) AS t(id)")
    val leaf = sparkPlan.collectFirst { case node: LeafExecNode => node }.getOrElse {
      fail(s"expected a leaf node in test plan:\n$sparkPlan")
    }
    val stacked = CometSparkToColumnarExec(CometNativeColumnarToRowExec(cometFilter(leaf)))
    val write = cometIcebergWrite(stacked)

    val reverted = RevertNativeForTransitionHeavyStages(spark).revertToSpark(write)

    assert(reverted.isInstanceOf[IcebergWriteExec], s"expected IcebergWriteExec:\n$reverted")
    assert(
      reverted.collect { case _: FilterExec => true }.size == 1,
      s"expected exactly one Spark FilterExec:\n$reverted")
    assert(
      reverted.collect { case _: CometNativeColumnarToRowExec | _: CometSparkToColumnarExec =>
        true
      }.isEmpty,
      s"stacked transitions should be fully unwrapped:\n$reverted")
    assert(countCometExecs(reverted) == 0, s"expected no Comet operators:\n$reverted")
  }

  test("revertToSpark preserves a native parquet write with a leaf child") {
    val sparkPlan = createSparkPlan("SELECT id FROM VALUES (1) AS t(id)")
    val leaf = sparkPlan.collectFirst { case node: LeafExecNode => node }.getOrElse {
      fail(s"expected a leaf node in test plan:\n$sparkPlan")
    }
    withTempPath { dir =>
      val write = cometNativeWrite(leaf, captureDataWritingCommand(dir.getAbsolutePath))
      val reverted = RevertNativeForTransitionHeavyStages(spark).revertToSpark(write)
      val writeFiles = assertRestoredParquetWrite(reverted)
      assert(writeFiles.child eq leaf, s"expected the original leaf child:\n$reverted")
    }
  }

  test("revertToSpark preserves a native parquet write without duplicating its unary child") {
    val sparkPlan = createSparkPlan("SELECT id FROM VALUES (1), (2) AS t(id)")
    val leaf = sparkPlan.collectFirst { case node: LeafExecNode => node }.getOrElse {
      fail(s"expected a leaf node in test plan:\n$sparkPlan")
    }
    withTempPath { dir =>
      val write =
        cometNativeWrite(cometFilter(leaf), captureDataWritingCommand(dir.getAbsolutePath))
      val reverted = RevertNativeForTransitionHeavyStages(spark).revertToSpark(write)
      assertRestoredParquetWrite(reverted)
      assert(
        reverted.collect { case _: FilterExec => true }.size == 1,
        s"expected exactly one Spark FilterExec:\n$reverted")
      assert(countCometExecs(reverted) == 0, s"expected no Comet operators:\n$reverted")
    }
  }

  test("revertToSpark restores a native parquet write whose command has no WriteFilesExec") {
    val sparkPlan = createSparkPlan("SELECT id FROM VALUES (1) AS t(id)")
    val leaf = sparkPlan.collectFirst { case node: LeafExecNode => node }.getOrElse {
      fail(s"expected a leaf node in test plan:\n$sparkPlan")
    }
    withTempPath { dir =>
      val command = captureDataWritingCommand(dir.getAbsolutePath)
      val input = command.child match {
        case writeFiles: WriteFilesExec => writeFiles.child
        case other => other
      }
      val commandWithoutWriteFiles =
        command.withNewChildren(Seq(input)).asInstanceOf[DataWritingCommandExec]
      val write = cometNativeWrite(leaf, commandWithoutWriteFiles)
      val reverted = RevertNativeForTransitionHeavyStages(spark).revertToSpark(write)
      val restored = reverted match {
        case node: DataWritingCommandExec => node
        case other => fail(s"expected DataWritingCommandExec, got:\n$other")
      }
      assert(restored.child eq leaf, s"expected the original leaf child:\n$reverted")
      assert(
        restored.collect { case _: WriteFilesExec => true }.isEmpty,
        s"WriteFilesExec should not be reinserted when the original command lacked it:\n$reverted")
      assert(
        restored.collect { case _: CometNativeWriteExec => true }.isEmpty,
        s"native parquet write should be restored, not erased:\n$reverted")
    }
  }

  test("invalid original-plan alias skips the entire stage reversion") {
    withSQLConf(
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
      val sparkPlan = createSparkPlan("SELECT id FROM VALUES (1) AS t(id)")
      val leaf = sparkPlan.collectFirst { case node: LeafExecNode => node }.getOrElse {
        fail(s"expected a leaf node in test plan:\n$sparkPlan")
      }
      val aliasing = AliasingFallbackCometExec(leaf, leaf)
      val stagePlan = CometNativeColumnarToRowExec(aliasing)
      val rule = RevertNativeForTransitionHeavyStages(spark)
      assert(rule.countTransitions(stagePlan) == 1)

      val result = rule(stagePlan)

      assert(
        result eq stagePlan,
        s"invalid fallback must leave the whole stage unchanged:\n$result")
    }
  }

  test("invalid original-plan alias to a Comet child skips the entire stage reversion") {
    withSQLConf(
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
      val sparkPlan = createSparkPlan("SELECT id FROM VALUES (1) AS t(id)")
      val leaf = sparkPlan.collectFirst { case node: LeafExecNode => node }.getOrElse {
        fail(s"expected a leaf node in test plan:\n$sparkPlan")
      }
      val cometChild = cometFilter(leaf)
      val aliasing = AliasingFallbackCometExec(cometChild, cometChild)
      val stagePlan = CometNativeColumnarToRowExec(aliasing)
      val rule = RevertNativeForTransitionHeavyStages(spark)
      assert(rule.countTransitions(stagePlan) == 1)

      val result = rule(stagePlan)

      assert(
        result eq stagePlan,
        s"invalid nested fallback must leave the whole stage unchanged:\n$result")
    }
  }

  test("local TopK sparkFallback returns the supplied restored child") {
    val child = spark.range(20).queryExecution.sparkPlan
    val originalPlan = TakeOrderedAndProjectExec(5, Seq.empty, child.output, child)
    val local = CometLocalTopKExec(
      Operator.newBuilder().build(),
      originalPlan,
      child.output,
      5,
      Seq.empty,
      dynamicFilterEnabled = false,
      child,
      SerializedPlan(None))
    val restoredChild = spark.range(10).queryExecution.sparkPlan

    assert(local.sparkFallback(Seq(restoredChild)) eq restoredChild)
    intercept[CometExec.InvalidSparkFallbackException] {
      local.sparkFallback(Seq.empty)
    }
  }

  for (adaptive <- Seq(false, true)) {
    test(s"transition reversion preserves local TopK: AQE=$adaptive") {
      withSQLConf(
        CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
        withTempPath { path =>
          spark
            .range(0, 20, 1, 1)
            .selectExpr("CAST(id AS INT) AS k", "id * 10 AS payload")
            .write
            .parquet(path.getCanonicalPath)
          withParquetTable(path.getCanonicalPath, "topk_revert") {
            for (offset <- Seq(2, 7, 0); projection <- Seq("k", "payload")) {
              val query = s"SELECT $projection FROM topk_revert ORDER BY k LIMIT 5 OFFSET $offset"
              withSQLConf(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
                val plan = sql(query).queryExecution.executedPlan
                val local = collect(plan) { case topK: CometLocalTopKExec => topK }
                assert(local.size == 1, s"Expected a fused TopK before reversion:\n$plan")
                assert(local.head.child.isInstanceOf[CometNativeScanExec])
              }
              val df = sql(query)
              val expected = (offset until offset + 5).map { key =>
                if (projection == "k") Row(key) else Row(key * 10L)
              }
              withClue(query) {
                assert(df.collect().toSeq == expected)
              }
              val plan = stripAQEPlan(df.queryExecution.executedPlan)
              assert(countCometExecs(plan) == 0, s"Expected stage reversion:\n$plan")
              assert(
                collect(plan) { case topK: TakeOrderedAndProjectExec => topK }.size == 1,
                s"Reversion must restore offset and projection exactly once:\n$plan")
            }
          }
        }
      }
    }
  }

  test("revertToSpark removes all Comet operators from a plan with transitions") {
    withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {

      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan =
          createSparkPlan("SELECT id, id * 2 as doubled FROM test_data WHERE id > 5")
        val cometPlan = applyFullColumnarPipeline(sparkPlan)
        assert(countCometExecs(cometPlan) > 0, "Should have CometExec nodes before revert")

        val rule = RevertNativeForTransitionHeavyStages(spark)
        val result = rule.revertToSpark(cometPlan)
        assert(
          countCometExecs(result) == 0,
          s"All CometExec should be reverted. Plan:\n${result.treeString}")
      }
    }
  }

  test("non-AQE path applies rule per-stage via transformUp") {
    withSQLConf(
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "10",
      "spark.sql.adaptive.enabled" -> "false") {

      withTempView("test_data") {
        spark
          .range(10)
          .selectExpr("id", "id % 3 as grp")
          .createOrReplaceTempView("test_data")
        val sparkPlan = createSparkPlan("SELECT grp, count(*) FROM test_data GROUP BY grp")
        val cometPlan = applyCometExecRule(sparkPlan)

        // With high threshold, the non-AQE path should not revert anything
        val rule = RevertNativeForTransitionHeavyStages(spark)
        val result = rule.apply(cometPlan)
        assert(result eq cometPlan, "Non-AQE path should not revert when below threshold")
      }
    }
  }

  test("revert fires with unsupported UDF producing transitions") {
    withParquetTable((0 until 100).map(i => (i, i % 10, s"val_$i")), "tbl") {
      spark.udf.register("identity_udf", (x: Int) => x)
      val query = "SELECT _2, identity_udf(_1), max(_1) FROM tbl GROUP BY _2, identity_udf(_1)"

      // Without revert, plan should have transitions due from UDF
      withSQLConf(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
        val df = sql(query)
        df.collect()
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        assert(countC2RNodes(plan) > 0, "UDF should cause C2R transitions")
      }

      // With threshold 0, stage should be reverted
      withSQLConf(
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
        val (_, cometPlan) = checkSparkAnswer(query)
        val executedPlan = stripAQEPlan(cometPlan)
        assert(
          countCometExecs(executedPlan) == 0,
          s"Revert should have removed all CometExec nodes:\n${executedPlan.treeString}")
      }
    }
  }

  test("revert fires and produces correct results when transitions exceed threshold") {
    withParquetTable((0 until 100).map(i => (i, i % 10, s"val_$i")), "tbl") {
      val query = "SELECT _2, min(_1), sum(_1) FROM tbl GROUP BY _2"

      // Without revert, plan should have CometExec nodes with transitions
      withSQLConf(
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false",
        "spark.comet.exec.project.enabled" -> "false") {
        val df = sql(query)
        df.collect()
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        assert(countCometExecs(plan) > 0, "Plan without revert should have CometExec nodes")
        assert(countC2RNodes(plan) > 0, "Plan without revert should have C2R transitions")
      }

      // With revert enabled at threshold 0, all CometExec should be removed
      withSQLConf(
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0",
        "spark.comet.exec.project.enabled" -> "false") {
        val (_, cometPlan) = checkSparkAnswer(query)
        val executedPlan = stripAQEPlan(cometPlan)
        assert(
          countCometExecs(executedPlan) == 0,
          s"Revert should have removed all CometExec nodes:\n${executedPlan.treeString}")
      }
    }
  }

  test("AQE DPP remains executable when transition reversion restores a V1 scan") {
    assume(isSpark35Plus, "Comet AQE DPP query-stage optimizer rules require Spark 3.5+")
    import testImplicits._

    withTempDir { dir =>
      val factPath = s"${dir.getAbsolutePath}/fact"
      val dimPath = s"${dir.getAbsolutePath}/dim"
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        (0 until 400)
          .map(i => (i, i % 10, s"f$i"))
          .toDF("fact_id", "fact_key", "fact_str")
          .write
          .partitionBy("fact_key")
          .parquet(factPath)
        (0 until 10)
          .map(i => (i, i, s"d$i"))
          .toDF("dim_id", "dim_key", "dim_str")
          .write
          .parquet(dimPath)
      }

      withTempView("revert_dpp_fact", "revert_dpp_dim") {
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
          spark.read.parquet(factPath).createOrReplaceTempView("revert_dpp_fact")
          spark.read.parquet(dimPath).createOrReplaceTempView("revert_dpp_dim")

          val query =
            """SELECT f.fact_id, f.fact_str, d.dim_str
              |FROM revert_dpp_fact f JOIN revert_dpp_dim d
              |  ON f.fact_key = d.dim_key
              |WHERE d.dim_id < 10""".stripMargin

          for {
            adaptive <- Seq(false, true)
            transitionRevert <- Seq(false, true)
            projectEnabled <- Seq(false, true)
          } {
            withClue(
              s"AQE=$adaptive, transitionRevert=$transitionRevert, " +
                s"projectEnabled=$projectEnabled: ") {
              withSQLConf(
                SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
                SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
                CometConf.COMET_ENABLED.key -> "true",
                CometConf.COMET_EXEC_ENABLED.key -> "true",
                "spark.comet.exec.project.enabled" -> projectEnabled.toString,
                CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key ->
                  transitionRevert.toString,
                CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
                val df = sql(query)
                assert(df.collect().length == 400)

                if (adaptive && transitionRevert) {
                  val executedPlan = stripAQEPlan(df.queryExecution.executedPlan)
                  val scans = executedPlan.collect { case scan: FileSourceScanExec => scan }
                  assert(
                    scans.nonEmpty,
                    s"Transition reversion should restore Spark V1 scans:\n$executedPlan")
                  assert(
                    scans.exists(_.partitionFilters.exists(_.exists {
                      case inSub: InSubqueryExec =>
                        inSub.plan.isInstanceOf[CometSubqueryBroadcastExec] ||
                        inSub.plan.isInstanceOf[SubqueryBroadcastExec]
                      case _ => false
                    })),
                    s"Reverted scan should retain the executable DPP subquery:\n$executedPlan")
                  assert(
                    !scans.exists(_.partitionFilters.exists(_.exists {
                      case inSub: InSubqueryExec =>
                        inSub.plan.isInstanceOf[SubqueryAdaptiveBroadcastExec]
                      case _ => false
                    })),
                    "Reverted scan must not restore an unexecutable AQE DPP placeholder:\n" +
                      executedPlan)
                }
              }
            }
          }
        }
      }
    }
  }

  test("revertToSpark must not revert native operators across a shuffle stage boundary") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withParquetTable((0 until 100).map(i => (i, i % 10)), "tbl") {
        // A GROUP BY produces partial-agg -> native shuffle -> final-agg, i.e. two stages.
        val df = sql("SELECT _2, count(*) FROM tbl GROUP BY _2")
        df.collect()
        val cometPlan = stripAQEPlan(df.queryExecution.executedPlan)

        val shuffles = cometPlan.collect { case s: CometShuffleExchangeExec => s }
        assume(shuffles.nonEmpty, "test requires a native CometShuffleExchangeExec")
        assert(
          shuffles.map(s => countCometExecs(s.child)).sum > 0,
          "expected native CometExec operators below the shuffle")
        assert(
          invalidColumnarBoundaries(cometPlan).isEmpty,
          s"precondition: original plan should be valid:\n${cometPlan.treeString}")

        val rule = RevertNativeForTransitionHeavyStages(spark)
        val reverted = rule.revertToSpark(cometPlan)

        val invalid = invalidColumnarBoundaries(reverted)
        assert(
          invalid.isEmpty,
          "revertToSpark produced invalid columnar/row boundaries " +
            s"(${invalid.map(_.nodeName).mkString(", ")}):\n${reverted.treeString}")
      }
    }
  }

  test("revertToSpark leaves transitions below a shuffle that a stripped transition sat on") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withParquetTable((0 until 100).map(i => (i, i % 10)), "tbl") {
        val df = sql("SELECT _2, count(*) FROM tbl GROUP BY _2")
        df.collect()
        val cometPlan = stripAQEPlan(df.queryExecution.executedPlan)
        val shuffle = cometPlan
          .collectFirst { case s: CometShuffleExchangeExec => s }
          .getOrElse(fail(s"test requires a native shuffle:\n$cometPlan"))
        // The transition that must survive lives in the map stage, under the exchange.
        val preserved =
          if (shuffle.child.supportsColumnar) CometNativeColumnarToRowExec(shuffle.child)
          else CometSparkToColumnarExec(shuffle.child)
        val shuffleWithTransition = shuffle.withNewChildren(Seq(preserved))
        // Stacked transitions sit directly on the shuffle, the shape #6152 strips through.
        val onExchange =
          CometSparkToColumnarExec(CometNativeColumnarToRowExec(shuffleWithTransition))
        val write = cometIcebergWrite(onExchange)

        val reverted = RevertNativeForTransitionHeavyStages(spark).revertToSpark(write)
        val restoredWrite = reverted match {
          case node: IcebergWriteExec => node
          case other => fail(s"expected IcebergWriteExec, got:\n$other")
        }
        val restoredShuffle = restoredWrite.child match {
          case ColumnarToRowExec(exchange: CometShuffleExchangeExec) => exchange
          case exchange: CometShuffleExchangeExec => exchange
          case other =>
            fail(s"expected the shuffle under the restored write, got:\n$other")
        }
        assert(
          restoredShuffle.child eq preserved,
          "unwrapping the transition on the shuffle must not strip the stage below it:\n" +
            reverted.treeString)
      }
    }
  }

  test("transition-heavy reversion rejects a stripped stage-boundary root") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withParquetTable((0 until 100).map(i => (i, i % 10)), "tbl") {
        var cometPlan: SparkPlan = null
        withSQLConf(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
          val df = sql("SELECT _1, _2 FROM tbl DISTRIBUTE BY _2")
          df.collect()
          cometPlan = stripAQEPlan(df.queryExecution.executedPlan)
        }
        val exchange = cometPlan
          .collectFirst { case node: CometShuffleExchangeExec => node }
          .getOrElse(fail(s"test requires a native shuffle:\n$cometPlan"))
        val stagePlan = CometColumnarToRowExec(exchange)
        val rule = RevertNativeForTransitionHeavyStages(spark)
        assert(rule.countTransitions(stagePlan) == 1)

        var result: SparkPlan = null
        withSQLConf(
          CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
          CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
          result = rule(stagePlan)
        }

        assert(
          result eq stagePlan,
          "a transition-heavy stage whose stripped root is an exchange must stay unchanged:\n" +
            result.treeString)
      }
    }
  }

  test("transition-heavy revert restores row output for a columnar Spark scan") {
    withSQLConf(
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0",
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true") {
      withParquetTable((0 until 100).map(i => (i, i % 10)), "tbl") {
        val (_, plan) = checkSparkAnswer("SELECT _1, _2 FROM tbl")
        val executedPlan = stripAQEPlan(plan)
        val resultStageRoot = unwrapCodegen(executedPlan)

        val scan = resultStageRoot match {
          case transition: ColumnarToRowExec =>
            unwrapCodegen(transition.child) match {
              case child: FileSourceScanExec => child
              case other =>
                fail(s"expected a vectorized Spark scan under ColumnarToRow, got:\n$other")
            }
          case other =>
            fail(
              "expected ColumnarToRow over a vectorized Spark scan, got " +
                s"${other.getClass.getName}:\n$other")
        }
        assert(scan.supportsColumnar, s"the reverted scan must use its columnar path:\n$scan")
        assert(countCometExecs(executedPlan) == 0, s"the stage must be reverted:\n$executedPlan")
      }
    }
  }

  for (adaptive <- Seq(false, true); columnarRoot <- Seq(false, true)) {
    test(
      s"transition-heavy map-stage fallback supplies Arrow: AQE=$adaptive, columnar=$columnarRoot") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_MODE.key -> "native",
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
        val rows = (0 until 100).map(i => (i, i % 10))
        withParquetTable(rows, "tbl") {
          val df = sql("SELECT _1, _2 FROM tbl DISTRIBUTE BY _2")
          df.collect()
          val exchange = stripAQEPlan(df.queryExecution.executedPlan)
            .collectFirst { case node: CometShuffleExchangeExec => node }
            .getOrElse(fail("test requires a native shuffle"))
          assert(exchange.child.isInstanceOf[CometNativeScanExec])
          val input = if (columnarRoot) exchange.child else cometFilter(exchange.child)
          val stage = exchange.withNewChildren(
            Seq(CometSparkToColumnarExec(CometNativeColumnarToRowExec(input))))
          val rule = RevertNativeForTransitionHeavyStages(spark)
          assert(rule.countTransitions(stage.children.head) == 1)

          withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
            CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
            CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
            val reverted = rule(stage).asInstanceOf[CometShuffleExchangeExec]
            val bridge = reverted.child match {
              case node: CometSparkToColumnarExec => node
              case other => fail(s"expected an Arrow bridge after map-stage fallback:\n$other")
            }
            assert(bridge.child.supportsColumnar == columnarRoot)
            assert(bridge.child.collect { case _: FileSourceScanExec => true }.nonEmpty)
            assert(countCometExecs(bridge.child) == 0)
            // Execute the native shuffle, not just its Spark fallback child: Spark batches
            // satisfy supportsColumnar but cannot be cast to CometVector by the Arrow stream.
            SQLExecution.withNewExecutionId(df.queryExecution) {
              val actual = ColumnarToRowExec(reverted)
                .executeCollect()
                .map(row => (row.getInt(0), row.getInt(1)))
                .toSeq
              assert(actual.sorted == rows.sorted)
            }
          }
        }
      }
    }
  }

  for (adaptive <- Seq(false, true)) {
    test(s"transition-heavy revert preserves native exchange for DISTRIBUTE BY: AQE=$adaptive") {
      withSQLConf(
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0",
        CometConf.COMET_SHUFFLE_MODE.key -> "native",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.SHUFFLE_PARTITIONS.key -> "8",
        "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.parallelismFirst" -> "false",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "67108864") {
        withParquetTable((0 until 100).map(i => (i, i % 10)), "tbl") {
          val query = "SELECT _1, _2 FROM tbl DISTRIBUTE BY _2"
          var sparkAnswer: Seq[Row] = Seq.empty
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "false",
            CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
            sparkAnswer = sql(query).collect().toSeq
          }
          val df = sql(query)
          checkCometAnswer(df, sparkAnswer)
          val executedPlan = stripAQEPlan(df.queryExecution.executedPlan)
          val resultStageRoot = unwrapCodegen(executedPlan)

          assert(
            resultStageRoot.isInstanceOf[ColumnarToRowTransition],
            s"the result stage must end with a columnar-to-row transition:\n$executedPlan")

          if (adaptive) {
            val read = executedPlan
              .collectFirst { case node: AQEShuffleReadExec => node }
              .getOrElse(fail(s"expected a coalesced AQE shuffle read:\n$executedPlan"))
            assert(read.partitionSpecs.size < 8, s"shuffle must be coalesced:\n$read")
            val stage = read.child match {
              case node: ShuffleQueryStageExec => node
              case other => fail(s"expected a shuffle query stage:\n$other")
            }
            assert(stage.plan.isInstanceOf[CometShuffleExchangeExec])
          } else {
            val exchange = executedPlan
              .collectFirst { case node: CometShuffleExchangeExec => node }
              .getOrElse(fail(s"expected a native shuffle:\n$executedPlan"))
            assert(
              exchange.collect { case scan: CometNativeScanExec => scan }.nonEmpty,
              s"the map stage must retain its native scan:\n$executedPlan")
            assert(
              exchange.collect { case scan: FileSourceScanExec => scan }.isEmpty,
              s"fallback must not replace the scan below the exchange:\n$executedPlan")
          }
        }
      }
    }
  }

  test("non-AQE apply must not produce an invalid plan when the result stage reverts") {
    withSQLConf(
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
      // Threshold 0 forces the result stage (above the topmost shuffle) to revert.
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0",
      "spark.sql.adaptive.enabled" -> "false") {
      withParquetTable((0 until 100).map(i => (i, i % 10)), "tbl") {
        var cometPlan: SparkPlan = null
        withSQLConf(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
          val df = sql("SELECT _2, count(*) FROM tbl GROUP BY _2")
          df.collect()
          cometPlan = stripAQEPlan(df.queryExecution.executedPlan)
        }
        assume(
          cometPlan.collect { case s: CometShuffleExchangeExec => s }.nonEmpty,
          "test requires a native CometShuffleExchangeExec")

        val rule = RevertNativeForTransitionHeavyStages(spark)
        val result = rule.apply(cometPlan)

        val invalid = invalidColumnarBoundaries(result)
        assert(
          invalid.isEmpty,
          "rule.apply produced invalid columnar/row boundaries " +
            s"(${invalid.map(_.nodeName).mkString(", ")}):\n${result.treeString}")
      }
    }
  }

  for (adaptive <- Seq(false, true)) {
    test(s"transition reversion preserves incompatible aggregate buffers with AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "native",
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
        withParquetTable((0 until 256).map(i => (i % 4, i.toDouble)), "tbl") {
          val (_, plan) =
            checkSparkAnswer("SELECT _1, percentile(_2, 0.5) FROM tbl GROUP BY _1 ORDER BY _1")
          val executedPlan = stripAQEPlan(plan)
          val aggregates = collectCometAggregates(executedPlan)
          assert(aggregates.exists(_.modes == Seq(Partial)), s"$executedPlan")
          assert(aggregates.exists(_.modes == Seq(Final)), s"$executedPlan")
        }
      }
    }
  }

  test("transition reversion finds an incompatible aggregate below another aggregate") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
      withParquetTable((0 until 256).map(i => (i % 4, i.toDouble)), "tbl") {
        val query =
          """SELECT grouping_key, percentile(inner_percentile, 0.5)
            |FROM (
            |  SELECT _1 AS grouping_key, percentile(_2, 0.5) AS inner_percentile
            |  FROM tbl
            |  GROUP BY _1
            |) inner_aggregate
            |GROUP BY grouping_key""".stripMargin
        val (_, plan) = checkSparkAnswer(query)
        val executedPlan = stripAQEPlan(plan)
        val aggregates = collectCometAggregates(executedPlan)
        assert(
          executedPlan.collect { case _: CometShuffleExchangeExec => true }.size == 1,
          s"test requires one exchange below the nested aggregates:\n$executedPlan")
        assert(aggregates.count(_.modes == Seq(Partial)) == 2, s"$executedPlan")
        assert(aggregates.count(_.modes == Seq(Final)) == 2, s"$executedPlan")
      }
    }
  }

  for (adaptive <- Seq(false, true)) {
    test(s"transition reversion does not split native COUNT stages with AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "native",
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
        withParquetTable((0 until 256).map(i => (i % 4, i)), "tbl") {
          val (_, plan) = checkSparkAnswer("SELECT _1, count(*) FROM tbl GROUP BY _1 ORDER BY _1")
          val executedPlan = stripAQEPlan(plan)
          val aggregates = collectCometAggregates(executedPlan)
          assert(aggregates.exists(_.modes == Seq(Partial)), s"$executedPlan")
          assert(aggregates.exists(_.modes == Seq(Final)), s"$executedPlan")
        }
      }
    }
  }

  test("transition reversion preserves an incompatible native partial producer") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
      withParquetTable((0 until 256).map(i => (i % 4, i.toDouble)), "tbl") {
        val nativePlan =
          sql("SELECT _1, percentile(_2, 0.5) FROM tbl GROUP BY _1").queryExecution.executedPlan
        val partial = nativePlan
          .collectFirst {
            case aggregate: CometHashAggregateExec if aggregate.modes == Seq(Partial) => aggregate
          }
          .getOrElse(fail(s"expected a native partial aggregate:\n$nativePlan"))
        val producerWithTransition = partial.withNewChildren(
          Seq(CometSparkToColumnarExec(CometNativeColumnarToRowExec(partial.child))))
        val reverter = RevertNativeForTransitionHeavyStages(spark)
        assert(reverter.countTransitions(producerWithTransition) == 1)

        var reverted: SparkPlan = null
        withSQLConf(
          CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
          CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
          reverted = reverter(producerWithTransition)
        }
        assert(reverted eq producerWithTransition)
      }
    }
  }
}
