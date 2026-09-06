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

package org.apache.spark.sql.comet.execution.shuffle

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv}
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning, RoundRobinPartitioning, SinglePartition}
import org.apache.spark.sql.comet.{CometBaseAggregateExec, CometCollectLimitExec, CometHashAggregateExec, CometLocalTableScanExec, CometNativeExec, CometScanWrapper, CometSortAggregateExec, CometSortExec, CometSparkToColumnarExec, CometTakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.{CollectLimitExec, ColumnarToRowTransition, LocalTableScanExec, SortExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, ObjectType}

import org.apache.comet.{CometConf, CometExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.{isCometShuffleEnabled, isCometShuffleManagerEnabled}
import org.apache.comet.rules.{CometExecRule, RevertNativeForTransitionHeavyStages}
import org.apache.comet.serde.{Compatible, OperatorOuterClass}

/**
 * Plans against the actual composite manager without requiring the optional Celeborn client. Only
 * Spark-fallback queries execute: the local test backend must never receive a native dependency.
 * Native plan assertions below are not transport or live-cluster integration tests.
 */
class CometCelebornShufflePlanningSuite extends CometTestBase {

  override protected val shuffleManager: String =
    classOf[CometCelebornPlanningTestShuffleManager].getName

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_SHUFFLE_MODE.key, "auto")
      .set(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key, "false")
      .set("spark.io.encryption.enabled", "false")
      .set("spark.celeborn.client.spark.stageRerun.enabled", "true")

  private def manager: CometCelebornPlanningTestShuffleManager =
    SparkEnv.get.shuffleManager.asInstanceOf[CometCelebornPlanningTestShuffleManager]

  private def nativeChild(
      attributes: Seq[Attribute] = Seq(AttributeReference("value", LongType)()))
      : CometNativeExec = {
    val original = sparkLeaf(attributes)
    CometScanWrapper(OperatorOuterClass.Operator.getDefaultInstance, original)
  }

  // Let Spark supply its version-specific LocalTableScanExec constructor arguments.
  private def sparkLeaf(attributes: Seq[Attribute] = Seq(AttributeReference("value", LongType)()))
      : LocalTableScanExec =
    spark.sessionState.planner
      .plan(LocalRelation(attributes))
      .next()
      .asInstanceOf[LocalTableScanExec]

  private def reasons(plan: SparkPlan): Set[String] =
    plan.getTagValue(CometExplainInfo.FALLBACK_REASONS).getOrElse(Set.empty[String])

  private def cometExchanges(plan: SparkPlan): Seq[CometShuffleExchangeExec] =
    collect(plan) { case exchange: CometShuffleExchangeExec => exchange }

  private def assertSparkExchange(plan: SparkPlan): Unit = {
    assert(cometExchanges(plan).isEmpty, s"unexpected Comet shuffle:\n$plan")
    assert(
      collect(plan) { case exchange: ShuffleExchangeExec => exchange }.nonEmpty,
      s"expected a Spark shuffle:\n$plan")
  }

  private def input: DataFrame =
    spark.range(0, 32, 1, 4).selectExpr("id + 1 AS value")

  private def collectionAggregatePlan: SparkPlan =
    input
      .selectExpr("value % 3 AS grouping_key", "value")
      .groupBy("grouping_key")
      .agg(expr("collect_list(value)"))
      .queryExecution
      .executedPlan

  private def assertNativeExecutionLoaded(): Unit = {
    val plan = input.queryExecution.executedPlan
    assert(collect(plan) { case op: CometNativeExec => op }.nonEmpty, s"$plan")
  }

  private def specialOperators(child: SparkPlan): (CollectLimitExec, TakeOrderedAndProjectExec) =
    (
      CollectLimitExec(3, child),
      TakeOrderedAndProjectExec(
        3,
        Seq(SortOrder(child.output.head, Ascending)),
        child.output,
        child))

  private def assertSpecialSupport(expected: Boolean): Unit = {
    val (limit, topK) = specialOperators(nativeChild())
    assert(CometCollectLimitExec.getSupportLevel(limit).isInstanceOf[Compatible] == expected)
    assert(
      CometTakeOrderedAndProjectExec.getSupportLevel(topK).isInstanceOf[Compatible] == expected)
    val rule = CometExecRule(spark)
    assert(rule(limit).isInstanceOf[CometCollectLimitExec] == expected)
    assert(rule(topK).isInstanceOf[CometTakeOrderedAndProjectExec] == expected)
  }

  // Spark core configuration cannot be changed through RuntimeConfig. Deliberately modify the
  // session's copy only, proving it cannot override the manager already owned by SparkEnv.
  private def withSessionConfOverride(settings: (String, String)*)(f: => Unit): Unit = {
    val conf = spark.sessionState.conf
    val previous = settings.map { case (key, _) => key -> Option(conf.getConfString(key, null)) }
    try {
      settings.foreach { case (key, value) => conf.setConfString(key, value) }
      f
    } finally {
      previous.foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

  test("the actual composite manager loads Comet and default auto preserves Spark shuffle") {
    val conf = spark.sessionState.conf
    assert(isCometShuffleManagerEnabled(conf))
    assertNativeExecutionLoaded()
    assert(CometConf.COMET_SHUFFLE_MODE.get(conf) == "auto")
    assert(!isCometShuffleEnabled(conf))
    assertSpecialSupport(expected = false)
  }

  test("native opt-in, execution, and shuffle flags gate exchanges and special producers") {
    for {
      mode <- Seq("auto", "jvm", "native")
      nativeExecution <- Seq(false, true)
      shuffleEnabled <- Seq(false, true)
    } {
      withSQLConf(
        CometConf.COMET_SHUFFLE_MODE.key -> mode,
        CometConf.COMET_EXEC_ENABLED.key -> nativeExecution.toString,
        CometConf.COMET_SHUFFLE_ENABLED.key -> shuffleEnabled.toString) {
        val expected = mode == "native" && nativeExecution && shuffleEnabled
        assert(isCometShuffleEnabled(spark.sessionState.conf) == expected)
        val exchange = ShuffleExchangeExec(SinglePartition, nativeChild())
        assert(
          CometShuffleExchangeExec.shuffleSupported(exchange).contains(CometNativeShuffle) ==
            expected)
        assertSpecialSupport(expected)
      }
    }
  }

  test("supported native partitionings never select JVM Comet shuffle") {
    withSQLConf(
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_SHUFFLE_NATIVE_RANGE_PARTITIONING_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_ENABLED.key -> "true") {
      val child = nativeChild()
      val partitionings = Seq(
        SinglePartition,
        HashPartitioning(child.output, 2),
        RangePartitioning(Seq(SortOrder(child.output.head, Ascending)), 2),
        RoundRobinPartitioning(2))
      partitionings.foreach { partitioning =>
        val exchange = ShuffleExchangeExec(partitioning, child)
        assert(CometShuffleExchangeExec.shuffleSupported(exchange).contains(CometNativeShuffle))
        assert(reasons(exchange).isEmpty)
      }
    }
  }

  test("unsupported native partitioning falls back without trying Comet columnar shuffle") {
    withSQLConf(
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_SHUFFLE_NATIVE_HASH_PARTITIONING_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_NATIVE_RANGE_PARTITIONING_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_ENABLED.key -> "false") {
      val child = nativeChild()
      val partitionings = Seq(
        HashPartitioning(child.output, 2),
        RangePartitioning(Seq(SortOrder(child.output.head, Ascending)), 2),
        RoundRobinPartitioning(2))
      partitionings.foreach { partitioning =>
        val exchange = ShuffleExchangeExec(partitioning, child)
        assert(CometShuffleExchangeExec.shuffleSupported(exchange).isEmpty)
        assert(reasons(exchange).exists(_.contains("disabled")))
        assert(reasons(exchange).exists(_.contains("columnar shuffle")))
      }
    }
  }

  test("unsupported output and hash-key types retain the Spark exchange") {
    withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      val objectChild =
        nativeChild(Seq(AttributeReference("value", ObjectType(classOf[String]))()))
      val objectExchange = ShuffleExchangeExec(SinglePartition, objectChild)
      assert(CometShuffleExchangeExec.shuffleSupported(objectExchange).isEmpty)
      assert(reasons(objectExchange).exists(_.contains("unsupported shuffle data type")))

      val arrayChild = nativeChild(Seq(AttributeReference("value", ArrayType(IntegerType))()))
      val arrayExchange = ShuffleExchangeExec(HashPartitioning(arrayChild.output, 2), arrayChild)
      assert(CometShuffleExchangeExec.shuffleSupported(arrayExchange).isEmpty)
      assert(reasons(arrayExchange).exists(_.contains("unsupported hash partitioning data type")))
    }
  }

  test("Spark and already-unwrapped Comet children do not enter the native createExec branch") {
    withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      val leaf = sparkLeaf()
      val children = Seq(
        leaf,
        CometSparkToColumnarExec(leaf),
        CometLocalTableScanExec(leaf, Nil, leaf.output),
        CometCollectLimitExec(CollectLimitExec(3, leaf), 3, 0, leaf))
      children.foreach { child =>
        val exchange = ShuffleExchangeExec(SinglePartition, child)
        assert(CometShuffleExchangeExec.shuffleSupported(exchange).isEmpty)
        assert(reasons(exchange).exists(_.contains("Comet")))
        assert(CometExecRule(spark)(exchange).isInstanceOf[ShuffleExchangeExec])
      }
    }
  }

  test("fallback is sticky after AQE reshapes the child or native mode becomes available") {
    val child = nativeChild()
    val exchange = ShuffleExchangeExec(SinglePartition, child)
    withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "auto") {
      assert(CometShuffleExchangeExec.shuffleSupported(exchange).isEmpty)
      assert(reasons(exchange).nonEmpty)
    }
    withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      val reshaped =
        exchange.withNewChildren(Seq(nativeChild())).asInstanceOf[ShuffleExchangeExec]
      assert(CometShuffleExchangeExec.shuffleSupported(reshaped).isEmpty)
      assert(CometExecRule(spark)(reshaped).isInstanceOf[ShuffleExchangeExec])
      assert(
        CometShuffleExchangeExec
          .shuffleSupported(ShuffleExchangeExec(SinglePartition, child))
          .contains(CometNativeShuffle))
    }
  }

  test("a session manager override cannot enable unsupported JVM Comet shuffle") {
    withSessionConfOverride("spark.shuffle.manager" -> classOf[CometShuffleManager].getName) {
      Seq("auto", "jvm").foreach { mode =>
        withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> mode) {
          assert(!isCometShuffleEnabled(spark.sessionState.conf))
          assert(
            CometShuffleExchangeExec
              .shuffleSupported(ShuffleExchangeExec(SinglePartition, nativeChild()))
              .isEmpty)
          assertSpecialSupport(expected = false)
        }
      }
      withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "native") {
        assert(isCometShuffleEnabled(spark.sessionState.conf))
      }
    }
  }

  test("runtime encryption and disabled stage reruns cannot be masked by session overrides") {
    val runtimeConfigurations = Seq(
      new SparkConf(false).set("spark.io.encryption.enabled", "true"),
      new SparkConf(false).set("spark.celeborn.client.spark.stageRerun.enabled", "false"))
    runtimeConfigurations.foreach { runtimeConf =>
      val support = CometCelebornPlanningTestShuffleManager.planningSupport(runtimeConf)
      manager.withPlanningSupport(support) {
        withSessionConfOverride(
          "spark.io.encryption.enabled" -> "false",
          "spark.celeborn.client.spark.stageRerun.enabled" -> "true") {
          withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "native") {
            assertNativeExecutionLoaded()
            assert(!isCometShuffleEnabled(spark.sessionState.conf))
            assert(
              CometShuffleExchangeExec
                .shuffleSupported(ShuffleExchangeExec(SinglePartition, nativeChild()))
                .isEmpty)
            assertSpecialSupport(expected = false)
          }
        }
      }
    }
  }

  test("forced local fallback protects special operators as well as ordinary exchanges") {
    manager.withPlanningSupport(CelebornNativeShufflePlanningSupport(fallbackPolicy = "ALWAYS")) {
      withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "native") {
        assert(!isCometShuffleEnabled(spark.sessionState.conf))
        assert(
          CometShuffleExchangeExec
            .shuffleSupported(ShuffleExchangeExec(SinglePartition, nativeChild()))
            .isEmpty)
        assertSpecialSupport(expected = false)
      }
    }
  }

  test("fallback partition thresholds use the exchange's reducer count") {
    manager.withPlanningSupport(
      CelebornNativeShufflePlanningSupport(fallbackPartitionThreshold = 2L)) {
      withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "native") {
        val child = nativeChild()
        assert(isCometShuffleEnabled(spark.sessionState.conf))
        assertSpecialSupport(expected = true)
        assert(
          CometShuffleExchangeExec
            .shuffleSupported(ShuffleExchangeExec(SinglePartition, child))
            .contains(CometNativeShuffle))
        val exchange = ShuffleExchangeExec(HashPartitioning(child.output, 2), child)
        assert(CometShuffleExchangeExec.shuffleSupported(exchange).isEmpty)
        assert(reasons(exchange).nonEmpty)
      }
    }
  }

  test("single-partition thresholds also block CollectLimit and TakeOrdered") {
    manager.withPlanningSupport(
      CelebornNativeShufflePlanningSupport(fallbackPartitionThreshold = 1L)) {
      withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "native") {
        assert(!isCometShuffleEnabled(spark.sessionState.conf))
        assertSpecialSupport(expected = false)
      }
    }
    manager.withPlanningSupport(
      CelebornNativeShufflePlanningSupport(
        fallbackPolicy = "NEVER",
        fallbackPartitionThreshold = 1L)) {
      withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "native") {
        assert(isCometShuffleEnabled(spark.sessionState.conf))
        assertSpecialSupport(expected = true)
      }
    }
  }

  for (skipShuffle <- Seq(false, true)) {
    test(
      s"aggregate fallback restores native ancestors and survives re-entry: skip=$skipShuffle") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "native") {
        val partial = collect(collectionAggregatePlan) {
          case aggregate: CometHashAggregateExec if aggregate.modes == Seq(Partial) => aggregate
        }.head
        val rule = CometExecRule(spark)
        val sorted =
          rule(SortExec(Seq(SortOrder(partial.output.head, Ascending)), false, partial))
        assert(sorted.isInstanceOf[CometSortExec], s"$sorted")

        val exchange = ShuffleExchangeExec(HashPartitioning(Seq(sorted.output.head), 4), sorted)
        if (skipShuffle) {
          exchange.setTagValue(CometExecRule.SKIP_COMET_SHUFFLE_TAG, ())
        }
        var restored: SparkPlan = exchange
        manager.withPlanningSupport(
          CelebornNativeShufflePlanningSupport(fallbackPartitionThreshold = 2L)) {
          restored = rule(exchange)
          assertSparkExchange(restored)
          assert(restored.children.head.isInstanceOf[SortExec], s"$restored")
          assert(collect(restored) { case aggregate: CometHashAggregateExec =>
            aggregate
          }.isEmpty)
          val sparkPartial = collect(restored) { case aggregate: BaseAggregateExec =>
            aggregate
          }.head
          assert(sparkPartial.getTagValue(CometExecRule.COMET_UNSAFE_PARTIAL).isDefined)
        }

        // Even if the runtime policy now permits native shuffle, AQE must not reconvert the
        // producer after its exchange committed to Spark's intermediate buffer representation.
        val reentered = rule(restored)
        assertSparkExchange(reentered)
        assert(reentered.children.head.isInstanceOf[SortExec], s"$reentered")
        assert(collect(reentered) { case aggregate: CometHashAggregateExec => aggregate }.isEmpty)
      }
    }
  }

  test("an outer fallback exchange preserves completed native aggregate stages") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      val finalAggregate = collect(collectionAggregatePlan) {
        case aggregate: CometHashAggregateExec if aggregate.modes == Seq(Final) => aggregate
      }.head
      val originalAggregates = collect(finalAggregate) { case aggregate: CometHashAggregateExec =>
        aggregate
      }
      assert(originalAggregates.exists(_.modes == Seq(Partial)))

      manager.withPlanningSupport(
        CelebornNativeShufflePlanningSupport(fallbackPartitionThreshold = 2L)) {
        val exchange =
          ShuffleExchangeExec(
            HashPartitioning(Seq(finalAggregate.output.head), 4),
            finalAggregate)
        val restored = CometExecRule(spark)(exchange)
        assert(restored.isInstanceOf[ShuffleExchangeExec])
        val retainedAggregates = collect(restored) { case aggregate: CometHashAggregateExec =>
          aggregate
        }
        assert(
          retainedAggregates.map(aggregate => (aggregate.modes, aggregate.output)) ==
            originalAggregates.map(aggregate => (aggregate.modes, aggregate.output)),
          s"$restored")
        assert(
          retainedAggregates.forall(
            _.originalPlan.getTagValue(CometExecRule.COMET_UNSAFE_PARTIAL).isEmpty))
      }
    }
  }

  // SortAggregateExec carries the same TypedImperativeAggregate buffer formats as
  // ObjectHashAggregateExec, so preserveSparkAggregateBuffers must restore a Comet sort-aggregate
  // Partial too. Regression guard: that predicate matched only CometHashAggregateExec before
  // CometSortAggregateExec existed, which would have let a native collect_list buffer reach a
  // Spark Final across a fallen-back Celeborn exchange.
  test("a Comet sort-aggregate Partial is restored when a Celeborn exchange falls back") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      SQLConf.USE_OBJECT_HASH_AGG.key -> "false") {
      val partial = collect(collectionAggregatePlan) {
        case aggregate: CometSortAggregateExec if aggregate.modes == Seq(Partial) => aggregate
      }.head

      manager.withPlanningSupport(
        CelebornNativeShufflePlanningSupport(fallbackPartitionThreshold = 2L)) {
        val exchange =
          ShuffleExchangeExec(HashPartitioning(Seq(partial.output.head), 4), partial)
        val restored = CometExecRule(spark)(exchange)
        assertSparkExchange(restored)
        assert(
          collect(restored) { case aggregate: CometBaseAggregateExec => aggregate }.isEmpty,
          s"$restored")
        val sparkPartial = collect(restored) { case aggregate: BaseAggregateExec =>
          aggregate
        }.head
        assert(sparkPartial.getTagValue(CometExecRule.COMET_UNSAFE_PARTIAL).isDefined)
      }
    }
  }

  test("transition reversion preserves native aggregate buffers across a Celeborn exchange") {
    manager.withPlanningSupport(CelebornNativeShufflePlanningSupport()) {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "native",
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
        val query = spark
          .range(0, 256, 1, 4)
          .selectExpr("id % 4 AS grouping_key", "CAST(id AS DOUBLE) AS value")
          .groupBy("grouping_key")
          .agg(expr("percentile(value, 0.5)").as("percentile_value"))
        val nativePlan = query.queryExecution.executedPlan

        assert(cometExchanges(nativePlan).nonEmpty, s"$nativePlan")
        assert(
          collect(nativePlan) {
            case aggregate: CometHashAggregateExec if aggregate.modes == Seq(Final) => aggregate
          }.nonEmpty,
          s"expected a native final aggregate before transition reversion:\n$nativePlan")
        assert(
          collect(nativePlan) { case transition: ColumnarToRowTransition => transition }.nonEmpty,
          s"test requires a result-stage transition:\n$nativePlan")

        var reverted: SparkPlan = null
        withSQLConf(
          CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
          CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
          reverted = RevertNativeForTransitionHeavyStages(spark)(nativePlan)
        }
        assert(
          collect(reverted) {
            case aggregate: CometHashAggregateExec if aggregate.modes == Seq(Final) => aggregate
          }.nonEmpty,
          "transition reversion must not make Spark consume a native percentile buffer:\n" +
            reverted)
      }
    }
  }

  for (adaptive <- Seq(false, true)) {
    for {
      percentile <- Seq("percentile", "percentile_approx")
      mergeFunction <- Seq("first", "last")
    } {
      test(s"unsupported $mergeFunction merge preserves $percentile buffers with AQE=$adaptive") {
        manager.withPlanningSupport(CelebornNativeShufflePlanningSupport()) {
          withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
            SQLConf.SHUFFLE_PARTITIONS.key -> "4",
            CometConf.COMET_SHUFFLE_MODE.key -> "native") {
            // FIRST/LAST cannot merge natively. Tag the incompatible percentile producer
            // before the first DISTINCT exchange is materialized, not just at the later
            // exchange that falls back. Its grouping key makes FIRST/LAST deterministic.
            val query = spark
              .range(0, 18, 1, 4)
              .selectExpr("id % 3 AS grouping_key", "id % 5 AS value")
              .groupBy("grouping_key")
              .agg(
                expr("count(DISTINCT value)").as("distinct_values"),
                expr(s"$percentile(value, 0.5)").as("percentile_value"),
                expr(s"$mergeFunction(grouping_key)").as("merge_value"))
            val nativeRegistrations = manager.nativeRegistrations.get()
            val (_, executedPlan) = checkSparkAnswer(query)
            assertSparkExchange(executedPlan)
            assert(manager.nativeRegistrations.get() == nativeRegistrations)
            assert(collect(executedPlan) { case aggregate: CometHashAggregateExec =>
              aggregate
            }.isEmpty)
            assert(collect(executedPlan) {
              case aggregate: BaseAggregateExec
                  if aggregate.aggregateExpressions.exists(expression =>
                    expression.mode == PartialMerge &&
                      expression.aggregateFunction.prettyName == mergeFunction) =>
                aggregate
            }.nonEmpty)
          }
        }
      }
    }

    test(s"fallback restores incompatible partial-merge ancestors with AQE=$adaptive") {
      manager.withPlanningSupport(
        CelebornNativeShufflePlanningSupport(fallbackPartitionThreshold = 2L)) {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.SHUFFLE_PARTITIONS.key -> "4",
          "spark.sql.requireAllClusterKeysForDistribution" -> "false",
          CometConf.COMET_SHUFFLE_MODE.key -> "native") {
          // Range's existing partitioning satisfies the initial DISTINCT-id distribution, so
          // PartialMerge can convert above Partial before the later exchange falls back.
          val query = spark
            .range(0, 18, 1, 4)
            .selectExpr("id % 3 AS grouping_key", "id", "id % 5 AS value")
            .groupBy("grouping_key")
            .agg(
              expr("count(DISTINCT id)").as("distinct_ids"),
              expr("percentile_approx(value, 0.5)").as("aggregate_value"))
          val nativeRegistrations = manager.nativeRegistrations.get()
          val (_, executedPlan) = checkSparkAnswer(query)
          assertSparkExchange(executedPlan)
          assert(manager.nativeRegistrations.get() == nativeRegistrations)
          assert(collect(executedPlan) { case aggregate: CometHashAggregateExec =>
            aggregate
          }.isEmpty)
          assert(collect(executedPlan) {
            case aggregate: BaseAggregateExec
                if aggregate.aggregateExpressions.exists(_.mode == PartialMerge) =>
              aggregate
          }.nonEmpty)
        }
      }
    }

    for {
      fallback <- Seq("partition threshold", "unsupported array hash key")
      function <- Seq("collect_list", "collect_set", "avg")
    } {
      test(s"native $fallback preserves $function aggregate buffers with AQE=$adaptive") {
        val complexKey = fallback == "unsupported array hash key"
        val support = if (complexKey) {
          CelebornNativeShufflePlanningSupport()
        } else {
          CelebornNativeShufflePlanningSupport(fallbackPartitionThreshold = 2L)
        }
        manager.withPlanningSupport(support) {
          withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
            SQLConf.SHUFFLE_PARTITIONS.key -> "4",
            CometConf.COMET_SHUFFLE_MODE.key -> "native") {
            val grouping = if (complexKey) "array(id % 3)" else "id % 3"
            val aggregate =
              if (function == "avg") "avg(value)" else s"sort_array($function(value))"
            val query = spark
              .range(0, 18, 1, 4)
              .selectExpr(s"$grouping AS grouping_key", "id AS value")
              .groupBy("grouping_key")
              .agg(expr(aggregate).as("aggregate_value"))

            val nativeRegistrations = manager.nativeRegistrations.get()
            val (_, executedPlan) = checkSparkAnswer(query)
            assertSparkExchange(executedPlan)
            assert(manager.nativeRegistrations.get() == nativeRegistrations)

            val nativeAggregates = collect(executedPlan) {
              case aggregate: CometHashAggregateExec => aggregate
            }
            if (function == "avg") {
              // AVG's intermediate state is Spark-compatible; native partials remain safe.
              assert(nativeAggregates.nonEmpty, s"$executedPlan")
            } else {
              // A Spark final cannot deserialize Comet's ArrayType collect_list/collect_set
              // state as its BinaryType buffer. Both halves must agree when the exchange falls
              // back, not just when an aggregate operator itself is unsupported.
              assert(nativeAggregates.isEmpty, s"$executedPlan")
            }
          }
        }
      }
    }

    test(
      s"QueryExecution plans native Celeborn exchanges only with explicit opt-in: AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        CometConf.COMET_SHUFFLE_MODE.key -> "native") {
        val nativeRegistrations = manager.nativeRegistrations.get()
        val plan = input.repartition(2, col("value")).queryExecution.executedPlan
        assert(plan.isInstanceOf[AdaptiveSparkPlanExec] == adaptive)
        val exchanges = cometExchanges(plan)
        assert(exchanges.nonEmpty, s"expected native exchange:\n$plan")
        assert(exchanges.forall(_.shuffleType == CometNativeShuffle))
        assert(manager.nativeRegistrations.get() == nativeRegistrations)
      }
    }

    for (mode <- Seq("auto", "jvm")) {
      test(s"QueryExecution executes the Spark fallback in mode=$mode with AQE=$adaptive") {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          CometConf.COMET_SHUFFLE_MODE.key -> mode) {
          val nativeRegistrations = manager.nativeRegistrations.get()
          val sparkRegistrations = manager.sparkRegistrations.get()
          val query = input.repartition(2, col("value"))
          assertSparkExchange(query.queryExecution.executedPlan)
          checkAnswer(query, (1L to 32L).map(Row(_)))
          assert(cometExchanges(query.queryExecution.executedPlan).isEmpty)
          assert(manager.nativeRegistrations.get() == nativeRegistrations)
          assert(manager.sparkRegistrations.get() > sparkRegistrations)
        }
      }
    }

    test(s"unsupported native repartition executes Spark fallback with AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        CometConf.COMET_SHUFFLE_MODE.key -> "native",
        CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_ENABLED.key -> "false") {
        val nativeRegistrations = manager.nativeRegistrations.get()
        val query = input.repartition(2)
        assertSparkExchange(query.queryExecution.executedPlan)
        checkAnswer(query, (1L to 32L).map(Row(_)))
        assert(cometExchanges(query.queryExecution.executedPlan).isEmpty)
        assert(manager.nativeRegistrations.get() == nativeRegistrations)
      }
    }

    test(s"unavailable push completion executes Spark shuffles with AQE=$adaptive") {
      val reason = "Celeborn client cannot safely observe native push completion"
      manager.withPlanningSupport(
        CelebornNativeShufflePlanningSupport(unavailableReason = Some(reason))) {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          CometConf.COMET_SHUFFLE_MODE.key -> "native") {
          assertNativeExecutionLoaded()
          assert(!isCometShuffleEnabled(spark.sessionState.conf))
          val exchange = ShuffleExchangeExec(SinglePartition, nativeChild())
          assert(CometShuffleExchangeExec.shuffleSupported(exchange).isEmpty)
          assert(reasons(exchange).contains(reason))
          assertSpecialSupport(expected = false)

          val nativeRegistrations = manager.nativeRegistrations.get()
          val sparkRegistrations = manager.sparkRegistrations.get()
          val repartitioned = input.repartition(2, col("value"))
          assertSparkExchange(repartitioned.queryExecution.executedPlan)
          checkAnswer(repartitioned, (1L to 32L).map(Row(_)))

          val limit = input.limit(3)
          val topK = input.orderBy(col("value").desc).limit(3)
          assert(collect(limit.queryExecution.executedPlan) { case op: CometCollectLimitExec =>
            op
          }.isEmpty)
          assert(collect(topK.queryExecution.executedPlan) {
            case op: CometTakeOrderedAndProjectExec => op
          }.isEmpty)
          checkAnswer(limit, Seq(Row(1L), Row(2L), Row(3L)))
          checkAnswer(topK, Seq(Row(32L), Row(31L), Row(30L)))
          assert(cometExchanges(repartitioned.queryExecution.executedPlan).isEmpty)
          assert(manager.nativeRegistrations.get() == nativeRegistrations)
          assert(manager.sparkRegistrations.get() > sparkRegistrations)
        }
      }
    }

    test(s"QueryExecution protects hidden limit and topK shuffles with AQE=$adaptive") {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "native") {
          val limit = input.limit(3).queryExecution.executedPlan
          val topK = input.orderBy(col("value").desc).limit(3).queryExecution.executedPlan
          assert(collect(limit) { case op: CometCollectLimitExec => op }.nonEmpty, s"$limit")
          assert(
            collect(topK) { case op: CometTakeOrderedAndProjectExec => op }.nonEmpty,
            s"$topK")
        }
        withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "auto") {
          val limit = input.limit(3)
          val topK = input.orderBy(col("value").desc).limit(3)
          assert(collect(limit.queryExecution.executedPlan) { case op: CometCollectLimitExec =>
            op
          }.isEmpty)
          assert(collect(topK.queryExecution.executedPlan) {
            case op: CometTakeOrderedAndProjectExec => op
          }.isEmpty)
          checkAnswer(limit, Seq(Row(1L), Row(2L), Row(3L)))
          checkAnswer(topK, Seq(Row(32L), Row(31L), Row(30L)))
        }
      }
    }
  }
}

/** Spark-reflective test constructor; no Celeborn classes or service are needed. */
class CometCelebornPlanningTestShuffleManager(conf: SparkConf, isDriver: Boolean)
    extends CometCelebornShuffleManager(
      conf,
      isDriver,
      (sparkConf, _) => new SortShuffleManager(sparkConf),
      planningSupportFactory = CometCelebornPlanningTestShuffleManager.planningSupport) {

  val nativeRegistrations = new AtomicInteger()
  val sparkRegistrations = new AtomicInteger()
  @volatile private var supportOverride: Option[CelebornNativeShufflePlanningSupport] = None

  // Test controls emulate policies captured when different applications start. They never mutate
  // SparkEnv or a production manager, and every override is restored even if an assertion fails.
  private[shuffle] def withPlanningSupport(support: CelebornNativeShufflePlanningSupport)(
      f: => Unit): Unit = {
    val previous = supportOverride
    try {
      supportOverride = Some(support)
      f
    } finally {
      supportOverride = previous
    }
  }

  override def nativeShuffleFallbackReason(numPartitions: Int): Option[String] =
    supportOverride match {
      case Some(support) => support.fallbackReason(numPartitions)
      case None => super.nativeShuffleFallbackReason(numPartitions)
    }

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    dependency match {
      case _: CometShuffleDependency[_, _, _] => nativeRegistrations.incrementAndGet()
      case _ => sparkRegistrations.incrementAndGet()
    }
    super.registerShuffle(shuffleId, dependency)
  }
}

private[shuffle] object CometCelebornPlanningTestShuffleManager {
  // This fake captures application configuration just like the real manager. Reflection against
  // the optional Celeborn client is covered separately; these tests exercise planner routing.
  def planningSupport(conf: SparkConf): CelebornNativeShufflePlanningSupport = {
    val unavailable = if (conf.getBoolean("spark.io.encryption.enabled", false)) {
      Some("Native Celeborn shuffle does not support spark.io.encryption.enabled=true")
    } else if (!conf.getBoolean("spark.celeborn.client.spark.stageRerun.enabled", true)) {
      Some("Native Celeborn shuffle requires stage reruns")
    } else {
      None
    }
    CelebornNativeShufflePlanningSupport(unavailableReason = unavailable)
  }
}
