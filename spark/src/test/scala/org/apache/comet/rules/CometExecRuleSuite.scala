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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._
import scala.util.Random

import org.scalatest.PrivateMethodTester._

import org.apache.logging.log4j.Level
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, ExpressionInfo, In, InSet, KnownFloatingPointNormalized, Literal, Not}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate, Final, Min, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, LogicalQueryStage, QueryStageExec, ShuffleQueryStageExec, SimpleCost, SimpleCostEvaluator}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, DoubleType, FloatType, StructField, StructType}

import org.apache.comet.{CometConf, CometCoverageStats, CometExplainInfo, CometSparkSessionExtensions, ExtendedExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark40Plus, isSpark42Plus, withFallbackReason}
import org.apache.comet.serde.{CometAggregateExpressionSerde, Compatible, ExprOuterClass, QueryPlanSerde, Unsupported}
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator}

/**
 * Test suite specifically for CometExecRule transformation logic. Tests the rule's ability to
 * transform Spark operators to Comet operators, fallback mechanisms, configuration handling, and
 * edge cases.
 */
class CometExecRuleSuite extends CometTestBase {

  // The observers are active only during the DPP lifecycle regression below. AQE can prepare
  // subqueries on different threads, so publish the callbacks and pair each thread's invocations.
  @volatile private var beforeCometPreparation: SparkPlan => Unit = (_: SparkPlan) => ()
  @volatile private var afterCometPreparation: SparkPlan => Unit = (_: SparkPlan) => ()

  override protected def createSparkSession: SparkSessionType = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    SparkSession
      .builder()
      .config(sparkContext.getConf)
      .withExtensions { extensions =>
        extensions.injectQueryStagePrepRule { _ =>
          new Rule[SparkPlan] {
            override def apply(plan: SparkPlan): SparkPlan = {
              beforeCometPreparation(plan)
              plan
            }
          }
        }
        new CometSparkSessionExtensions().apply(extensions)
        extensions.injectQueryStagePrepRule { _ =>
          new Rule[SparkPlan] {
            override def apply(plan: SparkPlan): SparkPlan = {
              afterCometPreparation(plan)
              plan
            }
          }
        }
      }
      .getOrCreate()
      .asInstanceOf[SparkSessionType]
  }

  /** Helper method to apply CometExecRule and return the transformed plan */
  private def applyCometExecRule(plan: SparkPlan): SparkPlan = {
    CometExecRule(spark).apply(stripAQEPlan(plan))
  }

  /** Create a test data frame that is used in all tests */
  private def createTestDataFrame = {
    val testSchema = new StructType(
      Array(
        StructField("id", DataTypes.IntegerType, nullable = true),
        StructField("name", DataTypes.StringType, nullable = true)))
    FuzzDataGenerator.generateDataFrame(new Random(42), spark, testSchema, 100, DataGenOptions())
  }

  /** Create a SparkPlan from the specified SQL with Comet disabled */
  private def createSparkPlan(spark: SparkSession, sql: String): SparkPlan = {
    var sparkPlan: SparkPlan = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val df = spark.sql(sql)
      sparkPlan = df.queryExecution.executedPlan
    }
    sparkPlan
  }

  /** Count the number of the specified operator in the plan */
  private def countOperators(plan: SparkPlan, opClass: Class[_]): Int = {
    stripAQEPlan(plan).collect {
      case stage: QueryStageExec =>
        countOperators(stage.plan, opClass)
      case op if op.getClass.isAssignableFrom(opClass) => 1
    }.sum
  }

  /**
   * Build a Spark plan containing an `ObjectHashAggregateExec` and hand it to `f`. `collect_list`
   * is a `TypedImperativeAggregate`, so Spark plans it as `ObjectHashAggregateExec` rather than
   * `HashAggregateExec`. Each call builds a fresh plan, which matters because fallback reasons
   * accumulate on plan-node tags.
   */
  private def withObjectHashAggPlan(f: SparkPlan => Unit): Unit = {
    withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "true") {
      withTempView("test_data") {
        createTestDataFrame.createOrReplaceTempView("test_data")
        val sparkPlan =
          createSparkPlan(spark, "SELECT id, collect_list(name) FROM test_data GROUP BY id")
        assert(countOperators(sparkPlan, classOf[ObjectHashAggregateExec]) > 0)
        f(sparkPlan)
      }
    }
  }

  /** The partial-mode `ObjectHashAggregateExec` in `plan`. */
  private def partialObjectHashAgg(plan: SparkPlan): ObjectHashAggregateExec =
    stripAQEPlan(plan).collectFirst {
      case a: ObjectHashAggregateExec if a.aggregateExpressions.forall(_.mode == Partial) => a
    }.get

  /** A native final aggregate over a shuffle stage, as reused by AQE replanning. */
  private def createAdaptiveAggregate(): CometHashAggregateExec = {
    val plan = createSparkPlan(
      spark,
      "SELECT id % 3 AS k, SUM(id) AS total FROM range(0, 100, 1, 2) GROUP BY id % 3")
    val aggregate = applyCometExecRule(plan).asInstanceOf[CometHashAggregateExec]
    val shuffle = aggregate.child.asInstanceOf[CometShuffleExchangeExec]
    aggregate
      .withNewChildren(Seq(ShuffleQueryStageExec(0, shuffle, shuffle.canonicalized)))
      .asInstanceOf[CometHashAggregateExec]
  }

  test("CometExecRule preserves the current direct AQE logical link") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST.key -> "Range") {
      val originalTags =
        Seq(Some(SparkPlan.LOGICAL_PLAN_TAG), Some(SparkPlan.LOGICAL_PLAN_INHERITED_TAG), None)
      originalTags.foreach { originalTag =>
        withClue(s"original logical tag: $originalTag") {
          val aggregate = createAdaptiveAggregate()
          val original = aggregate.originalPlan
          val originalLogicalPlan = original.logicalLink.get
          original.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
          original.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
          originalTag.foreach(original.setTagValue(_, originalLogicalPlan))

          var current: SparkPlan = aggregate
          (1 to 2).foreach { _ =>
            val logicalStage = LogicalQueryStage(originalLogicalPlan, current)
            val replanned = spark.sessionState.planner.plan(logicalStage).next()
            assert(replanned eq current)
            assert(replanned.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).exists(_ eq logicalStage))

            current = applyCometExecRule(replanned)
            assert(current.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).exists(_ eq logicalStage))
          }
        }
      }
    }
  }

  test("CometExecRule repairs ordinary and inherited logical links from the original plan") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST.key -> "Range") {
      val originalTags =
        Seq(Some(SparkPlan.LOGICAL_PLAN_TAG), Some(SparkPlan.LOGICAL_PLAN_INHERITED_TAG), None)
      for (originalTag <- originalTags; hasDirectLink <- Seq(false, true)) {
        withClue(s"original logical tag: $originalTag, ordinary direct link: $hasDirectLink") {
          val aggregate = createAdaptiveAggregate()
          val original = aggregate.originalPlan
          val originalLogicalPlan = original.logicalLink.get
          original.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
          original.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
          originalTag.foreach(original.setTagValue(_, originalLogicalPlan))

          aggregate.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
          aggregate.setTagValue(
            SparkPlan.LOGICAL_PLAN_INHERITED_TAG,
            LogicalQueryStage(originalLogicalPlan, aggregate))
          if (hasDirectLink) {
            aggregate.setTagValue(SparkPlan.LOGICAL_PLAN_TAG, LocalRelation(aggregate.output))
          }

          val transformed = applyCometExecRule(aggregate)
          if (originalTag.isDefined) {
            assert(transformed.logicalLink.exists(_ eq originalLogicalPlan))
          } else {
            assert(transformed.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).isEmpty)
            assert(transformed.getTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG).isEmpty)
          }
        }
      }
    }
  }

  test("AQE DPP broadcast roots retain temporary logical links after an unchanged replan") {
    assume(isSpark35Plus, "Native AQE DPP requires Spark 3.5+")
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      withTempDir { dir =>
        withTempView("dpp_link_fact", "dpp_link_dim") {
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark
              .range(64)
              .selectExpr("CAST(id % 8 AS INT) AS k", "id AS v")
              .write
              .partitionBy("k")
              .parquet(s"$dir/fact")
            spark
              .range(32)
              .selectExpr(
                "CAST(id % 8 AS INT) AS k",
                "id AS v",
                "IF(id % 2 = 0, 'DE', 'US') AS country")
              .write
              .parquet(s"$dir/dim")
          }
          spark.read.parquet(s"$dir/fact").createOrReplaceTempView("dpp_link_fact")
          spark.read.parquet(s"$dir/dim").createOrReplaceTempView("dpp_link_dim")

          assert(
            spark.sessionState.conf.getConf(SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS).isEmpty)
          type Replan = (SparkPlan, LogicalPlan)
          val pending = new ThreadLocal[List[Option[Replan]]] {
            override def initialValue(): List[Option[Replan]] = Nil
          }
          val observed = new ConcurrentLinkedQueue[(CometBroadcastExchangeExec, LogicalPlan)]()
          val tempTag = AdaptiveSparkPlanExec.TEMP_LOGICAL_PLAN_TAG
          val costEvaluator = SimpleCostEvaluator(forceOptimizeSkewedJoin = false)
          beforeCometPreparation = plan => {
            val replan = plan match {
              case broadcast: CometBroadcastExchangeExec =>
                broadcast.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).collect {
                  case stage: LogicalQueryStage =>
                    assert(stage.physicalPlan eq broadcast)
                    assert(broadcast.getTagValue(tempTag).exists(_ eq stage.logicalPlan))
                    (broadcast.clone(), stage.logicalPlan)
                }
              case _ => None
            }
            pending.set(replan :: pending.get())
          }
          afterCometPreparation = plan => {
            val replan = pending.get().head
            val remaining = pending.get().tail
            if (remaining.isEmpty) pending.remove() else pending.set(remaining)
            replan.foreach { case (previous, logicalPlan) =>
              val broadcast = plan.asInstanceOf[CometBroadcastExchangeExec]
              assert(broadcast.logicalLink.exists(_ eq logicalPlan))
              assert(broadcast.getTagValue(tempTag).exists(_ eq logicalPlan))
              // Spark rejects an equal-cost candidate when its physical tree is unchanged.
              // Pin both inputs to that decision, including Comet's retained temporary link.
              assert(previous == broadcast)
              assert(costEvaluator.evaluateCost(previous) == SimpleCost(0))
              assert(costEvaluator.evaluateCost(broadcast) == SimpleCost(0))
              observed.add(
                (broadcast.clone().asInstanceOf[CometBroadcastExchangeExec], logicalPlan))
            }
          }
          try {
            val df = sql("""
                |SELECT /*+ BROADCAST(d) */ f.k, f.total, d.total
                |FROM (SELECT k, SUM(v) AS total FROM dpp_link_fact GROUP BY k) f
                |JOIN (SELECT k, SUM(v) AS total FROM dpp_link_dim
                |      WHERE country = 'DE' GROUP BY k) d ON f.k = d.k
                |""".stripMargin)
            QueryTest.checkAnswer(
              df,
              (0 until 8 by 2).map(k => Row(k, 224L + 8L * k, 48L + 4L * k)),
              checkToRDD = false)
            val plan = df.queryExecution.executedPlan
            assert(collect(plan) { case b: CometBroadcastHashJoinExec => b }.nonEmpty)
            assert(collectWithSubqueries(plan) { case s: CometSubqueryBroadcastExec =>
              s
            }.nonEmpty)
            assert(!observed.isEmpty, "Expected a DPP broadcast root with a direct logical stage")
            observed.iterator().asScala.foreach { case (broadcast, logicalPlan) =>
              // Give the isolated snapshot conflicting links to pin Spark's TEMP-over-direct
              // precedence, which would otherwise be invisible after Comet repairs both.
              broadcast.setLogicalLink(LogicalQueryStage(logicalPlan, broadcast))
              val stage = BroadcastQueryStageExec(0, broadcast, broadcast.canonicalized)
              val setStageLink = PrivateMethod[Unit](Symbol("setLogicalLinkForNewQueryStage"))
              plan
                .asInstanceOf[AdaptiveSparkPlanExec]
                .invokePrivate(setStageLink(stage, broadcast))
              assert(stage.logicalLink.exists(_ eq logicalPlan))
            }
          } finally {
            beforeCometPreparation = _ => ()
            afterCometPreparation = _ => ()
          }
        }
      }
    }
  }

  test("expression-level fallback reasons are rolled up onto the operator that falls back") {
    // Extended explain only walks plan nodes, so a reason recorded on a sub-expression is
    // invisible unless CometExecRule lifts it onto the enclosing operator. Disabling a single
    // expression makes the Project fall back with the reason living on the Multiply node.
    // See https://github.com/apache/datafusion-comet/issues/5230.
    // This also pins the ordering inside `convertToComet`: strict mode is on in CometTestBase, so
    // if the roll-up stopped running before the strict check, planning here would throw.
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan = createSparkPlan(spark, "SELECT id * 2 as doubled FROM test_data")
      assert(countOperators(sparkPlan, classOf[ProjectExec]) == 1)

      withSQLConf(
        CometConf.getExprEnabledConfigKey("Multiply") -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        val project = stripAQEPlan(transformedPlan).collectFirst { case p: ProjectExec => p }.get

        val reasons = project
          .getTagValue(CometExplainInfo.FALLBACK_REASONS)
          .getOrElse(Set.empty[String])
        assert(
          reasons.exists(_.contains("Multiply")),
          s"expected the Multiply reason on the ProjectExec, got: $reasons")
        // The generic catch-all message must not appear: a real reason was available.
        assert(
          !reasons.exists(_.contains("is not supported")),
          s"a real reason was available but the generic message was used too: $reasons")
      }
    }
  }

  for (dataType <- Seq(FloatType, DoubleType)) {
    test(s"floating ${dataType.sql} IN serialization preserves prunable literal lists") {
      withSQLConf("spark.sql.legacy.nullInEmptyListBehavior" -> "false") {
        val value = AttributeReference("value", dataType)()
        val other = AttributeReference("other", dataType)()
        def literal(v: Double): Literal = dataType match {
          case FloatType => Literal(v.toFloat)
          case DoubleType => Literal(v)
        }
        val ordinary = Seq(1.0d, 3.0d).map(literal)
        val infinities = Seq(Double.PositiveInfinity, Double.NegativeInfinity).map(literal)
        val nullLiteral = Literal.create(null, dataType)
        val lists: Seq[(Seq[Expression], Boolean, Int)] = Seq(
          (ordinary, false, 0),
          (ordinary :+ nullLiteral, false, 0),
          (infinities, false, 0),
          (infinities :+ nullLiteral, false, 0),
          (Seq(nullLiteral), false, 0),
          (Seq(value, other), true, 0)) ++
          Seq(Double.PositiveInfinity, Double.NegativeInfinity)
            .map(v => (ordinary :+ literal(v), false, 0)) ++
          Seq(Double.NaN)
            .flatMap(v => Seq(ordinary, infinities).map(list => (list :+ literal(v), true, 0))) ++
          Seq(0.0d, -0.0d)
            .flatMap(v =>
              Seq(ordinary, infinities).map(list => (list :+ literal(v), false, 1))) ++
          Seq((ordinary ++ Seq(literal(0.0d), literal(-0.0d)), false, 0)) ++
          (if (isSpark35Plus) Seq((Seq.empty[Expression], false, 0)) else Nil)
        for ((list, needsNormalization, extraStaticCandidates) <- lists;
          asSet <- Seq(false, true) if !asSet || list.forall(_.isInstanceOf[Literal]);
          negate <- Seq(false, true);
          alreadyNormalized <- Seq(false, true)) {
          withClue(s"list=$list, asSet=$asSet, negate=$negate, normalized=$alreadyNormalized: ") {
            val needle = if (alreadyNormalized) {
              KnownFloatingPointNormalized(NormalizeNaNAndZero(value))
            } else {
              value
            }
            val in = if (asSet) {
              InSet(needle, list.collect { case l: Literal => l.value }.toSet)
            } else {
              In(needle, list)
            }
            val result = QueryPlanSerde
              .exprToProto(if (negate) Not(in) else in, Seq(value, other))
              .get
            // NOT InSet uses a separate Not node; NOT In is fused into the membership node.
            val serialized = if (result.hasNot) result.getNot.getChild else result
            assert(serialized.hasIn)
            assert(result.hasNot == (asSet && negate))
            assert(serialized.getIn.getNegated == (negate && !asSet))
            val serializedValue = serialized.getIn.getInValue
            if (needsNormalization || alreadyNormalized) {
              assert(serializedValue.hasNormalizeNanAndZero)
              assert(serializedValue.getNormalizeNanAndZero.getChild.hasBound)
            } else {
              assert(serializedValue.hasBound)
            }
            assert(serialized.getIn.getListsCount == list.size + extraStaticCandidates)
            for (i <- list.indices) {
              val candidate = serialized.getIn.getLists(i)
              if (list(i).isInstanceOf[Literal]) {
                assert(candidate.hasLiteral)
              } else {
                assert(candidate.hasNormalizeNanAndZero)
                assert(candidate.getNormalizeNanAndZero.getChild.hasBound)
              }
            }
            for (i <- list.size until list.size + extraStaticCandidates) {
              assert(serialized.getIn.getLists(i).hasLiteral)
            }
          }
        }
      }
    }

    test(
      s"floating ${dataType.sql} IN serialization retains normalized operand fallback reasons") {
      val expressionNames = Seq("Literal", "KnownFloatingPointNormalized")
      for (disabled <- None +: expressionNames.map(Some(_));
        literalValue <- Seq(false, true);
        negate <- Seq(false, true)) {
        val configs = expressionNames.map { name =>
          CometConf.getExprEnabledConfigKey(name) -> (!disabled.contains(name)).toString
        }
        withSQLConf(configs: _*) {
          withClue(s"disabled=$disabled, literalValue=$literalValue, negate=$negate: ") {
            val value = AttributeReference("value", dataType)()
            val other = AttributeReference("other", dataType)()
            val literals = dataType match {
              case FloatType => Seq(Literal(Float.NaN), Literal(3.0f))
              case DoubleType => Seq(Literal(Double.NaN), Literal(3.0d))
            }
            // Exercise temporary literals and normalizers in both the value and the list.
            val in =
              if (literalValue) In(literals.head, Seq(value, other)) else In(value, literals)
            val expr = if (negate) Not(in) else in
            val result = QueryPlanSerde.exprToProto(expr, Seq(value, other))
            val reasons = in
              .getTagValue(CometExplainInfo.FALLBACK_REASONS)
              .getOrElse(Set.empty[String])
            disabled match {
              case Some(name) =>
                assert(result.isEmpty)
                val key = CometConf.getExprEnabledConfigKey(name)
                assert(
                  reasons == Set(s"Expression support is disabled. Set $key=true to enable it."))
              case None =>
                assert(result.exists(_.hasIn))
                assert(result.get.getIn.getNegated == negate)
                assert(reasons.isEmpty)
            }
          }
        }
      }
    }

    test(s"floating ${dataType.sql} IN planning retains normalized operand fallback reasons") {
      val expressionNames = Seq("Literal", "KnownFloatingPointNormalized")
      for (disabled <- None +: expressionNames.map(Some(_));
        strict <- Seq(false, true);
        literalValue <- Seq(false, true);
        negate <- Seq(false, true)) {
        val configs = Seq(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          "spark.sql.optimizer.inSetConversionThreshold" -> "100",
          CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true",
          CometConf.COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST.key -> "Range",
          CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false",
          CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false",
          CometConf.COMET_STRICT_FALLBACK_REASONS.key -> strict.toString) ++
          expressionNames.map { name =>
            CometConf.getExprEnabledConfigKey(name) -> (!disabled.contains(name)).toString
          }
        withSQLConf(configs: _*) {
          withClue(
            s"disabled=$disabled, strict=$strict, literalValue=$literalValue, negate=$negate: ") {
            val column = s"CAST(id AS ${dataType.sql})"
            val predicate = if (literalValue) {
              s"CAST(1 AS ${dataType.sql}) IN ($column, -$column)"
            } else {
              // Keep a dynamic candidate here: all-literal non-NaN zero lists now stay static and
              // intentionally avoid the normalization wrappers this fallback test exercises.
              s"$column IN (CAST(0 AS ${dataType.sql}), -$column)"
            }
            val expression = if (negate) s"NOT ($predicate)" else predicate
            val df = sql(s"SELECT $expression AS hit FROM range(0, 4, 1, 1)")
            val optimized = df.queryExecution.optimizedPlan
            val expressions = optimized.flatMap(_.expressions)
            val membership = expressions.flatMap(_.collect { case in: In => in })
            // A folded predicate, singleton equality, or InSet would miss this serializer.
            assert(membership.size == 1 && membership.head.list.size == 2, optimized.toString)
            assert(membership.head.value.isInstanceOf[Literal] == literalValue)
            assert(expressions.exists(_.exists {
              case Not(_: In) => true
              case _ => false
            }) == negate)

            // Planning itself used to throw in strict mode, before a native task could run.
            val plan = df.queryExecution.executedPlan
            val projects = plan.collect { case p: ProjectExec => p }
            disabled match {
              case Some(name) =>
                assert(projects.size == 1, plan.toString)
                assert(plan.find(_.isInstanceOf[CometProjectExec]).isEmpty, plan.toString)
                val key = CometConf.getExprEnabledConfigKey(name)
                val reasons = projects.head
                  .getTagValue(CometExplainInfo.FALLBACK_REASONS)
                  .getOrElse(Set.empty[String])
                assert(
                  reasons == Set(s"Expression support is disabled. Set $key=true to enable it."))
              case None =>
                assert(projects.isEmpty, plan.toString)
                assert(plan.find(_.isInstanceOf[CometProjectExec]).isDefined, plan.toString)
                assert(
                  !plan.exists(
                    _.getTagValue(CometExplainInfo.FALLBACK_REASONS).exists(_.nonEmpty)))
            }
          }
        }
      }
    }
  }

  test("strict mode fails an operator that Comet declined without recording a reason") {
    // The bug this guards against is a serde returning None and forgetting to say why, which the
    // generic "<operator> is not supported" message used to hide. No serde in the tree is in that
    // state (the whole test corpus runs with strict mode on, which is what enforces it), so drive
    // the check directly with the shape such a serde produces: a handled operator whose children
    // are all native and which carries no reason on itself or its expressions.
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan = createSparkPlan(spark, "SELECT id * 2 as doubled FROM test_data")
      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val nativeChild = stripAQEPlan(applyCometExecRule(sparkPlan)).collectFirst {
          case op: CometNativeExec => op
        }.get
        val rule = CometExecRule(spark)

        // ProjectExec has a registered serde, so Comet did attempt this operator.
        val strictOp = ProjectExec(nativeChild.output, nativeChild)
        assert(CometExecRule.allExecs.contains(strictOp.getClass))
        val e = intercept[IllegalStateException] {
          rule.reportUnexplainedFallback(strictOp)
        }
        assert(e.getMessage.contains("recorded no fallback reason"))
        assert(e.getMessage.contains(strictOp.nodeName))

        // Production default: no throw, and the generic message so users still see something.
        withSQLConf(CometConf.COMET_STRICT_FALLBACK_REASONS.key -> "false") {
          val lenientOp = ProjectExec(nativeChild.output, nativeChild)
          rule.reportUnexplainedFallback(lenientOp)
          val reasons = lenientOp
            .getTagValue(CometExplainInfo.FALLBACK_REASONS)
            .getOrElse(Set.empty[String])
          assert(reasons == Set(s"${lenientOp.nodeName} is not supported"))
        }
      }
    }
  }

  test("strict fallback reason checking is off by default and on for Comet's own suites") {
    // The strict check turns "a serde returned None without saying why" into a hard failure. It
    // must stay off in production, where the generic "<operator> is not supported" message is the
    // right user-facing behaviour, and on for every Comet suite so the bug class cannot ship
    // again. Enabling it in CometTestBase is what actually exercises it: the whole test corpus
    // runs with it on. See https://github.com/apache/datafusion-comet/issues/5230.
    assert(!CometConf.COMET_STRICT_FALLBACK_REASONS.defaultValue.get)
    assert(CometConf.COMET_STRICT_FALLBACK_REASONS.get(spark.sessionState.conf))
  }

  test("strict mode does not fire for operators Comet never attempted to convert") {
    // Strict mode must only fire when a serde actually attempted the operator and declined. An
    // operator Comet has no handler for was never attempted, so demanding a specific reason
    // would be wrong - it keeps the generic message.
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan = createSparkPlan(spark, "SELECT id FROM test_data")
      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "false") {
        // With local table scan disabled the leaf has no Comet handler applied, and planning
        // must complete rather than throw.
        val transformedPlan = applyCometExecRule(sparkPlan)
        assert(transformedPlan != null)
      }
    }
  }

  test("ObjectHashAggregate records a reason when it declines because Comet shuffle is off") {
    // CometObjectHashAggregateExec deliberately declines when Comet shuffle is disabled, because
    // converting it would leave a Comet partial aggregate feeding a Spark final aggregate. That
    // decline used to return None from `convert` without recording why, so strict mode saw an
    // unexplained fallback and the generic "<operator> is not supported" message hid the real
    // cause. See https://github.com/apache/datafusion-comet/issues/5500.
    //
    // Both strict settings matter and are covered here: strict mode turns a missing reason into a
    // hard failure, while the lenient production default is where the generic message used to
    // stand in for the real cause, so it is the half a user actually sees.
    Seq(true, false).foreach { strictFallbackReasons =>
      // A fresh plan per iteration: fallback reasons accumulate on plan-node tags, so reusing
      // one plan would let the first iteration's reasons satisfy the second.
      withObjectHashAggPlan { sparkPlan =>
        withSQLConf(
          CometConf.COMET_SHUFFLE_ENABLED.key -> "false",
          CometConf.COMET_STRICT_FALLBACK_REASONS.key -> strictFallbackReasons.toString,
          CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
          val transformedPlan = applyCometExecRule(sparkPlan)
          // Assert on the partial stage: its child is the Comet local table scan, so it is the
          // node whose children are all native and which therefore reaches the strict
          // unexplained-fallback check in CometExecRule.
          val partialAgg = partialObjectHashAgg(transformedPlan)

          val reasons = partialAgg
            .getTagValue(CometExplainInfo.FALLBACK_REASONS)
            .getOrElse(Set.empty[String])
          assert(
            reasons.exists(_.contains("Comet shuffle is not enabled")),
            "expected the shuffle-disabled reason on the ObjectHashAggregateExec, " +
              s"got: $reasons")
          // The generic catch-all must not appear: a real reason was available.
          assert(
            !reasons.contains(s"${partialAgg.nodeName} is not supported"),
            s"a real reason was available but the generic message was used too: $reasons")

          // The reason must survive into the rendered extended-explain output, which is what
          // the user actually reads.
          val explained = new ExtendedExplainInfo().getFallbackReasons(transformedPlan)
          // Match the tail, not the "Comet shuffle is not enabled" prefix: with shuffle off,
          // CometShuffleExchangeExec.shuffleSupported tags the ShuffleExchangeExec with its own
          // "Comet shuffle is not enabled: ..." reason, and getFallbackReasons flattens every
          // node's tags into one unattributed set. The prefix alone would match that instead.
          assert(
            explained.exists(_.contains("would split the aggregate across Comet and Spark")),
            s"expected the aggregate's own reason in extended explain output, got: $explained")
        }
      }
    }
  }

  test("CometObjectHashAggregateExec reports the shuffle-disabled decline in getSupportLevel") {
    // The decline belongs in getSupportLevel, not convert: that is where CometExecRule attaches
    // the fallback reason centrally, and it matches CometCollectLimitExec and
    // CometTakeOrderedAndProjectExec, which gate on the same shuffle predicate.
    // See https://github.com/apache/datafusion-comet/issues/5500.
    withObjectHashAggPlan { sparkPlan =>
      val agg = partialObjectHashAgg(sparkPlan)

      withSQLConf(CometConf.COMET_SHUFFLE_ENABLED.key -> "false") {
        CometObjectHashAggregateExec.getSupportLevel(agg) match {
          case Unsupported(Some(notes)) =>
            // Assert the aggregate-specific tail, not the "Comet shuffle is not enabled"
            // prefix: that prefix is shared with CometCollectLimitExec and
            // CometTakeOrderedAndProjectExec, so matching it alone would not notice this
            // message losing the half that explains the split.
            assert(
              notes.contains("would split the aggregate across Comet and Spark"),
              s"expected the aggregate-specific shuffle-disabled reason, got: $notes")
          case other =>
            fail(s"expected Unsupported with a shuffle-disabled reason, got: $other")
        }
      }

      // Enabling shuffle must leave eligibility untouched.
      withSQLConf(CometConf.COMET_SHUFFLE_ENABLED.key -> "true") {
        assert(CometObjectHashAggregateExec.getSupportLevel(agg).isInstanceOf[Compatible])
      }
    }
  }

  test("ObjectHashAggregate still converts when Comet shuffle is enabled") {
    // Guards the other half of https://github.com/apache/datafusion-comet/issues/5500: adding the
    // fallback reason must not change which aggregates Comet accepts.
    withObjectHashAggPlan { sparkPlan =>
      withSQLConf(
        CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) > 0)
      }
    }
  }

  test(
    "CometExecRule should apply basic operator transformations, but only when Comet is enabled") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT id, id * 2 as doubled FROM test_data WHERE id % 2 == 0")

      // Count original Spark operators
      assert(countOperators(sparkPlan, classOf[ProjectExec]) == 1)
      assert(countOperators(sparkPlan, classOf[FilterExec]) == 1)

      for (cometEnabled <- Seq(true, false)) {
        withSQLConf(
          CometConf.COMET_ENABLED.key -> cometEnabled.toString,
          CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {

          val transformedPlan = applyCometExecRule(sparkPlan)

          if (cometEnabled) {
            assert(countOperators(transformedPlan, classOf[ProjectExec]) == 0)
            assert(countOperators(transformedPlan, classOf[FilterExec]) == 0)
            assert(countOperators(transformedPlan, classOf[CometProjectExec]) == 1)
            assert(countOperators(transformedPlan, classOf[CometFilterExec]) == 1)
          } else {
            assert(countOperators(transformedPlan, classOf[ProjectExec]) == 1)
            assert(countOperators(transformedPlan, classOf[FilterExec]) == 1)
            assert(countOperators(transformedPlan, classOf[CometProjectExec]) == 0)
            assert(countOperators(transformedPlan, classOf[CometFilterExec]) == 0)
          }
        }
      }
    }
  }

  test("CometExecRule should apply hash aggregate transformations") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      // Count original Spark operators
      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 0)
        assert(
          countOperators(
            transformedPlan,
            classOf[CometHashAggregateExec]) == originalHashAggCount)
      }
    }
  }

  test("CometExecRule should allow COUNT Comet partial and Spark final hash aggregate") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      // Count original Spark operators
      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // COUNT's buffer is compatible in this direction. Keeping the Final in Spark also keeps
        // the AQE/count-bug rewrites that prevent the reverse direction from being admitted.
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1)
      }
    }
  }

  test("CometExecRule should not allow Spark partial and Comet final hash aggregate") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      // Count original Spark operators
      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // COUNT still blocks Spark Partial to Comet Final, independently of the safe reverse
        // direction, so if the partial cannot be converted, neither should the final.
        assert(
          countOperators(transformedPlan, classOf[HashAggregateExec]) == originalHashAggCount)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  test("CometExecRule should allow safe Comet partial and Spark final hash aggregate") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      // Query uses only safe aggregates (MIN, MAX) with compatible intermediate buffers
      val sparkPlan =
        createSparkPlan(spark, "SELECT MIN(id), MAX(id) FROM test_data GROUP BY (id % 3)")

      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // Safe aggregates allow mixed execution: partial can be Comet, final stays Spark
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // final only
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // partial
      }
    }
  }

  test("CometExecRule should allow safe Spark partial and Comet final hash aggregate") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      // Query uses only safe aggregates (MIN, MAX) with compatible intermediate buffers
      val sparkPlan =
        createSparkPlan(spark, "SELECT MIN(id), MAX(id) FROM test_data GROUP BY (id % 3)")

      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // Safe aggregates allow mixed execution: partial stays Spark, final can be Comet
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // partial only
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // final
      }
    }
  }

  test("CometExecRule should allow SUM mixed Comet partial and Spark final") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")
      val sparkPlan =
        createSparkPlan(spark, "SELECT SUM(id) FROM test_data GROUP BY (id % 3)")
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)
      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        // SUM buffer matches Spark: partial converts to Comet, final stays Spark.
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // final
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // partial
      }
    }
  }

  test("CometExecRule should allow SUM mixed Spark partial and Comet final") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")
      val sparkPlan =
        createSparkPlan(spark, "SELECT SUM(id) FROM test_data GROUP BY (id % 3)")
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)
      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // partial
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // final
      }
    }
  }

  test("CometExecRule should not allow AVG Comet partial and Spark final before buffer repair") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")
      val sparkPlan =
        createSparkPlan(spark, "SELECT AVG(id) FROM test_data GROUP BY (id % 3)")
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)
      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        // Matching field types do not make native AVG's empty (null, 0) state safe for Spark.
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 2)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  test("CometExecRule should not allow try_sum mixed execution") {
    assume(isSpark35Plus, "try_sum was added in Spark 3.5")
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")
      val sparkPlan =
        createSparkPlan(spark, "SELECT try_sum(id) FROM test_data GROUP BY (id % 3)")
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)
      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        // TRY-mode SUM uses a Comet-internal buffer column, so mixing is unsafe:
        // the partial must also fall back to Spark.
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 2)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  test("CometExecRule should not allow decimal AVG mixed execution") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")
      // Precision must be large enough (prec + 4 > 15) that Spark's own DecimalAggregates
      // optimizer rule does not rewrite AVG to operate on the unscaled Long value, which would
      // sidestep the decimal buffer path this test is meant to exercise.
      val sparkPlan =
        createSparkPlan(
          spark,
          "SELECT AVG(CAST(id AS DECIMAL(20, 2))) FROM test_data GROUP BY (id % 3)")
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)
      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        // Decimal AVG is deferred (its overflow path nulls count differently from Spark), so
        // mixed execution is unsafe and the partial must also fall back to Spark.
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 2)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  for (distinct <- Seq(false, true)) {
    test(
      s"unsafe aggregate buffers fall back when native shuffle is ineligible (distinct=$distinct)") {
      withTempView("test_data") {
        createTestDataFrame.createOrReplaceTempView("test_data")
        val aggregates = "AVG(id)" + (if (distinct) ", SUM(DISTINCT id)" else "")

        for (fallback <- Seq("disabled hash partitioning", "prior shuffle fallback", "none")) {
          withSQLConf(
            CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> "native",
            CometConf.COMET_SHUFFLE_NATIVE_HASH_PARTITIONING_ENABLED.key ->
              (fallback != "disabled hash partitioning").toString) {
            val sparkPlan =
              createSparkPlan(spark, s"SELECT $aggregates FROM test_data GROUP BY (id % 3)")
            val aggregateCount = countOperators(sparkPlan, classOf[HashAggregateExec])
            assert(aggregateCount == (if (distinct) 4 else 2))
            if (fallback == "prior shuffle fallback") {
              // Tag only the lowest exchange. A DISTINCT plan's upper exchange must inherit
              // the native-only refusal from its now-Spark merge inputs, not from another tag.
              val lowerShuffle = stripAQEPlan(sparkPlan).collect {
                case shuffle: ShuffleExchangeExec => shuffle
              }.last
              withFallbackReason(lowerShuffle, fallback)
            }
            val transformed = applyCometExecRule(sparkPlan)
            val nativeExpected = fallback == "none"

            // Shuffle is enabled, but a native-only shuffle can still fall back. The distinct
            // rewrite also has intermediate PartialMerge and mixed Partial/PartialMerge stages.
            for (plan <- Seq(transformed, applyCometExecRule(transformed))) {
              assert(
                countOperators(plan, classOf[CometHashAggregateExec]) ==
                  (if (nativeExpected) aggregateCount else 0))
              assert(
                countOperators(plan, classOf[HashAggregateExec]) ==
                  (if (nativeExpected) 0 else aggregateCount))
            }
            // AQE reapplies the rule to an exchange without its Final aggregate. The tagged
            // Partial must remain in Spark in that stage-only pass too.
            transformed.collect { case shuffle: ShuffleExchangeExec => shuffle }.foreach {
              shuffle =>
                val stage = applyCometExecRule(shuffle)
                assert(countOperators(stage, classOf[CometHashAggregateExec]) == 0)
            }
          }
        }
      }
    }
  }

  test("aggregate buffer direction opt-ins are independent") {
    // A policy-only handler opts into consuming Spark state. Its inherited producer policy
    // must stay false; serializing any expression is outside the scope of this fixture.
    val reverseOnly = new CometAggregateExpressionSerde[Min] {
      override def supportsSparkPartialToNativeFinal(fn: Min): Boolean = true

      override def convert(
          aggExpr: AggregateExpression,
          expr: Min,
          inputs: Seq[Attribute],
          binding: Boolean,
          conf: SQLConf): Option[ExprOuterClass.AggExpr] = None
    }
    val fn = Min(Literal(1L))
    assert(reverseOnly.supportsSparkPartialToNativeFinal(fn))
    assert(!reverseOnly.supportsNativePartialToSparkFinal(fn))
  }

  test("restored partial records its reason when its current child is not native") {
    // Wrap a converted input to prevent a re-entrant serde call from supplying the reason.
    // This planner-only fixture never executes its synthetic buffer boundary.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      withTempView("test_data") {
        createTestDataFrame.createOrReplaceTempView("test_data")
        val plan = applyCometExecRule(
          createSparkPlan(spark, "SELECT AVG(id) FROM test_data GROUP BY (id % 3)"))
        val partial = plan.collectFirst {
          case agg: CometHashAggregateExec if agg.modes == Seq(Partial) => agg
        }.get
        val sparkFinal = plan.collectFirst {
          case agg: CometHashAggregateExec if agg.modes == Seq(Final) =>
            agg.originalPlan.asInstanceOf[HashAggregateExec]
        }.get
        val nonNativeChild = InputAdapter(partial.child)
        val restored = CometExecRule(spark).revertUnsafePartialAggregates(
          sparkFinal.copy(child = partial.copy(child = nonNativeChild)))
        val sparkPartial = restored.children.head
        assert(sparkPartial.isInstanceOf[HashAggregateExec])
        assert(sparkPartial.children.head.isInstanceOf[InputAdapter])
        assert(sparkPartial.children.head.output == nonNativeChild.output)
        val reason = sparkPartial.getTagValue(CometExecRule.COMET_UNSAFE_PARTIAL).get
        assert(sparkPartial.getTagValue(CometExplainInfo.FALLBACK_REASONS).get.contains(reason))
        assert(new ExtendedExplainInfo().getFallbackReasons(sparkPartial).contains(reason))
      }
    }
  }

  test("unrepaired native aggregate buffers warn once without rewriting query stages") {
    // Construct the stage placeholder emitted by CometExchangeSink, including a native merge
    // above it. No SQL reproduction or materialization is assumed: this pins the diagnostic
    // when repair stops at a stage, and the absence of warnings for unrelated inner producers.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      withTempView("test_data") {
        createTestDataFrame.createOrReplaceTempView("test_data")
        val plan = applyCometExecRule(
          createSparkPlan(spark, "SELECT AVG(id) FROM test_data GROUP BY (id % 3)"))
        val partial = plan.collectFirst {
          case agg: CometHashAggregateExec if agg.modes == Seq(Partial) => agg
        }.get
        val nativeFinal = plan.collectFirst {
          case agg: CometHashAggregateExec if agg.modes == Seq(Final) => agg
        }.get
        val sparkFinal = nativeFinal.originalPlan.asInstanceOf[HashAggregateExec]
        val sparkPartial = partial.originalPlan.asInstanceOf[HashAggregateExec]
        val exchange = ShuffleExchangeExec(
          org.apache.spark.sql.catalyst.plans.physical.SinglePartition,
          partial)
        val stage = ShuffleQueryStageExec(0, exchange, exchange.canonicalized)
        val placeholder = CometSinkPlaceHolder(
          org.apache.comet.serde.OperatorOuterClass.Operator.getDefaultInstance,
          stage,
          stage)
        val nativeMerge = partial.copy(
          aggregateExpressions = partial.aggregateExpressions.map(_.copy(mode = PartialMerge)),
          child = placeholder)
        val warning = "Comet could not restore a native intermediate buffer producer"
        val rule = CometExecRule(spark)
        for {
          (child, shouldWarn) <- Seq(
            nativeMerge -> true,
            placeholder -> true,
            sparkPartial.copy(child = nativeFinal) -> false,
            nativeFinal -> false)
          logFallback <- Seq("false", "true")
        } {
          withSQLConf(CometConf.COMET_EXPLAIN_FALLBACK_LOG_ENABLED.key -> logFallback) {
            val consumer = sparkFinal.copy(child = child)
            val appender = new LogAppender("unrepaired aggregate buffers")
            withLogAppender(appender, Seq("org.apache.comet"), Some(Level.WARN)) {
              assert(rule.revertUnsafePartialAggregates(consumer) eq consumer)
              assert(rule.revertUnsafePartialAggregates(consumer) eq consumer)
            }
            assert(consumer.child eq child)
            val warnings =
              appender.loggingEvents.count(_.getMessage.getFormattedMessage.contains(warning))
            assert(warnings == (if (shouldWarn) 1 else 0), s"$child: $warnings")
            assert(
              new ExtendedExplainInfo()
                .getFallbackReasons(consumer)
                .exists(_.contains(warning)) ==
                shouldWarn)
          }
        }
        assert(stage.plan eq exchange)
        assert(exchange.child eq partial)
      }
    }
  }

  test("CometExecRule should not allow decimal SUM mixed execution") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")
      // Precision must be large enough (prec + 4 > 15) that Spark's own DecimalAggregates
      // optimizer rule does not rewrite SUM to operate on the unscaled Long value, which would
      // sidestep the decimal buffer path this test is meant to exercise.
      val sparkPlan =
        createSparkPlan(
          spark,
          "SELECT SUM(CAST(id AS DECIMAL(20, 2))) FROM test_data GROUP BY (id % 3)")
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)
      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        // Native decimal SUM makes precision overflow sticky (or throws eagerly in ANSI),
        // while Spark's generated scalar Partial can recover after a later cancelling input.
        // Keep the Partial in Spark even though the emitted buffer field types match.
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 2)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  test("CometExecRule should allow AVG mixed Spark partial and Comet final") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")
      val sparkPlan =
        createSparkPlan(spark, "SELECT AVG(id) FROM test_data GROUP BY (id % 3)")
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)
      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // partial
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // final
      }
    }
  }

  test("CometExecRule should allow BloomFilter mixed Comet partial and Spark final") {
    assume(!isSpark42Plus, "https://github.com/apache/datafusion-comet/issues/4142")
    val funcId = new FunctionIdentifier("bloom_filter_agg")
    spark.sessionState.functionRegistry.registerFunction(
      funcId,
      new ExpressionInfo(classOf[BloomFilterAggregate].getName, "bloom_filter_agg"),
      (children: Seq[Expression]) =>
        children.size match {
          case 1 => new BloomFilterAggregate(children.head)
          case 2 => new BloomFilterAggregate(children.head, children(1))
          case 3 => new BloomFilterAggregate(children.head, children(1), children(2))
        })
    try {
      withTempView("test_data") {
        createTestDataFrame.createOrReplaceTempView("test_data")

        // Cast to bigint: Spark 3.4's bloom_filter_agg only accepts a long-typed first
        // argument; later versions widened it to any integral type.
        val sparkPlan =
          createSparkPlan(spark, "SELECT bloom_filter_agg(CAST(id AS BIGINT)) FROM test_data")

        val originalObjectAggCount = countOperators(sparkPlan, classOf[ObjectHashAggregateExec])
        assert(originalObjectAggCount == 2)

        withSQLConf(
          CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
          CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
          val transformedPlan = applyCometExecRule(sparkPlan)

          // BloomFilter is mixed-safe: partial converts to Comet, final stays Spark.
          assert(countOperators(transformedPlan, classOf[ObjectHashAggregateExec]) == 1)
          assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1)
        }
      }
    } finally {
      spark.sessionState.functionRegistry.dropFunction(funcId)
    }
  }

  test("CometExecRule should allow BloomFilter mixed Spark partial and Comet final") {
    assume(!isSpark42Plus, "https://github.com/apache/datafusion-comet/issues/4142")
    val funcId = new FunctionIdentifier("bloom_filter_agg")
    spark.sessionState.functionRegistry.registerFunction(
      funcId,
      new ExpressionInfo(classOf[BloomFilterAggregate].getName, "bloom_filter_agg"),
      (children: Seq[Expression]) =>
        children.size match {
          case 1 => new BloomFilterAggregate(children.head)
          case 2 => new BloomFilterAggregate(children.head, children(1))
          case 3 => new BloomFilterAggregate(children.head, children(1), children(2))
        })
    try {
      withTempView("test_data") {
        createTestDataFrame.createOrReplaceTempView("test_data")

        // Cast to bigint: Spark 3.4's bloom_filter_agg only accepts a long-typed first
        // argument; later versions widened it to any integral type.
        val sparkPlan =
          createSparkPlan(spark, "SELECT bloom_filter_agg(CAST(id AS BIGINT)) FROM test_data")

        val originalObjectAggCount = countOperators(sparkPlan, classOf[ObjectHashAggregateExec])
        assert(originalObjectAggCount == 2)

        withSQLConf(
          CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
          CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
          val transformedPlan = applyCometExecRule(sparkPlan)

          assert(countOperators(transformedPlan, classOf[ObjectHashAggregateExec]) == 1)
          assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1)
        }
      }
    } finally {
      spark.sessionState.functionRegistry.dropFunction(funcId)
    }
  }

  // Regression tests for https://github.com/apache/datafusion-comet/issues/4813. An aggregate with
  // an incompatible intermediate buffer (percentile_approx) combined with a distinct aggregate is
  // rewritten by Spark into a multi-stage plan whose partial is separated from the final by
  // intermediate PartialMerge stages. If part of that chain runs in Comet and part in Spark the
  // incompatible buffer crosses the boundary and crashes, so the whole chain must fall back.
  test(
    "CometExecRule should not split distinct aggregate with incompatible buffer (Spark final)") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan = createSparkPlan(
        spark,
        "SELECT percentile_approx(id, 0.5), COUNT(DISTINCT name) FROM test_data")

      // The distinct rewrite produces a multi-stage ObjectHashAggregate chain.
      assert(countOperators(sparkPlan, classOf[ObjectHashAggregateExec]) > 1)

      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // percentile_approx has an incompatible buffer, so with the final forced to Spark the
        // entire partial/merge chain must also stay in Spark.
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  test(
    "CometExecRule should not split distinct aggregate with incompatible buffer (Spark part)") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan = createSparkPlan(
        spark,
        "SELECT percentile_approx(id, 0.5), COUNT(DISTINCT name) FROM test_data")

      assert(countOperators(sparkPlan, classOf[ObjectHashAggregateExec]) > 1)

      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // With the partial/merge stages forced to Spark, no Comet aggregate may consume their
        // incompatible buffers either.
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  test("CometExecRule should allow approx_count_distinct mixed Comet partial and Spark final") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      // approx_count_distinct stores its registers in Spark's identical packed-Long buffer, so
      // it is mixed-safe: the Comet partial can feed a Spark final.
      val sparkPlan =
        createSparkPlan(
          spark,
          "SELECT approx_count_distinct(id) FROM test_data GROUP BY (id % 3)")

      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // final only
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // partial
      }
    }
  }

  test("CometExecRule should allow approx_count_distinct mixed Spark partial and Comet final") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(
          spark,
          "SELECT approx_count_distinct(id) FROM test_data GROUP BY (id % 3)")

      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // partial only
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // final
      }
    }
  }

  test("CometExecRule should not convert hash aggregate when grouping key contains map type") {
    // Spark 3.4/3.5 reject `array<map<...>>` as a grouping key in the analyzer (not orderable),
    // so the plan never reaches CometExecRule on those versions. The guard we're exercising
    // (containsMapType) only matters on Spark 4.0+, which permits the GROUP BY to be analyzed.
    assume(isSpark40Plus)
    // Arrow's row format, used by DataFusion's grouped hash aggregate for composite keys, does
    // not support Map at any nesting level. Grouping by a type that transitively contains a map
    // (e.g. array<map<int,int>>) must stay on Spark to avoid a native row-encoding crash.
    val sparkPlan = createSparkPlan(
      spark,
      """SELECT count(*)
        |FROM VALUES (ARRAY(MAP(1, 2), MAP(1, 3))),
        |            (ARRAY(MAP(2, 3), MAP(1, 3))) AS t(a)
        |GROUP BY a""".stripMargin)

    val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
    assert(originalHashAggCount == 2)

    withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
      val transformedPlan = applyCometExecRule(sparkPlan)

      assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == originalHashAggCount)
      assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
    }
  }

  test("CometExecRule should apply broadcast exchange transformations") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan = createSparkPlan(
        spark,
        "SELECT /*+ BROADCAST(b) */ a.id, b.name FROM test_data a JOIN test_data b ON a.id = b.id")

      // Count original Spark operators
      val originalBroadcastExchangeCount =
        countOperators(sparkPlan, classOf[BroadcastExchangeExec])
      assert(originalBroadcastExchangeCount == 1)

      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[BroadcastExchangeExec]) == 0)
        assert(
          countOperators(
            transformedPlan,
            classOf[CometBroadcastExchangeExec]) == originalBroadcastExchangeCount)
      }
    }
  }

  test("CometExecRule should apply shuffle exchange transformations") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT id, COUNT(*) FROM test_data GROUP BY id ORDER BY id")

      // Count original Spark operators
      val originalShuffleExchangeCount = countOperators(sparkPlan, classOf[ShuffleExchangeExec])
      assert(originalShuffleExchangeCount == 2)

      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[ShuffleExchangeExec]) == 0)
        assert(
          countOperators(
            transformedPlan,
            classOf[CometShuffleExchangeExec]) == originalShuffleExchangeCount)
      }
    }
  }

  test("CometExecRule should not wrap shuffle in CometColumnarShuffle when both sides are JVM") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      val originalShuffleExchangeCount = countOperators(sparkPlan, classOf[ShuffleExchangeExec])
      assert(originalShuffleExchangeCount == 1)
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)

      // Disable partial aggregate so both aggregates fall back to Spark JVM. The shuffle between
      // them would otherwise be wrapped with CometColumnarShuffle, which adds unnecessary
      // row<->arrow conversion overhead when neither side can consume columnar output.
      // See https://github.com/apache/datafusion-comet/issues/4004.
      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // Both aggregates should remain JVM
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 2)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)

        // The shuffle should remain a Spark ShuffleExchangeExec (not wrapped in Comet)
        assert(countOperators(transformedPlan, classOf[CometShuffleExchangeExec]) == 0)
        assert(
          countOperators(transformedPlan, classOf[ShuffleExchangeExec]) ==
            originalShuffleExchangeCount)
      }
    }
  }

  test("CometExecRule should not revert columnar shuffle when the revert config is disabled") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      assert(countOperators(sparkPlan, classOf[ShuffleExchangeExec]) == 1)
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)

      // Both aggregates fall back to JVM as in the prior test, but the revert optimization is
      // disabled, so the shuffle should still be wrapped in CometColumnarShuffle.
      withSQLConf(
        CometConf.COMET_SHUFFLE_REVERT_REDUNDANT_COLUMNAR_ENABLED.key -> "false",
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 2)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)

        assert(countOperators(transformedPlan, classOf[ShuffleExchangeExec]) == 0)
        assert(countOperators(transformedPlan, classOf[CometShuffleExchangeExec]) == 1)
      }
    }
  }

  test("CometExecRule should not revert columnar shuffle when both aggregates go native") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      assert(countOperators(sparkPlan, classOf[ShuffleExchangeExec]) == 1)
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)

      // With default settings both aggregates convert to Comet native, so the shuffle between
      // them has a Comet consumer on both sides and must remain columnar - the revert must not
      // fire here.
      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 0)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 2)

        assert(countOperators(transformedPlan, classOf[ShuffleExchangeExec]) == 0)
        assert(countOperators(transformedPlan, classOf[CometShuffleExchangeExec]) == 1)
      }
    }
  }

  private val PLAN_ONLY_PREFIX = "[Comet plan-only]"

  /**
   * Runs `f` over a Parquet table `tbl`. The source list must be set before the table is created,
   * since `withParquetTable` bakes the V1/V2 choice into the temp view.
   */
  private def withPlanOnlyTable(
      aqe: Boolean = true,
      planOnly: Boolean = true,
      useV1: Boolean = true)(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> (if (useV1) "parquet" else ""),
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
      CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> planOnly.toString) {
      withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl")(f)
    }
  }

  /** Plans `sql` in plan-only mode and asserts nothing was offloaded to native. */
  private def runPlanOnlyAndAssertReverted(
      sql: String,
      useV1: Boolean = true,
      aqe: Boolean = true): Unit = {
    withPlanOnlyTable(aqe = aqe, useV1 = useV1) {
      val executed = stripAQEPlan(spark.sql(sql).queryExecution.executedPlan)
      val cometNodes = executed.collect { case p: CometPlan => p }
      assert(
        cometNodes.isEmpty,
        s"plan-only mode must not offload; found Comet operators: $cometNodes")
      val expectedScan = if (useV1) classOf[FileSourceScanExec] else classOf[BatchScanExec]
      assert(
        executed.exists(p => expectedScan.isInstance(p)),
        s"expected ${expectedScan.getSimpleName}, got:\n$executed")
    }
  }

  /** Runs `f` and returns the `[Comet plan-only]` reports that `CometRule` logged. */
  private def capturePlanOnlyReports(f: => Unit): Seq[String] = {
    val appender = new LogAppender("Comet plan-only reports")
    withLogAppender(
      appender,
      loggerNames = Seq(classOf[CometRule].getName),
      level = Some(Level.WARN)) {
      f
    }
    appender.loggingEvents
      .map(_.getMessage.getFormattedMessage)
      .filter(_.startsWith(PLAN_ONLY_PREFIX))
      .toSeq
  }

  /** The `Comet accelerated N out of M eligible operators` counts in a plan-only report. */
  private def coverageOf(report: String): (Int, Int) = {
    val pattern = """Comet accelerated (\d+) out of (\d+) eligible operators""".r
    pattern
      .findFirstMatchIn(report)
      .map(m => (m.group(1).toInt, m.group(2).toInt))
      .getOrElse(fail(s"report has no coverage summary:\n$report"))
  }

  /**
   * Runs `query` normally, then in plan-only mode, and asserts the one report containing `marker`
   * has the same coverage as the executed plan. Returns the executed plan.
   */
  private def assertReportMatchesExecuted(query: String, marker: String): SparkPlan = {
    val df = sql(query)
    df.collect()
    val plan = df.queryExecution.executedPlan
    val executed = CometCoverageStats.forPlan(plan)
    withSQLConf(CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true") {
      val reports = capturePlanOnlyReports(sql(query).collect())
      val matching = reports.filter(_.contains(marker))
      assert(
        matching.size == 1,
        s"expected one report containing '$marker', got:\n${reports.mkString("\n\n")}")
      assert(
        coverageOf(matching.head) ==
          (executed.cometOperators, executed.cometOperators + executed.sparkOperators),
        s"report disagrees with the executed plan ($executed):\n${matching.head}")
    }
    plan
  }

  for {
    useV1 <- Seq(true, false)
    aqe <- Seq(true, false)
  } {
    val label = s"${if (useV1) "V1" else "V2"} scan, AQE=$aqe"
    test(s"plan-only mode: $label") {
      runPlanOnlyAndAssertReverted(
        "SELECT _2, count(*) FROM tbl GROUP BY _2",
        useV1 = useV1,
        aqe = aqe)
    }
  }

  test("plan-only mode: scalar subquery is also reverted") {
    runPlanOnlyAndAssertReverted("SELECT _1 FROM tbl WHERE _1 > (SELECT max(_2) FROM tbl)")
  }

  test("plan-only mode: same query with the config off runs on Comet") {
    withPlanOnlyTable(planOnly = false) {
      val plan =
        spark.sql("SELECT _2, count(*) FROM tbl GROUP BY _2").queryExecution.executedPlan
      val cometNodes = stripAQEPlan(plan).collect { case p: CometPlan => p }
      assert(cometNodes.nonEmpty, "expected Comet operators when plan-only mode is disabled")
    }
  }

  // Subqueries are planned before the outer query, which must still get its own report.
  for (aqe <- Seq(true, false)) {
    test(s"plan-only mode: report describes the outer query, not just a subquery (AQE=$aqe)") {
      withPlanOnlyTable(aqe = aqe) {
        val reports = capturePlanOnlyReports {
          spark.sql("SELECT _1 FROM tbl WHERE _1 > (SELECT max(_2) FROM tbl)").collect()
        }
        // Filter appears only in the outer plan.
        assert(
          reports.exists(_.contains("Filter")),
          s"no report describes the outer query:\n${reports.mkString("\n\n")}")
        // One for the subquery, one for the outer query.
        assert(
          reports.size == 2,
          s"expected two reports, got ${reports.size}:\n${reports.mkString("\n\n")}")
      }
    }
  }

  test("plan-only mode: coverage accounts for post-columnar stage reversion") {
    withSQLConf(CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false") {
      // AQE off: Spark applies the post-columnar rules to the whole plan once, as the preview does.
      withPlanOnlyTable(aqe = false, planOnly = false) {
        // SUM and MAX can be split across a stage boundary in either direction, which leaves the
        // stage free to revert. COUNT cannot, and would block the reversion under test.
        val query = "SELECT _2, sum(_1), max(_1) FROM tbl GROUP BY _2"

        var unreverted = 0
        withSQLConf(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
          val df = sql(query)
          df.collect()
          unreverted = CometCoverageStats.forPlan(df.queryExecution.executedPlan).cometOperators
          assert(unreverted > 0)
        }

        withSQLConf(
          CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
          CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
          val executed = CometCoverageStats.forPlan(assertReportMatchesExecuted(query, ""))
          // Guard against a vacuous test: reversion must actually fire.
          assert(
            executed.cometOperators < unreverted,
            s"stage reversion did not fire, so this test is vacuous: $executed")
        }
      }
    }
  }

  // These paths plan without a SQL execution ID and must still report once per query.
  for (aqe <- Seq(true, false)) {
    test(s"plan-only mode: one report per query without a SQL execution ID (AQE=$aqe)") {
      withPlanOnlyTable(aqe = aqe) {
        val query = "SELECT _2, count(*) FROM tbl GROUP BY _2"

        val viaRdd = capturePlanOnlyReports(spark.sql(query).rdd.count())
        assert(
          viaRdd.size == 1,
          s"expected one report for df.rdd.count(), got ${viaRdd.size}:\n" +
            viaRdd.mkString("\n\n"))

        val viaExecutedPlan =
          capturePlanOnlyReports(spark.sql(query).queryExecution.executedPlan)
        assert(
          viaExecutedPlan.size == 1,
          s"expected one report for executedPlan, got ${viaExecutedPlan.size}:\n" +
            viaExecutedPlan.mkString("\n\n"))
      }
    }
  }

  // The outer report must count the subquery's converted operators. AQE off so the preview and
  // the executed plan are the same single pass.
  test("plan-only mode: outer report coverage matches normal planning for a scalar subquery") {
    withPlanOnlyTable(aqe = false, planOnly = false) {
      val plan = assertReportMatchesExecuted(
        "SELECT _1 FROM tbl WHERE _1 > (SELECT max(_2) FROM tbl)",
        marker = "Filter")
      assert(
        CometCoverageStats.forPlan(plan).cometOperators > 0,
        "test query must be partly accelerated for the comparison to mean anything")
    }
  }

  // AQE can collapse a plan whose stage materializes empty into an empty relation that shares
  // nothing with the reported plan.
  for {
    aqe <- Seq(true, false)
    // `toRdd.count` runs without a SQL execution ID, like PySpark's `df.rdd`.
    (action, runIt) <- Seq[(String, org.apache.spark.sql.DataFrame => Unit)](
      "collect" -> (df => df.collect()),
      "toRdd.count" -> (df => df.queryExecution.toRdd.count()))
    // In the second shape `RemoveRedundantSorts` drops the root sort, so the mark lands on the
    // join and the empty re-plan's root inherits the unmarked sort's tags. In the third the
    // global aggregate survives the empty join, so the re-plan is not itself empty. The fourth
    // is empty from the start, which must not be mistaken for a re-plan.
    (shape, query, marker) <- Seq(
      (
        "aggregate",
        "SELECT id % 2 AS k, count(*) AS n FROM range(20) WHERE id < 0 GROUP BY id % 2",
        "HashAggregate"),
      (
        "join under a removed root sort",
        """SELECT a.id FROM range(0, 20, 1, 2) a
          |JOIN range(0, 20, 1, 2) b ON a.id % 7 = b.id % 7
          |WHERE a.id < 0
          |SORT BY a.id % 7""".stripMargin,
        "SortMergeJoin"),
      (
        "global aggregate over a join",
        """SELECT count(*) FROM range(0, 20, 1, 2) a
          |JOIN range(0, 20, 1, 2) b ON a.id % 7 = b.id % 7
          |WHERE a.id < 0""".stripMargin,
        "SortMergeJoin"),
      ("initially empty", "SELECT id FROM range(0) DISTRIBUTE BY id", "Exchange"))
  } {
    test(s"plan-only mode: one report when the plan becomes empty ($shape, AQE=$aqe, $action)") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true") {
        val reports = capturePlanOnlyReports(runIt(spark.sql(query)))
        assert(
          reports.size == 1,
          s"expected one report, got ${reports.size}:\n${reports.mkString("\n\n")}")
        assert(
          reports.head.contains(marker),
          s"the report does not describe the query:\n${reports.head}")
      }
    }
  }

  for {
    aqe <- Seq(true, false)
    (action, runIt) <- Seq[(String, org.apache.spark.sql.DataFrame => Unit)](
      "collect" -> (df => df.collect()),
      "toRdd.count" -> (df => df.queryExecution.toRdd.count()))
  } {
    test(s"plan-only mode: a subquery referenced twice is reported once (AQE=$aqe, $action)") {
      withPlanOnlyTable(aqe = aqe) {
        val reports = capturePlanOnlyReports {
          runIt(spark.sql("""SELECT _1,
                            |  (SELECT max(_2) FROM tbl
                            |   WHERE _1 > (SELECT min(_2) FROM tbl)) AS a,
                            |  (SELECT max(_2) FROM tbl
                            |   WHERE _1 > (SELECT min(_2) FROM tbl)) AS b
                            |FROM tbl""".stripMargin))
        }
        // The `min` subquery, the `max` subquery and the outer query.
        assert(
          reports.size == 3,
          s"expected three reports, got ${reports.size}:\n${reports.mkString("\n\n")}")
      }
    }
  }

  test("plan-only mode: the report does not depend on spark.comet.explain.format") {
    withSQLConf(
      CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.key ->
        CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_FALLBACK) {
      withPlanOnlyTable() {
        val reports = capturePlanOnlyReports(sql("SELECT _1 + 1 FROM tbl").collect())
        assert(reports.size == 1, s"expected one report, got:\n${reports.mkString("\n\n")}")
        val (accelerated, eligible) = coverageOf(reports.head)
        assert(
          accelerated > 0 && accelerated == eligible,
          s"unexpected coverage:\n${reports.head}")
      }
    }
  }

  private val dppQuery = "SELECT f.fact_id, f.fact_str, d.dim_str FROM fact f " +
    "JOIN dim d ON f.fact_key = d.dim_key WHERE d.dim_id < 10"

  /** Registers a `fact` table partitioned on the join key plus a small `dim` table. */
  private def withDppTables(f: => Unit): Unit = {
    withTempDir { dir =>
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        val sess = spark
        import sess.implicits._
        (0 until 400)
          .map(i => (i, i % 10, s"f$i"))
          .toDF("fact_id", "fact_key", "fact_str")
          .write
          .partitionBy("fact_key")
          .parquet(s"${dir.getAbsolutePath}/fact")
        (0 until 10)
          .map(i => (i, i, s"d$i"))
          .toDF("dim_id", "dim_key", "dim_str")
          .write
          .parquet(s"${dir.getAbsolutePath}/dim")
      }
      spark.read.parquet(s"${dir.getAbsolutePath}/fact").createOrReplaceTempView("fact")
      spark.read.parquet(s"${dir.getAbsolutePath}/dim").createOrReplaceTempView("dim")
      withTempView("fact", "dim")(f)
    }
  }

  // The DPP build must be previewed below its BroadcastExchangeExec so that stage reversion fires
  // as in real planning. Reversion is forced on so a missed reversion changes the number.
  test("plan-only mode: outer report coverage matches normal planning for a DPP subquery") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      // AQE off: the preview describes the pre-adaptive plan.
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0",
      CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false") {
      withDppTables {
        val plan = assertReportMatchesExecuted(dppQuery, marker = "BroadcastHashJoin")
        // `exists` walks children only, and a DPP subquery hangs off the scan's expressions.
        assert(
          plan.collectWithSubqueries { case p: SubqueryBroadcastExec => p }.nonEmpty,
          s"test query must produce a DPP subquery:\n$plan")
      }
    }
  }

  // Under AQE the DPP build is planned mid-execution by `PlanAdaptiveDynamicPruningFilters`,
  // after the outer query was reported, and must still get a report.
  test("plan-only mode: a DPP subquery planned mid-execution is reported under AQE") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true") {
      withDppTables {
        val reports = capturePlanOnlyReports(sql(dppQuery).collect())
        assert(
          reports.size == 2,
          "expected a report for the outer query and one for the DPP build plan, got " +
            s"${reports.size}:\n${reports.mkString("\n\n")}")
        assert(
          reports.count(_.contains("BroadcastHashJoin")) == 1,
          s"exactly one report should describe the outer query:\n${reports.mkString("\n\n")}")
      }
    }
  }

  // Per-stage and per-re-optimization applications must not add reports.
  test("plan-only mode: one report for a multi-stage query under AQE") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withPlanOnlyTable() {
        val query = "SELECT a._2, count(*) FROM tbl a JOIN tbl b ON a._1 = b._2 " +
          "GROUP BY a._2 ORDER BY 1"
        val reports = capturePlanOnlyReports(sql(query).collect())
        assert(
          reports.size == 1,
          s"expected one report, got ${reports.size}:\n${reports.mkString("\n\n")}")
      }
    }
  }

  test("scan conversion must run before operator conversion") {
    withTempPath { path =>
      createTestDataFrame.write.parquet(path.toString)
      withTempView("test_data") {
        spark.read.parquet(path.toString).createOrReplaceTempView("test_data")
        val query = "SELECT id, id * 2 as doubled FROM test_data WHERE id % 2 == 0"

        // One plan per rule application. Fallback reasons are recorded as tags on the Spark
        // nodes, and CometNativeScan.isSupported declines a scan already carrying one, so
        // reusing the plan the exec rule just refused would hold the second case down.
        val forExecRule = stripAQEPlan(createSparkPlan(spark, query))
        val forCometRule = stripAQEPlan(createSparkPlan(spark, query))
        assert(countOperators(forExecRule, classOf[FileSourceScanExec]) == 1)
        assert(countOperators(forCometRule, classOf[FileSourceScanExec]) == 1)

        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          // Off by default, but pinned here: with it on, CometExecRule bridges the unconverted
          // scan with a CometSparkToColumnarExec and converts the operators above it, which is a
          // different path from the one under test.
          CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "false") {
          // CometExecRule builds its native plan up from the nodes CometScanRule produces, so on
          // its own it leaves the scan on Spark's reader.
          assert(
            countOperators(
              CometExecRule(spark).apply(forExecRule),
              classOf[FileSourceScanExec]) == 1)
          // CometRule runs both phases, in that order. This fails if the scan phase is ever
          // reordered or dropped.
          assert(
            countOperators(
              CometRule(spark).apply(forCometRule),
              classOf[CometNativeScanExec]) == 1)
        }
      }
    }
  }

}
