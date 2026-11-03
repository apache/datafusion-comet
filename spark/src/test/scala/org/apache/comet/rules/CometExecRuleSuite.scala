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

import java.util.concurrent.{ConcurrentLinkedQueue, FutureTask, TimeUnit}

import scala.jdk.CollectionConverters._
import scala.util.Random

import org.scalatest.PrivateMethodTester._

import org.apache.logging.log4j.Level
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, AttributeReference, BoundReference, Descending, Expression, ExpressionInfo, GreaterThan, In, InSet, KnownFloatingPointNormalized, Literal, MakeDate, Not, RuntimeReplaceable, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate, Count, Final, Min, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, NormalizeNaNAndZero}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, IdentityBroadcastMode, RoundRobinPartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.CometExec.nativeLeaves
import org.apache.spark.sql.comet.execution.shuffle.{CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, BroadcastQueryStageExec, LogicalQueryStage, QueryStageExec, ShuffleQueryStageExec, SimpleCost, SimpleCostEvaluator}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BaseJoinExec, BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, HashedRelationBroadcastMode, SortMergeJoinExec}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, DoubleType, FloatType, StructField, StructType}

import org.apache.comet.{CometConf, CometCoverageStats, CometExplainInfo, CometSparkSessionExtensions, ConfigEntry, ExtendedExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark40Plus, isSpark42Plus, withFallbackReason}
import org.apache.comet.serde.{CometAggregateExpressionSerde, Compatible, ExprOuterClass, OperatorOuterClass, QueryPlanSerde, Unsupported}
import org.apache.comet.shims.ShimCometWindowGroupLimit
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

  test("columnar-to-row identity broadcasts retain every build row") {
    for (direct <- Seq(false, true)) {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        CometConf.COMET_EXEC_RANGE_ENABLED.key -> "true",
        CometConf.COMET_DIRECT_COLUMNAR_TO_ROW_ENABLED.key -> direct.toString,
        CometConf.COMET_DIRECT_COLUMNAR_TO_ROW_MIN_BATCH_SIZE.key -> "1") {
        val build = applyCometExecRule(
          createSparkPlan(spark, "SELECT CAST(id AS INT) AS y FROM range(2024, 2028, 1, 2)"))
        assert(build.isInstanceOf[CometNativeExec], build.treeString)
        val mode = IdentityBroadcastMode
        val original = BroadcastExchangeExec(mode, build)
        val exchange = CometBroadcastExchangeExec(original, build.output, mode, build)
        val rows = CometColumnarToRowExec(exchange)
          .executeBroadcast[Array[InternalRow]]()
          .value
        // Two rows per partition expose row reuse in both JVM conversion paths.
        withClue(s"direct=$direct: ") {
          assert(rows.map(_.getInt(0)).sorted.toSeq == Seq(2024, 2025, 2026, 2027))
        }
      }
    }
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

  test("make_date preserves row demand below lazy consumers, including reused native plans") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
      val input = createSparkPlan(spark, "SELECT * FROM VALUES (2024), (2025) AS t(y)")
      val sample = createSparkPlan(
        spark,
        "SELECT * FROM (SELECT * FROM range(8)) TABLESAMPLE (50 PERCENT)").collectFirst {
        case op: SampleExec => op
      }.get
      val consumers: Seq[(String, SparkPlan => SparkPlan)] = Seq(
        ("LIMIT", child => CollectLimitExec(1, child)),
        ("LIMIT", child => LocalLimitExec(1, child)),
        ("LIMIT", child => GlobalLimitExec(1, child)),
        ("SAMPLE", child => sample.withNewChildren(Seq(child))),
        ("FILTER", child => FilterExec(GreaterThan(child.output.head, Literal(0)), child)))
      for (ansi <- Seq(false, true); boundedYear <- Seq(false, true)) {
        def project: SparkPlan = ProjectExec(
          Seq(
            input.output.head,
            Alias(
              MakeDate(
                if (boundedYear) Literal(2024) else input.output.head,
                Literal(1),
                Literal(1),
                ansi),
              "date")()),
          input)
        val native = applyCometExecRule(project)
        assert(countOperators(native, classOf[CometProjectExec]) == 1, native.treeString)
        for ((boundary, consume) <- consumers; reused <- Seq(false, true)) {
          val child = if (reused) applyCometExecRule(project) else project
          val consumer = consume(child)
          val converted = applyCometExecRule(consumer)
          for (result <- Seq(converted, applyCometExecRule(converted))) {
            // A reused local-table sink has no native child operator for a new cap.
            val capped = consumer.isInstanceOf[CollectLimitExec] && !reused
            val fallback = (ansi || !boundedYear) && !capped
            withClue(
              s"ansi=$ansi boundedYear=$boundedYear reused=$reused $boundary\n${result.treeString}") {
              assert(countOperators(result, classOf[ProjectExec]) == (if (fallback) 1 else 0))
              assert(
                countOperators(result, classOf[CometProjectExec]) == (if (fallback) 0 else 1))
              assert(new ExtendedExplainInfo()
                .getFallbackReasons(result)
                .exists(
                  _.contains(s"make_date requires Spark evaluation below $boundary")) == fallback)
            }
          }
        }

        // These consumers evaluate their complete input independently of the outer limit.
        val materializers: Seq[SparkPlan => SparkPlan] = Seq(
          child => SortExec(Seq(SortOrder(child.output.head, Ascending)), false, child),
          child => ShuffleExchangeExec(SinglePartition, child))
        materializers.foreach { materialize =>
          val result = applyCometExecRule(LocalLimitExec(1, materialize(project)))
          assert(countOperators(result, classOf[CometProjectExec]) == 1, result.treeString)
          assert(
            !new ExtendedExplainInfo()
              .getFallbackReasons(result)
              .exists(_.contains("make_date requires Spark evaluation below LIMIT")))
        }
      }
    }
  }

  test("make_date ANSI-off projections retain guards for throwing inputs and siblings") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
      val rows = Seq((2024.toShort, Some(1), 1), (2024.toShort, Some(13), 0))
      withTempPath { path =>
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          spark.createDataFrame(rows).coalesce(1).write.parquet(path.getCanonicalPath)
        }
        withParquetTable(path.getCanonicalPath, "make_date_non_ansi") {
          // A literal or widened small-integer year cannot overflow the Date32 representation.
          // The invalid month is harmless with ANSI off, including below a lazy row consumer.
          for (year <- Seq("2024", "_1")) {
            val df = sql(s"SELECT make_date($year, _2, 1) FROM make_date_non_ansi LIMIT 10")
            checkSparkAnswerAndOperator(df, includeClasses = Seq(classOf[CometProjectExec]))
          }

          val deferred = sql("""
          |SELECT r, IF(r < 0, dt, DATE '2024-01-01'), r + 1 FROM (
          |  SELECT rand(7L) AS r, make_date(2024, _2, 1) AS dt FROM make_date_non_ansi
          |)
          |""".stripMargin)
          checkSparkAnswerAndOperator(deferred, includeClasses = Seq(classOf[CometProjectExec]))
          assert(
            countOperators(deferred.queryExecution.executedPlan, classOf[CometProjectExec]) == 2)

          // A harmless date producer can still yield NULL and hide a throwing operand in
          // its consumer. Repeated r keeps the date and arithmetic in separate projections.
          val consumers = Seq(
            ("unix_date(dt) + element_at(array(1), i)", true),
            ("unix_date(dt) - element_at(array(1), i)", true),
            ("unix_date(dt) * element_at(array(1), i)", true),
            ("unix_date(dt) + 1", false),
            ("unix_date(dt) + i", false),
            ("i + unix_date(dt)", false))
          for ((consumer, fallback) <- consumers) {
            val df = sql(s"""
              |SELECT r, r + 1, $consumer AS result FROM (
              |  SELECT rand(7L) AS r, make_date(2024, _2, 1) AS dt, _3 AS i
              |  FROM make_date_non_ansi
              |)
              |""".stripMargin)
            checkSparkAnswer(df)
            val plan = df.queryExecution.executedPlan
            withClue(s"consumer=$consumer\n${plan.treeString}") {
              assert(countOperators(plan, classOf[ProjectExec]) == (if (fallback) 1 else 0))
              assert(countOperators(plan, classOf[CometProjectExec]) == (if (fallback) 1 else 2))
              assert(countOperators(plan, classOf[CometNativeScanExec]) == 1)
            }
          }

          // Index zero throws even with ANSI off. A limit can skip a throwing date argument,
          // and a NULL date can skip an unrelated right operand in Spark's arithmetic.
          val guarded = Seq(
            "SELECT make_date(2024, 1, element_at(array(1), _3)) FROM make_date_non_ansi LIMIT 1",
            "SELECT unix_date(make_date(2024, _2, 1)) + element_at(array(1), _3) FROM make_date_non_ansi")
          guarded.foreach { query =>
            val df = sql(query)
            val capped = query.endsWith("LIMIT 1")
            if (capped) {
              checkSparkAnswerAndOperator(df, includeClasses = Seq(classOf[CometProjectExec]))
            } else {
              checkSparkAnswer(df)
            }
            val plan = df.queryExecution.executedPlan
            assert(countOperators(plan, classOf[ProjectExec]) == (if (capped) 0 else 1))
            assert(countOperators(plan, classOf[CometProjectExec]) == (if (capped) 1 else 0))
            if (capped) assert(countOperators(plan, classOf[CometLocalLimitExec]) == 1)
          }
        }

      }

      // The date's day attribute looks harmless locally, but its lower producer can throw.
      withParquetTable(Seq((Some(1), 1), (None, 0)), "make_date_non_ansi_input") {
        val df = sql("""
          |SELECT r, make_date(2024, m, d), r + 1 FROM (
          |  SELECT rand(7L) AS r, _1 AS m, element_at(array(1), _2) AS d
          |  FROM make_date_non_ansi_input
          |)
          |""".stripMargin)
        checkSparkAnswer(df)
        val plan = df.queryExecution.executedPlan
        assert(countOperators(plan, classOf[ProjectExec]) == 2, plan.treeString)
        assert(countOperators(plan, classOf[CometProjectExec]) == 0, plan.treeString)
      }
    }
  }

  test("make_date collect limit reuses the native projection input cap") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
      withTempPath { path =>
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          spark
            .createDataFrame(Seq((2024, 1, "1"), (2024, 13, "bad")))
            .coalesce(1)
            .write
            .parquet(path.getCanonicalPath)
        }
        withParquetTable(path.getCanonicalPath, "make_date_limit_cap") {
          for (capEnabled <- Seq(false, true)) {
            withSQLConf(CometConf.COMET_EXEC_LOCAL_LIMIT_ENABLED.key -> capEnabled.toString) {
              val df = sql("SELECT make_date(_1, _2, 1) FROM make_date_limit_cap LIMIT 1")
              if (capEnabled) {
                checkSparkAnswerAndOperator(df, includeClasses = Seq(classOf[CometProjectExec]))
              } else {
                checkSparkAnswer(df)
              }
              val converted = stripAQEPlan(df.queryExecution.executedPlan)
              for (plan <- Seq(converted, applyCometExecRule(converted))) {
                assert(countOperators(plan, classOf[ProjectExec]) == (if (capEnabled) 0 else 1))
                assert(
                  countOperators(plan, classOf[CometProjectExec]) == (if (capEnabled) 1 else 0))
                assert(
                  countOperators(plan, classOf[CometLocalLimitExec]) == (if (capEnabled) 1
                                                                         else 0))
                assert(new ExtendedExplainInfo()
                  .getFallbackReasons(plan)
                  .exists(
                    _.contains("make_date requires Spark evaluation below LIMIT")) != capEnabled)
              }

              // The cap must still evaluate a bad date when it belongs to the consumed prefix.
              val (sparkError, cometError) = checkSparkAnswerMaybeThrows(
                sql("SELECT make_date(_1, _2, 1) FROM make_date_limit_cap LIMIT 2"))
              Seq(sparkError, cometError).foreach { error =>
                assert(
                  error.exists(e => causeChain(e).exists(_.toString.contains("MonthOfYear"))))
              }
            }
          }

          // A cap immediately below the date does not protect this lower producing projection.
          // Repeated r keeps both projections, and LIMIT skips the second row's invalid cast.
          val producer = sql("""
          |SELECT r, make_date(y, m, 1), r + 1 FROM (
          |  SELECT rand(7L) AS r, _1 AS y, CAST(_3 AS INT) AS m FROM make_date_limit_cap
          |) LIMIT 1
          |""".stripMargin)
          checkSparkAnswer(producer)
          val plan = producer.queryExecution.executedPlan
          assert(countOperators(plan, classOf[ProjectExec]) == 2, plan.treeString)
          assert(countOperators(plan, classOf[CometProjectExec]) == 0, plan.treeString)
        }
      }
    }
  }

  test("make_date shuffle keys remain eager below a limit") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_SHUFFLE_NATIVE_HASH_PARTITIONING_ENABLED.key -> "true") {
      for (invalid <- Seq(false, true)) {
        val rows = Seq.fill(20)((2024, 1, 1)) :+ ((2024, if (invalid) 13 else 1, 1))
        withParquetTable(rows, "make_date_shuffle") {
          val df = spark
            .table("make_date_shuffle")
            .repartition(8, expr("make_date(_1, _2, _3)"))
            .limit(10)
          val plan = stripAQEPlan(df.queryExecution.executedPlan)
          val exchanges = plan.collect { case exchange: CometShuffleExchangeExec => exchange }
          withClue(plan.treeString) {
            assert(exchanges.size == 1)
            assert(exchanges.head.shuffleType == CometNativeShuffle)
            assert(
              exchanges.head.originalPlan.expressions.exists(_.exists(_.isInstanceOf[MakeDate])))
          }
          if (invalid) {
            // LIMIT cannot hide an invalid key, even when enough valid input rows precede it.
            val (sparkError, cometError) = checkSparkAnswerMaybeThrows(df)
            Seq("Spark" -> sparkError, "Comet" -> cometError).foreach { case (engine, error) =>
              withClue(s"$engine: ") {
                assert(
                  error.exists(e => causeChain(e).exists(_.toString.contains("MonthOfYear"))))
              }
            }
          } else {
            checkSparkAnswer(df)
          }
        }
      }
    }
  }

  test("make_date join input demand reaches local sorts before hash join rewriting") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_FORCE_SHJ.key -> "true",
      CometConf.COMET_FORCE_SHJ_MAX_BUILD_SIZE.key -> "-1") {
      val left = createSparkPlan(spark, "SELECT * FROM VALUES (2024), (2025) AS t(y)")
      val right = createSparkPlan(spark, "SELECT * FROM VALUES (2024) AS t(y)")
      val date = MakeDate(left.output.head, Literal(1), Literal(1), failOnError = false)
      val projection = ProjectExec(Seq(left.output.head, Alias(date, "date")()), left)
      def sort(child: SparkPlan): SparkPlan =
        SortExec(Seq(SortOrder(child.output.head, Ascending)), false, child)
      val join = SortMergeJoinExec(
        Seq(left.output.head),
        Seq(right.output.head),
        Inner,
        None,
        sort(applyCometExecRule(projection)),
        sort(right))
      val converted = applyCometExecRule(join)
      for (result <- Seq(converted, applyCometExecRule(converted))) {
        assert(countOperators(result, classOf[SortMergeJoinExec]) == 1, result.treeString)
        assert(countOperators(result, classOf[CometProjectExec]) == 0, result.treeString)
        assert(countOperators(result, classOf[ProjectExec]) == 1, result.treeString)
        assert(
          new ExtendedExplainInfo()
            .getFallbackReasons(result)
            .exists(_.contains("make_date requires Spark evaluation below JOIN")))
      }
    }
  }

  test("make_date remains deferred across conditional projections") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
      SQLConf.DATETIME_JAVA8API_ENABLED.key -> "false") {
      val selectedDate = java.sql.Date.valueOf("2024-01-01")
      val rows = Seq((2024, 13, None), (2024, 1, Some(selectedDate)))
      withParquetTable(rows, "make_date_deferred") {
        def checkProjection(query: String)(checkSparkRows: Seq[Row] => Unit): SparkPlan = {
          var expected = Seq.empty[Row]
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            expected = sql(query).collect().toSeq
          }
          checkSparkRows(expected)
          val df = sql(query)
          checkCometAnswer(df, expected)
          stripAQEPlan(df.queryExecution.executedPlan)
        }

        def assertDeferredProjection(plan: SparkPlan): Unit = {
          assert(countOperators(plan, classOf[ProjectExec]) == 2, plan.treeString)
          assert(countOperators(plan, classOf[CometProjectExec]) == 0, plan.treeString)
          assert(countOperators(plan, classOf[CometNativeScanExec]) == 1, plan.treeString)
          assert(
            new ExtendedExplainInfo()
              .getFallbackReasons(plan)
              .exists(_.contains("make_date requires Spark evaluation below PROJECT")))
        }

        val expressions = Seq(
          "IF(r < 0, dt, DATE '2024-01-01')",
          "COALESCE(IF(r >= 0, DATE '2024-01-01', NULL), dt)")
        for (aqe <- Seq(false, true); expression <- expressions) {
          withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
            // Reusing nondeterministic r prevents the optimizer from merging the projections.
            val query = s"""
              |SELECT r, $expression AS d FROM (
              |  SELECT rand(7L) AS r, make_date(_1, _2, 1) AS dt FROM make_date_deferred
              |)
              |""".stripMargin
            val plan = checkProjection(query) { expected =>
              assert(expected.map(_.getDate(1)) == Seq.fill(2)(selectedDate))
            }
            withClue(s"aqe=$aqe expression=$expression\n${plan.treeString}") {
              assertDeferredProjection(plan)
            }
          }
        }

        for (aqe <- Seq(false, true)) {
          withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
            val plan = checkProjection("""
              |SELECT r, datediff(nullable_date, dt) AS d, r + 1 AS repeated_r FROM (
              |  SELECT rand(7L) AS r, _3 AS nullable_date, make_date(_1, _2, 1) AS dt
              |  FROM make_date_deferred
              |)
              |""".stripMargin) { expected =>
              assert(expected.size == 2)
              assert(expected.map(_.get(1)).toSet == Set(null, 0))
            }
            assertDeferredProjection(plan)
          }
        }

        // A nullable date can also hide a throwing sibling produced by the lower projection.
        withParquetTable(Seq((None, "bad"), (Some(2024), "1")), "make_date_sibling") {
          withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
            val consumers = Seq(
              "unix_date(dt) + rhs",
              "date_add(dt, rhs)",
              "(dt IS NULL) OR (rhs > 0)",
              "IF(dt IS NULL, 0, rhs)")
            for (expression <- consumers) {
              val plan = checkProjection(s"""
                |SELECT r, $expression AS result, r + 1 AS repeated_r FROM (
                |  SELECT rand(7L) AS r, make_date(_1, 1, 1) AS dt, CAST(_2 AS INT) AS rhs
                |  FROM make_date_sibling
                |)
                |""".stripMargin) { expected =>
                assert(expected.size == 2)
              }
              assertDeferredProjection(plan)
            }

            // Selecting dt forces the date eagerly, but the IF branch can still skip rhs.
            val selected = checkProjection("""
              |SELECT r, dt, IF(dt IS NULL, 0, rhs) AS result, r + 1 AS repeated_r FROM (
              |  SELECT rand(7L) AS r, make_date(_1, 1, 1) AS dt, CAST(_2 AS INT) AS rhs
              |  FROM make_date_sibling
              |)
              |""".stripMargin) { expected =>
              assert(expected.map(row => (row.getDate(1), row.getInt(2))).toSet ==
                Set((null, 0), (selectedDate, 1)))
            }
            assertDeferredProjection(selected)

            // A deferred source column stays native; it cannot expose a skipped error.
            val safeSibling = checkProjection("""
              |SELECT r, dt, IF(dt IS NULL, 0, rhs) AS result, r + 1 AS repeated_r FROM (
              |  SELECT rand(7L) AS r, make_date(_1, 1, 1) AS dt, _1 AS rhs
              |  FROM make_date_sibling
              |)
              |""".stripMargin) { expected =>
              assert(expected.map(row => (row.getDate(1), row.getInt(2))).toSet ==
                Set((null, 0), (selectedDate, 2024)))
            }
            assert(
              countOperators(safeSibling, classOf[CometProjectExec]) == 2,
              safeSibling.treeString)
            assert(
              countOperators(safeSibling, classOf[CometNativeScanExec]) == 1,
              safeSibling.treeString)

            // Repeated r and r2 retain all three projections. The date and skipped cast can
            // originate on either side of the intervening alias, so neither placement is safe.
            val siblingProducers = Seq(
              (
                "cast below date",
                0,
                """
                |SELECT r, r + 1 AS repeated_r, rand(11L) AS r2,
                |       make_date(y, 1, 1) AS dt, raw_rhs AS rhs FROM (
                |  SELECT rand(7L) AS r, _1 AS y, CAST(_2 AS INT) AS raw_rhs
                |  FROM make_date_sibling
                |)
                |""".stripMargin),
              (
                "cast above date",
                1,
                """
                |SELECT r, r + 1 AS repeated_r, rand(11L) AS r2,
                |       original_date AS dt, CAST(s AS INT) AS rhs FROM (
                |  SELECT rand(7L) AS r, make_date(_1, 1, 1) AS original_date, _2 AS s
                |  FROM make_date_sibling
                |)
                |""".stripMargin))
            for ((placement, nativeProjects, producer) <- siblingProducers) {
              val plan = checkProjection(s"""
                |SELECT r, repeated_r, r2, r2 + 1, dt, IF(dt IS NULL, 0, rhs) AS result
                |FROM ($producer)
                |""".stripMargin) { expected =>
                assert(expected.map(row => (row.getDate(4), row.getInt(5))).toSet ==
                  Set((null, 0), (selectedDate, 1)))
              }
              withClue(s"$placement\n${plan.treeString}") {
                assert(countOperators(plan, classOf[ProjectExec]) == 3 - nativeProjects)
                assert(countOperators(plan, classOf[CometProjectExec]) == nativeProjects)
                assert(countOperators(plan, classOf[CometNativeScanExec]) == 1)
              }
            }
          }
        }

        // Each expression references dt once: repeated uses would force Spark to evaluate it
        // eagerly and would not exercise the eager-reference analysis for that parent.
        val eagerExpressions = Seq(
          "year(dt)",
          "month(dt)",
          "date_add(dt, 1)",
          "greatest(dt, DATE '2024-02-01')",
          "hash(dt, 1)",
          "unix_date(dt) + 1")
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          // YEAR evaluates its child only when its enclosing IF selects that branch.
          val maskedYear = checkProjection("""
            |SELECT r, IF(r < 0, YEAR(dt), 1) AS result, r + 1 AS repeated_r FROM (
            |  SELECT rand(7L) AS r, make_date(_1, _2, 1) AS dt FROM make_date_deferred
            |)
            |""".stripMargin) { expected =>
            assert(expected.map(_.getInt(1)) == Seq(1, 1))
          }
          assertDeferredProjection(maskedYear)

          eagerExpressions.foreach { expression =>
            val plan = checkProjection(s"""
              |SELECT r, $expression AS result, IF(r < 0, 0, r + 1) AS repeated_r FROM (
              |  SELECT rand(7L) AS r, make_date(_1, _2, 1) AS dt
              |  FROM make_date_deferred WHERE _2 BETWEEN 1 AND 12
              |)
              |""".stripMargin) { expected =>
              assert(expected.size == 1)
            }
            withClue(s"expression=$expression\n${plan.treeString}") {
              assert(countOperators(plan, classOf[CometProjectExec]) == 2)
              assert(countOperators(plan, classOf[CometNativeScanExec]) == 1)
              assert(
                !new ExtendedExplainInfo()
                  .getFallbackReasons(plan)
                  .exists(_.contains("make_date requires Spark evaluation below PROJECT")))
            }
          }
        }
      }
    }
  }

  test("make_date keeps deferred arguments in their producing projections") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val rows = Seq((None, None, "bad"), (Some(2024), Some(1), "1"))
      withParquetTable(rows, "make_date_arguments") {
        def checkProjection(
            query: String,
            expectedDates: Set[Any],
            dateIndex: Int = 1): SparkPlan = {
          var expected = Seq.empty[Row]
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            expected = sql(query).collect().toSeq
          }
          assert(expected.map(_.get(dateIndex)).toSet == expectedDates)
          val df = sql(query)
          checkCometAnswer(df, expected)
          val plan = stripAQEPlan(df.queryExecution.executedPlan)
          assert(countOperators(plan, classOf[CometNativeScanExec]) == 1, plan.treeString)
          plan
        }

        val cases = Seq(
          ("_1 AS y, CAST(_3 AS INT) AS m", "make_date(y, m, 1)", "", true),
          ("_2 AS m, CAST(_3 AS INT) AS d", "make_date(2024, m, d)", "", true),
          // Reading a column is safe even if make_date skips the argument.
          ("_1 AS y, _2 AS m", "make_date(y, m, 1)", "", false),
          // The year is always evaluated, so its producer can remain native.
          (
            "CAST(_3 AS INT) + 2023 AS y, _2 AS m",
            "make_date(y, m, 1)",
            "WHERE _1 IS NOT NULL",
            false))
        for ((arguments, date, filter, deferred) <- cases) {
          // Repeated r keeps the projections separate without forcing the date arguments.
          val query = s"""
            |SELECT r, unix_date($date) AS d, r + 1 AS repeated_r FROM (
            |  SELECT rand(7L) AS r, $arguments FROM make_date_arguments $filter
            |)
            |""".stripMargin
          val expectedDates: Set[Any] = if (filter.isEmpty) Set(null, 19723) else Set(19723)
          val plan = checkProjection(query, expectedDates)
          withClue(s"arguments=$arguments date=$date\n${plan.treeString}") {
            assert(countOperators(plan, classOf[ProjectExec]) == (if (deferred) 2 else 0))
            assert(countOperators(plan, classOf[CometProjectExec]) == (if (deferred) 0 else 2))
            val reasons = new ExtendedExplainInfo().getFallbackReasons(plan)
            assert(reasons.exists(_.contains("make_date requires Spark evaluation")) == deferred)
          }
        }

        // Broadcast joins can forward a streamed-side expression without evaluating it. The raw
        // string join key does not force the month cast when the upper date sees a null year.
        for (buildLeft <- Seq(false, true); deferred <- Seq(true, false)) {
          val month = if (deferred) "CAST(_3 AS INT)" else "_2"
          val probe = s"""(
            |  SELECT rand(7L) AS r, _1 AS y, _3 AS s, $month AS m
            |  FROM make_date_arguments
            |) p""".stripMargin
          val build = "(SELECT 'bad' AS k UNION ALL SELECT '1' AS k) b"
          val inputs = if (buildLeft) s"$build JOIN $probe" else s"$probe JOIN $build"
          val joined = checkProjection(
            s"""
              |SELECT /*+ BROADCAST(b) */ p.r, unix_date(make_date(p.y, p.m, 1)) AS d,
              |       p.r + 1 AS repeated_r
              |FROM $inputs ON p.s = b.k
              |""".stripMargin,
            Set(null, 19723))
          withClue(s"buildLeft=$buildLeft deferred=$deferred\n${joined.treeString}") {
            // Exclude the literal projections used to construct the broadcast input.
            val sparkProjects = joined.collect {
              case p: ProjectExec if p.output.exists(_.name == "r") => p
            }
            val nativeProjects = joined.collect {
              case p: CometProjectExec if p.output.exists(_.name == "r") => p
            }
            assert(sparkProjects.size == (if (deferred) 2 else 0))
            assert(nativeProjects.size == (if (deferred) 0 else 2))
            assert(
              countOperators(joined, classOf[BroadcastHashJoinExec]) ==
                (if (deferred) 1 else 0))
            assert(
              countOperators(joined, classOf[CometBroadcastHashJoinExec]) ==
                (if (deferred) 0 else 1))
            val buildSides = joined.collect {
              case join: BroadcastHashJoinExec => join.buildSide
              case join: CometBroadcastHashJoinExec => join.buildSide
            }
            assert(buildSides == Seq(if (buildLeft) BuildLeft else BuildRight))
          }
        }

        // A join key forces the cast before the date can skip its month argument.
        val keyRequired = checkProjection(
          """
            |SELECT /*+ BROADCAST(b) */ p.r, unix_date(make_date(p.y, p.m, 1)), p.r + 1 FROM (
            |  SELECT rand(7L) AS r, _1 AS y,
            |         CAST(COALESCE(CAST(_2 AS STRING), '1') AS INT) AS m
            |  FROM make_date_arguments
            |) p JOIN (SELECT 1 AS k UNION ALL SELECT 2 AS k) b ON p.m = b.k
            |""".stripMargin,
          Set(null, 19723))
        assert(
          keyRequired.collect {
            case p: CometProjectExec if p.output.exists(_.name == "r") => p
          }.size == 2,
          keyRequired.treeString)
        assert(
          countOperators(keyRequired, classOf[CometBroadcastHashJoinExec]) == 1,
          keyRequired.treeString)

        // Repeated r and r2 retain an intervening projection that only aliases the month.
        val aliased = checkProjection(
          """
            |SELECT r, repeated_r, r2, r2 + 1, unix_date(make_date(y, month_alias, 1)) FROM (
            |  SELECT r, r + 1 AS repeated_r, rand(11L) AS r2, y, m AS month_alias FROM (
            |    SELECT rand(7L) AS r, _1 AS y, CAST(_3 AS INT) AS m FROM make_date_arguments
            |  )
            |)
            |""".stripMargin,
          Set(null, 19723),
          dateIndex = 4)
        assert(countOperators(aliased, classOf[ProjectExec]) == 3, aliased.treeString)
        assert(countOperators(aliased, classOf[CometProjectExec]) == 0, aliased.treeString)
      }
    }
  }

  test("make_date arguments remain deferred for unmatched broadcast join rows") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withParquetTable(Seq((1, "2024"), (2, "bad")), "make_date_join_probe") {
        withParquetTable(Seq(Tuple1(1)), "make_date_join_build") {
          def checkJoined(query: String, dateIndex: Int, projects: Int, scans: Int): SparkPlan = {
            var expected = Seq.empty[Row]
            withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
              expected = sql(query).collect().toSeq
            }
            assert(expected.size == 1 && expected.head.getInt(dateIndex) == 19723)
            val df = sql(query)
            checkCometAnswer(df, expected)
            val plan = stripAQEPlan(df.queryExecution.executedPlan)
            withClue(plan.treeString) {
              assert(plan.collect {
                case p: ProjectExec if p.output.exists(_.name == "r") => p
              }.size == projects)
              assert(plan.collect {
                case p: CometProjectExec if p.output.exists(_.name == "r") => p
              }.isEmpty)
              assert(countOperators(plan, classOf[CometNativeScanExec]) == scans)
            }
            plan
          }

          val probe = """(
            |  SELECT _1 AS k, rand(7L) AS r, CAST(_2 AS INT) AS y
            |  FROM make_date_join_probe
            |) p""".stripMargin
          val build = "make_date_join_build b"
          for (buildLeft <- Seq(false, true)) {
            val inputs = if (buildLeft) s"$build JOIN $probe" else s"$probe JOIN $build"
            val plan = checkJoined(
              s"""
                |SELECT /*+ BROADCAST(b) */ p.r, p.r + 1, unix_date(make_date(p.y, 1, 1))
                |FROM $inputs ON p.k = b._1
                |""".stripMargin,
              dateIndex = 2,
              projects = 2,
              scans = 2)
            withClue(s"buildLeft=$buildLeft\n${plan.treeString}") {
              val joins = plan.collect { case join: BroadcastHashJoinExec => join }
              assert(joins.map(_.buildSide) == Seq(if (buildLeft) BuildLeft else BuildRight))
            }
          }

          // Repeated r2 retains the alias projection above the join that discards the bad row.
          val aliased = checkJoined(
            s"""
              |SELECT r, repeated_r, r2, r2 + 1, unix_date(make_date(date_year, 1, 1)) FROM (
              |  SELECT /*+ BROADCAST(b) */ p.r, p.r + 1 AS repeated_r, rand(11L) AS r2,
              |         p.y AS date_year FROM $probe JOIN $build ON p.k = b._1
              |)
              |""".stripMargin,
            dateIndex = 4,
            projects = 3,
            scans = 2)
          assert(countOperators(aliased, classOf[BroadcastHashJoinExec]) == 1, aliased.treeString)

          // The outer join requires y only after the inner join accepts a row. Null-safe equality
          // keeps an inferred isNotNull(y) filter from evaluating the cast before either join.
          // Spark also inserts a projection between the joins to drop the inner join keys.
          val nested = checkJoined(
            s"""
              |SELECT /*+ BROADCAST(b, c) */ p.r, p.r + 1, unix_date(make_date(p.y, 1, 1))
              |FROM $probe JOIN $build ON p.k = b._1
              |JOIN (SELECT _1 + 2023 AS y FROM make_date_join_build) c ON p.y <=> c.y
              |""".stripMargin,
            dateIndex = 2,
            projects = 3,
            scans = 3)
          assert(countOperators(nested, classOf[BroadcastHashJoinExec]) == 2, nested.treeString)
        }
      }
    }
  }

  test("make_date single-argument count evaluates its nullable argument eagerly") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val rows = Seq((0, Some(2024), 1), (0, None, 1), (1, Some(2025), 1))
      withParquetTable(rows, "make_date_count") {
        val query = "SELECT _1, count(make_date(_2, _3, 1)), sum(_3) " +
          "FROM make_date_count GROUP BY _1"
        val df = sql(query)
        checkCometAnswer(df, Seq(Row(0, 1L, 2L), Row(1, 1L, 1L)))
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        assert(countOperators(plan, classOf[HashAggregateExec]) == 0, plan.treeString)
        assert(countOperators(plan, classOf[CometHashAggregateExec]) == 2, plan.treeString)

        // A physical plan bypasses the optimizer's COUNT(non-nullable) -> COUNT(1) rewrite.
        // Spark does not evaluate this invalid date, so it must not reach a native update.
        val partial = createSparkPlan(spark, query).collectFirst {
          case aggregate: HashAggregateExec
              if aggregate.aggregateExpressions.forall(_.mode == Partial) =>
            aggregate
        }.get
        val skippedDate = MakeDate(Literal(2024), Literal(13), Literal(1), failOnError = true)
        assert(!skippedDate.nullable)
        val nonNullableCount =
          partial.copy(aggregateExpressions = partial.aggregateExpressions.map {
            case expression if expression.aggregateFunction.isInstanceOf[Count] =>
              expression.copy(aggregateFunction = Count(Seq(skippedDate)))
            case expression => expression
          })
        assert(MakeDateEvaluation.reason(MakeDateEvaluation.preserve(nonNullableCount)).isDefined)
      }
    }
  }

  test("make_date aggregate arguments retain Spark execution") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.USE_OBJECT_HASH_AGG.key -> "true") {
      val rows = Seq((2024, 1, Some(1)), (2024, 13, None), (2025, 1, Some(1)))
      withParquetTable(rows, "make_date_aggregate") {
        val cases = Seq(
          (
            "unix_date(min(make_date(_1, _2, 1)) FILTER (WHERE _2 <= 12))",
            classOf[HashAggregateExec],
            Row(19723)),
          (
            "sort_array(collect_list(unix_date(make_date(_1, _2, 1))) " +
              "FILTER (WHERE _2 <= 12))",
            classOf[ObjectHashAggregateExec],
            Row(Seq(19723, 20089))),
          (
            "count(make_date(_1, _2, 1)) FILTER (WHERE _2 <= 12)",
            classOf[HashAggregateExec],
            Row(2L)),
          ("count(_3, make_date(_1, _2, 1))", classOf[HashAggregateExec], Row(2L)))
        cases.foreach { case (expression, aggregateClass, expected) =>
          val df = sql(s"SELECT $expression FROM make_date_aggregate")
          val plan = stripAQEPlan(df.queryExecution.executedPlan)
          assert(
            plan.exists { op =>
              aggregateClass.isInstance(op) &&
              op.expressions.exists(_.exists(_.isInstanceOf[MakeDate]))
            },
            plan.treeString)
          val name = aggregateClass.getSimpleName.stripSuffix("Exec")
          assert(
            new ExtendedExplainInfo()
              .getFallbackReasons(plan)
              .exists(_.contains(s"make_date requires Spark evaluation in $name")),
            plan.treeString)
          checkCometAnswer(df, Seq(expected))
        }
      }
    }
  }

  test("make_date top-k respects input ordering and deferred output projection") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      // Ordered top-k takes one row per partition before the final selection.
      SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true") {
      val template = createSparkPlan(
        spark,
        "SELECT * FROM VALUES (1), (13) AS t(m) ORDER BY m LIMIT 1").collectFirst {
        case topK: TakeOrderedAndProjectExec => topK
      }.get
      val input = template.child
      val date = Alias(MakeDate(Literal(2024), input.output.head, Literal(1), true), "date")()
      def project(child: SparkPlan): ProjectExec = ProjectExec(input.output :+ date, child)
      val order = template.sortOrder
      for (sorted <- Seq(false, true)) {
        val child = project(if (sorted) SortExec(order, false, input) else input)
        assert(SortOrder.orderingSatisfies(child.outputOrdering, order) == sorted)
        val result = applyCometExecRule(template.copy(projectList = child.output, child = child))
        if (sorted) {
          assert(result.isInstanceOf[TakeOrderedAndProjectExec], result.treeString)
          assert(countOperators(result, classOf[CometProjectExec]) == 0, result.treeString)
          assert(
            new ExtendedExplainInfo()
              .getFallbackReasons(result)
              .exists(_.contains("make_date requires Spark evaluation below LIMIT")))
          assert(
            result.executeCollect().map(row => (row.getInt(0), row.getInt(1))).toSeq ==
              Seq((1, 19723)))
        } else {
          assert(result.isInstanceOf[CometTakeOrderedAndProjectExec], result.treeString)
          assert(countOperators(result, classOf[CometProjectExec]) == 1, result.treeString)
        }
      }
      val deferred = applyCometExecRule(template.copy(projectList = input.output :+ date))
      assert(deferred.isInstanceOf[TakeOrderedAndProjectExec], deferred.treeString)
      assert(
        new ExtendedExplainInfo()
          .getFallbackReasons(deferred)
          .exists(_.contains("make_date requires Spark evaluation in TakeOrderedAndProject")))
      assert(
        deferred.executeCollect().map(row => (row.getInt(0), row.getInt(1))).toSeq ==
          Seq((1, 19723)))
    }
  }

  test("make_date below a window group limit retains Spark evaluation") {
    val windowLimitClass = ShimCometWindowGroupLimit.windowGroupLimitClass
    assume(windowLimitClass.isDefined, "WindowGroupLimit requires Spark 3.5+")
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_WINDOW_GROUP_LIMIT_ENABLED.key -> "true") {
      val plan = createSparkPlan(
        spark,
        """
        |SELECT p, y FROM (
        |  SELECT p, y, row_number() OVER (PARTITION BY p ORDER BY y) AS rn
        |  FROM VALUES (0, 2024), (0, 2025) AS t(p, y)
        |) WHERE rn <= 1
        |""".stripMargin)
      val limit = plan.collectFirst { case op if windowLimitClass.get.isInstance(op) => op }.get
      val input = limit.children.head
      val year = input.output.find(_.name == "y").get
      val date = Alias(MakeDate(year, Literal(1), Literal(1), false), "date")()
      val projected = ProjectExec(input.output :+ date, input)
      val result = applyCometExecRule(limit.withNewChildren(Seq(projected)))
      assert(windowLimitClass.get.isInstance(result), result.treeString)
      assert(
        result.exists {
          case p: ProjectExec => p.projectList.exists(_.exists(_.isInstanceOf[MakeDate]))
          case _ => false
        },
        result.treeString)
      assert(
        new ExtendedExplainInfo()
          .getFallbackReasons(result)
          .exists(_.contains("make_date requires Spark evaluation below LIMIT")),
        result.treeString)
    }
  }

  test("make_date operator opt-out retains invalid-date errors and expression guards") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.ANSI_ENABLED.key -> "true",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true") {
      val input = createSparkPlan(spark, "SELECT * FROM VALUES (2024) AS t(y)")
      val date = Alias(MakeDate(input.output.head, Literal(1), Literal(1), true), "date")()
      // Tag the Spark plan before disabling the setting.
      val guarded = MakeDateEvaluation.preserve(LocalLimitExec(1, ProjectExec(Seq(date), input)))
      assert(countOperators(guarded, classOf[ProjectExec]) == 1, guarded.treeString)
      assert(MakeDateEvaluation.reason(guarded).isDefined, guarded.treeString)
      withSQLConf(CometConf.COMET_EXEC_MAKE_DATE_OPERATOR_FALLBACK_ENABLED.key -> "false") {
        assert(MakeDateEvaluation.reason(guarded).isEmpty)
        val converted = applyCometExecRule(guarded)
        assert(countOperators(converted, classOf[CometProjectExec]) == 1, converted.treeString)
        assert(countOperators(converted, classOf[CometLocalLimitExec]) == 1, converted.treeString)
        withParquetTable(Seq((2024, 1, 1), (2025, 1, 1)), "make_date_opt_out") {
          checkSparkAnswerAndOperator(
            sql("SELECT unix_date(make_date(_1, _2, _3)) FROM make_date_opt_out LIMIT 1"),
            Seq(classOf[CometProjectExec], classOf[CometCollectLimitExec]))
          val (_, joinPlan) = checkSparkAnswerAndOperator(
            sql("""
              |SELECT /*+ BROADCAST(b) */ unix_date(p.d), b._1 FROM (
              |  SELECT _1 AS y, make_date(_1, _2, _3) AS d FROM make_date_opt_out
              |) p JOIN make_date_opt_out b ON p.y = b._1
              |""".stripMargin),
            Seq(classOf[CometProjectExec], classOf[CometBroadcastHashJoinExec]))
          val join = joinPlan.collectFirst { case j: CometBroadcastHashJoinExec => j }.get
          assert(
            join.left.exists {
              case p: CometProjectExec =>
                p.originalPlan.expressions.exists(_.exists(_.isInstanceOf[MakeDate]))
              case _ => false
            },
            joinPlan.treeString)

          val invalidDate = sql("SELECT make_date(_1, _2 + 12, _3) FROM make_date_opt_out")
          checkCometOperators(stripAQEPlan(invalidDate.queryExecution.executedPlan))
          val (sparkError, cometError) = checkSparkAnswerMaybeThrows(invalidDate)
          Seq("Spark" -> sparkError, "Comet" -> cometError).foreach { case (engine, error) =>
            withClue(s"$engine: ") {
              assert(error.exists(e => causeChain(e).exists(_.toString.contains("MonthOfYear"))))
            }
          }
        }

        // A nullable left operand must still skip a throwing date on the right of arithmetic.
        withParquetTable(Seq((None, 13), (Some(1), 1)), "make_date_opt_out_expression") {
          val df = sql("""
            |SELECT _1 + unix_date(make_date(2024, _2, 1)) FROM make_date_opt_out_expression
            |""".stripMargin)
          val plan = stripAQEPlan(df.queryExecution.executedPlan)
          assert(countOperators(plan, classOf[ProjectExec]) == 1, plan.treeString)
          assert(
            new ExtendedExplainInfo()
              .getFallbackReasons(plan)
              .exists(_.contains("make_date requires Spark evaluation under Add")),
            plan.treeString)
          checkCometAnswer(df, Seq(Row(null), Row(19724)))
        }
      }
    }
  }

  test("make_date fallback reuses native broadcast rows in Spark's broadcast representation") {
    for (nestedLoop <- Seq(false, true)) {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        CometConf.COMET_EXEC_RANGE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val input = createSparkPlan(spark, "SELECT * FROM VALUES (2024) AS t(y)")
        val date = MakeDate(input.output.head, Literal(1), Literal(1), failOnError = false)
        val probe =
          applyCometExecRule(ProjectExec(Seq(input.output.head, Alias(date, "date")()), input))
        val build = applyCometExecRule(
          createSparkPlan(spark, "SELECT CAST(id AS INT) AS y FROM range(2024, 2028, 1, 2)"))
        assert(build.isInstanceOf[CometNativeExec], build.treeString)
        val key = build.output.head
        val mode =
          if (nestedLoop) IdentityBroadcastMode
          else HashedRelationBroadcastMode(Seq(BoundReference(0, key.dataType, key.nullable)))
        val sparkExchange = BroadcastExchangeExec(mode, build)
        val exchange = CometBroadcastExchangeExec(sparkExchange, build.output, mode, build)
        val reused = ReusedExchangeExec(build.output.map(_.newInstance()), exchange)
        val inputs = Seq[SparkPlan](
          exchange,
          reused,
          BroadcastQueryStageExec(0, exchange, exchange.canonicalized),
          BroadcastQueryStageExec(1, reused, reused.canonicalized))
        inputs.zipWithIndex.foreach { case (broadcast, index) =>
          // Model a native plan supplied by an earlier planning pass; its parent must restore it.
          val native = if (nestedLoop) {
            val original = BroadcastNestedLoopJoinExec(probe, broadcast, BuildRight, Inner, None)
            CometBroadcastNestedLoopJoinExec.createExec(
              OperatorOuterClass.Operator.getDefaultInstance,
              original)
          } else {
            val original = BroadcastHashJoinExec(
              Seq(probe.output.head),
              Seq(broadcast.output.head),
              Inner,
              BuildRight,
              None,
              probe,
              broadcast)
            CometBroadcastHashJoinExec.createExec(
              OperatorOuterClass.Operator.getDefaultInstance,
              original)
          }
          val converted = applyCometExecRule(native)
          for (result <- Seq(converted, applyCometExecRule(converted))) {
            val join = result.asInstanceOf[BaseJoinExec]
            assert(join.isInstanceOf[BroadcastNestedLoopJoinExec] == nestedLoop)
            val restored = join.right.asInstanceOf[CometColumnarToRowExec]
            assert(restored.output == broadcast.output)
            assert(restored.child eq broadcast)
            assert(countOperators(join.left, classOf[CometProjectExec]) == 0)
          }
          if (index == 0) {
            val restored = converted
              .asInstanceOf[BaseJoinExec]
              .right
              .asInstanceOf[CometColumnarToRowExec]
            val relation = restored.executeBroadcast[Any]().value
            val rows = if (nestedLoop) {
              relation.asInstanceOf[Array[InternalRow]].toVector
            } else {
              // HashedRelation is private[execution]; inspect Spark's broadcast through its keys/get
              // methods.
              val keys = PrivateMethod[Iterator[InternalRow]](Symbol("keys"))
              val get = PrivateMethod[Iterator[InternalRow]](Symbol("get"))
              val hashed = relation.asInstanceOf[AnyRef]
              (hashed invokePrivate keys()).flatMap { key =>
                (hashed invokePrivate get(key)).map(_.copy()).toVector
              }.toVector
            }
            // Restore the complete build relation without re-executing its native input.
            withClue(s"nestedLoop=$nestedLoop: ") {
              assert(rows.map(_.getInt(0)).sorted == Seq(2024, 2025, 2026, 2027))
            }
          }
        }
      }
    }
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

  test("placeholder cleanup preserves AQE stages behind nested and leaf wrappers") {
    val input = createSparkPlan(spark, "SELECT * FROM VALUES (1), (2) AS t(id)")
    val exchange = ShuffleExchangeExec(HashPartitioning(input.output, 2), input)
    val stage = ShuffleQueryStageExec(0, exchange, exchange.canonicalized)
    val nativeOp = OperatorOuterClass.Operator.getDefaultInstance
    def sink(child: SparkPlan): SparkPlan = CometSinkPlaceHolder(nativeOp, child, child)
    def scan(child: SparkPlan): SparkPlan = CometScanWrapper(nativeOp, child)
    val specs = Seq(CoalescedPartitionSpec(0, 2, Some(16L)))

    for {
      child <- Seq(sink(sink(stage)), sink(scan(stage)), scan(sink(stage)), scan(scan(stage)))
      wrapRead <- Seq(false, true)
    } {
      val read = AQEShuffleReadExec(child, specs)
      val input = if (wrapRead) scan(read) else read
      val cleaned = CometExecRule.removePlaceholders(ProjectExec(read.output, input))
      val cleanedRead = cleaned.children.head.asInstanceOf[AQEShuffleReadExec]
      assert(cleanedRead.child eq stage)
      assert(cleanedRead.partitionSpecs eq specs)
      assert(!cleaned.exists {
        case _: CometSinkPlaceHolder | _: CometScanWrapper => true
        case _ => false
      })
      assert(CometExecRule.removePlaceholders(cleaned) eq cleaned)
    }
  }

  test("aggregate fallback removes nested placeholders around an existing AQE stage") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_SHUFFLE_NATIVE_HASH_PARTITIONING_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_ENABLED.key -> "true") {
      withTempView("test_data") {
        createTestDataFrame.createOrReplaceTempView("test_data")
        val original =
          createSparkPlan(
            spark,
            "SELECT AVG(CAST(id AS DECIMAL(20, 2))) FROM test_data GROUP BY (id % 3)")
        val partial = original.collectFirst {
          case agg: HashAggregateExec if agg.aggregateExpressions.forall(_.mode == Partial) => agg
        }.get
        // Keep a previously converted input stage below the aggregate being planned. Its
        // output attributes must match the Partial's original input, including grouping keys.
        val exchange =
          applyCometExecRule(ShuffleExchangeExec(RoundRobinPartitioning(4), partial.child))
            .asInstanceOf[CometShuffleExchangeExec]
        val stage = ShuffleQueryStageExec(0, exchange, exchange.canonicalized)
        val staged = original.transformUp {
          case agg: HashAggregateExec if agg eq partial => agg.copy(child = stage)
        }

        // The Partial initially converts, but its upper hash shuffle cannot. Repair restores
        // Spark's decimal AVG buffer producer and revisits the wrapped round-robin input stage.
        val result = applyCometExecRule(staged)
        assert(result.collect { case agg: HashAggregateExec => agg }.size == 2)
        assert(!result.exists(_.isInstanceOf[CometHashAggregateExec]))
        val restoredPartial = result.collectFirst {
          case agg: HashAggregateExec if agg.aggregateExpressions.forall(_.mode == Partial) =>
            agg
        }.get
        assert(restoredPartial.getTagValue(CometExecRule.COMET_UNSAFE_PARTIAL).isDefined)
        assert(!result.exists {
          case _: CometSinkPlaceHolder | _: CometScanWrapper => true
          case _ => false
        })
        assert(restoredPartial.child eq stage)
        assert(result.collect { case s: ShuffleQueryStageExec => s } == Seq(stage))
      }
    }
  }

  test("aggregate fallback executes after an AQE round-robin repartition") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "4",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_SHUFFLE_NATIVE_HASH_PARTITIONING_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_ENABLED.key -> "true") {
      val data = Seq((1, Some(10)), (1, Some(20)), (2, None), (2, Some(40)), (3, None))
      withParquetTable(data, "test_data") {
        val df = sql("""
            |SELECT k, AVG(CAST(v AS DECIMAL(20, 2))) FROM
            |  (SELECT /*+ REPARTITION(4) */ _1 AS k, _2 AS v FROM test_data)
            |GROUP BY k
            |""".stripMargin)
        QueryTest.checkAnswer(
          df,
          Seq(Row(1, BigDecimal("15.000000")), Row(2, BigDecimal("40.000000")), Row(3, null)))
        val plan = df.queryExecution.executedPlan
        assert(plan.asInstanceOf[AdaptiveSparkPlanExec].isFinalPlan)
        val nativeShuffles = collect(plan) { case s: CometShuffleExchangeExec => s }
        assert(nativeShuffles.size == 1)
        assert(nativeShuffles.head.shuffleType == CometNativeShuffle)
        assert(nativeShuffles.head.outputPartitioning == RoundRobinPartitioning(4))
        val aggregates = collect(plan) { case a: HashAggregateExec => a }
        assert(aggregates.size == 2)
        assert(collect(plan) { case a: CometHashAggregateExec => a }.isEmpty)
        val partial = aggregates.find(_.aggregateExpressions.forall(_.mode == Partial)).get
        assert(partial.getTagValue(CometExecRule.COMET_UNSAFE_PARTIAL).isDefined)
        assert(collect(plan) {
          case p: CometSinkPlaceHolder => p
          case p: CometScanWrapper => p
        }.isEmpty)
      }
    }
  }

  test("CometExecRule preserves the current direct AQE logical link") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_RANGE_ENABLED.key -> "true") {
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

  test("collect limit preserves filter column pruning for column-only projections") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withParquetTable(Seq((0, "a", 1), (1, "b", 2), (2, "c", 3)), "limit_columns") {
        for (projection <- Seq("_1", "_1 AS value", "_3 AS a, _1 AS b, _3 AS c")) {
          val df = sql(s"""SELECT $projection FROM limit_columns
              |WHERE _1 % 2 = 0 AND length(_2) > 0 AND _3 > 0 LIMIT 1""".stripMargin)
          val (_, plan) = checkSparkAnswerAndOperator(
            df,
            includeClasses = Seq(classOf[CometProjectExec], classOf[CometFilterExec]))
          val project = plan.collectFirst { case p: CometProjectExec => p }.get
          // Native filter output pruning requires a direct Project -> Filter edge, including
          // aliases, reordered columns, and duplicates that outnumber the distinct inputs.
          assert(project.child.isInstanceOf[CometFilterExec])
          assert(project.child.output.size > project.references.size)
          val serialized =
            OperatorOuterClass.Operator.parseFrom(project.serializedPlanOpt.plan.get)
          assert(serialized.getChildren(0).hasFilter)
        }
      }
    }
  }

  test("collect limit projection input cap respects disabled native local limits") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_RANGE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_LOCAL_LIMIT_ENABLED.key -> "false") {
      val original =
        createSparkPlan(spark, "SELECT id + 1 AS value FROM range(0, 10, 1, 1) LIMIT 2")
      val result = applyCometExecRule(original)
      assert(result.collect { case p: CometProjectExec => p }.size == 1)
      assert(!result.exists(_.isInstanceOf[ProjectExec]))
      assert(!result.exists(_.isInstanceOf[CometLocalLimitExec]))
      assert(result.executeCollect().map(_.getLong(0)).toSeq == Seq(1L, 2L))
    }
  }

  test("collect limit projection input cap preserves AQE links and repeated planning") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_RANGE_ENABLED.key -> "true") {
      val original = createSparkPlan(spark, "SELECT id + 1 AS value FROM range(0, 10, 1, 1)")
      val project = applyCometExecRule(original).asInstanceOf[CometProjectExec]
      val stage = LogicalQueryStage(project.originalPlan.logicalLink.get, project)
      val reused = spark.sessionState.planner.plan(stage).next()
      assert(reused eq project)

      val result = applyCometExecRule(CollectLimitExec(2, reused))
      val bounded = result.collectFirst { case p: CometProjectExec => p }.get
      assert(bounded.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).exists(_ eq stage))
      assert(bounded.child.asInstanceOf[CometLocalLimitExec].limit == 2)
      assert(bounded.nativeOp.getChildren(0).getLimit.getLimit == 2)
      val serialized = OperatorOuterClass.Operator.parseFrom(bounded.serializedPlanOpt.plan.get)
      assert(serialized.getChildren(0).getLimit.getLimit == 2)

      // AQE may wrap a previously converted projection in a newly planned collect limit.
      val repeated = applyCometExecRule(CollectLimitExec(2, bounded))
      val again = repeated.collectFirst { case p: CometProjectExec => p }.get
      assert(again.nativeOp eq bounded.nativeOp)
      assert(again.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).exists(_ eq stage))
      assert(repeated.collect { case cap: CometLocalLimitExec => cap }.size == 1)
    }
  }

  test("collect limit projection input cap does not cross a filter or sort") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_RANGE_ENABLED.key -> "true") {
      Seq(false, true).foreach { sorted =>
        val original = createSparkPlan(spark, "SELECT id + 1 AS value FROM range(0, 10, 1, 1)")
        val project = applyCometExecRule(original).asInstanceOf[CometProjectExec]
        val child = if (sorted) {
          SortExec(
            Seq(SortOrder(project.output.head, Descending)),
            global = false,
            child = project)
        } else {
          FilterExec(GreaterThan(project.output.head, Literal(2L)), project)
        }
        val result = applyCometExecRule(CollectLimitExec(1, child))
        assert(!result.exists(_.isInstanceOf[CometLocalLimitExec]))
        val expected = if (sorted) 10L else 3L
        assert(result.executeCollect().map(_.getLong(0)).toSeq == Seq(expected))
      }
    }
  }

  test("CometExecRule repairs ordinary and inherited logical links from the original plan") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_RANGE_ENABLED.key -> "true") {
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

  private def serializedLeafKinds(op: CometNativeExec): Seq[String] =
    nativeLeaves(OperatorOuterClass.Operator.parseFrom(op.serializedPlanOpt.plan.get))
      .map(_.getOpStructCase.name)

  private def withAdaptiveAggregateConf(f: => Unit): Unit =
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_RANGE_ENABLED.key -> "true")(f)

  test("CometExecRule points an AQE-reused aggregate's stale Scan at its ShuffleScan stage") {
    withAdaptiveAggregateConf {
      val reused = createAdaptiveAggregate()
      assert(nativeLeaves(reused.nativeOp).map(_.getOpStructCase.name) == Seq("SCAN"))

      val refreshed = applyCometExecRule(reused).asInstanceOf[CometHashAggregateExec]
      assert(nativeLeaves(refreshed.nativeOp).map(_.getOpStructCase.name) == Seq("SHUFFLE_SCAN"))
      assert(serializedLeafKinds(refreshed) == Seq("SHUFFLE_SCAN"))
      assert(refreshed.child.isInstanceOf[ShuffleQueryStageExec])
      assert(refreshed.getTagValue(CometExplainInfo.EXTENSION_INFO).isEmpty)

      // A second pass finds nothing stale and keeps the node as it is.
      val again = applyCometExecRule(refreshed).asInstanceOf[CometHashAggregateExec]
      assert(again.nativeOp eq refreshed.nativeOp)
      assert(again.serializedPlanOpt.plan.get sameElements refreshed.serializedPlanOpt.plan.get)
    }
  }

  test("CometExecRule keeps an AQE-reused aggregate's Scan with shuffle direct read off") {
    withAdaptiveAggregateConf {
      withSQLConf(CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> "false") {
        val reused = createAdaptiveAggregate()
        val result = applyCometExecRule(reused).asInstanceOf[CometHashAggregateExec]
        assert(result.nativeOp eq reused.nativeOp)
        assert(serializedLeafKinds(result) == Seq("SCAN"))
        assert(result.getTagValue(CometExplainInfo.EXTENSION_INFO).isEmpty)
      }
    }
  }

  test("CometExecRule keeps an AQE-reused aggregate's Scan while AQE replans") {
    withAdaptiveAggregateConf {
      // AQE adopts a replan that differs from its current plan, so the stale Scan is left for
      // the preparation of the stage or final plan that the aggregate ends up in.
      val reused = createAdaptiveAggregate()
      val result = CometExecRule(spark, queryStagePrep = true)
        .apply(reused)
        .asInstanceOf[CometHashAggregateExec]
      assert(result == reused)
      assert(result.nativeOp eq reused.nativeOp)
      assert(serializedLeafKinds(result) == Seq("SCAN"))
    }
  }

  test("CometExecRule points a stale Scan at the ShuffleScan stage behind an AQE shuffle read") {
    withAdaptiveAggregateConf {
      // AQE can put a read between the stage and the aggregate before preparing the stage that
      // the aggregate is in. The ShuffleScan then reads the partitions that the read specifies.
      val reused = createAdaptiveAggregate()
      val read = AQEShuffleReadExec(reused.child, Seq(CoalescedPartitionSpec(0, 2, Some(16L))))
      val refreshed = applyCometExecRule(reused.withNewChildren(Seq(read)))
        .asInstanceOf[CometHashAggregateExec]
      assert(nativeLeaves(refreshed.nativeOp).map(_.getOpStructCase.name) == Seq("SHUFFLE_SCAN"))
      assert(serializedLeafKinds(refreshed) == Seq("SHUFFLE_SCAN"))
      val refreshedRead = refreshed.child.asInstanceOf[AQEShuffleReadExec]
      assert(refreshedRead.child eq reused.child)
      assert(refreshed.getTagValue(CometExplainInfo.EXTENSION_INFO).isEmpty)
    }
  }

  test("CometExecRule keeps an AQE-reused aggregate's Scan and says so when its leaves differ") {
    withAdaptiveAggregateConf {
      val stringType = QueryPlanSerde.serializeDataType(DataTypes.StringType).get
      def retype(op: OperatorOuterClass.Operator): OperatorOuterClass.Operator =
        if (op.hasScan) {
          val fields = Seq.fill(op.getScan.getFieldsCount)(stringType)
          op.toBuilder
            .setScan(op.getScan.toBuilder.clearFields().addAllFields(fields.asJava))
            .build()
        } else {
          op.toBuilder
            .clearChildren()
            .addAllChildren(op.getChildrenList.asScala.map(retype).asJava)
            .build()
        }
      // A second Scan child gives the plan more leaves than the aggregate has inputs.
      def addScanLeaf(op: OperatorOuterClass.Operator): OperatorOuterClass.Operator =
        op.toBuilder.addChildren(nativeLeaves(op).head).build()

      Seq("field types" -> (retype _), "leaf count" -> (addScanLeaf _)).foreach {
        case (mismatch, change) =>
          withClue(s"$mismatch: ") {
            val reused = createAdaptiveAggregate()
            val mismatched = reused.copy(nativeOp = change(reused.nativeOp))

            val result = applyCometExecRule(mismatched).asInstanceOf[CometHashAggregateExec]
            assert(result.nativeOp eq mismatched.nativeOp)
            assert(nativeLeaves(result.nativeOp).forall(_.hasScan))
            // The node stays native, with an info message rather than a fallback reason.
            val info = CometExecRule.STALE_SCAN_KEPT
            assert(result.getTagValue(CometExplainInfo.EXTENSION_INFO).contains(Set(info)))
            assert(!CometSparkSessionExtensions.hasFallbackReason(result))
            val explain = new ExtendedExplainInfo().generateVerboseInfo(result)
            assert(explain.contains(s"CometHashAggregate [COMET-INFO: $info]"), explain)
          }
      }
    }
  }

  /**
   * Runs `f` with AQE DPP on and the fact and dim views of the DPP broadcast link tests, with
   * shuffle direct read set to `directRead`.
   */
  private def withDppLinkViews(directRead: Boolean)(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> directRead.toString) {
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
          f
        }
      }
    }
  }

  /**
   * Runs the DPP broadcast link query, checks its answer and that the join and the pruning
   * broadcast are native, and returns the executed plan.
   */
  private def checkDppLinkQuery(): SparkPlan = {
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
    plan
  }

  /**
   * Runs `f` while observing each AQE replan of a broadcast root linked to a logical query stage.
   * Before Comet prepares the root, `capture` takes the root and its stage. Afterwards `check`
   * gets that captured value, the prepared root and the stage's logical plan.
   */
  private def withReplanObserver[T](
      capture: (CometBroadcastExchangeExec, LogicalQueryStage) => T)(
      check: (T, CometBroadcastExchangeExec, LogicalPlan) => Unit)(f: => Unit): Unit = {
    val pending = new ThreadLocal[List[Option[(T, LogicalPlan)]]] {
      override def initialValue(): List[Option[(T, LogicalPlan)]] = Nil
    }
    beforeCometPreparation = plan => {
      val replan = plan match {
        case broadcast: CometBroadcastExchangeExec =>
          broadcast.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).collect {
            case stage: LogicalQueryStage => (capture(broadcast, stage), stage.logicalPlan)
          }
        case _ => None
      }
      pending.set(replan :: pending.get())
    }
    afterCometPreparation = plan => {
      val replan = pending.get().head
      val remaining = pending.get().tail
      if (remaining.isEmpty) pending.remove() else pending.set(remaining)
      replan.foreach { case (captured, logicalPlan) =>
        check(captured, plan.asInstanceOf[CometBroadcastExchangeExec], logicalPlan)
      }
    }
    try {
      f
    } finally {
      beforeCometPreparation = _ => ()
      afterCometPreparation = _ => ()
    }
  }

  test("AQE DPP broadcast roots retain temporary logical links after an unchanged replan") {
    assume(isSpark35Plus, "Native AQE DPP requires Spark 3.5+")
    // With direct read, the aggregate under the broadcast reads the dim side's shuffle stage
    // through a ShuffleScan only once its stage is prepared, so each replan stays unchanged.
    Seq(false, true).foreach { directRead =>
      withDppLinkViews(directRead)(checkUnchangedDppReplans())
    }
  }

  private def checkUnchangedDppReplans(): Unit = {
    val observed = new ConcurrentLinkedQueue[(CometBroadcastExchangeExec, LogicalPlan)]()
    val tempTag = AdaptiveSparkPlanExec.TEMP_LOGICAL_PLAN_TAG
    val costEvaluator = SimpleCostEvaluator(forceOptimizeSkewedJoin = false)
    withReplanObserver { (broadcast, stage) =>
      assert(stage.physicalPlan eq broadcast)
      assert(broadcast.getTagValue(tempTag).exists(_ eq stage.logicalPlan))
      broadcast.clone()
    } { (previous, broadcast, logicalPlan) =>
      assert(broadcast.logicalLink.exists(_ eq logicalPlan))
      assert(broadcast.getTagValue(tempTag).exists(_ eq logicalPlan))
      // Spark rejects an equal-cost candidate when its physical tree is unchanged.
      // Pin both inputs to that decision, including Comet's retained temporary link.
      assert(previous == broadcast)
      assert(costEvaluator.evaluateCost(previous) == SimpleCost(0))
      assert(costEvaluator.evaluateCost(broadcast) == SimpleCost(0))
      observed.add((broadcast.clone().asInstanceOf[CometBroadcastExchangeExec], logicalPlan))
    } {
      val plan = checkDppLinkQuery()
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
    }
  }

  /**
   * Runs `f` while comparing each plan that Comet prepares as a query stage preparation rule with
   * the plan it returns: `capture` takes what to compare from the plan Comet receives, and
   * `check` gets that and the plan Comet returns.
   */
  private def withPreparationObserver[T](capture: SparkPlan => T)(check: (T, SparkPlan) => Unit)(
      f: => Unit): Unit = {
    val pending = new ThreadLocal[List[T]] {
      override def initialValue(): List[T] = Nil
    }
    beforeCometPreparation = plan => pending.set(capture(plan) :: pending.get())
    afterCometPreparation = plan => {
      val captured = pending.get().head
      val remaining = pending.get().tail
      if (remaining.isEmpty) pending.remove() else pending.set(remaining)
      check(captured, plan)
    }
    try {
      f
    } finally {
      beforeCometPreparation = _ => ()
      afterCometPreparation = _ => ()
    }
  }

  test("AQE replans keep a broadcast side's native aggregate over its shuffle stage as it was") {
    // AQE adopts a replan that differs from its current plan, after which it cannot link a stage
    // built over a reused operator back into its logical plan. A broadcast stage it cannot link
    // no longer fixes the join's build side, which broke the exchange reuse that Spark's
    // SPARK-34637 DPP test checks on Spark 3.4. The aggregate under the broadcast is the root of
    // its native block, so a refreshed native plan there changes the plan that AQE compares.
    def reusedOverStage(plan: SparkPlan): Seq[SparkPlan] = plan.collect {
      case native: CometNativeExec
          if native.children.exists(_.isInstanceOf[ShuffleQueryStageExec]) =>
        native
    }
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> "true") {
      withParquetTable((0 until 1000).map(i => (i % 7, i.toLong)), "reuse_fact") {
        withParquetTable((0 until 7).map(i => (i, i * 2)), "reuse_dim") {
          val replans = new ConcurrentLinkedQueue[(Seq[SparkPlan], Seq[SparkPlan])]()
          withPreparationObserver(reusedOverStage) { (reused, plan) =>
            if (reused.nonEmpty) replans.add((reused, reusedOverStage(plan)))
          } {
            val df = sql("""
                |SELECT /*+ BROADCAST(f) */ f.k, f.s, d._2
                |FROM (SELECT _1 AS k, SUM(_2) AS s FROM reuse_fact GROUP BY _1) f
                |JOIN reuse_dim d ON f.k = d._1
                |""".stripMargin)
            checkSparkAnswerAndOperator(df)
          }
          assert(!replans.isEmpty, "Expected a replan over a native operator AQE reuses")
          replans.asScala.foreach { case (reused, prepared) =>
            assert(prepared == reused)
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
          CometConf.COMET_CONVERT_FROM_RANGE_ENABLED.key -> "true",
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

  if (isSpark40Plus) {
    Seq(
      ("StructsToJson", "to_json(named_struct('value', id))"),
      ("ParseUrl", "parse_url(concat('https://example.com/', id), 'HOST')"),
      ("LengthOfJsonArray", "json_array_length(concat('[', id, ']'))")).foreach {
      case (name, sqlExpression) =>
        test(s"issue #5499: $name shim retains current fallback reasons on its wrapper") {
          val plan = sql(s"SELECT $sqlExpression FROM range(2)").queryExecution.analyzed
          val original = plan.expressions
            .flatMap(_.collect {
              case expr: RuntimeReplaceable if expr.getClass.getSimpleName == name => expr
            })
            .head
          val wrapper = original.replacement
          if (name == "LengthOfJsonArray") assert(wrapper.isInstanceOf[StaticInvoke])
          else assert(wrapper.isInstanceOf[Invoke])
          val tag = CometExplainInfo.FALLBACK_REASONS
          wrapper.setTagValue(tag, Set("unrelated conversion"))
          val key = CometConf.getExprEnabledConfigKey(name)
          val expected = Set(s"Expression support is disabled. Set $key=true to enable it.")
          withSQLConf(key -> "false") {
            CometExplainInfo.withFallbackReasonScope {
              // Call the shim directly so the generic Invoke handler cannot supply an unrelated
              // reason and make this test pass when copy-back is broken.
              assert(
                QueryPlanSerde
                  .sparkVersionSpecificExprToProtoInternal(
                    wrapper,
                    plan.children.head.output,
                    binding = true)
                  .isEmpty)
              assert(wrapper.getTagValue(tag).contains(expected))
              assert(CometExplainInfo.currentFallbackReasons(wrapper) == expected)
            }
          }
        }
    }
  }

  test("fallback reasons accumulate on unscoped expressions and remain sticky on operators") {
    val expr = Literal(42)
    val tag = CometExplainInfo.FALLBACK_REASONS
    withFallbackReason(expr, "unscoped first")
    withFallbackReason(expr, "unscoped second")
    assert(expr.getTagValue(tag).contains(Set("unscoped first", "unscoped second")))

    val op = spark.sessionState.planner.plan(LocalRelation(Seq.empty[Attribute])).next()
    withFallbackReason(op, "prior rule veto")
    CometExplainInfo.withFallbackReasonScope {
      withFallbackReason(expr, "current expression reason")
      withFallbackReason(expr, "another expression reason")
      val current = Set("current expression reason", "another expression reason")
      assert(expr.getTagValue(tag).contains(current))
      assert(CometExplainInfo.currentFallbackReasons(expr) == current)
      withFallbackReason(op, "current operator reason")
      assert(op.getTagValue(tag).contains(Set("prior rule veto", "current operator reason")))
    }
    withFallbackReason(expr, "after scope")
    assert(
      expr
        .getTagValue(tag)
        .contains(Set("current expression reason", "another expression reason", "after scope")))
  }

  test("issue #5499: child threads do not inherit expression fallback reasons") {
    val source = Literal(true)
    val reason = "parent conversion only"
    val tag = CometExplainInfo.FALLBACK_REASONS
    CometExplainInfo.withFallbackReasonScope {
      withFallbackReason(source, reason)
      // Create the child while the parent scope is active: an inheritable ThreadLocal would
      // expose the parent's journal and retain it beyond the parent's conversion.
      val task = new FutureTask[Set[String]](() => {
        CometExplainInfo.withExpressionFallbackReasonScope {
          val owner = Alias(source, "child")()
          QueryPlanSerde.liftFallbackReasons(source, owner)
          owner.getTagValue(tag).getOrElse(Set.empty[String])
        }
      })
      val thread = new Thread(task, "comet-fallback-provenance")
      thread.setDaemon(true)
      thread.start()
      try assert(task.get(10, TimeUnit.SECONDS).isEmpty)
      finally thread.join(10000)
      val owner = Alias(source, "parent")()
      QueryPlanSerde.liftFallbackReasons(source, owner)
      assert(owner.getTagValue(tag).contains(Set(reason)))
    }
  }

  test("issue #5499: aggregate preflight retains current reasons without importing stale tags") {
    withObjectHashAggPlan { plan =>
      val agg = partialObjectHashAgg(plan)
      val tag = CometExplainInfo.FALLBACK_REASONS
      val expression = agg.aggregateExpressions.head
      val key =
        CometConf.getExprEnabledConfigKey(expression.aggregateFunction.getClass.getSimpleName)
      val stale = "issue-5499-stale-aggregate-preflight"
      expression.setTagValue(tag, Set(stale))
      expression.aggregateFunction.setTagValue(tag, Set(stale))
      val expected = s"Expression support is disabled. Set $key=true to enable it."
      val rule = CometExecRule(spark)
      withSQLConf(key -> "false") {
        CometExplainInfo.withFallbackReasonScope {
          val outer = Literal(true)
          withFallbackReason(outer, "outer conversion")
          assert(!rule.canAggregateBeConverted(agg, Partial))
          assert(agg.getTagValue(tag).contains(Set(expected)))
          assert(new ExtendedExplainInfo().generateExtendedInfo(agg).contains(expected))
          assert(CometExplainInfo.currentFallbackReasons(outer) == Set("outer conversion"))
          assert(CometExplainInfo.currentFallbackReasons(expression).isEmpty)
        }
      }
    }
    withObjectHashAggPlan { plan =>
      val agg = partialObjectHashAgg(plan)
      agg.aggregateExpressions.head.setTagValue(
        CometExplainInfo.FALLBACK_REASONS,
        Set("issue-5499-stale-successful-preflight"))
      assert(CometExecRule(spark).canAggregateBeConverted(agg, Partial))
      assert(agg.getTagValue(CometExplainInfo.FALLBACK_REASONS).isEmpty)
    }
  }

  test("issue #5499: outermost conversion exceptions do not leak fallback reasons") {
    val source = Literal(true)
    val tag = CometExplainInfo.FALLBACK_REASONS
    intercept[IllegalArgumentException] {
      CometExplainInfo.withFallbackReasonScope {
        withFallbackReason(source, "failed conversion")
        throw new IllegalArgumentException("conversion failed")
      }
    }
    // Reuse the calling thread and source node after the exception, with its old tag intact.
    CometExplainInfo.withExpressionFallbackReasonScope {
      val owner = Alias(source, "next_conversion")()
      QueryPlanSerde.liftFallbackReasons(source, owner)
      assert(owner.getTagValue(tag).isEmpty)
      withFallbackReason(source, "current conversion")
      QueryPlanSerde.liftFallbackReasons(source, owner)
      assert(owner.getTagValue(tag).contains(Set("current conversion")))
    }
  }

  test("issue #5499: stale expression reasons cannot satisfy strict fallback checking") {
    val literal = Literal.TrueLiteral
    val tag = CometExplainInfo.FALLBACK_REASONS
    val previous = literal.getTagValue(tag)
    val stale = "issue-5499-unrelated-query"
    try {
      literal.setTagValue(tag, Set(stale))
      withTempView("fallback_provenance") {
        createTestDataFrame.createOrReplaceTempView("fallback_provenance")
        val sparkPlan = createSparkPlan(spark, "SELECT id * 2 FROM fallback_provenance")
        withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
          val nativeChild = stripAQEPlan(applyCometExecRule(sparkPlan)).collectFirst {
            case op: CometNativeExec => op
          }.get
          val rule = CometExecRule(spark)
          def operator = ProjectExec(Seq(Alias(literal, "value")()), nativeChild)
          CometExplainInfo.withFallbackReasonScope {
            val op = operator
            rule.rollUpFallbackReasons(op)
            assert(op.getTagValue(tag).isEmpty)
            intercept[IllegalStateException](rule.reportUnexplainedFallback(op))
            withSQLConf(CometConf.COMET_STRICT_FALLBACK_REASONS.key -> "false") {
              rule.reportUnexplainedFallback(op)
              assert(op.getTagValue(tag).contains(Set(s"${op.nodeName} is not supported")))
            }
            // A copied tag is not a current reason, even if its source was tagged in this scope.
            val source = Literal(true)
            withFallbackReason(source, stale)
            val copy = Literal(true)
            assert(source == copy && !(source eq copy))
            copy.copyTagsFrom(source)
            val copied = Alias(copy, "copied")()
            val copiedProject = ProjectExec(Seq(copied), nativeChild)
            rule.rollUpFallbackReasons(copiedProject)
            assert(copiedProject.getTagValue(tag).isEmpty)
            intercept[IllegalStateException](rule.reportUnexplainedFallback(copiedProject))
          }
          // A current reason equal to the stale string must still count.
          CometExplainInfo.withFallbackReasonScope {
            val op = operator
            withFallbackReason(literal, stale)
            intercept[IllegalArgumentException] {
              CometExplainInfo.withFallbackReasonScope {
                withFallbackReason(literal, "nested conversion")
                throw new IllegalArgumentException("conversion failed")
              }
            }
            rule.rollUpFallbackReasons(op)
            assert(op.getTagValue(tag).contains(Set(stale)))
            rule.reportUnexplainedFallback(op)
          }
          CometExplainInfo.withFallbackReasonScope {
            val op = operator
            rule.rollUpFallbackReasons(op)
            intercept[IllegalStateException](rule.reportUnexplainedFallback(op))
          }
          // Legitimate literal failures must survive both serialization and operator roll-up.
          val unsupported =
            Literal.create(new Object, org.apache.spark.sql.types.ObjectType(classOf[Object]))
          val disabledKey = CometConf.getExprEnabledConfigKey("Literal")
          Seq(unsupported -> Seq.empty[(String, String)], literal -> Seq(disabledKey -> "false"))
            .foreach { case (value, configs) =>
              withSQLConf(configs: _*) {
                CometExplainInfo.withFallbackReasonScope {
                  val op = ProjectExec(Seq(Alias(value, "value")()), nativeChild)
                  assert(
                    QueryPlanSerde.exprToProto(op.projectList.head, nativeChild.output).isEmpty)
                  rule.rollUpFallbackReasons(op)
                  val expected = if (value eq literal) {
                    s"Expression support is disabled. Set $disabledKey=true to enable it."
                  } else {
                    s"Unsupported data type ${unsupported.dataType}"
                  }
                  assert(op.getTagValue(tag).contains(Set(expected)))
                  assert(new ExtendedExplainInfo().generateExtendedInfo(op).contains(expected))
                  rule.reportUnexplainedFallback(op)
                }
              }
            }
        }
      }
    } finally {
      previous match {
        case Some(reasons) => literal.setTagValue(tag, reasons)
        case None => literal.unsetTagValue(tag)
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

  test("CometExecRule should allow AVG mixed Comet partial and Spark final") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")
      val sparkPlan =
        createSparkPlan(spark, "SELECT AVG(id) FROM test_data GROUP BY (id % 3)")
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)
      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // final
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // partial
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
        val aggregates = "AVG(CAST(id AS DECIMAL(20, 2)))" +
          (if (distinct) ", SUM(DISTINCT id)" else "")

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
          createSparkPlan(
            spark,
            "SELECT AVG(CAST(id AS DECIMAL(20, 2))) FROM test_data GROUP BY (id % 3)"))
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
          createSparkPlan(
            spark,
            "SELECT AVG(CAST(id AS DECIMAL(20, 2))) FROM test_data GROUP BY (id % 3)"))
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

  test("buffer repair walks through the sort below a sort aggregate") {
    // Spark puts a SortExec between a sort aggregate and its exchange, converted or not. Repair
    // must rebuild through it to restore the native Partial, and when repair stops at a stage it
    // must still find the native Partial below the sort and warn.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.USE_OBJECT_HASH_AGG.key -> "false",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native") {
      withTempView("test_data") {
        createTestDataFrame.createOrReplaceTempView("test_data")
        val plan = applyCometExecRule(
          createSparkPlan(spark, "SELECT collect_list(id) FROM test_data GROUP BY (id % 3)"))
        val nativeFinal = plan
          .collectFirst { case agg: CometSortAggregateExec if agg.modes == Seq(Final) => agg }
          .getOrElse(fail(s"Expected a native final sort aggregate in:\n$plan"))
        val partial = plan
          .collectFirst { case agg: CometSortAggregateExec if agg.modes == Seq(Partial) => agg }
          .getOrElse(fail(s"Expected a native partial sort aggregate in:\n$plan"))
        val sparkFinal = nativeFinal.originalPlan.asInstanceOf[SortAggregateExec]
        val cometSort = nativeFinal.child.asInstanceOf[CometSortExec]
        val sorts: Seq[SparkPlan => SparkPlan] = Seq(
          child => cometSort.copy(child = child),
          child => cometSort.originalPlan.withNewChildren(Seq(child)))
        val rule = CometExecRule(spark)

        sorts.foreach { sortOver =>
          val repaired =
            rule.revertUnsafePartialAggregates(sparkFinal.copy(child = sortOver(cometSort.child)))
          assert(repaired.collect { case agg: CometSortAggregateExec => agg }.isEmpty, repaired)
          val sparkPartial = repaired.collectFirst {
            case agg: SortAggregateExec if agg.aggregateExpressions.forall(_.mode == Partial) =>
              agg
          }.get
          assert(sparkPartial.getTagValue(CometExecRule.COMET_UNSAFE_PARTIAL).isDefined)

          val exchange = ShuffleExchangeExec(
            org.apache.spark.sql.catalyst.plans.physical.SinglePartition,
            partial)
          val stage = ShuffleQueryStageExec(0, exchange, exchange.canonicalized)
          val placeholder = CometSinkPlaceHolder(
            org.apache.comet.serde.OperatorOuterClass.Operator.getDefaultInstance,
            stage,
            stage)
          val consumer = sparkFinal.copy(child = sortOver(placeholder))
          assert(rule.revertUnsafePartialAggregates(consumer) eq consumer)
          val reasons = new ExtendedExplainInfo().getFallbackReasons(consumer)
          assert(reasons.exists(_.contains("could not restore a native intermediate buffer")))
        }
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
    assume(!isSpark42Plus, "https://github.com/apache/datafusion-comet/issues/4968")
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
    assume(!isSpark42Plus, "https://github.com/apache/datafusion-comet/issues/4968")
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

  test("CometExecRule should not split distinct collect_list aggregate (Spark final)") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT collect_list(id), COUNT(DISTINCT name) FROM test_data")

      assert(countOperators(sparkPlan, classOf[ObjectHashAggregateExec]) > 1)

      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // A native PartialMerge can decode a Spark partial's collect_list buffer, so a Spark
        // partial alone does not keep the merge stages above it in Spark. They must be disabled
        // too, or their native list state would reach the Spark final.
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
          ((executed.cometOperators, executed.cometOperators + executed.sparkOperators)),
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

  /**
   * The leaf operators that `CometExecRule` converts to Arrow in the plan of `query`, with every
   * Spark-to-Arrow conversion off apart from what `confs` turns on.
   */
  private def convertedLeaves(query: String, confs: (String, String)*): Seq[SparkPlan] = {
    var leaves = Seq.empty[SparkPlan]
    withSQLConf(
      Seq(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        CometConf.COMET_EXEC_RANGE_ENABLED.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "false") ++
        sparkToArrowConversionConfs(enabled = false) ++ confs: _*) {
      leaves = applyCometExecRule(createSparkPlan(spark, query)).collect {
        case c: CometSparkToColumnarExec => c.child
      }
    }
    leaves
  }

  /** Runs `f` with a query over each leaf operator that has a `spark.comet.convert` config. */
  private def withConversionQueries(f: Seq[(ConfigEntry[Boolean], String)] => Unit): Unit = {
    withTempView("rdd_input", "cached_input", "row_data_source_input") {
      val schema = StructType(Seq(StructField("a", DataTypes.IntegerType)))
      spark
        .createDataFrame(spark.sparkContext.parallelize(Seq(Row(1), Row(2)), 1), schema)
        .createOrReplaceTempView("rdd_input")
      spark.range(10).selectExpr("CAST(id AS INT) AS a").createOrReplaceTempView("cached_input")
      spark.catalog.cacheTable("cached_input")
      rowDataSourceDataFrame(schema, Seq(Row(1), Row(2)))
        .createOrReplaceTempView("row_data_source_input")
      try {
        f(
          Seq(
            CometConf.COMET_CONVERT_FROM_RANGE_ENABLED -> "SELECT id + 1 FROM range(10)",
            CometConf.COMET_CONVERT_FROM_IN_MEMORY_CACHE_ENABLED ->
              "SELECT a + 1 FROM cached_input",
            CometConf.COMET_CONVERT_FROM_RDD_ENABLED -> "SELECT a + 1 FROM rdd_input",
            // Spark plans this as an RDDScanExec before 4.1, so it also checks that the RDD
            // config leaves it alone there.
            CometConf.COMET_CONVERT_FROM_ONE_ROW_RELATION_ENABLED -> "SELECT 1 AS a",
            CometConf.COMET_CONVERT_FROM_ROW_DATA_SOURCE_ENABLED ->
              "SELECT a + 1 FROM row_data_source_input"))
      } finally {
        spark.catalog.uncacheTable("cached_input")
      }
    }
  }

  test("each spark.comet.convert config converts only its own leaf operator") {
    withConversionQueries { queries =>
      for ((entry, query) <- queries; (enabled, _) <- queries) {
        withClue(s"${enabled.key}=true, $query: ") {
          assert(convertedLeaves(query, enabled.key -> "true").nonEmpty == (entry eq enabled))
        }
      }
    }
  }

  test("only the OneRowRelation conversion is on by default") {
    withConversionQueries { queries =>
      // CometTestBase turns every conversion on, so set each back to its default.
      val defaults = queries.map { case (entry, _) => entry.key -> entry.defaultValueString }
      for ((entry, query) <- queries) {
        withClue(s"$query: ") {
          assert(
            convertedLeaves(query, defaults: _*).nonEmpty ==
              (entry eq CometConf.COMET_CONVERT_FROM_ONE_ROW_RELATION_ENABLED))
        }
      }
    }
  }

  test("the deprecated sparkToColumnar settings still convert what they converted before") {
    val switch = CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true"
    val list = CometConf.COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST.key
    val rowDataSource = CometConf.COMET_CONVERT_FROM_ROW_DATA_SOURCE_ENABLED
    withConversionQueries { queries =>
      // Without a list, the switch converts the operators the list used to name by default,
      // which did not include RowDataSourceScan.
      queries.foreach { case (entry, query) =>
        assert(convertedLeaves(query, switch).nonEmpty == (entry ne rowDataSource), query)
      }
      // A list replaces them.
      val range = queries.head._2
      assert(convertedLeaves(range, switch, list -> "RDDScan").isEmpty)
      assert(convertedLeaves(range, switch, list -> "Range").nonEmpty)
      // A list that names RowDataSourceScan still converts it.
      val rowDataSourceQuery = queries.find(_._1 eq rowDataSource).get._2
      assert(convertedLeaves(rowDataSourceQuery, switch, list -> "RowDataSourceScan").nonEmpty)
    }
    // An operator without a config of its own is converted only when the list names it.
    val values = "SELECT a FROM VALUES (1), (2) AS t(a)"
    assert(convertedLeaves(values, switch).isEmpty)
    assert(convertedLeaves(values, switch, list -> "LocalTableScan").nonEmpty)
    assert(convertedLeaves(values, list -> "LocalTableScan").isEmpty)
  }

  test("the deprecated sparkToColumnar settings warn once for each config they stand in for") {
    val switch = CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true"
    val list = CometConf.COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST.key
    val range = "SELECT id + 1 FROM range(10)"
    CometExecRule.warnedDeprecatedConversions.clear()
    val appender = new LogAppender("deprecated Spark-to-Arrow settings")
    withConversionQueries { queries =>
      val rowDataSource =
        queries.find(_._1 eq CometConf.COMET_CONVERT_FROM_ROW_DATA_SOURCE_ENABLED).get._2
      withLogAppender(appender, Seq(classOf[CometExecRule].getName), Some(Level.WARN)) {
        assert(convertedLeaves(range, switch).nonEmpty)
        assert(convertedLeaves(range, switch).nonEmpty)
        // With its own config on, the range does not need the deprecated settings.
        assert(
          convertedLeaves(
            range,
            switch,
            CometConf.COMET_CONVERT_FROM_RANGE_ENABLED.key -> "true").nonEmpty)
        // Converting an operator without a config of its own is not deprecated.
        assert(
          convertedLeaves(
            "SELECT a FROM VALUES (1), (2) AS t(a)",
            switch,
            list -> "LocalTableScan").nonEmpty)
        assert(convertedLeaves(rowDataSource, switch, list -> "RowDataSourceScan").nonEmpty)
      }
    }
    val warnings = appender.loggingEvents
      .map(_.getMessage.getFormattedMessage)
      .filter(_.contains(CometConf.COMET_SPARK_TO_ARROW_ENABLED.key))
    assert(warnings.size == 2, warnings)
    assert(warnings.head.contains(s"${CometConf.COMET_CONVERT_FROM_RANGE_ENABLED.key}=true"))
    assert(
      warnings(1).contains(s"${CometConf.COMET_CONVERT_FROM_ROW_DATA_SOURCE_ENABLED.key}=true"))
  }

}
