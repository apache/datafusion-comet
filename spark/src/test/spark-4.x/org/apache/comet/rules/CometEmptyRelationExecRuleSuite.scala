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

import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.comet._
import org.apache.spark.sql.execution.{ApplyColumnarRulesAndInsertTransitions, CoalesceExec, EmptyRelationExec, LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BooleanType, DecimalType, IntegerType, StructType}

import org.apache.comet.CometConf

class CometEmptyRelationExecRuleSuite extends CometTestBase {

  import testImplicits._

  private def emptyRelation(output: Seq[Attribute]): SparkPlan = {
    EmptyRelationExec(LocalRelation(output))
  }

  private def localTableScan(output: Seq[Attribute]): LocalTableScanExec =
    spark.sessionState.planner.plan(LocalRelation(output)).next().asInstanceOf[LocalTableScanExec]

  private def prepareEmptyInputPlan(plan: SparkPlan, native: Boolean): SparkPlan = {
    val required = EnsureRequirements().apply(plan)
    val converted = if (native) CometExecRule(spark).apply(required) else required
    ApplyColumnarRulesAndInsertTransitions(Seq.empty, false).apply(converted)
  }

  private def collectEmptyInputPlan(plan: SparkPlan): Seq[Row] = {
    val toRow = CatalystTypeConverters.createToScalaConverter(plan.schema)
    plan.executeCollect().map(row => toRow(row).asInstanceOf[Row]).toSeq
  }

  private def withCometDisabled(plan: => SparkPlan): SparkPlan = {
    var result: SparkPlan = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      result = plan
    }
    result
  }

  test("EmptyRelationExec preserves attributes and zero partitions") {
    val attributes = Seq(
      AttributeReference("id", IntegerType, nullable = false)(),
      AttributeReference("amount", DecimalType(18, 4), nullable = true)(),
      AttributeReference("nested", ArrayType(IntegerType, containsNull = false))())
    val original = emptyRelation(attributes)
    val converted = CometExecRule(spark).apply(original).asInstanceOf[CometEmptyRelationExec]
    assert(converted.output == original.output)
    assert(converted.schema == original.schema)
    assert(converted.children.isEmpty)
    assert(converted.executeColumnar().getNumPartitions == 0)
    assert(converted.doExecuteAsArrowStream().getNumPartitions == 0)
    assert(converted.executeCollect().isEmpty)
    assert(
      converted.canonicalized ==
        CometExecRule(spark).apply(emptyRelation(attributes.map(_.newInstance()))).canonicalized)

    withSQLConf(CometConf.COMET_EXEC_EMPTY_RELATION_ENABLED.key -> "false") {
      assert(CometExecRule(spark).apply(emptyRelation(attributes)).getClass == original.getClass)
    }
    val unsupported = emptyRelation(Seq(AttributeReference("empty_struct", StructType(Nil))()))
    assert(CometExecRule(spark).apply(unsupported).getClass == unsupported.getClass)
  }

  test("EmptyRelationExec preserves the eliminated subtree in explain output") {
    val attributes = Seq(AttributeReference("id", IntegerType, nullable = false)())
    val eliminated = Project(attributes, LocalRelation(attributes, Seq(InternalRow(1))))
    val original = EmptyRelationExec(eliminated)
    val converted = CometExecRule(spark).apply(original).asInstanceOf[CometEmptyRelationExec]

    val explained = converted.treeString
    assert(explained.contains("CometEmptyRelation"), explained)
    assert(explained.contains("Project"), explained)
    assert(explained.contains("LocalRelation"), explained)
    assert(converted.children.isEmpty)
    assert(converted.executeCollect().isEmpty)
  }

  test("EmptyRelationExec supports global and grouped COUNT and SUM") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      withTempView("empty_aggregate_input") {
        Seq((1, 2)).toDF("k", "v").createOrReplaceTempView("empty_aggregate_input")
        val queries = Seq(
          "SELECT count(*), count(v), sum(v) FROM empty_aggregate_input" -> Seq(
            Row(0L, 0L, null)),
          "SELECT sum(v) FROM empty_aggregate_input" -> Seq(Row(null)),
          "SELECT count(*) FROM empty_aggregate_input" -> Seq(Row(0L)),
          "SELECT k, count(*), sum(v) FROM empty_aggregate_input GROUP BY k" -> Seq.empty)
        for {
          (query, expected) <- queries
          singleEmptyPartition <- Seq(false, true)
        } {
          def original: SparkPlan = withCometDisabled {
            sql(query).queryExecution.sparkPlan.transformUp { case leaf: LocalTableScanExec =>
              val empty = emptyRelation(leaf.output)
              // COALESCE(1) turns zero partitions into one empty columnar input stream.
              if (singleEmptyPartition) CoalesceExec(1, empty) else empty
            }
          }
          val native = prepareEmptyInputPlan(original, native = true)
          withClue(s"$query\n$native") {
            assert(
              collectEmptyInputPlan(prepareEmptyInputPlan(original, native = false)) == expected)
            assert(collectEmptyInputPlan(native) == expected)
            assert(native.collect { case e: CometEmptyRelationExec => e }.size == 1)
            assert(native.collect { case a: CometHashAggregateExec => a }.size == 2)
            assert(native.collect {
              case a: org.apache.spark.sql.execution.aggregate.HashAggregateExec => a
            }.isEmpty)
          }
        }
      }
    }
  }

  test("EmptyRelationExec supports empty build and probe hash joins") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
      for {
        broadcast <- Seq(false, true)
        joinType <- Seq(Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti)
        if !(broadcast && joinType == FullOuter)
        (emptyLeft, emptyRight) <- Seq((false, true), (true, false), (true, true))
      } {
        def original: SparkPlan = {
          val leftKey = AttributeReference("l", IntegerType, nullable = true)()
          val rightKey = AttributeReference("r", IntegerType, nullable = true)()
          val left =
            if (emptyLeft) emptyRelation(Seq(leftKey))
            else
              localTableScan(Seq(leftKey))
                .copy(rows = Seq(InternalRow(1), InternalRow(1), InternalRow(null)))
          val right =
            if (emptyRight) emptyRelation(Seq(rightKey))
            else localTableScan(Seq(rightKey)).copy(rows = Seq(InternalRow(1), InternalRow(null)))
          val buildSide = if (joinType == RightOuter) BuildLeft else BuildRight
          if (broadcast) {
            BroadcastHashJoinExec(
              Seq(leftKey),
              Seq(rightKey),
              joinType,
              buildSide,
              None,
              left,
              right)
          } else {
            ShuffledHashJoinExec(
              Seq(leftKey),
              Seq(rightKey),
              joinType,
              buildSide,
              None,
              left,
              right)
          }
        }
        val native = prepareEmptyInputPlan(original, native = true)
        withClue(s"$joinType broadcast=$broadcast empty=($emptyLeft,$emptyRight)\n$native") {
          val expected = collectEmptyInputPlan(prepareEmptyInputPlan(original, native = false))
          assert(
            collectEmptyInputPlan(native).groupBy(identity).map { case (r, rs) =>
              r -> rs.size
            } ==
              expected.groupBy(identity).map { case (r, rs) => r -> rs.size })
          assert(native.collect { case e: CometEmptyRelationExec => e }.nonEmpty)
          val joins = native.collect {
            case j: CometBroadcastHashJoinExec => j
            case j: CometHashJoinExec => j
          }
          assert(joins.size == 1)
        }
      }
    }
  }

  test("EmptyRelationExec retains incompatible aggregate buffer fallback") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false") {
      def original: SparkPlan = withCometDisabled {
        sql("SELECT avg(v) FROM VALUES (CAST(1 AS DECIMAL(38, 0))) AS t(v)").queryExecution.sparkPlan
          .transformUp { case leaf: LocalTableScanExec =>
            emptyRelation(leaf.output)
          }
      }
      val native = prepareEmptyInputPlan(original, native = true)
      assert(collectEmptyInputPlan(native) == Seq(Row(null)))
      assert(native.collect { case e: CometEmptyRelationExec => e }.size == 1)
      assert(native.collect { case a: CometHashAggregateExec => a }.isEmpty)
      assert(native.collect {
        case a: org.apache.spark.sql.execution.aggregate.HashAggregateExec => a
      }.size == 2)
    }
  }

  test("EmptyRelationExec retains existence sort-merge join fallback") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.key -> "true") {
      val leftKey = AttributeReference("l", IntegerType, nullable = true)()
      val rightKey = AttributeReference("r", IntegerType, nullable = true)()
      val left = localTableScan(Seq(leftKey)).copy(rows = Seq(InternalRow(1), InternalRow(null)))
      val join = SortMergeJoinExec(
        Seq(leftKey),
        Seq(rightKey),
        ExistenceJoin(AttributeReference("exists", BooleanType, nullable = false)()),
        None,
        left,
        emptyRelation(Seq(rightKey)))
      val native = prepareEmptyInputPlan(join, native = true)
      assert(
        collectEmptyInputPlan(native).sortBy(_.toString) ==
          Seq(Row(1, false), Row(null, false)).sortBy(_.toString))
      assert(native.collect { case e: CometEmptyRelationExec => e }.size == 1)
      assert(native.collect { case j: SortMergeJoinExec => j }.size == 1)
      assert(native.collect { case j: CometSortMergeJoinExec => j }.isEmpty)
    }
  }

  test("EmptyRelationExec supports reused broadcast and AQE broadcast stages") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
      val leftKey = AttributeReference("l", IntegerType, nullable = true)()
      val rightKey = AttributeReference("r", IntegerType, nullable = true)()
      val left = localTableScan(Seq(leftKey)).copy(rows = Seq(InternalRow(1), InternalRow(null)))
      def join(right: SparkPlan): SparkPlan = BroadcastHashJoinExec(
        Seq(leftKey),
        Seq(rightKey),
        LeftOuter,
        BuildRight,
        None,
        left,
        right)
      val first = prepareEmptyInputPlan(join(emptyRelation(Seq(rightKey))), native = true)
      val exchange = first.collectFirst { case e: CometBroadcastExchangeExec => e }.get
      assert(collectEmptyInputPlan(first) == Seq(Row(1, null), Row(null, null)))

      for (adaptive <- Seq(false, true)) {
        val reused = ReusedExchangeExec(exchange.output, exchange)
        val native = if (adaptive) {
          val input = BroadcastQueryStageExec(0, reused, reused.canonicalized)
          prepareEmptyInputPlan(join(input), native = true)
        } else {
          // Non-AQE reuse runs after native conversion; its consumers already have native plans.
          first.transformUp { case _: CometBroadcastExchangeExec => reused }
        }
        withClue(s"AQE=$adaptive\n$native") {
          assert(collectEmptyInputPlan(native) == Seq(Row(1, null), Row(null, null)))
          assert(native.collect { case j: CometBroadcastHashJoinExec => j }.size == 1)
          assert(collect(native) { case e: ReusedExchangeExec => e }.nonEmpty)
        }
      }
      assert(exchange.metrics("numOutputRows").value == 0)
    }
  }
}
