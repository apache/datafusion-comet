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

package org.apache.comet.serde

import scala.reflect.ClassTag

import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.comet._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, LogicalQueryStage}
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus
import org.apache.comet.rules.CometExecRule
import org.apache.comet.shims.ShimCometWindowGroupLimit

class CometEvaluationMaskSuite extends CometTestBase {
  private val limitReason = "unbase64 requires Spark evaluation below LIMIT"
  private val joinReason = "unbase64 requires Spark evaluation in first-match join conditions"
  private val decodeMessage = "Last unit does not have enough valid bits"

  private def withModes(f: (Boolean, Boolean) => Unit): Unit = {
    for (aqe <- Seq(false, true); dispatch <- Seq(false, true)) {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> dispatch.toString) {
        withClue(s"AQE=$aqe, dispatcher=$dispatch: ")(f(aqe, dispatch))
      }
    }
  }

  private def withInputs(inputs: (String, String)*)(f: => Unit): Unit = {
    withTempPath { dir =>
      withTempView(inputs.map(_._1): _*) {
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          inputs.foreach { case (name, query) =>
            val path = s"${dir.getCanonicalPath}/$name"
            sql(query).coalesce(1).write.parquet(path)
            val input = spark.read.parquet(path)
            assert(input.inputFiles.length == 1)
            input.createOrReplaceTempView(name)
          }
        }
        f
      }
    }
  }

  private def sparkPlan(query: String): SparkPlan = {
    var plan: SparkPlan = null
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      plan = sql(query).queryExecution.executedPlan
    }
    plan
  }

  private def applyRule(plan: SparkPlan): SparkPlan =
    CometExecRule(spark).apply(stripAQEPlan(plan))
  private def count[T <: SparkPlan: ClassTag](plan: SparkPlan): Int =
    collect(plan) { case node: T => node }.size
  private def aggregateCounts(plan: SparkPlan): (Int, Int) =
    (count[CometHashAggregateExec](plan), count[ObjectHashAggregateExec](plan))
  private def nativeScans(plan: SparkPlan): Int = collect(plan) {
    case _: CometScanExec | _: CometNativeScanExec => true
  }.size
  private def original(plan: SparkPlan): SparkPlan = plan match {
    case comet: CometExec => comet.originalPlan
    case other => other
  }
  private def hasDecoder(expr: Expression): Boolean = expr.exists(_.isInstanceOf[UnBase64])
  private def malformed(error: Throwable): Boolean =
    causeChain(error).exists(e => Option(e.getMessage).exists(_.contains(decodeMessage)))
  private def decodeError(body: => Any): Unit = assert(malformed(intercept[Exception](body)))
  private def decodeErrors(query: String): Unit = {
    val (sparkError, cometError) = checkSparkAnswerMaybeThrows(sql(query))
    Seq(sparkError, cometError).foreach(error => assert(error.exists(malformed), query))
  }

  test("LIMIT masks follow the decoder policy with ANSI, strict and compound inputs") {
    assert(QueryPlanSerde.exprSerdeMap.collect { case (cls, _: RequiresSparkEvaluationMask[_]) =>
      cls
    }.toSet == Set(classOf[UnBase64]))
    val input = AttributeReference("encoded", StringType)()
    withInputs(
      "masked" -> "SELECT * FROM VALUES (1, 'YWJj'), (2, 'A') AS t(k, bad)",
      "valid" -> "SELECT * FROM VALUES (1, 'YWJj'), (1, 'YWFh'), (1, NULL) AS t(k, bad)",
      "inner_left" -> "SELECT 1 AS k, X'616262' AS expected",
      "one_group" -> "SELECT 1 AS k, 'YWJj' AS bad") {
      withModes { (_, _) =>
        for (ansi <- Seq(false, true)) {
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi.toString) {
            for (child <- Seq(input, Literal("YWJj"), Concat(Seq(input, Literal(""))));
              strict <- Seq(false, true)) {
              val expressions = Seq(UnBase64(child, strict), new LimitUnBase64(child, strict))
              assert(
                expressions.forall(QueryPlanSerde.evaluationMaskName(_).contains("unbase64")))
            }
            // Existing unbase64/to_binary SQL tests exercise native and dispatched serialization.
            Seq(
              Add(Literal(Int.MaxValue), Literal(1)),
              Cast(input, IntegerType),
              Concat(Seq(input, Literal(""))),
              FormatString(Literal("%s"), input),
              Like(input, Literal("%"), escapeChar = '\\')).foreach { expr =>
              assert(QueryPlanSerde.evaluationMaskName(expr).isEmpty)
            }
            Seq(
              "SELECT hex(unbase64(bad)) FROM masked LIMIT 1",
              "SELECT bad FROM masked WHERE unbase64(bad) <=> X'616263' LIMIT 1",
              "SELECT hex(unbase64(concat(bad, ''))) FROM masked LIMIT 1",
              "SELECT hex(to_binary(bad, 'base64')) FROM masked LIMIT 1").foreach { query =>
              checkSparkAnswerAndFallbackReason(query, limitReason)
            }
            decodeErrors("SELECT hex(unbase64(bad)) FROM masked")
            decodeErrors("SELECT hex(unbase64(bad)) FROM masked WHERE bad = 'A' LIMIT 1")
            checkSparkAnswerAndOperator(sql("SELECT hex(unbase64(bad)) FROM valid"))
            checkSparkAnswerAndOperator(
              sql("SELECT /*+ BROADCAST(r) */ l.* FROM inner_left l " +
                "INNER JOIN valid r ON l.k = r.k AND unbase64(r.bad) > l.expected"))
            // AQE can remove this sort after Partial materializes; offset-only collection stays native.
            checkSparkAnswerAndOperator(
              sql("SELECT unbase64(bad) AS decoded, collect_list(k) FROM one_group " +
                "GROUP BY bad ORDER BY decoded OFFSET 1"))
          }
        }
      }
    }
  }

  test("whole-tree JVM dispatch preserves an unregistered decoder subclass below LIMIT") {
    val name = "comet_test_unbase64"
    spark.sessionState.functionRegistry.createOrReplaceTempFunction(
      name,
      children => new LimitUnBase64(children.head),
      "scala_udf")
    try {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true") {
        withInputs("subclass_input" -> "SELECT * FROM VALUES ('YQ=='), ('A') AS t(encoded)") {
          val query = s"SELECT format_string('%d', length($name(encoded))) FROM subclass_input"
          val native = sql(query).queryExecution.executedPlan.collect {
            case p: CometProjectExec => p
          }
          assert(
            native.size == 1 && native.head.nativeOp.getProjection
              .getProjectList(0)
              .hasJvmScalarUdf)
          val df = sql(s"$query LIMIT 1")
          val (_, plan) = checkSparkAnswerAndFallbackReason(df, limitReason)
          assert(count[ProjectExec](plan) == 1 && count[CometProjectExec](plan) == 0)
          assert(nativeScans(plan) == 1)
          checkAnswer(df, Seq(Row("1")))
          decodeErrors(s"$query LIMIT 2")
        }
      }
    } finally {
      spark.sessionState.catalog.dropTempFunction(name, ignoreIfNotExists = true)
    }
  }

  test("unbase64 LIMIT masks survive physical boundaries and native reuse") {
    withSQLConf(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
      withModes { (_, _) =>
        val input = sparkPlan("SELECT * FROM VALUES (1, 'YWJj'), (2, 'A') AS t(id, bad)")
        def project = ProjectExec(
          Seq(input.output.head, Alias(UnBase64(input.output(1)), "decoded")()),
          input)
        val staged = applyRule(
          GlobalLimitExec(1, ShuffleExchangeExec(SinglePartition, LocalLimitExec(1, project))))
        assert(count[CometGlobalLimitExec](staged) == 1)
        assert(count[CometLocalTableScanExec](staged) == 1)
        val exchange = collect(staged) { case s: ShuffleExchangeLike => s }.head
        for (plan <- Seq(applyRule(staged), applyRule(exchange))) {
          assert(count[LocalLimitExec](plan) == 1)
          assert(count[ProjectExec](plan) == 1)
          assert(count[CometProjectExec](plan) == 0)
        }
        val barriers: Seq[SparkPlan => SparkPlan] = Seq(
          child => SortExec(Seq(SortOrder(child.output.head, Ascending)), global = false, child),
          child => ShuffleExchangeExec(SinglePartition, child))
        barriers.foreach { barrier =>
          val blocking = barrier(project)
          // Materialization is safe for both fresh plans and native subtrees reused by AQE.
          for (child <- Seq(blocking, applyRule(blocking))) {
            val plan = applyRule(LocalLimitExec(1, child))
            withClue(blocking.nodeName) {
              assert(count[CometProjectExec](plan) == 1)
              assert(count[ProjectExec](plan) == 0)
            }
          }
        }
        val bridges: Seq[SparkPlan => SparkPlan] = Seq(
          child => child,
          child => RowToColumnarExec(ColumnarToRowExec(child)),
          child => CometSparkToColumnarExec(CometColumnarToRowExec(child)),
          child => CometSparkToColumnarExec(CometNativeColumnarToRowExec(child)))
        bridges.foreach { bridge =>
          val decoded = project
          val native = applyRule(ProjectExec(decoded.output, decoded))
          assert(count[CometProjectExec](native) == 2)
          val logicalStage = LogicalQueryStage(LocalRelation(native.output), native)
          native.setLogicalLink(logicalStage)
          val transformed = applyRule(GlobalLimitExec(1, bridge(native)))
          for (plan <- Seq(transformed, applyRule(transformed))) {
            assert(count[ProjectExec](plan) == 2)
            assert(count[CometProjectExec](plan) == 0)
            assert(collect(plan) { case r: RowToColumnarTransition => r }.isEmpty)
            val link = plan.children.head.getTagValue(SparkPlan.LOGICAL_PLAN_TAG)
            assert(link.exists(_ eq logicalStage))
          }
        }
      }
    }
  }

  test("unbase64 chooses compatible aggregate buffers before AQE materialization") {
    withSQLConf(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
      withModes { (aqe, _) =>
        for {
          decoderInResult <- Seq(false, true)
          boundary <- Seq("direct", "sort", "shuffle", "top-k", "offset")
        } {
          val result = if (decoderInResult) {
            "unbase64(bad) AS decoded, collect_list(k) AS collected"
          } else {
            "bad, collect_list(unbase64(bad)) AS collected"
          }
          val aggregate = sparkPlan(
            s"SELECT $result FROM " +
              "VALUES (1, 'YWJj'), (2, 'YWJj') AS t(k, bad) GROUP BY bad")
          assert(count[ObjectHashAggregateExec](aggregate) == 2)
          val native = applyRule(aggregate)
          assert(count[CometHashAggregateExec](native) == 2)
          for (child <- Seq(aggregate, native)) {
            val order = Seq(SortOrder(child.output.head, Ascending))
            val plan = boundary match {
              case "direct" => CollectLimitExec(1, child)
              case "sort" => CollectLimitExec(1, SortExec(order, global = false, child))
              case "shuffle" => CollectLimitExec(1, ShuffleExchangeExec(SinglePartition, child))
              case "top-k" => TakeOrderedAndProjectExec(1, order, child.output, child)
              case "offset" =>
                CollectLimitExec(-1, SortExec(order, global = false, child), offset = 1)
            }
            // Final does not reevaluate decoder inputs, and offset-only never stops early.
            val sparkBuffers = decoderInResult &&
              (boundary == "direct" || (aqe && boundary != "offset"))
            val transformed = applyRule(plan)
            withClue(s"decoderInResult=$decoderInResult, boundary=$boundary: ") {
              for (replanned <- Seq(transformed, applyRule(transformed))) {
                assert(aggregateCounts(replanned) == (if (sparkBuffers) (0, 2) else (2, 0)))
              }
            }
          }
        }
      }
    }
  }

  test("unbase64 keeps aggregate buffers compatible through an actual AQE join change") {
    withSQLConf(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "10485760",
      "spark.sql.join.preferSortMergeJoin" -> "true",
      "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold" -> "0",
      CometConf.COMET_FORCE_SHJ.key -> "false") {
      // One-file inputs keep the malformed suffix last; two bad groups retain LIMIT.
      withInputs(
        "aqe_valid" -> """SELECT 1 AS k, base64(cast(cast(id AS STRING) AS BINARY)) AS bad
                        |FROM range(0, 12, 1, 1)""".stripMargin,
        "aqe_bad_last" ->
          "SELECT k, CASE WHEN bad='MTE=' THEN 'A' ELSE bad END AS bad FROM aqe_valid",
        "aqe_bad_only" -> """SELECT 1 AS k, CASE WHEN id=0 THEN 'A' ELSE 'B' END AS bad
                           |FROM range(0, 2, 1, 1)""".stripMargin,
        "aqe_right" -> "SELECT 'MA==' AS bad") {
        withModes { (aqe, _) =>
          for (join <- Seq("INNER", "LEFT SEMI")) {
            withClue(s"join=$join: ") {
              def query(input: String, decoder: String = "bad", agg: String = "collect_list(k)")
                  : String =
                s"""SELECT a.decoded, a.collected FROM (
                   |  SELECT bad, unbase64($decoder) AS decoded, $agg AS collected
                   |  FROM $input GROUP BY bad
                   |) a $join JOIN aqe_right b ON a.bad = b.bad LIMIT 1""".stripMargin
              def checkSuccess(input: String, agg: String = "collect_list(k)"): SparkPlan = {
                val text = query(input, agg = agg)
                var expected = Seq.empty[Row]
                withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
                  expected = sql(text).collect().toSeq
                }
                val df = sql(text)
                val initial = df.queryExecution.executedPlan
                val initialBuffers = aggregateCounts(initial)
                assert(
                  collect(initial) {
                    case p if original(p).isInstanceOf[SortMergeJoinExec] => p
                  }.size == 1,
                  initial.toString)
                checkAnswer(df, expected)
                if (aqe && agg == "collect_list(k)") {
                  assert(initialBuffers == (0, 2), initial.toString)
                }
                val plan = df.queryExecution.executedPlan
                if (aqe) {
                  assert(plan.asInstanceOf[AdaptiveSparkPlanExec].isFinalPlan)
                  assert(
                    collect(plan) {
                      case j: BroadcastHashJoinExec => j.buildSide
                      case j: CometBroadcastHashJoinExec => j.buildSide
                    } == Seq(BuildRight),
                    plan.toString)
                }
                plan
              }
              val valid = checkSuccess("aqe_valid")
              assert(aggregateCounts(valid) == (if (aqe) (0, 2) else (2, 0)))
              // SEMI streams the left side; INNER may materialize a left broadcast candidate.
              if (aqe && join == "LEFT SEMI") checkSuccess("aqe_bad_last")
              else if (!aqe) decodeErrors(query("aqe_bad_last"))
              decodeErrors(query("aqe_valid", "CASE WHEN bad='MA==' THEN 'A' ELSE bad END"))
              // ObjectHash evaluates results even before a nonmatching join discards them.
              decodeErrors(query("aqe_bad_only"))
              val compatible = checkSuccess("aqe_valid", "max(k)")
              assert(count[CometHashAggregateExec](compatible) > 0)
              if (aqe && join == "LEFT SEMI") {
                // HashAggregate can leave this nonmatching output projection lazy.
                checkSuccess("aqe_bad_only", "max(k)")
              }
            }
          }
        }
      }
    }
  }

  test("ordered top-K masks child decoders but keeps its output projection native") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "true",
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        ("org.apache.spark.sql.catalyst.optimizer.EliminateSorts," +
          "org.apache.spark.sql.catalyst.optimizer.CollapseProject")) {
      withModes { (aqe, _) =>
        for {
          location <- Seq("child project", "child filter", "output projection")
          comet <- Seq(false, true)
          asRDD <- Seq(false, true)
        } {
          withSQLConf(CometConf.COMET_ENABLED.key -> comet.toString) {
            withClue(s"location=$location, Comet=$comet, RDD=$asRDD: ") {
              // Each ordered partition must stop before its fourth, malformed row.
              def query(limit: Int): DataFrame = {
                val input = spark
                  .range(0, 8, 1, 2)
                  .selectExpr("id", "if (id % 4 < 3, 'YWJj', 'A') AS encoded")
                val ordered = location match {
                  case "child project" =>
                    input.selectExpr("id", "hex(unbase64(encoded)) AS decoded").orderBy("id")
                  case "child filter" =>
                    input.filter("unbase64(encoded) = X'616263'").orderBy("id")
                  case _ =>
                    input.orderBy("id").selectExpr("id", "hex(unbase64(encoded)) AS decoded")
                }
                ordered.limit(limit)
              }
              def run(df: DataFrame): Seq[Row] =
                (if (asRDD) df.rdd.collect() else df.collect()).toSeq

              val df = query(1)
              val inOutput = location == "output projection"
              assert(
                run(df) == Seq(Row(0L, if (location == "child filter") "YWJj" else "616263")))
              val plan = df.queryExecution.executedPlan
              assert(plan.isInstanceOf[AdaptiveSparkPlanExec] == aqe)
              val topKs = collect(plan) {
                case node if original(node).isInstanceOf[TakeOrderedAndProjectExec] => node
              }
              assert(topKs.size == 1, plan.toString)
              val topK = original(topKs.head).asInstanceOf[TakeOrderedAndProjectExec]
              val child = topKs.head.children.head
              assert(SortOrder.orderingSatisfies(child.outputOrdering, topK.sortOrder))
              assert(child.outputPartitioning.numPartitions == 2)
              assert(collect(child) {
                case _: SortExec | _: CometSortExec | _: ShuffleExchangeLike => true
              }.isEmpty)
              val decoders = collect(child) {
                case p: ProjectExec if p.projectList.exists(hasDecoder) => p
                case f: FilterExec if hasDecoder(f.condition) => f
              }
              assert(decoders.size == (if (inOutput) 0 else 1), plan.toString)
              assert(topK.projectList.exists(hasDecoder) == inOutput)
              assert(
                topKs.head.isInstanceOf[CometTakeOrderedAndProjectExec] == (comet && inOutput))
              decodeError(run(query(4)))
            }
          }
        }
      }
    }
  }

  test("first-match joins preserve candidate order and leave decoder-free joins native") {
    val strategies = Seq(
      ("BROADCAST", "=", classOf[BroadcastHashJoinExec], classOf[CometBroadcastHashJoinExec]),
      ("SHUFFLE_HASH", "=", classOf[ShuffledHashJoinExec], classOf[CometHashJoinExec]),
      ("MERGE", "=", classOf[SortMergeJoinExec], classOf[CometSortMergeJoinExec]),
      (
        "BROADCAST",
        "<=",
        classOf[BroadcastNestedLoopJoinExec],
        classOf[CometBroadcastNestedLoopJoinExec]))
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SORT_MERGE_JOIN_WITH_JOIN_FILTER_ENABLED.key -> "true") {
      withInputs(
        "mask_left" -> "SELECT * FROM VALUES (1, X'616262'), (2, X'616262') AS t(k, expected)",
        "mask_forward" -> "SELECT * FROM VALUES (1, 'YWJj'), (1, 'A') AS t(k, bad)",
        "mask_reverse" -> "SELECT * FROM VALUES (1, 'A'), (1, 'YWJj') AS t(k, bad)") {
        withModes { (aqe, _) =>
          for ((hint, equality, sparkJoin, nativeJoin) <- strategies;
            kind <- Seq("SEMI", "ANTI")) {
            withClue(s"$hint, predicate=$equality, LEFT $kind: ") {
              // Hash joins visit duplicate keys backwards; merge and nested-loop visit forwards.
              val table =
                if (hint == "MERGE" || equality == "<=") "mask_forward" else "mask_reverse"
              val reversed = if (table == "mask_forward") "mask_reverse" else "mask_forward"
              def query(residual: String, input: String = table): String =
                s"""SELECT /*+ $hint(r) */ l.k FROM mask_left l LEFT $kind JOIN $input r
                   |ON l.k $equality r.k $residual""".stripMargin
              val (sparkPlan, cometPlan) = checkSparkAnswerAndFallbackReason(
                query("AND unbase64(r.bad) > l.expected"),
                joinReason)
              Seq(sparkPlan, cometPlan).foreach { plan =>
                assert(plan.isInstanceOf[AdaptiveSparkPlanExec] == aqe)
                if (aqe) assert(plan.asInstanceOf[AdaptiveSparkPlanExec].isFinalPlan)
                val joins = collect(plan) { case node if sparkJoin.isInstance(node) => node }
                assert(
                  joins.size == 1 && joins.head.expressions.exists(hasDecoder),
                  plan.toString)
                assert(count[CometBroadcastExchangeExec](plan) == 0, plan.toString)
                assert(
                  count[BroadcastExchangeLike](plan) ==
                    (if (hint == "BROADCAST") 1 else 0),
                  plan.toString)
              }
              assert(collect(cometPlan) { case _: CometScanExec | _: CometNativeScanExec =>
                true
              }.size == 2)
              decodeErrors(query("AND unbase64(r.bad) < l.expected"))
              decodeErrors(query("AND unbase64(r.bad) > l.expected", reversed))
              val (_, nativePlan) = checkSparkAnswerAndOperator(query(""))
              assert(collect(nativePlan) { case p if nativeJoin.isInstance(p) => p }.size == 1)
            }
          }
        }
      }
    }
  }

  test("window group limits drain partitions unless an outer LIMIT stops consumption") {
    assume(isSpark35Plus, "WindowGroupLimit requires Spark 3.5+")
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        ("org.apache.spark.sql.catalyst.optimizer.EliminateSorts," +
          "org.apache.spark.sql.catalyst.optimizer.CollapseProject"),
      "spark.sql.optimizer.windowGroupLimitThreshold" -> "1000",
      "spark.sql.execution.topKSortFallbackThreshold" -> "0",
      CometConf.COMET_EXEC_WINDOW_GROUP_LIMIT_ENABLED.key -> "true") {
      val decoder = "unbase64(IF(id = 0, 'YWJj', 'A'))"
      def unpartitioned(project: Boolean, limit: Int = 1, order: String = "id"): String = {
        val decoded =
          if (project)
            s", first_value($decoder) OVER (ORDER BY $order " +
              "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS decoded"
          else ""
        val filter = if (project) "" else s"WHERE hex($decoder) = '616263'"
        s"""SELECT * FROM (
           |SELECT id, row_number() OVER (ORDER BY $order) AS rn $decoded
           |FROM range(0, 2, 1, 1) $filter) WHERE rn <= $limit""".stripMargin
      }
      def partitioned(malformed: Boolean, limit: String = ""): String = {
        val encoded =
          if (malformed) "IF(id = 15, 'A', 'YWJj')"
          else "IF(id % 2 = 0, 'YWJj', 'YWFh')"
        // Sort below the decoder; put the bad row beyond WindowExec's group lookahead.
        s"""SELECT k, id, hex(decoded) FROM (
           |SELECT k, id, decoded, row_number() OVER (PARTITION BY k ORDER BY id) AS rn
           |FROM (SELECT k, id, unbase64(bad) AS decoded FROM (
           |SELECT id, id DIV 4 AS k, $encoded AS bad FROM range(0, 16, 1, 1)
           |ORDER BY k, id))) WHERE rn <= 1 $limit""".stripMargin
      }
      def checkInput(plan: SparkPlan, partitioned: Boolean, native: Boolean): Unit = {
        val windows = collect(plan) {
          case node if ShimCometWindowGroupLimit.extract(original(node)).isDefined => node
        }
        assert(windows.size == 1, plan.toString)
        val window = windows.head
        val fields = ShimCometWindowGroupLimit.extract(original(window)).get
        assert(fields.partitionSpec.nonEmpty == partitioned)
        assert(window.isInstanceOf[CometExec] == native, plan.toString)
        val child = window.children.head match {
          case w: WholeStageCodegenExec => w.child
          case other => other
        }
        val required = original(window).requiredChildOrdering.head
        assert(SortOrder.orderingSatisfies(child.outputOrdering, required))
        assert(child.isInstanceOf[CometExec] == native, plan.toString)
        assert(original(child).expressions.exists(hasDecoder), plan.toString)
      }
      withModes { (_, dispatch) =>
        for (project <- Seq(false, true)) {
          val (sparkPlan, cometPlan) =
            checkSparkAnswerAndFallbackReason(unpartitioned(project), limitReason)
          Seq(sparkPlan, cometPlan).foreach(checkInput(_, partitioned = false, native = false))
        }
        decodeErrors(unpartitioned(project = false, limit = 2))
        val sorted = unpartitioned(project = false, order = "id DESC")
        if (dispatch) {
          withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
            val unordered = collect(sparkPlan(unpartitioned(project = false))) {
              case w if ShimCometWindowGroupLimit.extract(w).isDefined => w
            }.head.transformExpressions { case order: SortOrder =>
              order.copy(direction = Descending)
            }
            assert(
              !SortOrder.orderingSatisfies(
                unordered.children.head.outputOrdering,
                unordered.requiredChildOrdering.head))
            assert(collect(applyRule(unordered)) {
              case f: CometFilterExec if hasDecoder(f.condition) => f
            }.size == 1)
          }
        }
        decodeErrors(sorted)
        val (sparkValid, cometValid) = checkSparkAnswerAndOperator(partitioned(malformed = false))
        checkInput(sparkValid, partitioned = true, native = false)
        checkInput(cometValid, partitioned = true, native = true)
        decodeErrors(partitioned(malformed = true))
        val (sparkLimited, cometLimited) = checkSparkAnswerAndFallbackReason(
          partitioned(malformed = true, limit = "LIMIT 1"),
          limitReason)
        Seq(sparkLimited, cometLimited).foreach(checkInput(_, partitioned = true, native = false))
        checkInput(
          applyRule(LocalLimitExec(1, stripAQEPlan(cometValid))),
          partitioned = true,
          native = false)
      }
    }
  }
}

// Preserve the subclass through binding without capturing the suite in the dispatched expression.
private[serde] class LimitUnBase64(input: Expression, strict: Boolean = false)
    extends UnBase64(input, strict) {
  override protected def withNewChildInternal(newChild: Expression): UnBase64 =
    new LimitUnBase64(newChild, failOnError)
}
