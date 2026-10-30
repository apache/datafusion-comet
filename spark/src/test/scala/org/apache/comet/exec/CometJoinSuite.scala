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

package org.apache.comet.exec

import java.nio.charset.StandardCharsets

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode}
import org.apache.spark.sql.{Column, CometTestBase, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{And, Ascending, AttributeReference, BoundReference, Cast, Descending, DynamicPruningExpression, Expression, IsNotNull, Literal, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, Join, JoinHint, LogicalPlan, NO_BROADCAST_HASH, PREFER_SHUFFLE_HASH}
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometBroadcastHashJoinExec, CometBroadcastNestedLoopJoinExec, CometFilterExec, CometHashJoin, CometHashJoinExec, CometNativeScanExec, CometProjectExec, CometRangeExec, CometSampleExec, CometSortExec, CometSortMergeJoinExec, CometSparkToColumnarExec, CometUnionExec, CometWindowExec, CometWindowGroupLimitExec}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.{CoalesceExec, ColumnarToRowTransition, CommandResultExec, InputAdapter, LocalTableScanExec, SortExec, SparkPlan, SparkStrategy, SQLExecution, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions.{array, count, lit, struct, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, MetadataBuilder, StructField, StructType}

import org.apache.comet.{CometConf, CometExecIterator, CometExplainInfo, ExtendedExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.{hasFallbackReason, isSpark35Plus, isSpark40Plus}
import org.apache.comet.rules.CometShuffledHashJoinStrategy
import org.apache.comet.serde.OperatorOuterClass

class CometJoinSuite extends CometTestBase {

  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(CometConf.COMET_SHUFFLE_ENABLED.key -> "true") {
        testFun
      }
    }
  }

  for ((hint, joinClass) <- Seq(
      "SHUFFLE_HASH" -> classOf[CometHashJoinExec],
      "MERGE" -> classOf[CometSortMergeJoinExec],
      "BROADCAST" -> classOf[CometBroadcastHashJoinExec]);
    adaptive <- Seq(false, true)) {
    test(s"join identity preserves exchange reuse: $hint, AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_MODE.key -> "native") {
        withParquetTable(Seq((0, 10), (1, 11), (2, 12)), "l") {
          withParquetTable(Seq((0, 100), (1, 101)), "r") {
            def branch(joinType: String): DataFrame =
              sql(s"""SELECT /*+ $hint(r) */ l._1, l._2 FROM l
                     |$joinType JOIN r ON l._1 = r._1
                     |WHERE l._1 IS NOT NULL""".stripMargin)
                .repartition(2, $"_2")

            def nativeJoin(df: DataFrame): SparkPlan =
              collectFirst(df.queryExecution.executedPlan) {
                case join if joinClass.isInstance(join) => join
              }.getOrElse(fail(s"Expected native $hint join"))

            val semi = branch("LEFT SEMI")
            val anti = branch("LEFT ANTI")
            // The explicit null check defeats InferFiltersFromConstraints masking this bug.
            // Shuffle on the payload: a join-key shuffle can be optimized away.
            checkSparkAnswerAndOperator(semi.unionAll(anti), classOf[ReusedExchangeExec])
            checkAnswer(semi.unionAll(anti), Seq(Row(0, 10), Row(1, 11), Row(2, 12)))
            assert(!nativeJoin(semi).sameResult(nativeJoin(anti)))

            val same = branch("LEFT SEMI")
            assert(nativeJoin(semi).sameResult(nativeJoin(same)))
            assert(nativeJoin(semi).semanticHash() == nativeJoin(same).semanticHash())
            val (_, reusedPlan) =
              checkSparkAnswerAndOperator(semi.unionAll(same), classOf[ReusedExchangeExec])
            if (adaptive) {
              assert(reusedPlan.isInstanceOf[AdaptiveSparkPlanExec])
            }
            assertExchangeReuseOver(reusedPlan, "Expected equivalent post-join exchange reuse") {
              case join if joinClass.isInstance(join) => join
            }
          }
        }
      }
    }
  }

  for (adaptive <- Seq(false, true)) {
    test(s"null-aware anti join identity preserves exchange reuse: AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        CometConf.COMET_SHUFFLE_MODE.key -> "native") {
        withParquetTable(Seq((Some(0), 10), (Some(1), 11), (None, 12)), "l") {
          withParquetTable(Seq(Tuple1(0)), "r") {
            def branch(predicate: String): DataFrame =
              sql(s"SELECT l._1, l._2 FROM l WHERE $predicate").repartition(2, $"_2")
            val anti = branch(
              "NOT EXISTS (SELECT /*+ BROADCAST(r) */ 1 FROM r " +
                "WHERE r._1 = l._1 AND r._1 IS NOT NULL)")
            val nullAware = branch("l._1 NOT IN (SELECT r._1 FROM r WHERE r._1 IS NOT NULL)")
            checkSparkAnswerAndOperator(anti.unionAll(nullAware), classOf[ReusedExchangeExec])
            checkAnswer(anti.unionAll(nullAware), Seq(Row(1, 11), Row(1, 11), Row(null, 12)))
            def nativeJoin(df: DataFrame): CometBroadcastHashJoinExec =
              collectFirst(df.queryExecution.executedPlan) {
                case join: CometBroadcastHashJoinExec => join
              }.getOrElse(fail("Expected native broadcast hash join"))
            assert(!nativeJoin(anti).sameResult(nativeJoin(nullAware)))
            val same = branch("l._1 NOT IN (SELECT r._1 FROM r WHERE r._1 IS NOT NULL)")
            assert(nativeJoin(nullAware).sameResult(nativeJoin(same)))
            assert(nativeJoin(nullAware).semanticHash() == nativeJoin(same).semanticHash())
            val (_, reusedPlan) =
              checkSparkAnswerAndOperator(nullAware.unionAll(same), classOf[ReusedExchangeExec])
            if (adaptive) {
              assert(reusedPlan.isInstanceOf[AdaptiveSparkPlanExec])
            }
            assertExchangeReuseOver(reusedPlan, "Expected equivalent null-aware join reuse") {
              case join: CometBroadcastHashJoinExec => join
            }
          }
        }
      }
    }
  }

  test("join - self join") {
    val df1 = testData.select(testData("key")).as("df1")
    val df2 = testData.select(testData("key")).as("df2")

    checkAnswer(
      df1.join(df2, $"df1.key" === $"df2.key"),
      sql("SELECT a.key, b.key FROM testData a JOIN testData b ON a.key = b.key")
        .collect()
        .toSeq)
  }

  test("SortMergeJoin with TimestampType key runs natively") {
    withSQLConf(
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Kathmandu",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true") {
      withTable("t1", "t2") {
        sql("CREATE TABLE t1(name STRING, time TIMESTAMP) USING PARQUET")
        sql(
          "INSERT OVERWRITE t1 VALUES " +
            "('a', timestamp'2019-01-01 11:11:11'), " +
            "('b', timestamp'2020-05-05 05:05:05')")

        sql("CREATE TABLE t2(name STRING, time TIMESTAMP) USING PARQUET")
        sql(
          "INSERT OVERWRITE t2 VALUES " +
            "('a', timestamp'2019-01-01 11:11:11'), " +
            "('c', timestamp'2021-07-07 07:07:07')")

        checkSparkAnswerAndOperator(
          sql("SELECT * FROM t1 JOIN t2 ON t1.time = t2.time"),
          Seq(classOf[CometSortMergeJoinExec]))
      }
    }
  }

  test("SortMergeJoin with floating-point key runs natively under strict floating point") {
    // CometSortOrder is the admission gate for the sort orders this join synthesizes, so
    // narrowing it for scalar floats (#5506) admits FP-keyed sort-merge joins under strict mode
    // too. Comet does not normalize the join keys itself: it compares `join_on`, and correctness
    // rests on Catalyst's NormalizeFloatingNumbers having already wrapped both sides. This test
    // pins that, so a future change to that rule fails here rather than silently.
    //
    // The fixtures are local relations rather than Parquet tables because a Parquet round trip
    // canonicalizes every NaN payload, which would collapse the two NaN cases into one.
    val left = Seq(
      (1, -0.0d),
      (2, 0.0d),
      (3, 1.0d),
      (4, java.lang.Double.longBitsToDouble(0x7ff8000000000002L)),
      (5, java.lang.Double.longBitsToDouble(0xfff8000000000002L)))
    val right = Seq(
      (10, 0.0d),
      (20, -0.0d),
      (30, 1.0d),
      (40, java.lang.Double.longBitsToDouble(0xfff8000000000002L)),
      (50, Double.NaN))

    withSQLConf(
      CometConf.COMET_EXEC_STRICT_FLOATING_POINT.key -> "true",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true") {
      withTempView("fp_left", "fp_right") {
        left.toDF("lid", "k").createOrReplaceTempView("fp_left")
        right.toDF("rid", "k").createOrReplaceTempView("fp_right")

        // LocalTableScanExec has no native counterpart enabled by default and is only the source
        // of the unmodified key bits here.
        checkSparkAnswerAndOperator(
          sql("SELECT l.lid, r.rid FROM fp_left l JOIN fp_right r ON l.k = r.k"),
          Seq(classOf[CometSortMergeJoinExec]),
          classOf[LocalTableScanExec])
      }
    }
  }

  test("SortMergeJoin with TimestampType key supports outer joins") {
    withSQLConf(
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Kathmandu",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true") {
      withTable("t1", "t2") {
        sql("CREATE TABLE t1(id INT, time TIMESTAMP) USING PARQUET")
        sql(
          "INSERT OVERWRITE t1 VALUES " +
            "(1, timestamp'2019-01-01 11:11:11'), " +
            "(2, timestamp'2020-05-05 05:05:05'), " +
            "(3, timestamp'2021-07-07 07:07:07')")

        sql("CREATE TABLE t2(id INT, time TIMESTAMP) USING PARQUET")
        sql(
          "INSERT OVERWRITE t2 VALUES " +
            "(10, timestamp'2019-01-01 11:11:11'), " +
            "(20, timestamp'2022-02-02 02:02:02')")

        for (joinType <- Seq("LEFT OUTER", "RIGHT OUTER", "FULL OUTER")) {
          checkSparkAnswerAndOperator(
            sql(s"SELECT * FROM t1 $joinType JOIN t2 ON t1.time = t2.time"),
            Seq(classOf[CometSortMergeJoinExec]))
        }
      }
    }
  }

  test("SortMergeJoin with composite (string, timestamp) key runs natively") {
    withSQLConf(
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true") {
      withTable("t1", "t2") {
        sql("CREATE TABLE t1(name STRING, time TIMESTAMP) USING PARQUET")
        sql(
          "INSERT OVERWRITE t1 VALUES " +
            "('a', timestamp'2019-01-01 11:11:11'), " +
            "('b', timestamp'2019-01-01 11:11:11'), " +
            "('a', timestamp'2020-05-05 05:05:05')")

        sql("CREATE TABLE t2(name STRING, time TIMESTAMP) USING PARQUET")
        sql(
          "INSERT OVERWRITE t2 VALUES " +
            "('a', timestamp'2019-01-01 11:11:11'), " +
            "('b', timestamp'2020-05-05 05:05:05'), " +
            "('a', timestamp'2020-05-05 05:05:05')")

        checkSparkAnswerAndOperator(
          sql(
            "SELECT * FROM t1 JOIN t2 " +
              "ON t1.name = t2.name AND t1.time = t2.time"),
          Seq(classOf[CometSortMergeJoinExec]))
      }
    }
  }

  test("SortMergeJoin with nullable TimestampType key runs natively") {
    withSQLConf(
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true") {
      withTable("t1", "t2") {
        sql("CREATE TABLE t1(id INT, time TIMESTAMP) USING PARQUET")
        sql(
          "INSERT OVERWRITE t1 VALUES " +
            "(1, timestamp'2019-01-01 11:11:11'), " +
            "(2, CAST(NULL AS TIMESTAMP)), " +
            "(3, timestamp'2020-05-05 05:05:05')")

        sql("CREATE TABLE t2(id INT, time TIMESTAMP) USING PARQUET")
        sql(
          "INSERT OVERWRITE t2 VALUES " +
            "(10, timestamp'2019-01-01 11:11:11'), " +
            "(20, CAST(NULL AS TIMESTAMP)), " +
            "(30, timestamp'2022-02-02 02:02:02')")

        // Inner join: NULL = NULL must not match in Spark semantics.
        checkSparkAnswerAndOperator(
          sql("SELECT * FROM t1 JOIN t2 ON t1.time = t2.time"),
          Seq(classOf[CometSortMergeJoinExec]))

        // Full outer join: NULL-keyed rows from both sides surface as unmatched.
        checkSparkAnswerAndOperator(
          sql("SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.time = t2.time"),
          Seq(classOf[CometSortMergeJoinExec]))
      }
    }
  }

  test("SortMergeJoin with TimestampType key across mixed write-time session timezones") {
    // TimestampType is an instant (UTC microseconds); only the parsing of literal
    // strings depends on the session timezone. Writing each side under a different
    // session zone with wall-clock literals that resolve to the same UTC instant
    // must still produce a join match.
    withSQLConf(
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true") {
      withTable("t1", "t2") {
        // t1 written in America/Los_Angeles. 03:11:11 -0800 == 11:11:11 UTC.
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
          sql("CREATE TABLE t1(name STRING, time TIMESTAMP) USING PARQUET")
          sql(
            "INSERT OVERWRITE t1 VALUES " +
              "('a', timestamp'2019-01-01 03:11:11'), " +
              "('b', timestamp'2020-05-04 22:05:05')")
        }

        // t2 written in Asia/Tokyo. 20:11:11 +0900 == 11:11:11 UTC, so the 'a' and
        // 'a2' rows share a UTC instant with t1's 'a' row.
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Tokyo") {
          sql("CREATE TABLE t2(name STRING, time TIMESTAMP) USING PARQUET")
          sql(
            "INSERT OVERWRITE t2 VALUES " +
              "('a', timestamp'2019-01-01 20:11:11'), " +
              "('c', timestamp'2021-07-07 16:07:07')")
        }

        // Read at a third session timezone to confirm the equality is on the
        // stored UTC instant rather than the displayed wall-clock value.
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
          checkSparkAnswerAndOperator(
            sql("SELECT * FROM t1 JOIN t2 ON t1.time = t2.time"),
            Seq(classOf[CometSortMergeJoinExec]))
        }
      }
    }
  }

  private def nativeHashJoins(plan: SparkPlan): Seq[SparkPlan] = {
    collect(plan) {
      case join: CometBroadcastHashJoinExec => join
      case join: CometHashJoinExec => join
    }
  }

  for {
    strategy <- Seq("BROADCAST", "SHUFFLE_HASH")
    buildLeft <- Seq(false, true)
    adaptive <- Seq(false, true)
  } {
    test(
      s"join dynamic filter prunes probe rows: $strategy, buildLeft=$buildLeft, AQE=$adaptive") {
      withSQLConf(
        CometConf.COMET_BATCH_SIZE.key -> "16",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        withParquetTable((0 until 1000).map(i => (i, i + 10000L)), "dynamic_probe") {
          // Sparse keys inside the bounds exercise membership, not just min/max.
          // Repeated key 20 must still produce both distinct payloads.
          withParquetTable(
            Seq((10, 101L), (20, 201L), (20, 202L), (900, 901L)),
            "dynamic_build") {
            val from = if (buildLeft) {
              "dynamic_build b JOIN dynamic_probe p ON b._1 = p._1"
            } else {
              "dynamic_probe p JOIN dynamic_build b ON p._1 = b._1"
            }
            val query = s"SELECT /*+ $strategy(b) */ p._2, b._2, p._1 FROM $from"
            var unfilteredProbeRows = 0L
            for (enabled <- Seq(false, true)) {
              withSQLConf(
                CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ENABLED.key -> enabled.toString) {
                val (_, plan) = checkSparkAnswerAndOperator(sql(query))
                val joins = nativeHashJoins(plan)
                assert(joins.size == 1, s"Expected one executed native hash join:\n$plan")
                val join = joins.head
                val expectedSide = if (buildLeft) BuildLeft else BuildRight
                join match {
                  case hash: CometBroadcastHashJoinExec =>
                    assert(strategy == "BROADCAST")
                    assert(hash.buildSide == expectedSide)
                    assert(hash.nativeOp.getHashJoin.getDynamicFilterEnabled == enabled)
                  case hash: CometHashJoinExec =>
                    assert(strategy == "SHUFFLE_HASH")
                    assert(hash.buildSide == expectedSide)
                    assert(hash.nativeOp.getHashJoin.getDynamicFilterEnabled == enabled)
                  case other => fail(s"Unexpected hash join: $other")
                }
                assert(join.metrics("output_rows").value == 4L)
                val probeRows = join.metrics("input_rows").value
                if (enabled) {
                  val evaluated = join.metrics("dynamic_filter_join_rows_evaluated").value
                  val pruned = join.metrics("dynamic_filter_join_rows_pruned").value
                  val bypassed = join.metrics("dynamic_filter_join_rows_bypassed").value
                  assert(evaluated > 0L && pruned > 0L)
                  assert(probeRows + pruned == evaluated + bypassed)
                  assert(probeRows < unfilteredProbeRows)
                  assert(evaluated + bypassed <= unfilteredProbeRows)
                  assert(join.metrics("dynamic_filter_join_eval_time").value > 0L)
                } else {
                  assert(!join.metrics.contains("dynamic_filter_join_rows_pruned"))
                  unfilteredProbeRows = probeRows
                }
              }
            }
          }
        }
      }
    }
  }

  test("join dynamic filter rejects rows before an intermediate join") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.JOIN_REORDER_ENABLED.key -> "false",
      SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
      CometConf.COMET_BATCH_SIZE.key -> "128") {
      withParquetTable((0 until 1000).map(i => (i, i.toLong)), "early_fact") {
        withParquetTable((0 until 2000).map(i => (i % 1000, i)), "early_dimension") {
          withParquetTable(Seq((42, 1), (42, 2)), "early_selection") {
            for (buildLeft <- Seq(false, true); enabled <- Seq(false, true)) {
              val from = if (buildLeft) {
                "early_dimension d JOIN early_fact f"
              } else {
                "early_fact f JOIN early_dimension d"
              }
              val query = "SELECT /*+ BROADCAST(d), BROADCAST(s) */ " +
                "f._1 AS selected_key, f._2 AS payload, d._2 AS detail, s._2 AS selection " +
                s"FROM $from ON f._1 = d._1 " +
                "JOIN early_selection s ON f._1 = s._1"
              withSQLConf(
                CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ENABLED.key -> enabled.toString) {
                val (_, plan) = checkSparkAnswerAndOperator(
                  sql(query),
                  Seq(classOf[CometBroadcastHashJoinExec]))
                val joins = collect(plan) { case join: CometBroadcastHashJoinExec => join }
                assert(joins.size == 2, s"Expected two native broadcast joins:\n$plan")
                val outputs = joins.map(_.metrics("output_rows").value).sorted
                assert(outputs == (if (enabled) Seq(2L, 4L) else Seq(4L, 2000L)))
                if (enabled) {
                  assert(
                    joins.map(_.metrics("dynamic_filter_early_rows_pruned").value).sum == 999L)
                  assert(
                    joins
                      .map(_.metrics("dynamic_filter_early_rows_evaluated").value)
                      .sum == 1000L)
                  // The ancestor consumer must not hide the scan from the intermediate join's
                  // own reader filter. collect visits the outer join before the intermediate one.
                  assert(joins.last.metrics("dynamic_filter_join_filters_attached").value > 0L)
                  val projections = collect(plan) { case project: CometProjectExec => project }
                  assert(projections.nonEmpty)
                  assert(projections.forall(_.metrics("output_rows").value > 0L))
                }
              }
            }
          }
        }
      }
    }
  }

  test("join dynamic filters stack across three joins and preserve reader attachment") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.JOIN_REORDER_ENABLED.key -> "false",
      SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
      CometConf.COMET_BATCH_SIZE.key -> "128") {
      withParquetTable((0 until 1000).map(i => (i, i.toLong)), "stacked_fact") {
        withParquetTable((0 until 2000).map(i => (i % 1000, i)), "stacked_dimension") {
          withParquetTable(Seq((42, 1), (43, 2)), "stacked_selection") {
            withParquetTable(Seq((42, 1), (42, 2)), "stacked_final") {
              for (buildLeft <- Seq(false, true); enabled <- Seq(false, true)) {
                val from = if (buildLeft) {
                  "stacked_dimension d JOIN stacked_fact f"
                } else {
                  "stacked_fact f JOIN stacked_dimension d"
                }
                val query = "SELECT /*+ BROADCAST(d), BROADCAST(s), BROADCAST(t) */ " +
                  "f._1 AS selected_key, f._2 AS payload, d._2 AS detail, " +
                  "s._2 AS selection, t._2 AS final_selection " +
                  s"FROM $from ON f._1 = d._1 " +
                  "JOIN stacked_selection s ON f._1 = s._1 " +
                  "JOIN stacked_final t ON f._1 = t._1"
                withSQLConf(
                  CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ENABLED.key -> enabled.toString) {
                  val (_, plan) = checkSparkAnswerAndOperator(
                    sql(query),
                    Seq(classOf[CometBroadcastHashJoinExec]))
                  val joins = collect(plan) { case join: CometBroadcastHashJoinExec => join }
                  assert(joins.size == 3, s"Expected three native broadcast joins:\n$plan")
                  val outputs = joins.map(_.metrics("output_rows").value).sorted
                  assert(outputs == (if (enabled) Seq(2L, 2L, 4L) else Seq(4L, 4L, 2000L)))
                  if (enabled) {
                    // Each ancestor installs a consumer beneath the first join, so both must
                    // evaluate rows even when one has already filtered most of the batch.
                    assert(
                      joins.count(_.metrics("dynamic_filter_early_rows_evaluated").value > 0L)
                        == 2)
                    assert(
                      joins.map(_.metrics("dynamic_filter_early_rows_pruned").value).sum == 999L)
                    assert(joins.last.metrics("dynamic_filter_join_filters_attached").value > 0L)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  test("join dynamic filter prunes a projected Parquet reader through null-check conjunctions") {
    withTempPath { probePath =>
      withSQLConf(
        CometConf.COMET_BATCH_SIZE.key -> "1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1") {
        // Keep an unused physical column so reader attachment must remap the join key.
        // A matching key with a null payload must still fail the retained probe filter.
        spark
          .range(0, 10000, 1, 1)
          .selectExpr(
            "id AS unused",
            "CASE WHEN id = 2500 THEN CAST(NULL AS BIGINT) ELSE id END AS payload",
            "CAST(id AS INT) AS probe_key")
          .write
          .option("parquet.block.size", "1024")
          .parquet(probePath.getCanonicalPath)

        withParquetTable(probePath.getCanonicalPath, "dynamic_reader_probe") {
          // Keep both keys in the completed reader domain. Only key 2600 survives the
          // payload null check, which must not disappear when the reader filter attaches.
          withParquetTable(Seq(Tuple1(2500), Tuple1(2600)), "dynamic_reader_build") {
            val queries = Seq(
              (
                BuildRight,
                "SELECT /*+ BROADCAST(b) */ p.probe_key, p.payload " +
                  "FROM dynamic_reader_probe p JOIN dynamic_reader_build b " +
                  "ON p.probe_key = b._1 WHERE p.payload IS NOT NULL"),
              (
                BuildLeft,
                "SELECT /*+ BROADCAST(b) */ p.probe_key, p.payload " +
                  "FROM dynamic_reader_build b JOIN dynamic_reader_probe p " +
                  "ON p.probe_key = b._1 WHERE p.payload IS NOT NULL"))
            for ((buildSide, query) <- queries) {
              var unfilteredBytes = 0L
              for (enabled <- Seq(false, true)) {
                withSQLConf(
                  CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ENABLED.key -> enabled.toString) {
                  val (_, plan) = checkSparkAnswerAndOperator(
                    sql(query),
                    Seq(classOf[CometBroadcastHashJoinExec], classOf[CometNativeScanExec]))
                  val joins = collect(plan) { case join: CometBroadcastHashJoinExec => join }
                  assert(joins.size == 1, s"Expected one native broadcast hash join:\n$plan")
                  assert(joins.head.buildSide == buildSide)
                  val probeScans = collect(plan) {
                    case scan: CometNativeScanExec if scan.output.exists(_.name == "probe_key") =>
                      scan
                  }
                  assert(probeScans.size == 1, s"Expected one native probe scan:\n$plan")
                  val probeFilters = collect(plan) {
                    case filter: CometFilterExec if filter.output.exists(_.name == "probe_key") =>
                      filter
                  }
                  assert(probeFilters.size == 1, s"Expected one native probe filter:\n$plan")
                  // Verify Spark really built the two-check shape. Keep payload in the
                  // output so a separate probe projection does not block reader attachment.
                  val nullCheckedColumns = probeFilters.head.condition match {
                    case And(
                          IsNotNull(left: AttributeReference),
                          IsNotNull(right: AttributeReference)) =>
                      Set(left.name, right.name)
                    case condition =>
                      fail(s"Expected two direct-column null checks, found $condition")
                  }
                  assert(nullCheckedColumns == Set("probe_key", "payload"))
                  val scanMetrics = probeScans.head.metrics
                  val bytes = scanMetrics("bytes_scanned").value
                  assert(joins.head.metrics("output_rows").value == 1L)
                  if (enabled) {
                    assert(joins.head.metrics("dynamic_filter_join_filters_attached").value > 0L)
                    assert(joins.head.metrics("dynamic_filter_join_filters_skipped").value == 0L)
                    assert(
                      probeFilters.head.metrics("output_rows").value > 0L,
                      "Execution-local reader attachment must preserve probe filter metrics")
                    assert(scanMetrics("row_groups_pruned_statistics").value > 0L)
                    assert(
                      bytes < unfilteredBytes,
                      s"Reader filter should reduce scanned bytes: enabled=$bytes, " +
                        s"disabled=$unfilteredBytes")
                  } else {
                    unfilteredBytes = bytes
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  test("join dynamic filter crosses deterministic residuals only with explicit opt-in") {
    withTempPath { probePath =>
      withSQLConf(
        CometConf.COMET_BATCH_SIZE.key -> "1",
        CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1") {
        spark
          .range(0, 10000, 1, 1)
          .selectExpr("id AS payload", "CAST(id AS INT) AS probe_key")
          .write
          .option("parquet.block.size", "1024")
          .parquet(probePath.getCanonicalPath)
        withParquetTable(probePath.getCanonicalPath, "dynamic_residual_probe") {
          // Key 2506 cannot pass the residual, although it belongs to the join domain.
          withParquetTable(Seq(Tuple1(2500), Tuple1(2506)), "dynamic_residual_build") {
            for (buildLeft <- Seq(false, true)) {
              val from = if (buildLeft) {
                "dynamic_residual_build b JOIN dynamic_residual_probe p"
              } else {
                "dynamic_residual_probe p JOIN dynamic_residual_build b"
              }
              val query = "SELECT /*+ BROADCAST(b) */ p.probe_key, p.payload FROM " + from +
                " ON p.probe_key = b._1 WHERE pmod(p.payload, 7) > 0"
              var unfilteredBytes = 0L
              for (allowed <- Seq(false, true)) {
                withSQLConf(
                  CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ALLOW_DETERMINISTIC_FILTER_PUSHDOWN.key -> allowed.toString) {
                  val (_, plan) = checkSparkAnswerAndOperator(
                    sql(query),
                    Seq(classOf[CometBroadcastHashJoinExec], classOf[CometNativeScanExec]))
                  checkAnswer(sql(query), Seq(Row(2500, 2500L)))
                  val joins = collect(plan) { case join: CometBroadcastHashJoinExec => join }
                  assert(joins.size == 1)
                  val filters = collect(plan) {
                    case filter: CometFilterExec if filter.output.exists(_.name == "probe_key") =>
                      filter
                  }
                  assert(filters.size == 1, s"Expected one native residual filter:\n$plan")
                  assert(filters.head.condition.deterministic)
                  assert(filters.head.nativeOp.getFilter.getAllowRuntimeFilterPushdown == allowed)
                  val scans = collect(plan) {
                    case scan: CometNativeScanExec if scan.output.exists(_.name == "probe_key") =>
                      scan
                  }
                  assert(scans.size == 1)
                  val metrics = scans.head.metrics
                  val bytes = metrics("bytes_scanned").value
                  assert(joins.head.metrics("output_rows").value == 1L)
                  assert(filters.head.metrics("output_rows").value > 0L)
                  if (allowed) {
                    assert(joins.head.metrics("dynamic_filter_join_filters_attached").value > 0L)
                    assert(metrics("row_groups_pruned_statistics").value > 0L)
                    assert(bytes < unfilteredBytes)
                  } else {
                    unfilteredBytes = bytes
                    assert(joins.head.metrics("dynamic_filter_join_filters_attached").value == 0L)
                    assert(joins.head.metrics("dynamic_filter_join_filters_skipped").value > 0L)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  test("join dynamic filter preserves Parquet schema conversion errors") {
    withTempPath { probePath =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
        spark
          .range(100, 104, 1, 1)
          .selectExpr("CAST(id AS INT) AS probe_key", "id AS payload")
          .write
          .parquet(probePath.getCanonicalPath)
        withTempView("dynamic_schema_probe") {
          // INT64 -> INT32 is invalid even when the stored values fit. Keep the
          // payload projected so discarding the nonmatching keys cannot hide it.
          spark.read
            .schema("probe_key INT, payload INT")
            .parquet(probePath.getCanonicalPath)
            .createOrReplaceTempView("dynamic_schema_probe")
          withParquetTable(Seq(Tuple1(0)), "dynamic_schema_build") {
            for {
              (comet, dynamicFilter) <- Seq((false, false), (true, false), (true, true))
              allowed <- Seq(false, true)
              // Without a residual, reader filters can attach regardless of the permission flag.
              residual <- Seq("", " WHERE pmod(p.payload, 2) >= 0")
            } {
              withSQLConf(
                CometConf.COMET_ENABLED.key -> comet.toString,
                CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ALLOW_DETERMINISTIC_FILTER_PUSHDOWN.key -> allowed.toString,
                CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ENABLED.key -> dynamicFilter.toString) {
                val df = sql(
                  "SELECT /*+ BROADCAST(b) */ p.probe_key, p.payload " +
                    "FROM dynamic_schema_probe p JOIN dynamic_schema_build b " +
                    "ON p.probe_key = b._1" + residual)
                val plan = df.queryExecution.executedPlan
                if (comet) {
                  val joins = collect(plan) { case join: CometBroadcastHashJoinExec => join }
                  assert(joins.size == 1, s"Expected one native broadcast hash join:\n$plan")
                  assert(joins.head.buildSide == BuildRight)
                  assert(joins.head.nativeOp.getHashJoin.getDynamicFilterEnabled == dynamicFilter)
                  val probes = collect(plan) {
                    case scan: CometNativeScanExec if scan.output.exists(_.name == "payload") =>
                      scan
                  }
                  assert(probes.size == 1, s"Expected one native probe scan:\n$plan")
                }
                withClue(s"comet=$comet, dynamicFilter=$dynamicFilter, allowed=$allowed, " +
                  s"residual=$residual: ") {
                  val error = intercept[SparkException](df.collect())
                  val chain = causeChain(error)
                  assert(
                    chain.exists(_.isInstanceOf[SchemaColumnConvertNotSupportedException]),
                    s"Expected a Parquet schema conversion error, found $chain")
                }
              }
            }
          }
        }
      }
    }
  }

  test("join dynamic filter preserves seeded rand probe order") {
    withTempPath { probePath =>
      withSQLConf(
        CometConf.COMET_BATCH_SIZE.key -> "16",
        CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ALLOW_DETERMINISTIC_FILTER_PUSHDOWN.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1") {
        // Reader pruning must not change which seeded random value is assigned to a row.
        // Force four ordered, one-row groups: pruning groups 0-2 would make key 3 receive
        // rand(42)'s first draw instead of its fourth draw.
        spark
          .range(0, 4, 1, 1)
          .selectExpr("CAST(id AS INT) AS key")
          .coalesce(1)
          .write
          .option("parquet.block.size", "1")
          .option("parquet.page.size.row.check.min", "1")
          .option("parquet.page.size.row.check.max", "1")
          .parquet(probePath.getCanonicalPath)

        val partFiles = probePath.listFiles().filter(_.getName.endsWith(".parquet"))
        assert(partFiles.length == 1, s"Expected one Parquet file, found ${partFiles.toSeq}")
        val reader = ParquetFileReader.open(
          HadoopInputFile.fromPath(
            new Path(partFiles.head.getAbsolutePath),
            spark.sparkContext.hadoopConfiguration))
        try {
          val rowGroups = reader.getFooter.getBlocks
          assert(rowGroups.size() == 4)
          assert((0 until rowGroups.size()).forall(i => rowGroups.get(i).getRowCount == 1L))
        } finally {
          reader.close()
        }

        withParquetTable(probePath.getCanonicalPath, "dynamic_reader_rand_probe") {
          withParquetTable(Seq(Tuple1(3)), "dynamic_reader_rand_build") {
            // Keep the Long suffix so Rand serializes into the native filter for this regression.
            val query =
              "SELECT /*+ BROADCAST(b) */ p.key " +
                "FROM (SELECT key FROM dynamic_reader_rand_probe " +
                "WHERE key IS NOT NULL AND rand(42L) < 0.5) p " +
                "JOIN dynamic_reader_rand_build b ON p.key = b._1"

            for (enabled <- Seq(false, true)) {
              withSQLConf(
                CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ENABLED.key -> enabled.toString) {
                val (_, plan) = checkSparkAnswerAndOperator(
                  sql(query),
                  Seq(
                    classOf[CometBroadcastHashJoinExec],
                    classOf[CometFilterExec],
                    classOf[CometNativeScanExec]))
                checkAnswer(sql(query), Seq(Row(3)))

                val joins = collect(plan) { case join: CometBroadcastHashJoinExec => join }
                assert(joins.size == 1, s"Expected one native broadcast hash join:\n$plan")
                assert(joins.head.metrics("output_rows").value == 1L)
                val probeScans = collect(plan) {
                  case scan: CometNativeScanExec if scan.output.exists(_.name == "key") => scan
                }
                assert(probeScans.size == 1, s"Expected one native probe scan:\n$plan")
                assert(probeScans.head.metrics("output_rows").value == 4L)
                assert(probeScans.head.metrics("row_groups_pruned_statistics").value == 0L)

                if (enabled) {
                  assert(joins.head.metrics("dynamic_filter_join_filters_attached").value == 0L)
                  assert(joins.head.metrics("dynamic_filter_join_filters_skipped").value == 1L)
                  assert(joins.head.metrics("dynamic_filter_join_rows_evaluated").value == 1L)
                  assert(joins.head.metrics("dynamic_filter_join_rows_pruned").value == 0L)
                }
              }
            }
          }
        }
      }
    }
  }

  test("join dynamic filter preserves nulls, empty builds and non-selective joins") {
    withSQLConf(
      CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ENABLED.key -> "true",
      CometConf.COMET_BATCH_SIZE.key -> "2",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable(
        (0 until 100).map(i => (Some(i), i.toLong)) :+ ((None, -1L)),
        "dynamic_probe") {
        for (build <- Seq(
            Seq((Some(10), 1L), (None, 2L), (Some(10), 3L), (Some(90), 4L)),
            Seq.empty[(Option[Int], Long)],
            Seq((None, 1L), (None, 2L)),
            (0 until 100).map(i => (Some(i), i.toLong)))) {
          withParquetTable(build, "dynamic_build") {
            val query = "SELECT /*+ BROADCAST(b) */ p._1, p._2, b._2 " +
              "FROM dynamic_probe p JOIN dynamic_build b ON p._1 = b._1"
            val (_, plan) = checkSparkAnswerAndOperator(sql(query))
            val joins = nativeHashJoins(plan)
            assert(joins.size == 1, s"Expected native hash join:\n$plan")
            if (build.size == 100) {
              assert(joins.head.metrics("dynamic_filter_join_rows_evaluated").value > 0L)
              assert(joins.head.metrics("dynamic_filter_join_rows_pruned").value == 0L)
            }
          }
        }
      }
    }
  }

  test("join dynamic filter leaves unsupported joins on their existing native path") {
    withSQLConf(
      CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      withParquetTable(Seq((Some(10), 1L), (Some(20), 2L), (None, 3L)), "dynamic_probe") {
        withParquetTable(Seq((Some(10), 4L), (None, 5L)), "dynamic_build") {
          val from = "FROM dynamic_probe p "
          // Spark 3.4 requires building the left side for a right outer shuffled hash join.
          val joins = Seq(
            "LEFT JOIN dynamic_build b ON p._1 = b._1" -> "b",
            "RIGHT JOIN dynamic_build b ON p._1 = b._1" -> "p",
            "FULL JOIN dynamic_build b ON p._1 = b._1" -> "b",
            "LEFT SEMI JOIN dynamic_build b ON p._1 = b._1" -> "b",
            "LEFT ANTI JOIN dynamic_build b ON p._1 = b._1" -> "b",
            "JOIN dynamic_build b ON p._1 <=> b._1" -> "b",
            "JOIN dynamic_build b ON p._1 + 1 = b._1" -> "b",
            "JOIN dynamic_build b ON CAST(p._1 AS STRING) = CAST(b._1 AS STRING)" -> "b")
          for ((joinClause, buildAlias) <- joins) {
            val query = s"SELECT /*+ SHUFFLE_HASH($buildAlias) */ p.* $from $joinClause"
            val (_, plan) = checkSparkAnswerAndOperator(sql(query))
            val native = nativeHashJoins(plan)
            assert(native.size == 1, s"Expected native hash join:\n$plan")
            assert(native.head.metrics("dynamic_filter_join_rows_evaluated").value == 0L)
            assert(native.head.metrics("dynamic_filter_join_rows_pruned").value == 0L)
          }
          // NOT IN must still observe build-side NULLs; never attach a filter here.
          withSQLConf(
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760",
            "spark.sql.optimizeNullAwareAntiJoin" -> "true") {
            val query = "SELECT * FROM dynamic_probe WHERE _1 NOT IN " +
              "(SELECT _1 FROM dynamic_build)"
            val (_, plan) = checkSparkAnswerAndOperator(sql(query))
            val native = collect(plan) { case join: CometBroadcastHashJoinExec => join }
            assert(native.size == 1, s"Expected native null-aware anti join:\n$plan")
            assert(native.head.nativeOp.getHashJoin.getNullAwareAntiJoin)
            assert(native.head.metrics("dynamic_filter_join_rows_evaluated").value == 0L)
          }
        }
      }
    }
  }

  test("join dynamic filter preserves a duplicate-heavy stored byte build") {
    withSQLConf(
      CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // Keep the key in Parquet so Spark cannot replace it with a folded literal.
      // Hint the larger duplicate-heavy relation as the physical build side.
      withParquetTable((0 until 65536).map(i => (1.toByte, i)), "dynamic_byte_build") {
        withParquetTable(Seq(Tuple1(1.toByte), Tuple1(2.toByte)), "dynamic_byte_probe") {
          val query = "SELECT /*+ BROADCAST(b) */ b._2, p._1 " +
            "FROM dynamic_byte_probe p JOIN dynamic_byte_build b ON p._1 = b._1"
          val (_, plan) = checkSparkAnswerAndOperator(sql(query))
          val joins = collect(plan) { case join: CometBroadcastHashJoinExec => join }
          assert(joins.size == 1, s"Expected native broadcast hash join:\n$plan")
          val join = joins.head
          assert(join.buildSide == BuildRight)
          assert(join.nativeOp.getHashJoin.getDynamicFilterEnabled)
          assert(join.metrics("output_rows").value == 65536L)
          assert(join.metrics("dynamic_filter_join_rows_evaluated").value > 0L)
          assert(join.metrics("dynamic_filter_join_filters_attached").value > 0L)
          val probeScans = collect(plan) {
            case scan: CometNativeScanExec if scan.output.size == 1 => scan
          }
          assert(probeScans.size == 1, s"Expected one native byte probe scan:\n$plan")
          val probeRows = probeScans.head.metrics("output_rows").value
          val residualPruned = join.metrics("dynamic_filter_join_rows_pruned").value
          assert(
            probeRows < 2L || residualPruned > 0L,
            "Expected the reader or residual filter to prune probe key 2, " +
              s"scan output=$probeRows residual pruned=$residualPruned")
        }
      }
    }
  }

  test("Broadcast HashJoin without join filter") {
    withSQLConf(
      CometConf.COMET_BATCH_SIZE.key -> "100",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "false",
      "spark.sql.join.forceApplyShuffledHashJoin" -> "true",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable((0 until 1000).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 1000).map(i => (i % 10, i + 2)), "tbl_b") {
          // Inner join: build left
          val df1 =
            sql("SELECT /*+ BROADCAST(tbl_a) */ * FROM tbl_a JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(
            df1,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastHashJoinExec]))

          // Right join: build left
          val df2 =
            sql("SELECT /*+ BROADCAST(tbl_a) */ * FROM tbl_a RIGHT JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(
            df2,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastHashJoinExec]))
        }
      }
    }
  }

  test("Broadcast HashJoin with join filter") {
    withSQLConf(
      CometConf.COMET_BATCH_SIZE.key -> "100",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "false",
      "spark.sql.join.forceApplyShuffledHashJoin" -> "true",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable((0 until 1000).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 1000).map(i => (i % 10, i + 2)), "tbl_b") {
          // Inner join: build left
          val df1 =
            sql(
              "SELECT /*+ BROADCAST(tbl_a) */ * FROM tbl_a JOIN tbl_b " +
                "ON tbl_a._2 = tbl_b._1 AND tbl_a._1 > tbl_b._2")
          checkSparkAnswerAndOperator(
            df1,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastHashJoinExec]))

          // Right join: build left
          val df2 =
            sql(
              "SELECT /*+ BROADCAST(tbl_a) */ * FROM tbl_a RIGHT JOIN tbl_b " +
                "ON tbl_a._2 = tbl_b._1 AND tbl_a._1 > tbl_b._2")
          checkSparkAnswerAndOperator(
            df2,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastHashJoinExec]))
        }
      }
    }
  }

  test("HashJoin without join filter") {
    withSQLConf(
      "spark.sql.join.forceApplyShuffledHashJoin" -> "true",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "false",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable((0 until 10).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 10).map(i => (i % 10, i + 2)), "tbl_b") {
          // Inner join: build left
          val df1 =
            sql(
              "SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df1)

          // Right join: build left
          val df2 =
            sql("SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a RIGHT JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df2)

          // Full join: build left
          val df3 =
            sql("SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a FULL JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df3)

          // TODO: Spark 3.4 returns SortMergeJoin for this query even with SHUFFLE_HASH hint.
          // Left join with build left and right join with build right in hash join is only supported
          // in Spark 3.5 or above. See SPARK-36612.
          //
          // Left join: build left
          // sql("SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a LEFT JOIN tbl_b ON tbl_a._2 = tbl_b._1")

          // Inner join: build right
          val df4 =
            sql(
              "SELECT /*+ SHUFFLE_HASH(tbl_b) */ * FROM tbl_a JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df4)

          // Left join: build right
          val df5 =
            sql("SELECT /*+ SHUFFLE_HASH(tbl_b) */ * FROM tbl_a LEFT JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df5)

          // Right join: build right
          val df6 =
            sql("SELECT /*+ SHUFFLE_HASH(tbl_b) */ * FROM tbl_a RIGHT JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df6)

          // Full join: build right
          val df7 =
            sql("SELECT /*+ SHUFFLE_HASH(tbl_b) */ * FROM tbl_a FULL JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df7)

          // Left semi and anti joins are only supported with build right in Spark.
          val left = sql("SELECT * FROM tbl_a")
          val right = sql("SELECT * FROM tbl_b")

          val df8 = left.join(right, left("_2") === right("_1"), "leftsemi")
          checkSparkAnswerAndOperator(df8)

          val df9 = left.join(right, left("_2") === right("_1"), "leftanti")
          checkSparkAnswerAndOperator(df9)
        }
      }
    }
  }

  private val forcedSmjQuery = "SELECT * FROM tbl_a JOIN tbl_b ON tbl_a._2 = tbl_b._1"

  private def withForcedSmjTables(f: => Unit): Unit = {
    withParquetTable((0 until 1000).map(i => (i, i % 5)), "tbl_a") {
      withParquetTable((0 until 10).map(i => (i % 10, i + 2)), "tbl_b") {
        f
      }
    }
  }

  private def rewriteReason(reason: String): String =
    s"Cannot rewrite SortMergeJoin to HashJoin: $reason"

  // Returns Spark's size estimate of the build side.
  private def assertSortMergeJoinKept(plan: SparkPlan): BigInt = {
    val joins = collect(plan) { case j: CometSortMergeJoinExec => j }
    assert(joins.size == 1, plan)
    assert(collect(plan) { case s: CometSortExec => s }.size == 2, plan)
    assert(collect(plan) { case j: CometHashJoinExec => j }.isEmpty, plan)
    val kept = joins.head
    assert(!hasFallbackReason(kept) && !hasFallbackReason(kept.originalPlan), plan)
    val reasons = new ExtendedExplainInfo().getFallbackReasons(plan)
    assert(!reasons.exists(_.contains("SortMergeJoin")), reasons)
    kept.originalPlan.logicalLink match {
      case Some(join: Join) => join.right.stats.sizeInBytes
      case other => fail(s"Expected a Join logical link, got $other")
    }
  }

  // The refusal is recorded as information on the kept join and rendered as COMET-INFO.
  private def assertKeptWithInfo(plan: SparkPlan, expected: String): Unit = {
    val join = collect(plan) { case j: CometSortMergeJoinExec => j }.head
    assert(join.getTagValue(CometExplainInfo.EXTENSION_INFO).exists(_.contains(expected)), plan)
    val verbose = new ExtendedExplainInfo().generateVerboseInfo(plan)
    assert(verbose.contains("[COMET-INFO:") && verbose.contains(expected), verbose)
  }

  private def assertRewrittenToHashJoin(plan: SparkPlan): Unit = {
    val joins = collect(plan) { case j: CometHashJoinExec => j }
    assert(joins.map(_.buildSide) == Seq(BuildRight), plan)
    assert(collect(plan) { case s: CometSortExec => s }.isEmpty, plan)
    assert(collect(plan) { case j: CometSortMergeJoinExec => j }.isEmpty, plan)
  }

  private def assertKeptWithReason(plan: SparkPlan, limit: Long): Unit = {
    val estimate = assertSortMergeJoinKept(plan)
    assert(estimate >= limit, estimate)
    assertKeptWithInfo(
      plan,
      rewriteReason(
        s"build side size estimate of $estimate bytes is not under the limit of $limit bytes"))
  }

  for (adaptive <- Seq(false, true)) {
    test(s"forceShuffledHashJoin keeps SortMergeJoin over maxBuildSize, AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        CometConf.COMET_FORCE_SHJ.key -> "true",
        CometConf.COMET_FORCE_SHJ_MAX_BUILD_SIZE.key -> "1") {
        withForcedSmjTables {
          val (_, cometPlan) = checkSparkAnswerAndOperator(sql(forcedSmjQuery))
          assertKeptWithReason(cometPlan, 1)
        }
      }
    }

    test(s"forceShuffledHashJoin rewrites SortMergeJoin under maxBuildSize, AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        CometConf.COMET_FORCE_SHJ.key -> "true",
        CometConf.COMET_FORCE_SHJ_MAX_BUILD_SIZE.key -> "1g") {
        withForcedSmjTables {
          val (_, cometPlan) = checkSparkAnswerAndOperator(sql(forcedSmjQuery))
          assertRewrittenToHashJoin(cometPlan)
        }
      }
    }
  }

  test("forceShuffledHashJoin matches Spark's strict size rule at the limit") {
    // AQE off, so the estimate read from the first plan is the one the later runs compare.
    withForcedSmjTables {
      var estimate = BigInt(0)
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        CometConf.COMET_FORCE_SHJ.key -> "true",
        CometConf.COMET_FORCE_SHJ_MAX_BUILD_SIZE.key -> "1") {
        val (_, cometPlan) = checkSparkAnswerAndOperator(sql(forcedSmjQuery))
        estimate = assertSortMergeJoinKept(cometPlan)
      }
      // A build side exactly at the limit is not under it, as in canBuildLocalHashMapBySize.
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        CometConf.COMET_FORCE_SHJ.key -> "true",
        CometConf.COMET_FORCE_SHJ_MAX_BUILD_SIZE.key -> estimate.toString) {
        val (_, cometPlan) = checkSparkAnswerAndOperator(sql(forcedSmjQuery))
        assertKeptWithReason(cometPlan, estimate.toLong)
      }
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        CometConf.COMET_FORCE_SHJ.key -> "true",
        CometConf.COMET_FORCE_SHJ_MAX_BUILD_SIZE.key -> (estimate + 1).toString) {
        val (_, cometPlan) = checkSparkAnswerAndOperator(sql(forcedSmjQuery))
        assertRewrittenToHashJoin(cometPlan)
      }
    }
  }

  test("forceShuffledHashJoin with a non-positive maxBuildSize rewrites regardless of size") {
    withSQLConf(
      CometConf.COMET_FORCE_SHJ.key -> "true",
      CometConf.COMET_FORCE_SHJ_MAX_BUILD_SIZE.key -> "0",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1") {
      withForcedSmjTables {
        val (_, cometPlan) = checkSparkAnswerAndOperator(sql(forcedSmjQuery))
        assertRewrittenToHashJoin(cometPlan)
      }
    }
  }

  test("forceShuffledHashJoin without maxBuildSize uses Spark's shuffled hash join size rule") {
    withForcedSmjTables {
      var estimate = BigInt(0)
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_FORCE_SHJ.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1") {
        val (_, cometPlan) = checkSparkAnswerAndOperator(sql(forcedSmjQuery))
        estimate = assertSortMergeJoinKept(cometPlan)
        assertKeptWithReason(cometPlan, 2)
      }
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_FORCE_SHJ.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> (estimate - 1).toString) {
        val (_, cometPlan) = checkSparkAnswerAndOperator(sql(forcedSmjQuery))
        assertRewrittenToHashJoin(cometPlan)
      }
    }
  }

  test("forceShuffledHashJoin uses Spark's default threshold when broadcasts are disabled") {
    // Disabling broadcasts says nothing about the hash table an executor can hold, so the limit
    // falls back to Spark's default threshold times the partition count. AQE is off so the
    // decision uses the planning estimate, which fileCompressionFactor scales past 10 MB without
    // writing a 10 MB table.
    val defaultThreshold = SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.defaultValue.get
    assert(defaultThreshold == 10L * 1024 * 1024)
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_FORCE_SHJ.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withForcedSmjTables {
        val (_, cometPlan) = checkSparkAnswerAndOperator(sql(forcedSmjQuery))
        assertRewrittenToHashJoin(cometPlan)
      }
      withSQLConf(SQLConf.FILE_COMPRESSION_FACTOR.key -> "1000000") {
        withForcedSmjTables {
          val (_, cometPlan) = checkSparkAnswerAndOperator(sql(forcedSmjQuery))
          assertKeptWithReason(cometPlan, defaultThreshold)
        }
      }
    }
  }

  test("forceShuffledHashJoin keeps a SortMergeJoin whose build side has no size estimate") {
    val schema = StructType(Seq(StructField("k", IntegerType), StructField("v", IntegerType)))
    def unsized(name: String): Unit = spark
      .createDataFrame(spark.sparkContext.parallelize((0 until 10).map(i => Row(i, i))), schema)
      .createOrReplaceTempView(name)
    val query = "SELECT * FROM rdd_a JOIN rdd_b ON rdd_a.k = rdd_b.k"
    withTempView("rdd_a", "rdd_b") {
      unsized("rdd_a")
      unsized("rdd_b")
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        CometConf.COMET_FORCE_SHJ.key -> "true") {
        val (_, cometPlan) = checkSparkAnswer(query)
        val kept = collect(cometPlan) {
          case j: SortMergeJoinExec => j
          case j: CometSortMergeJoinExec => j.originalPlan
        }
        assert(kept.size == 1, cometPlan)
        val estimate = kept.head.logicalLink match {
          case Some(join: Join) => join.right.stats.sizeInBytes
          case other => fail(s"Expected a Join logical link, got $other")
        }
        assert(estimate == spark.sessionState.conf.defaultSizeInBytes, estimate)
        val info = kept.head.getTagValue(CometExplainInfo.EXTENSION_INFO).toSeq.flatten
        val expected = rewriteReason(s"build side size estimate of $estimate bytes")
        assert(info.exists(_.contains(expected)), info)

        withSQLConf(CometConf.COMET_FORCE_SHJ_MAX_BUILD_SIZE.key -> "-1") {
          val (_, cometPlan) = checkSparkAnswer(query)
          val hashJoins = collect(cometPlan) {
            case j: ShuffledHashJoinExec => j
            case j: CometHashJoinExec => j
          }
          assert(hashJoins.size == 1, cometPlan)
        }
      }
    }
  }

  // Both joins are on the same key, so EnsureRequirements puts no sort between them.
  private def withChainedJoinTables(midRows: Int, midKeys: Int)(f: => Unit): Unit = {
    withParquetTable((0 until 10000).map(i => (i % 100, i)), "big") {
      withParquetTable((0 until 10).map(i => (i * 10, i)), "small") {
        withParquetTable((0 until midRows).map(i => (i % midKeys, i)), "mid") {
          f
        }
      }
    }
  }

  private def withForcedShjTables(adaptive: Boolean)(f: => Unit): Unit = withSQLConf(
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
    SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    SQLConf.SHUFFLE_PARTITIONS.key -> "2",
    CometConf.COMET_FORCE_SHJ.key -> "true",
    CometConf.COMET_FORCE_SHJ_MAX_BUILD_SIZE.key -> "-1") {
    withChainedJoinTables(midRows = 3000, midKeys = 100)(f)
  }

  // Whether a hash join is reachable without leaving the stage.
  private def reachesHashJoinInStage(plan: SparkPlan): Boolean = plan match {
    case _: CometHashJoinExec => true
    case _: ShuffleExchangeLike | _: QueryStageExec => false
    case p => p.children.exists(reachesHashJoinInStage)
  }

  // Whether a sort-merge join input is a sort over a hash join in the same stage. A Spark
  // sort-merge join reads the sort through codegen wrappers and a columnar to row transition.
  private def isSortOverHashJoin(input: SparkPlan): Boolean = input match {
    case s: CometSortExec => reachesHashJoinInStage(s.child)
    case s: SortExec => reachesHashJoinInStage(s.child)
    case t: ColumnarToRowTransition => isSortOverHashJoin(t.child)
    case w: WholeStageCodegenExec => isSortOverHashJoin(w.child)
    case a: InputAdapter => isSortOverHashJoin(a.child)
    case _ => false
  }

  private def checkSortRestoredOverHashJoin(query: String): Unit = {
    val (_, cometPlan) = checkSparkAnswer(query)
    val inputs = collect(cometPlan) {
      case j: CometSortMergeJoinExec => j.children
      case j: SortMergeJoinExec => j.children
    }.flatten
    assert(inputs.exists(isSortOverHashJoin), cometPlan)
  }

  private val chainedJoinQuery = "SELECT big._1, big._2, small._2, mid._2 FROM big " +
    "JOIN small ON big._1 = small._1 JOIN mid ON big._1 = mid._1"

  test("forceShuffledHashJoin restores ordering for a kept SortMergeJoin, AQE=false") {
    // The default limit, 3000 bytes times two partitions, falls between the two build sides.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_FORCE_SHJ.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "3000") {
      withChainedJoinTables(midRows = 3000, midKeys = 100) {
        checkSortRestoredOverHashJoin(chainedJoinQuery)
      }
    }
  }

  test("forceShuffledHashJoin restores ordering for a kept SortMergeJoin, AQE=true") {
    // Under AQE the decision uses the materialized shuffle sizes, so mid is made large enough
    // to stay over the limit while small stays under it.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      CometConf.COMET_FORCE_SHJ.key -> "true",
      CometConf.COMET_FORCE_SHJ_MAX_BUILD_SIZE.key -> "1000000",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "3000") {
      withChainedJoinTables(midRows = 300000, midKeys = 30000) {
        checkSortRestoredOverHashJoin(chainedJoinQuery)
      }
    }
  }

  // Each predicate refers to small, so neither the filter nor the semi join is pushed down.
  for (adaptive <- Seq(false, true)) {
    test(s"forceShuffledHashJoin restores ordering for a kept ExistenceJoin, AQE=$adaptive") {
      withForcedShjTables(adaptive) {
        checkSortRestoredOverHashJoin(
          "SELECT big._1, big._2, small._2 FROM big JOIN small ON big._1 = small._1 " +
            "WHERE small._2 < 0 OR EXISTS (SELECT 1 FROM mid WHERE mid._1 = big._1)")
      }
    }

    test(s"forceShuffledHashJoin restores ordering for a kept LeftSemi join, AQE=$adaptive") {
      withSQLConf(
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.catalyst.optimizer.PushLeftSemiLeftAntiThroughJoin") {
        withForcedShjTables(adaptive) {
          checkSortRestoredOverHashJoin(
            "SELECT big._1, big._2, small._2 FROM big JOIN small ON big._1 = small._1 " +
              "WHERE EXISTS (SELECT 1 FROM mid WHERE mid._1 = big._1 AND mid._2 > small._2)")
        }
      }
    }
  }

  // A window partitioned by the join key gets no sort of its own, since the sort-merge join's
  // output ordering satisfies it, so the window needs that ordering restored too.
  for (adaptive <- Seq(false, true)) {
    test(s"forceShuffledHashJoin restores ordering for a window over the join, AQE=$adaptive") {
      withForcedShjTables(adaptive) {
        val (_, cometPlan) = checkSparkAnswer(
          "SELECT big._1, big._2, small._2, count(*) OVER (PARTITION BY big._1) " +
            "FROM big JOIN small ON big._1 = small._1")
        assert(collect(cometPlan) { case j: CometHashJoinExec => j }.nonEmpty, cometPlan)
        assert(collect(cometPlan) { case w: CometWindowExec => w }.nonEmpty, cometPlan)
      }
    }
  }

  test("forceShuffledHashJoin chain keeps early join dynamic filtering") {
    withForcedShjTables(adaptive = false) {
      val query = "SELECT big._1, big._2, mid._2, small._2 FROM big " +
        "JOIN mid ON big._1 = mid._1 JOIN small ON big._1 = small._1"
      for (enabled <- Seq(false, true)) {
        withSQLConf(CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ENABLED.key -> enabled.toString) {
          val (_, plan) = checkSparkAnswerAndOperator(sql(query))
          val joins = collect(plan) { case join: CometHashJoinExec => join }
          assert(joins.map(_.buildSide) == Seq(BuildRight, BuildRight), plan)
          // big has 100 rows per key, mid 30, and small one row for keys 0 to 90 by 10.
          val outputs = joins.map(_.metrics("output_rows").value).sorted
          assert(outputs == (if (enabled) Seq(30000L, 30000L) else Seq(30000L, 300000L)))
          if (enabled) {
            assert(
              joins.map(_.metrics("dynamic_filter_early_rows_evaluated").value).sum == 10000L)
            assert(joins.map(_.metrics("dynamic_filter_early_rows_pruned").value).sum == 9000L)
          } else {
            assert(joins.forall(!_.metrics.contains("dynamic_filter_early_rows_pruned")))
          }
        }
      }
    }
  }

  private def checkPartitionsSortedOverHashJoin(df: => DataFrame): Unit = {
    // withSQLConf returns Unit on Spark 3.x, so the expected rows are assigned inside it.
    var expected = Seq.empty[String]
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      expected = df.collect().map(_.toString).sorted.toSeq
    }
    val cometDf = df
    assert(cometDf.collect().map(_.toString).sorted.toSeq == expected)
    val cometPlan = cometDf.queryExecution.executedPlan
    assert(collect(cometPlan) { case j: CometHashJoinExec => j }.nonEmpty, cometPlan)
    val sorted = cometDf.queryExecution.toRdd
      .mapPartitions { it =>
        val keys = it.map(_.getInt(0)).toArray
        Iterator(keys.sameElements(keys.sorted))
      }
      .collect()
    assert(sorted.forall(identity), sorted.mkString(", "))
  }

  private val sortLossJoin =
    "SELECT big._1 AS k, big._2 AS v FROM big JOIN small ON big._1 = small._1"

  // Spark drops a sort that the sort-merge join's ordering satisfies (#6770).
  for (adaptive <- Seq(false, true)) {
    test(s"forceShuffledHashJoin keeps sortWithinPartitions on the join key, AQE=$adaptive") {
      withForcedShjTables(adaptive) {
        checkPartitionsSortedOverHashJoin(sql(sortLossJoin).sortWithinPartitions("k"))
      }
    }

    // Unsorted partition values make the single dynamic partition writer reopen a closed file.
    test(s"forceShuffledHashJoin keeps the sort of a partitioned insert, AQE=$adaptive") {
      withForcedShjTables(adaptive) {
        withTable("out") {
          sql("CREATE TABLE out (v INT, w INT, k INT) USING parquet PARTITIONED BY (k)")
          val insert = sql(
            "INSERT OVERWRITE TABLE out " +
              "SELECT big._2, small._2, big._1 FROM big JOIN small ON big._1 = small._1")
          val written = insert.queryExecution.executedPlan match {
            case c: CommandResultExec => c.commandPhysicalPlan
            case p => p
          }
          assert(collect(written) { case j: CometHashJoinExec => j }.nonEmpty, written)
          assert(spark.table("out").count() == 1000)
          val files = spark.table("out").inputFiles
          val perPartition = files.groupBy(_.split("/").find(_.startsWith("k=")))
          assert(perPartition.size == 10, files.mkString("\n"))
          assert(perPartition.values.forall(_.length == 1), files.mkString("\n"))
        }
      }
    }

    for ((joinType, key) <- Seq("RIGHT" -> "big._1", "FULL" -> "coalesce(big._1, small._1)")) {
      test(
        s"forceShuffledHashJoin keeps sortWithinPartitions over a $joinType join, " +
          s"AQE=$adaptive") {
        withForcedShjTables(adaptive) {
          checkPartitionsSortedOverHashJoin(
            sql(s"SELECT $key AS k, big._2 AS v, small._2 AS w " +
              s"FROM small $joinType JOIN big ON small._1 = big._1")
              .sortWithinPartitions("k"))
        }
      }
    }
  }

  test("forceShuffledHashJoin keeps probe side order when no exchange is added") {
    withForcedShjTables(adaptive = false) {
      val streamed = spark.table("big").repartition(2, $"_1").sortWithinPartitions("_1")
      val build = spark.table("small")
      checkPartitionsSortedOverHashJoin(
        streamed
          .join(build, streamed("_1") === build("_1"), "left_outer")
          .select(streamed("_1").as("k"), streamed("_2"), build("_2"))
          .sortWithinPartitions("k"))
    }
  }

  // The cache reaches the native join with no declared order, so the join must declare it.
  test("forceShuffledHashJoin keeps the order of a sorted cache through an outer join") {
    withForcedShjTables(adaptive = false) {
      withSQLConf(
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "false",
        CometConf.COMET_CONVERT_FROM_IN_MEMORY_CACHE_ENABLED.key -> "true") {
        val streamed = spark
          .table("big")
          .repartition(2, $"_1")
          .sortWithinPartitions("_1", "_2")
          .cache()
        try {
          streamed.count()
          val build = spark.table("small")
          val valueOrder = SortOrder(BoundReference(1, IntegerType, nullable = true), Ascending)
          for {
            hint <- Seq(None, Some("shuffle_hash"))
            (joinType, buildSide) <- Seq("left_outer" -> BuildRight, "right_outer" -> BuildLeft)
          } {
            val hinted = hint.fold(build)(build.hint(_))
            val joined = if (joinType == "left_outer") {
              streamed.join(hinted, streamed("_1") === build("_1"), joinType)
            } else {
              hinted.join(streamed, build("_1") === streamed("_1"), joinType)
            }
            def df: DataFrame = joined
              .select(streamed("_1").as("k"), streamed("_2").as("v"), build("_2").as("w"))
              .sortWithinPartitions("k", "v")
            val plan = df.queryExecution.executedPlan
            val joins = collect(plan) { case j: CometHashJoinExec => j }
            assert(joins.map(_.buildSide) == Seq(buildSide), plan)
            assert(joins.head.nativeOp.getHashJoin.getStreamedSortOrdersCount > 0, plan)
            assert(collect(plan) { case c: CometSparkToColumnarExec => c }.size == 1, plan)
            assert(collect(plan) { case s @ (_: SortExec | _: CometSortExec) => s }.isEmpty, plan)
            sameRowsAsSpark(df)
            assert(
              partitionsInOrder(df, keyOrder(Ascending), valueOrder),
              s"$joinType with hint $hint is out of order\n$plan")
          }
        } finally {
          streamed.unpersist()
        }
      }
    }
  }

  // Spark drops the final sort for the checkpoint's order, which Comet cannot check.
  test("forceShuffledHashJoin keeps the order of a sorted checkpoint through an outer join") {
    withOrderedCheckpoint { streamed =>
      withSQLConf(CometConf.COMET_FORCE_SHJ.key -> "true") {
        val build = spark.range(10).selectExpr("id * 10 AS k")
        val order =
          (0 to 1).map(i => SortOrder(BoundReference(i, LongType, nullable = true), Ascending))
        for {
          (joinType, buildSide) <- Seq("left_outer" -> BuildRight, "right_outer" -> BuildLeft)
        } {
          def df: DataFrame = {
            val on = streamed("k") === build("k")
            val joined = if (joinType == "left_outer") {
              streamed.join(build, on, joinType)
            } else {
              build.join(streamed, on, joinType)
            }
            joined.select(streamed("k"), streamed("v")).sortWithinPartitions("k", "v")
          }
          val plan = df.queryExecution.executedPlan
          val context = s"$joinType\n$plan"
          assert(partitionsInOrder(df, order: _*), context)
          checkSparkAnswerAndFallbackReason(df, CometHashJoin.uncheckedStreamedOrderReason)
          val joins = collect(plan) { case j: ShuffledHashJoinExec => j.buildSide }
          assert(joins == Seq(buildSide), context)
          assert(collect(plan) { case j: CometHashJoinExec => j }.isEmpty, context)
          assert(
            collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty,
            context)
        }
      }
    }
  }

  test("forceShuffledHashJoin respects join strategy hints") {
    withSQLConf(
      CometConf.COMET_FORCE_SHJ.key -> "true",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withForcedSmjTables {
        def hinted(hint: String): DataFrame =
          sql(s"SELECT /*+ $hint */ * FROM tbl_a JOIN tbl_b ON tbl_a._2 = tbl_b._1")
        val (_, merge) = checkSparkAnswerAndOperator(hinted("MERGE(tbl_a)"))
        assert(collect(merge) { case j: CometSortMergeJoinExec => j }.size == 1, merge)
        assert(collect(merge) { case j: CometHashJoinExec => j }.isEmpty, merge)
        // BuildLeft proves Spark planned it: the forced conversion would build the smaller tbl_b.
        val (_, shuffleHash) = checkSparkAnswer(hinted("SHUFFLE_HASH(tbl_a)"))
        assert(
          collect(shuffleHash) { case j: CometHashJoinExec => j.buildSide } == Seq(BuildLeft),
          shuffleHash)
        val (_, broadcast) = checkSparkAnswer(hinted("BROADCAST(tbl_b)"))
        assert(collect(broadcast) { case j: CometBroadcastHashJoinExec => j }.nonEmpty, broadcast)
      }
    }
  }

  test("forceShuffledHashJoin ignores AQE's NO_BROADCAST_HASH hint") {
    var hints = Seq.empty[JoinHint]
    val recordJoinHints = new SparkStrategy {
      override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
        plan match {
          case j: Join if CometConf.COMET_ENABLED.get(SQLConf.get) => hints :+= j.hint
          case _ =>
        }
        Nil
      }
    }
    withForcedShjTables(adaptive = true) {
      // oneKey fills 1 of 10 shuffle partitions, so AQE adds NO_BROADCAST_HASH on re-plan.
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "10") {
        withParquetTable((0 until 100).map(i => (10, i)), "oneKey") {
          val strategies = spark.experimental.extraStrategies
          spark.experimental.extraStrategies = strategies :+ recordJoinHints
          try {
            val (_, cometPlan) = checkSparkAnswer(
              "SELECT big._2, oneKey._2 FROM big JOIN oneKey ON big._1 = oneKey._1")
            assert(collect(cometPlan) { case j: CometHashJoinExec => j }.nonEmpty, cometPlan)
            val sortMergeJoins = collect(cometPlan) {
              case j: SortMergeJoinExec => j
              case j: CometSortMergeJoinExec => j
            }
            assert(sortMergeJoins.isEmpty, cometPlan)
            val strategyHints =
              hints.flatMap(h => Seq(h.leftHint, h.rightHint)).flatten.flatMap(_.strategy)
            assert(strategyHints.contains(NO_BROADCAST_HASH), hints)
          } finally {
            spark.experimental.extraStrategies = strategies
          }
        }
      }
    }
  }

  test("forceShuffledHashJoin builds on the side AQE's PREFER_SHUFFLE_HASH hint names") {
    // Every shuffle partition of the right side fits a local hash map, so AQE hints that side on
    // re-plan. The skewed left side is smaller in all, so the strategy would build on it otherwise.
    var hints = Seq.empty[JoinHint]
    val recordJoinHints = new SparkStrategy {
      override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
        plan match {
          case j: Join if CometConf.COMET_ENABLED.get(SQLConf.get) => hints :+= j.hint
          case _ =>
        }
        Nil
      }
    }
    def isPreferred(hint: Option[HintInfo]): Boolean =
      hint.exists(_.strategy.contains(PREFER_SHUFFLE_HASH))
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
      SQLConf.SKEW_JOIN_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "16",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "131072",
      SQLConf.ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD.key -> "1572864",
      CometConf.COMET_EXEC_RANGE_ENABLED.key -> "true",
      CometConf.COMET_FORCE_SHJ.key -> "true") {
      val left = spark
        .range(90000)
        .selectExpr("IF(id < 60000, 0L, id) AS lk", "sha2(cast(id AS string), 256) AS ls")
      val right =
        spark.range(200000).selectExpr("id AS rk", "sha2(cast(id AS string), 256) AS rs")
      val strategies = spark.experimental.extraStrategies
      spark.experimental.extraStrategies = strategies :+ recordJoinHints
      try {
        val (_, cometPlan) = checkSparkAnswer(
          left
            .join(right, $"lk" === $"rk")
            .selectExpr("sum(length(ls))", "sum(length(rs))"))
        assert(hints.exists(h => isPreferred(h.rightHint) && !isPreferred(h.leftHint)), hints)
        assert(
          collect(cometPlan) { case j: CometHashJoinExec => j.buildSide } == Seq(BuildRight),
          cometPlan)
      } finally {
        spark.experimental.extraStrategies = strategies
      }
    }
  }

  test("forceShuffledHashJoin builds on the one buildable side PREFER_SHUFFLE_HASH names") {
    // tbl_b is the smaller table, so with no hint, or both sides hinted, the strategy builds on
    // it. A LEFT ANTI join cannot build on its left side, so a hint there changes nothing.
    val prefer = Some(HintInfo(strategy = Some(PREFER_SHUFFLE_HASH)))
    val left = JoinHint(prefer, None)
    val right = JoinHint(None, prefer)
    val both = JoinHint(prefer, prefer)
    withSQLConf(
      CometConf.COMET_FORCE_SHJ.key -> "true",
      CometConf.COMET_FORCE_SHJ_MAX_BUILD_SIZE.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withForcedSmjTables {
        def buildSide(query: String, hint: JoinHint): Seq[BuildSide] = {
          val join = sql(query).queryExecution.optimizedPlan.collectFirst { case j: Join => j }
          assert(join.isDefined, query)
          CometShuffledHashJoinStrategy(spark)(join.get.copy(hint = hint)).collect {
            case j: ShuffledHashJoinExec => j.buildSide
          }
        }
        val bigFirst = "SELECT * FROM tbl_a JOIN tbl_b ON tbl_a._2 = tbl_b._1"
        val smallFirst = "SELECT * FROM tbl_b JOIN tbl_a ON tbl_a._2 = tbl_b._1"
        val anti = "SELECT * FROM tbl_a LEFT ANTI JOIN tbl_b ON tbl_a._2 = tbl_b._1"
        for ((query, hint, expected) <- Seq(
            (bigFirst, JoinHint.NONE, BuildRight),
            (bigFirst, left, BuildLeft),
            (bigFirst, both, BuildRight),
            (smallFirst, JoinHint.NONE, BuildLeft),
            (smallFirst, right, BuildRight),
            (smallFirst, both, BuildLeft),
            (anti, left, BuildRight))) {
          assert(buildSide(query, hint) == Seq(expected), s"$query $hint")
        }
      }
    }
  }

  test("forceShuffledHashJoin leaves a runtime broadcast to AQE") {
    withForcedShjTables(adaptive = true) {
      withSQLConf(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
        val (_, cometPlan) = checkSparkAnswer(sql(sortLossJoin))
        val broadcasts = collect(cometPlan) {
          case j: CometBroadcastHashJoinExec => j
          case j: BroadcastHashJoinExec => j
        }
        assert(broadcasts.nonEmpty, cometPlan)
        assert(collect(cometPlan) { case j: CometHashJoinExec => j }.isEmpty, cometPlan)
        assert(collect(cometPlan) { case j: ShuffledHashJoinExec => j }.isEmpty, cometPlan)
      }
    }
  }

  test("forceShuffledHashJoin handles a skewed join under AQE") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "10",
      SQLConf.SKEW_JOIN_ENABLED.key -> "true",
      SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "100",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "100",
      CometConf.COMET_FORCE_SHJ.key -> "true") {
      withParquetTable((0 until 10000).map(i => (if (i < 9000) 0 else i % 100, i)), "skewed") {
        withParquetTable((0 until 100).map(i => (i, i)), "keys") {
          val (_, cometPlan) = checkSparkAnswer(
            "SELECT skewed._2, keys._2 FROM skewed JOIN keys ON skewed._1 = keys._1")
          val hashJoins = collect(cometPlan) {
            case j: ShuffledHashJoinExec => j
            case j: CometHashJoinExec => j.originalPlan
          }
          val skewJoins = hashJoins.collect { case j: ShuffledHashJoinExec if j.isSkewJoin => j }
          assert(skewJoins.nonEmpty, cometPlan)
        }
      }
    }
  }

  // A computed key stops Spark from rewriting the join to compare collation keys.
  test("forceShuffledHashJoin keeps a SortMergeJoin on collated keys") {
    assume(isSpark40Plus)
    withSQLConf(
      CometConf.COMET_FORCE_SHJ.key -> "true",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("c1", "c2") {
        sql("CREATE TABLE c1 (s STRING COLLATE UTF8_LCASE, v INT) USING parquet")
        sql("INSERT INTO c1 VALUES ('a', 1), ('A', 2), ('b', 3)")
        sql("CREATE TABLE c2 (s STRING COLLATE UTF8_LCASE, w INT) USING parquet")
        sql("INSERT INTO c2 VALUES ('a', 10), ('B', 20)")
        val (_, cometPlan) =
          checkSparkAnswer("SELECT v, w FROM c1 JOIN c2 ON trim(c1.s) = c2.s")
        assert(collect(cometPlan) { case j: ShuffledHashJoinExec => j }.isEmpty, cometPlan)
        assert(collect(cometPlan) { case j: CometHashJoinExec => j }.isEmpty, cometPlan)
      }
    }
  }

  // AQE discards a re-plan that hash joins the broadcast stage, so only its warning shows one.
  test("forceShuffledHashJoin leaves a planned broadcast stage to AQE") {
    val appender = new LogAppender("AQE re-plans")
    val logger = classOf[AdaptiveSparkPlanExec].getName
    val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val isUnconfigured = !context.getConfiguration.getLoggers.containsKey(logger)
    val threshold = 10L * 1024
    withForcedShjTables(adaptive = true) {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold.toString,
        SQLConf.ADAPTIVE_EXECUTION_LOG_LEVEL.key -> "WARN") {
        val bigSize = spark.table("big").queryExecution.optimizedPlan.stats.sizeInBytes
        val smallSize = spark.table("small").queryExecution.optimizedPlan.stats.sizeInBytes
        assert(bigSize > threshold, s"big is $bigSize bytes, not over $threshold")
        assert(smallSize <= threshold, s"small is $smallSize bytes, over $threshold")
        try {
          withLogAppender(appender, Seq(logger), Some(Level.WARN)) {
            val (_, cometPlan) = checkSparkAnswer(sql(sortLossJoin))
            val broadcasts = collect(cometPlan) {
              case j: CometBroadcastHashJoinExec => j
              case j: BroadcastHashJoinExec => j
            }
            assert(broadcasts.nonEmpty, cometPlan)
            assert(collect(cometPlan) { case j: CometHashJoinExec => j }.isEmpty, cometPlan)
            assert(collect(cometPlan) { case j: ShuffledHashJoinExec => j }.isEmpty, cometPlan)
          }
        } finally {
          // withLogAppender never removes the config it adds for an unconfigured logger.
          if (isUnconfigured) {
            context.getConfiguration.removeLogger(logger)
            context.updateLoggers()
          }
        }
      }
    }
    val messages = appender.loggingEvents.map(_.getMessage.getFormattedMessage)
    assert(messages.exists(_.contains("Final plan")), messages.mkString("\n"))
    val invalidStages = messages.filter(_.contains("Invalid broadcast query stage"))
    assert(invalidStages.isEmpty, invalidStages.mkString("\n"))
  }

  for ((key, value) <- Seq(
      CometConf.COMET_ENABLED.key -> "false",
      CometConf.COMET_EXEC_ENABLED.key -> "false",
      CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true")) {
    test(s"forceShuffledHashJoin does nothing with $key=$value") {
      withSQLConf(
        CometConf.COMET_FORCE_SHJ.key -> "true",
        key -> value,
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        withForcedSmjTables {
          val df = sql(forcedSmjQuery)
          df.collect()
          val plan = df.queryExecution.executedPlan
          assert(collect(plan) { case j: ShuffledHashJoinExec => j }.isEmpty, plan)
          assert(collect(plan) { case j: SortMergeJoinExec => j }.nonEmpty, plan)
        }
      }
    }
  }

  // Spark plans NOT IN as a null-aware anti join that streams the left side and reports that
  // side's ordering, so Spark drops a sort above the join when the side arrives sorted. Here that
  // side is a cached table, sorted within its partitions, that reaches the native plan through
  // Comet's JVM to native conversion. Comet runs the join with that side as DataFusion's build
  // side. Sparse keys make DataFusion build a hash map, which takes the batches last to first;
  // dense integer keys would take its array map path instead, which keeps them in order.
  private def withSortedCache(data: DataFrame)(f: => Unit): Unit = {
    withSQLConf(
      CometConf.COMET_CONVERT_FROM_IN_MEMORY_CACHE_ENABLED.key -> "true",
      CometConf.COMET_BATCH_SIZE.key -> "64",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val cached = data.repartition(2, data("_1")).sortWithinPartitions("_1").cache()
      try {
        cached.count()
        withTempView("streamed") {
          cached.createOrReplaceTempView("streamed")
          f
        }
      } finally {
        cached.unpersist()
      }
    }
  }

  private def withNotInTables(f: => Unit): Unit = {
    withParquetTable((0 until 10000).map(i => (i % 100 * 1000, i)), "big") {
      withParquetTable((0 until 10).map(i => (i * 10000, i)), "small") {
        f
      }
    }
  }

  /**
   * Checks the query's rows against Spark, that no sort runs above a broadcast hash join, and
   * that every partition is sorted by the query's first column, an integer. Returns the Comet
   * plan.
   */
  private def checkSortedPartitions(query: => DataFrame): SparkPlan = {
    val (_, plan) = checkSparkAnswer(query)
    // Spark dropped the sort above the join, so the order comes from the plan below it.
    val sorts = collect(plan) { case s @ (_: SortExec | _: CometSortExec) => s }
    assert(
      sorts.forall(_.find {
        case _: BroadcastHashJoinExec | _: CometBroadcastHashJoinExec => true
        case _ => false
      }.isEmpty),
      plan)
    val parts = query.queryExecution.toRdd.map(_.getInt(0)).glom().collect()
    parts.zipWithIndex.foreach { case (keys, i) =>
      assert(keys.sameElements(keys.sorted), s"partition $i is out of order\n$plan")
    }
    plan
  }

  private val orderedNotInFallback = "null-aware anti-join over an ordered streamed side"

  for (adaptive <- Seq(false, true)) {
    test(s"NOT IN over a sorted cached side falls back and keeps its order, AQE=$adaptive") {
      withNotInTables {
        withSortedCache(spark.table("big")) {
          withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
            val plan = checkSortedPartitions(
              sql("SELECT _1 AS k, _2 AS v FROM streamed WHERE _1 NOT IN (SELECT _1 FROM small)")
                .sortWithinPartitions("k"))
            val joins = collect(plan) { case j: BroadcastHashJoinExec => j }
            assert(joins.length == 1 && joins.head.isNullAwareAntiJoin, plan)
            assert(collect(plan) { case j: CometBroadcastHashJoinExec => j }.isEmpty, plan)
            assert(
              new ExtendedExplainInfo()
                .getFallbackReasons(plan)
                .exists(_.contains(orderedNotInFallback)),
              plan)
          }
        }
      }
    }
  }

  // Spark's sort aggregate trusts the same ordering, through an outer join that streams the NOT IN
  // output, so a NOT IN that loses it splits groups. Comet aggregates are off so that Spark's sort
  // aggregate runs over the joins, as it does for an aggregate Comet cannot convert.
  test("NOT IN below an outer hash join keeps its groups and order under a GROUP BY") {
    withNotInTables {
      withParquetTable((0 until 50).map(i => (i * 2000, i)), "dim") {
        withSortedCache(spark.table("big")) {
          withSQLConf(
            SQLConf.USE_OBJECT_HASH_AGG.key -> "false",
            CometConf.COMET_EXEC_AGGREGATE_ENABLED.key -> "false") {
            val plan = checkSortedPartitions(
              sql("""SELECT s.k, count(*) AS n, sort_array(collect_list(d._2)) AS w
                    |FROM (SELECT _1 AS k FROM streamed WHERE _1 NOT IN (SELECT _1 FROM small)) s
                    |LEFT JOIN dim d ON s.k = d._1
                    |GROUP BY s.k""".stripMargin).sortWithinPartitions("k"))
            assert(collect(plan) { case a: SortAggregateExec => a }.nonEmpty, plan)
          }
        }
      }
    }
  }

  test("NOT IN over an unordered streamed side runs natively") {
    withNotInTables {
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
        val (_, plan) = checkSparkAnswerAndOperator(
          sql("SELECT _1, _2 FROM big WHERE _1 NOT IN (SELECT _1 FROM small)"))
        val joins = collect(plan) { case j: CometBroadcastHashJoinExec => j }
        assert(joins.length == 1 && joins.head.isNullAwareAntiJoin, plan)
        assert(joins.head.outputOrdering.isEmpty, plan)
      }
    }
  }

  // With default settings the streamed side can be native from parquet. A window partitioned by
  // `_1` sorts its input natively, and `k` carries `_1` through the window, so the NOT IN key is a
  // window output that the optimizer cannot push below the sort. The ROWS frame keeps the window
  // on DataFusion's streaming window operator, so the join's build side spans many batches.
  private val windowedBig =
    "SELECT _1, _2, min(_1) OVER (PARTITION BY _1 ORDER BY _2 ROWS UNBOUNDED PRECEDING) AS k " +
      "FROM big"

  for (adaptive <- Seq(false, true)) {
    test(s"NOT IN over a native window side falls back and keeps its order, AQE=$adaptive") {
      withNotInTables {
        withSQLConf(
          CometConf.COMET_BATCH_SIZE.key -> "64",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
          val plan = checkSortedPartitions(
            sql(s"SELECT _1, _2 FROM ($windowedBig) t WHERE k NOT IN (SELECT _1 FROM small)")
              .sortWithinPartitions("_1"))
          assert(collect(plan) { case w: CometWindowExec => w }.nonEmpty, plan)
          val joins = collect(plan) { case j: BroadcastHashJoinExec => j }
          assert(joins.length == 1 && joins.head.isNullAwareAntiJoin, plan)
          assert(collect(plan) { case j: CometBroadcastHashJoinExec => j }.isEmpty, plan)
          // AQE plans the join again once the shuffle below the window completes. By then its
          // build side is Spark's broadcast stage, so Comet skips the new join without a reason,
          // and only the initial plan carries the reason the guard recorded.
          if (!adaptive) {
            assert(
              new ExtendedExplainInfo()
                .getFallbackReasons(plan)
                .exists(_.contains(orderedNotInFallback)),
              plan)
          }
        }
      }
    }
  }

  // The fallback only covers the null-aware join: a semi join over the same ordered side still
  // runs natively, since DataFusion probes with that side and keeps its order.
  test("LEFT SEMI join over the native window side used for NOT IN stays native") {
    withNotInTables {
      withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "64") {
        val plan = checkSortedPartitions(
          sql(s"SELECT /*+ BROADCAST(small) */ t._1, t._2 FROM ($windowedBig) t " +
            "LEFT SEMI JOIN small ON t.k = small._1").sortWithinPartitions("_1"))
        val joins = collect(plan) { case j: CometBroadcastHashJoinExec => j }
        assert(joins.length == 1 && !joins.head.isNullAwareAntiJoin, plan)
        assert(collect(plan) { case j: BroadcastHashJoinExec => j }.isEmpty, plan)
        assert(
          !new ExtendedExplainInfo()
            .getFallbackReasons(plan)
            .exists(_.contains(orderedNotInFallback)),
          plan)
      }
    }
  }

  test("BroadcastHashJoin with LeftAnti and NOT IN subquery (null-aware)") {
    withSQLConf(
      SQLConf.PREFER_SORTMERGEJOIN.key -> "false",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
      // Right side has no NULL: regular anti-semantics
      withParquetTable((0 until 10).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 5).map(i => (i, i + 100)), "tbl_b") {
          val df = sql("SELECT * FROM tbl_a WHERE _2 NOT IN (SELECT _1 FROM tbl_b)")
          checkSparkAnswerAndOperator(df)
        }
      }

      // Right side contains NULL: null-aware should suppress all left rows
      withParquetTable(Seq[(Int, Integer)]((1, 1), (2, 2), (3, 3)), "tbl_a") {
        withParquetTable(Seq[(Integer, Int)]((1, 100), (null, 200)), "tbl_b") {
          val df = sql("SELECT * FROM tbl_a WHERE _2 NOT IN (SELECT _1 FROM tbl_b)")
          checkSparkAnswerAndOperator(df)
        }
      }

      // Left side has NULL values: NOT IN filters them out (NULL vs anything is NULL)
      withParquetTable(Seq[(Int, Integer)]((1, 1), (2, null), (3, 3)), "tbl_a") {
        withParquetTable(Seq[(Integer, Int)]((2, 100), (4, 200)), "tbl_b") {
          val df = sql("SELECT * FROM tbl_a WHERE _2 NOT IN (SELECT _1 FROM tbl_b)")
          checkSparkAnswerAndOperator(df)
        }
      }

      // Empty subquery: NOT IN against an empty set returns all left rows, including NULL probe.
      withParquetTable(Seq[(Int, Integer)]((1, 1), (2, null), (3, 3)), "tbl_a") {
        withParquetTable(Seq.empty[(Integer, Int)], "tbl_b") {
          val df = sql("SELECT * FROM tbl_a WHERE _2 NOT IN (SELECT _1 FROM tbl_b)")
          checkSparkAnswerAndOperator(df)
        }
      }

      // Both sides have NULL keys: probe-side NULL and build-side NULL on the same query.
      withParquetTable(Seq[(Int, Integer)]((1, 1), (2, null), (3, 3)), "tbl_a") {
        withParquetTable(Seq[(Integer, Int)]((1, 100), (null, 200)), "tbl_b") {
          val df = sql("SELECT * FROM tbl_a WHERE _2 NOT IN (SELECT _1 FROM tbl_b)")
          checkSparkAnswerAndOperator(df)
        }
      }
    }
  }

  test("BroadcastHashJoin with LeftAnti (non-null-aware)") {
    withSQLConf(
      SQLConf.PREFER_SORTMERGEJOIN.key -> "false",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
      withParquetTable((0 until 10).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 5).map(i => (i, i + 100)), "tbl_b") {
          // BROADCAST(tbl_b) forces tbl_b as build-right side
          val df = sql(
            "SELECT /*+ BROADCAST(tbl_b) */ * FROM tbl_a LEFT ANTI JOIN tbl_b " +
              "ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df)
        }
      }

      // With NULL values on both sides - non-null-aware semantics: NULL keys don't match anything
      withParquetTable(Seq[(Int, Integer)]((1, 1), (2, null), (3, 3)), "tbl_a") {
        withParquetTable(Seq[(Integer, Int)]((1, 100), (null, 200)), "tbl_b") {
          val df = sql(
            "SELECT /*+ BROADCAST(tbl_b) */ * FROM tbl_a LEFT ANTI JOIN tbl_b " +
              "ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df)
        }
      }
    }
  }

  test("HashJoin with join filter") {
    withSQLConf(
      SQLConf.PREFER_SORTMERGEJOIN.key -> "false",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable((0 until 10).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 10).map(i => (i % 10, i + 2)), "tbl_b") {
          // Inner join: build left
          val df1 =
            sql(
              "SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a JOIN tbl_b " +
                "ON tbl_a._2 = tbl_b._1 AND tbl_a._1 > tbl_b._2")
          checkSparkAnswerAndOperator(df1)

          // Right join: build left
          val df2 =
            sql(
              "SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a RIGHT JOIN tbl_b " +
                "ON tbl_a._2 = tbl_b._1 AND tbl_a._1 > tbl_b._2")
          checkSparkAnswerAndOperator(df2)

          // Full join: build left
          val df3 =
            sql(
              "SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a FULL JOIN tbl_b " +
                "ON tbl_a._2 = tbl_b._1 AND tbl_a._1 > tbl_b._2")
          checkSparkAnswerAndOperator(df3)
        }
      }
    }
  }

  test("right key reduction requires an unfiltered left-semi join sorted over a Comet shuffle") {
    val config = CometConf.COMET_EXEC_SORT_MERGE_JOIN_RIGHT_KEY_REDUCTION_ENABLED
    val right = spark.range(1).queryExecution.sparkPlan
    val exchange = ShuffleExchangeExec(SinglePartition, right)
    val shuffle = CometShuffleExchangeExec(SinglePartition, right, exchange)
    val child = OperatorOuterClass.Operator.getDefaultInstance
    withSQLConf(
      config.key -> "true",
      CometConf.COMET_EXEC_SORT_MERGE_JOIN_WITH_JOIN_FILTER_ENABLED.key -> "true") {
      def eligible(
          input: SparkPlan,
          keys: Seq[Expression],
          kind: JoinType = LeftSemi,
          condition: Option[Expression] = None): Boolean = {
        val sort = SortExec(keys.map(SortOrder(_, Ascending)), global = false, input)
        val sorted = CometSortExec.createExec(child, sort)
        val join = SortMergeJoinExec(keys, keys, kind, condition, input, sorted)
        CometSortMergeJoinExec
          .convert(join, OperatorOuterClass.Operator.newBuilder(), child, child)
          .get
          .getSortMergeJoin
          .getRightKeyReductionEnabled
      }
      val keys = right.output
      assert(eligible(shuffle, keys))
      assert(!eligible(right, keys))
      assert(!eligible(CoalesceExec(1, shuffle), keys))
      assert(!eligible(shuffle, keys, Inner))
      assert(!eligible(shuffle, keys, condition = Some(Literal.TrueLiteral)))
      assert(!eligible(shuffle, Seq(Cast(keys.head, IntegerType))))
    }
  }

  test("right key reduction sorts fewer BIGINT rows and keeps a match in the final batch") {
    val config = CometConf.COMET_EXEC_SORT_MERGE_JOIN_RIGHT_KEY_REDUCTION_ENABLED
    val wideKey = 1L << 32
    val left = Seq[(Int, java.lang.Long, Int)](
      (1, null, 10),
      (1, null, 10),
      (2, 0L, 10),
      (3, 1L, 10),
      (3, 1L, 10),
      (4, null, 20),
      (5, 0L, 20),
      (6, wideKey, 20),
      (7, -1L, 20),
      (8, wideKey - 1, 20))
    val keys = Seq[(java.lang.Long, Int)]((null, 10), (1L, 10), (0L, 20), (wideKey, 20))
    val right = (0 until 32768).map(i => keys(i % keys.size)) :+ (Long.box(-1L) -> 20)

    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      CometConf.COMET_BATCH_SIZE.key -> "4096") {
      withParquetTable(left, "reduction_left") {
        withParquetTable(right, "reduction_right") {
          def run(condition: String, expected: Seq[Int]): DataFrame = {
            val df = sql(s"""SELECT /*+ MERGE(r) */ l._1 FROM reduction_left l
                            |LEFT SEMI JOIN reduction_right r ON $condition""".stripMargin)
            assert(df.collect().map(_.getInt(0)).sorted.toSeq == expected)
            df
          }
          val equality = "l._2 = r._1"
          val expected = Seq(2, 3, 3, 5, 6, 7)
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            val plan = run(equality, expected).queryExecution.executedPlan
            assert(collectFirst(plan) { case j: SortMergeJoinExec => j }.isDefined)
          }
          var baselineSortRows = 0L
          for {
            (adaptive, directRead, writer) <-
              Seq((false, true, "native"), (true, false, "jvm"), (true, true, "native"))
            enabled <- Seq(false, true)
          } {
            withSQLConf(
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
              SQLConf.SHUFFLE_PARTITIONS.key -> (if (adaptive) "2" else "1"),
              CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> directRead.toString,
              CometConf.COMET_SHUFFLE_MODE.key -> writer,
              config.key -> enabled.toString) {
              val plan = run(equality, expected).queryExecution.executedPlan
              val join = collectFirst(plan) { case j: CometSortMergeJoinExec => j }
                .getOrElse(fail(plan.treeString))
              assert(join.nativeOp.getSortMergeJoin.getRightKeyReductionEnabled == enabled)
              val sort = collectFirst(join.right) { case s: CometSortExec => s }
                .getOrElse(fail(plan.treeString))
              val sortRows = sort.longMetric("output_rows").value
              assert(sortRows > 0, plan.treeString)
              if (enabled) {
                val in = join.longMetric("right_key_reduction_input_rows").value
                val out = join.longMetric("right_key_reduction_output_rows").value
                assert(out > 0 && out < in, plan.treeString)
                if (!adaptive) {
                  // One file and one shuffle partition put the new key alone in the final batch.
                  assert(join.longMetric("right_key_reduction_bypass_rows").value == 8192L + 1)
                }
                assert(out == sortRows && join.longMetric("output_rows").value == expected.size)
                assert(!sort.metrics.contains("right_key_reduction_output_rows"))
                assert(sortRows < baselineSortRows, plan.treeString)
              } else {
                assert(!join.metrics.contains("right_key_reduction_input_rows"))
                baselineSortRows = sortRows
              }
            }
          }
          withSQLConf(
            config.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> "native",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
            for ((condition, ids) <- Seq(
                ("l._2 <=> r._1", Seq(1, 1, 2, 3, 3, 4, 5, 6, 7)),
                (s"$equality AND l._3 = r._2", Seq(3, 3, 5, 6, 7)))) {
              val plan = run(condition, ids).queryExecution.executedPlan
              val join = collectFirst(plan) { case j: CometSortMergeJoinExec => j }
                .getOrElse(fail(plan.treeString))
              assert(!join.nativeOp.getSortMergeJoin.getRightKeyReductionEnabled)
              assert(!join.metrics.contains("right_key_reduction_input_rows"))
            }
          }
        }
      }
    }
  }

  for (pool <- Seq("fair_unified", "greedy_unified")) {
    test(s"right key reduction admission with resident JVM execution memory ($pool)") {
      val rightRows = 32768
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        CometConf.COMET_BATCH_SIZE.key -> "4096",
        CometConf.COMET_SHUFFLE_MODE.key -> "native",
        CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> "true",
        CometConf.COMET_OFFHEAP_MEMORY_POOL_TYPE.key -> pool,
        CometConf.COMET_EXEC_SORT_MERGE_JOIN_RIGHT_KEY_REDUCTION_ENABLED.key -> "true") {
        withParquetTable(Seq(0, 1, 1, 3, 4).map(Tuple1(_)), "admission_left") {
          withParquetTable((0 until rightRows).map(i => Tuple1(i % 4)), "admission_right") {
            for (holdMemory <- Seq(false, true)) {
              val df = sql("""SELECT /*+ MERGE(r) */ l._1 FROM admission_left l
                           |LEFT SEMI JOIN admission_right r ON l._1 = r._1""".stripMargin)
              val plan = df.queryExecution.executedPlan
              val join = collectFirst(plan) { case j: CometSortMergeJoinExec => j }.get
              val sort = collectFirst(join.right) { case s: CometSortExec => s }.get
              assert(join.nativeOp.getSortMergeJoin.getRightKeyReductionEnabled)

              val result = SQLExecution.withSQLConfPropagated(spark) {
                plan
                  .execute()
                  .mapPartitions { rows =>
                    val context = TaskContext.get()
                    val manager = CometExecIterator.taskMemory(context, 0L).manager.internal
                    val consumer = new MemoryConsumer(manager, 0L, MemoryMode.OFF_HEAP) {
                      override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
                    }
                    try {
                      // The native iterator exists, but its plan is built only on the first pull.
                      if (holdMemory) {
                        require(consumer.acquireMemory(4096L) == 4096L)
                      }
                      Iterator.single(rows.map(_.getInt(0)).toVector)
                    } finally {
                      consumer.freeMemory(consumer.getUsed)
                    }
                  }
                  .collect()
                  .flatten
                  .sorted
                  .toSeq
              }
              assert(result == Seq(0, 1, 1, 3))
              val in = join.longMetric("right_key_reduction_input_rows").value
              val out = join.longMetric("right_key_reduction_output_rows").value
              if (holdMemory && pool == "fair_unified") {
                assert(in == 0 && out == 0)
                assert(sort.longMetric("output_rows").value == rightRows)
              } else {
                assert(in == rightRows && out > 0 && out < in)
                assert(sort.longMetric("output_rows").value == out)
              }
            }
          }
        }
      }
    }
  }

  test("SortMergeJoin without join filter") {
    withSQLConf(
      CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable((0 until 10).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 10).map(i => (i % 10, i + 2)), "tbl_b") {
          val df1 = sql("SELECT * FROM tbl_a JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df1)

          val df2 = sql("SELECT * FROM tbl_a LEFT JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df2)

          val df3 = sql("SELECT * FROM tbl_b LEFT JOIN tbl_a ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df3)

          val df4 = sql("SELECT * FROM tbl_a RIGHT JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df4)

          val df5 = sql("SELECT * FROM tbl_b RIGHT JOIN tbl_a ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df5)

          val df6 = sql("SELECT * FROM tbl_a FULL JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df6)

          val df7 = sql("SELECT * FROM tbl_b FULL JOIN tbl_a ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df7)

          val left = sql("SELECT * FROM tbl_a")
          val right = sql("SELECT * FROM tbl_b")

          val df8 = left.join(right, left("_2") === right("_1"), "leftsemi")
          checkSparkAnswerAndOperator(df8)

          val df9 = right.join(left, left("_2") === right("_1"), "leftsemi")
          checkSparkAnswerAndOperator(df9)

          val df10 = left.join(right, left("_2") === right("_1"), "leftanti")
          checkSparkAnswerAndOperator(df10)

          val df11 = right.join(left, left("_2") === right("_1"), "leftanti")
          checkSparkAnswerAndOperator(df11)
        }
      }
    }
  }

  test("SortMergeJoin with join filter") {
    withSQLConf(
      CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SORT_MERGE_JOIN_WITH_JOIN_FILTER_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable((0 until 10).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 10).map(i => (i % 10, i + 2)), "tbl_b") {
          val df1 = sql(
            "SELECT * FROM tbl_a JOIN tbl_b ON tbl_a._2 = tbl_b._1 AND " +
              "tbl_a._1 > tbl_b._2")
          checkSparkAnswerAndOperator(df1)

          val df2 = sql(
            "SELECT * FROM tbl_a LEFT JOIN tbl_b ON tbl_a._2 = tbl_b._1 " +
              "AND tbl_a._1 > tbl_b._2")
          checkSparkAnswerAndOperator(df2)

          val df3 = sql(
            "SELECT * FROM tbl_b LEFT JOIN tbl_a ON tbl_a._2 = tbl_b._1 " +
              "AND tbl_a._1 > tbl_b._2")
          checkSparkAnswerAndOperator(df3)

          val df4 = sql(
            "SELECT * FROM tbl_a RIGHT JOIN tbl_b ON tbl_a._2 = tbl_b._1 " +
              "AND tbl_a._1 > tbl_b._2")
          checkSparkAnswerAndOperator(df4)

          val df5 = sql(
            "SELECT * FROM tbl_b RIGHT JOIN tbl_a ON tbl_a._2 = tbl_b._1 " +
              "AND tbl_a._1 > tbl_b._2")
          checkSparkAnswerAndOperator(df5)

          val df6 = sql(
            "SELECT * FROM tbl_a FULL JOIN tbl_b ON tbl_a._2 = tbl_b._1 " +
              "AND tbl_a._1 > tbl_b._2")
          checkSparkAnswerAndOperator(df6)

          val df7 = sql(
            "SELECT * FROM tbl_b FULL JOIN tbl_a ON tbl_a._2 = tbl_b._1 " +
              "AND tbl_a._1 > tbl_b._2")
          checkSparkAnswerAndOperator(df7)

          val df8 = sql(
            "SELECT * FROM tbl_a LEFT SEMI JOIN tbl_b ON tbl_a._2 = tbl_b._1 " +
              "AND tbl_a._2 >= tbl_b._1")
          checkSparkAnswerAndOperator(df8)

          val df9 = sql(
            "SELECT * FROM tbl_b LEFT SEMI JOIN tbl_a ON tbl_a._2 = tbl_b._1 " +
              "AND tbl_a._2 >= tbl_b._1")
          checkSparkAnswerAndOperator(df9)

          val df10 = sql(
            "SELECT * FROM tbl_a LEFT ANTI JOIN tbl_b ON tbl_a._2 = tbl_b._1 " +
              "AND tbl_a._2 >= tbl_b._1")
          checkSparkAnswerAndOperator(df10)

          val df11 = sql(
            "SELECT * FROM tbl_b LEFT ANTI JOIN tbl_a ON tbl_a._2 = tbl_b._1 " +
              "AND tbl_a._2 >= tbl_b._1")
          checkSparkAnswerAndOperator(df11)
        }
      }
    }
  }

  test("full outer join") {
    withTempView("`left`", "`right`", "allNulls") {
      allNulls.createOrReplaceTempView("allNulls")

      upperCaseData.where($"N" <= 4).createOrReplaceTempView("`left`")
      upperCaseData.where($"N" >= 3).createOrReplaceTempView("`right`")

      val left = UnresolvedRelation(TableIdentifier("left"))
      val right = UnresolvedRelation(TableIdentifier("right"))

      checkSparkAnswer(left.join(right, $"left.N" === $"right.N", "full"))

      checkSparkAnswer(left.join(right, ($"left.N" === $"right.N") && ($"left.N" =!= 3), "full"))

      checkSparkAnswer(left.join(right, ($"left.N" === $"right.N") && ($"right.N" =!= 3), "full"))

      checkSparkAnswer(sql("""
          |SELECT l.a, count(*)
          |FROM allNulls l FULL OUTER JOIN upperCaseData r ON (l.a = r.N)
          |GROUP BY l.a
        """.stripMargin))

      checkSparkAnswer(sql("""
          |SELECT r.N, count(*)
          |FROM allNulls l FULL OUTER JOIN upperCaseData r ON (l.a = r.N)
          |GROUP BY r.N
          """.stripMargin))

      checkSparkAnswer(sql("""
          |SELECT l.N, count(*)
          |FROM upperCaseData l FULL OUTER JOIN allNulls r ON (l.N = r.a)
          |GROUP BY l.N
          """.stripMargin))

      checkSparkAnswer(sql("""
          |SELECT r.a, count(*)
          |FROM upperCaseData l FULL OUTER JOIN allNulls r ON (l.N = r.a)
          |GROUP BY r.a
        """.stripMargin))
    }
  }

  test("Broadcast hash join build-side batch coalescing") {
    // Use many shuffle partitions to produce many small broadcast batches,
    // then verify that coalescing reduces the build-side batch count to 1 per task.
    val numPartitions = 512
    withSQLConf(
      CometConf.COMET_BATCH_SIZE.key -> "100",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "false",
      "spark.sql.join.forceApplyShuffledHashJoin" -> "true",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> numPartitions.toString) {
      withParquetTable((0 until 10000).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 10000).map(i => (i % 10, i + 2)), "tbl_b") {
          // Force a shuffle on tbl_a before broadcast so the broadcast source has
          // numPartitions partitions, not just the number of parquet files.
          val query =
            s"""SELECT /*+ BROADCAST(a) */ *
               |FROM (SELECT /*+ REPARTITION($numPartitions) */ * FROM tbl_a) a
               |JOIN tbl_b ON a._2 = tbl_b._1""".stripMargin

          val (_, cometPlan) = checkSparkAnswerAndOperator(
            sql(query),
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastHashJoinExec]))

          val joins = collect(cometPlan) { case j: CometBroadcastHashJoinExec =>
            j
          }
          assert(joins.nonEmpty, "Expected CometBroadcastHashJoinExec in plan")

          val join = joins.head
          val buildBatches = join.metrics("build_input_batches").value

          // Without coalescing, build_input_batches would be ~numPartitions per task,
          // totaling ~numPartitions * numPartitions across all tasks.
          // With coalescing, each task gets 1 batch, so total ≈ numPartitions.
          assert(
            buildBatches <= numPartitions,
            s"Expected at most $numPartitions build batches (1 per task), got $buildBatches. " +
              "Broadcast batch coalescing may not be working.")

          val broadcasts = collect(cometPlan) { case b: CometBroadcastExchangeExec =>
            b
          }
          assert(broadcasts.nonEmpty, "Expected CometBroadcastExchangeExec in plan")

          val broadcast = broadcasts.head
          val coalescedBatches = broadcast.metrics("numCoalescedBatches").value
          val coalescedRows = broadcast.metrics("numCoalescedRows").value

          assert(
            coalescedBatches >= numPartitions,
            s"Expected at least $numPartitions coalesced batches, got $coalescedBatches")
          assert(coalescedRows == 10000, s"Expected 10000 coalesced rows, got $coalescedRows")
        }
      }
    }
  }

  test("Broadcast coalescing handles union children with different nullability") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withParquetTable(Seq((1, 10), (2, 20), (3, 30)), "t") {
        val (_, cometPlan) = checkSparkAnswerAndOperator(
          sql("""
              |SELECT /*+ BROADCAST(b) */ p._1, b.v
              |FROM t p JOIN (
              |  SELECT _1 AS k, 99 AS v FROM t
              |  UNION ALL
              |  SELECT _1 AS k, _2 + 1 AS v FROM t
              |) b ON p._1 = b.k
              |""".stripMargin),
          Seq(
            classOf[CometBroadcastExchangeExec],
            classOf[CometBroadcastHashJoinExec],
            classOf[CometUnionExec]))

        val broadcast = collect(cometPlan) { case b: CometBroadcastExchangeExec => b }.head
        assert(broadcast.metrics("numCoalescedBatches").value > 0L)
        assert(broadcast.metrics("numCoalescedRows").value == 6L)
      }
    }
  }

  test("Broadcast coalescing falls back for array field metadata mismatch") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key -> "false",
      SQLConf.IGNORE_MISSING_PARQUET_FIELD_ID.key -> "true") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        Seq((1, Seq(10)), (2, Seq(20))).toDF("k", "v").coalesce(1).write.parquet(path)

        def readWithFieldId(fieldId: Long) = {
          val metadata = new MetadataBuilder()
            .putLong("parquet.field.id", fieldId)
            .build()
          val schema = StructType(
            Seq(
              StructField("k", IntegerType, nullable = true),
              StructField(
                "v",
                ArrayType(IntegerType, containsNull = true),
                nullable = true,
                metadata)))
          spark.read.schema(schema).parquet(path)
        }

        withTempView("metadata_left", "metadata_right") {
          readWithFieldId(1).createOrReplaceTempView("metadata_left")
          readWithFieldId(2).createOrReplaceTempView("metadata_right")

          val (_, cometPlan) = checkSparkAnswerAndOperator(
            sql("""
                |SELECT /*+ BROADCAST(u) */ p.k, u.v
                |FROM metadata_left p JOIN (
                |  SELECT k, v FROM metadata_left
                |  UNION ALL
                |  SELECT k, v FROM metadata_right
                |) u ON p.k = u.k
                |""".stripMargin),
            Seq(
              classOf[CometBroadcastExchangeExec],
              classOf[CometBroadcastHashJoinExec],
              classOf[CometUnionExec]))

          val broadcast = collect(cometPlan) { case b: CometBroadcastExchangeExec => b }.head
          assert(broadcast.metrics("numCoalescedBatches").value == 0L)
          assert(broadcast.metrics("numCoalescedRows").value == 0L)
        }
      }
    }
  }

  test("Broadcast coalescing keeps the values of sliced batches") {
    // A grouped aggregate emits its groups as batch-size slices of one batch, so every slice after
    // the first reaches the JVM with offsets that start past zero. Coalescing those slices used to
    // prepend the bytes of the rows before each slice to its first value (#6779).
    withSQLConf(
      CometConf.COMET_BATCH_SIZE.key -> "100",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val rows = (0 until 1000).map { i =>
        val s = if (i % 7 == 0) null else s"value-$i"
        val bytes = if (s == null) null else s.getBytes(StandardCharsets.UTF_8)
        val array = if (i % 11 == 0) null else Seq(s, s"element-$i")
        (i, s, bytes, array, (s, i))
      }
      withParquetTable(rows, "t") {
        val (_, cometPlan) = checkSparkAnswerAndOperator(
          sql("""
              |SELECT /*+ BROADCAST(d) */ t._1, d._2, d._3, d._4, d._5
              |FROM t JOIN (SELECT DISTINCT * FROM t) d ON t._1 = d._1
              |""".stripMargin),
          Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastHashJoinExec]))

        val broadcast = collect(cometPlan) { case b: CometBroadcastExchangeExec => b }.head
        assert(broadcast.metrics("numCoalescedRows").value == rows.length)
      }
    }
  }

  // Reproducer for SPARK-43113: full outer SMJ with a join filter that references
  // a nullable column should not match when the filter evaluates to NULL.
  test("SPARK-43113: Full outer SMJ with NULL in join filter") {
    withTempView("l", "r") {
      // testData2: (a, b) — all non-null
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2))
        .toDF("a", "b")
        .createOrReplaceTempView("l")

      // testData3: (a, b) — b is nullable
      Seq((1, None), (2, Some(2)))
        .toDF("a", "b")
        .createOrReplaceTempView("r")

      val query =
        """select /*+ MERGE(r) */ *
          |from l
          |full outer join r
          |on l.a = r.a
          |and l.b < (r.b + 1)
          |and l.b < (r.a + 1)""".stripMargin

      val expected = Seq(
        (Some(1), Some(1), None, None),
        (Some(1), Some(2), None, None),
        (None, None, Some(1), None),
        (Some(2), Some(1), Some(2), Some(2)),
        (Some(2), Some(2), Some(2), Some(2)),
        (Some(3), Some(1), None, None),
        (Some(3), Some(2), None, None)).toDF("a", "b", "a", "b")

      val df = sql(query)
      checkAnswer(df, expected)
    }
  }

  test("Broadcast exchange respects AQE shuffle partition coalescing") {
    // When a shuffle feeds into a broadcast exchange, AQE may coalesce the shuffle
    // partitions. The broadcast collect should execute through the AQEShuffleReadExec
    // to use coalesced partitions rather than bypassing it.
    val numPartitions = 200
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> numPartitions.toString,
      SQLConf.PREFER_SORTMERGEJOIN.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true") {
      withParquetTable((0 until 100).map(i => (i, i % 5)), "small_tbl") {
        withParquetTable((0 until 10000).map(i => (i, i + 2)), "large_tbl") {
          val query =
            """SELECT /*+ BROADCAST(a) */ *
              |FROM (SELECT /*+ REBALANCE(_1) */ * FROM small_tbl) a
              |JOIN large_tbl b ON a._1 = b._1""".stripMargin

          val (_, cometPlan) = checkSparkAnswerAndOperator(
            sql(query),
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastHashJoinExec]))

          // The shuffle partitions feeding the broadcast should be coalesced by
          // AQE. AQEShuffleReadExec.executeColumnar() lazily builds its shuffleRDD
          // and, as a side effect, sets the "numPartitions" driver metric to
          // partitionSpecs.length. If the broadcast collect bypasses the wrapper
          // (the bug this test guards against), executeColumnar is never called
          // and the metric stays at its initial 0.
          val readExecs = collect(cometPlan) { case r: AQEShuffleReadExec => r }
          assert(readExecs.nonEmpty, "Expected AQEShuffleReadExec in plan")
          readExecs.foreach { r =>
            val coalesced = r.metrics("numPartitions").value
            assert(
              coalesced > 0,
              "AQEShuffleReadExec.numPartitions metric was never updated; the " +
                "broadcast collect likely bypassed AQEShuffleReadExec")
            assert(
              coalesced < numPartitions,
              s"Expected AQE to coalesce shuffle partitions below $numPartitions, " +
                s"got $coalesced")
          }
        }
      }
    }
  }

  test("BroadcastNestedLoopJoin with unequal filter") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // Include NULL keys: predicate `_1 > _1` returns NULL with a NULL operand, so
      // those rows must not contribute to the join output.
      val left: Seq[(Integer, Int)] =
        (0 until 100).map(i => ((i: Integer), i % 5)) ++ Seq[(Integer, Int)]((null, 7), (50, -1))
      val right: Seq[(Integer, Int)] =
        (0 until 10).map(i => ((i: Integer), i + 5)) ++ Seq[(Integer, Int)]((null, 1))
      withParquetTable(left, "tbl_a") {
        withParquetTable(right, "tbl_b") {
          val df =
            sql("SELECT /*+ BROADCAST(tbl_b) */ * FROM tbl_a JOIN tbl_b ON tbl_a._1 > tbl_b._1")
          checkSparkAnswerAndOperator(
            df,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastNestedLoopJoinExec]))
        }
      }
    }
  }

  test("BroadcastNestedLoopJoin cross join with count-only output") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val left: Seq[(Integer, Int)] =
        (0 until 100).map(i => ((i: Integer), i % 5)) ++ Seq[(Integer, Int)]((null, 9))
      val right: Seq[(Integer, String)] =
        (0 until 5).map(i => ((i: Integer), s"w_$i")) ++ Seq[(Integer, String)]((null, "w_null"))
      withParquetTable(left, "tbl_a") {
        withParquetTable(right, "tbl_b") {
          val df = sql("SELECT /*+ BROADCAST(tbl_b) */ count(*) FROM tbl_a, tbl_b")
          checkSparkAnswerAndOperator(
            df,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastNestedLoopJoinExec]))
        }
      }
    }
  }

  test("BroadcastNestedLoopJoin LEFT OUTER with inequality") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // NULL left keys must still appear in the output (LEFT OUTER preserves them)
      val left: Seq[(Integer, Int)] =
        (0 until 100).map(i => ((i: Integer), i % 5)) ++ Seq[(Integer, Int)]((null, 7), (50, -1))
      val right: Seq[(Integer, Int)] =
        (0 until 10).map(i => ((i: Integer), i + 5)) ++ Seq[(Integer, Int)]((null, 1))
      withParquetTable(left, "tbl_a") {
        withParquetTable(right, "tbl_b") {
          val df =
            sql(
              "SELECT /*+ BROADCAST(tbl_b) */ * FROM tbl_a LEFT JOIN tbl_b ON tbl_a._1 > tbl_b._1")
          checkSparkAnswerAndOperator(
            df,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastNestedLoopJoinExec]))
        }
      }
    }
  }

  test("BroadcastNestedLoopJoin LEFT SEMI with inequality") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // NULL keys never match (predicate evaluates to NULL
      val left: Seq[(Integer, Int)] =
        (0 until 100).map(i => ((i: Integer), i % 5)) ++ Seq[(Integer, Int)]((null, 7), (50, -1))
      val right: Seq[(Integer, Int)] =
        (0 until 10).map(i => ((i: Integer), i + 5)) ++ Seq[(Integer, Int)]((null, 1))
      withParquetTable(left, "tbl_a") {
        withParquetTable(right, "tbl_b") {
          val df =
            sql("SELECT /*+ BROADCAST(tbl_b) */ * FROM tbl_a LEFT SEMI JOIN tbl_b ON tbl_a._1 > tbl_b._1")
          checkSparkAnswerAndOperator(
            df,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastNestedLoopJoinExec]))
        }
      }
    }
  }

  test("BroadcastNestedLoopJoin LEFT ANTI with inequality") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // LEFT ANTI keeps left rows that have NO match (left rows with NULL keys must appear in the output)
      val left: Seq[(Integer, Int)] =
        (0 until 100).map(i => ((i: Integer), i % 5)) ++ Seq[(Integer, Int)]((null, 7), (50, -1))
      val right: Seq[(Integer, Int)] =
        (0 until 10).map(i => ((i: Integer), i + 5)) ++ Seq[(Integer, Int)]((null, 1))
      withParquetTable(left, "tbl_a") {
        withParquetTable(right, "tbl_b") {
          val df =
            sql("SELECT /*+ BROADCAST(tbl_b) */ * FROM tbl_a LEFT ANTI JOIN tbl_b ON tbl_a._1 > tbl_b._1")
          checkSparkAnswerAndOperator(
            df,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastNestedLoopJoinExec]))
        }
      }
    }
  }

  test("BroadcastNestedLoopJoin RIGHT OUTER with inequality (BuildLeft, swap path)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // RIGHT OUTER preserves right rows.
      val left: Seq[(Integer, Int)] =
        (0 until 10).map(i => ((i: Integer), i + 5)) ++ Seq[(Integer, Int)]((null, 9))
      val right: Seq[(Integer, Int)] =
        (0 until 100).map(i => ((i: Integer), i % 5)) ++ Seq[(Integer, Int)]((null, 7), (50, -1))
      withParquetTable(left, "tbl_a") {
        withParquetTable(right, "tbl_b") {
          val df =
            sql("SELECT /*+ BROADCAST(tbl_a) */ * FROM tbl_a RIGHT JOIN tbl_b ON tbl_a._1 < tbl_b._1")
          checkSparkAnswerAndOperator(
            df,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastNestedLoopJoinExec]))
        }
      }
    }
  }

  test("BroadcastNestedLoopJoin cross join without condition (materialized rows)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val left: Seq[(Integer, Int)] =
        (0 until 5).map(i => ((i: Integer), i * 10)) ++ Seq[(Integer, Int)]((null, 99))
      val right: Seq[(Integer, String)] =
        (0 until 4).map(i => ((i: Integer), s"v_$i")) ++ Seq[(Integer, String)]((null, "v_null"))
      withParquetTable(left, "tbl_a") {
        withParquetTable(right, "tbl_b") {
          val df =
            sql("SELECT /*+ BROADCAST(tbl_b) */ tbl_a._1, tbl_b._2 FROM tbl_a, tbl_b")
          checkSparkAnswerAndOperator(
            df,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastNestedLoopJoinExec]))
        }
      }
    }
  }

  test("BroadcastNestedLoopJoin LEFT OUTER without condition") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val left: Seq[(Integer, Int)] =
        (0 until 5).map(i => ((i: Integer), i * 10)) ++ Seq[(Integer, Int)]((null, 99))
      val right: Seq[(Integer, String)] =
        (0 until 4).map(i => ((i: Integer), s"v_$i")) ++ Seq[(Integer, String)]((null, "v_null"))
      withParquetTable(left, "tbl_a") {
        withParquetTable(right, "tbl_b") {
          val df =
            sql(
              "SELECT /*+ BROADCAST(tbl_b) */ tbl_a._1, tbl_b._2" +
                " FROM tbl_a LEFT JOIN tbl_b ON true")
          checkSparkAnswerAndOperator(
            df,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastNestedLoopJoinExec]))
        }
      }
    }
  }

  test("BroadcastNestedLoopJoin broadcast reuse across two joins") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // Same broadcast relation (tbl_b) feeds two separate BNLJs. Spark/AQE handles
      // broadcast-exchange reuse generically rather than inside BNLJ, so this verifies
      // we still produce correct results when reuse fires across CometBNLJ consumers.
      withParquetTable((0 until 50).map(i => (i, i % 10)), "tbl_a") {
        withParquetTable((0 until 50).map(i => (i, i + 1)), "tbl_c") {
          withParquetTable((0 until 5).map(i => (i, i * 10)), "tbl_b") {
            val df = sql(
              "SELECT count(*) FROM" +
                " (SELECT /*+ BROADCAST(tbl_b) */ tbl_a._1 AS k FROM tbl_a JOIN tbl_b" +
                "  ON tbl_a._1 > tbl_b._1) a" +
                " JOIN" +
                " (SELECT /*+ BROADCAST(tbl_b) */ tbl_c._1 AS k FROM tbl_c JOIN tbl_b" +
                "  ON tbl_c._1 > tbl_b._1) c" +
                " ON a.k = c.k")
            checkSparkAnswerAndOperator(
              df,
              Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastNestedLoopJoinExec]))
          }
        }
      }
    }
  }

  test("BroadcastNestedLoopJoin FULL OUTER falls back to Spark") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable((0 until 50).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 10).map(i => (i, i + 100)), "tbl_b") {
          val df =
            sql(
              "SELECT /*+ BROADCAST(tbl_b) */ * FROM tbl_a FULL OUTER JOIN tbl_b" +
                " ON tbl_a._1 > tbl_b._1")
          checkSparkAnswer(df)
        }
      }
    }
  }

  test("BroadcastNestedLoopJoin LEFT OUTER with BuildLeft falls back to Spark") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable((0 until 10).map(i => (i, i + 100)), "tbl_a") {
        withParquetTable((0 until 50).map(i => (i, i % 5)), "tbl_b") {
          // Broadcasting the preserved (left) side forces BuildLeft + LeftOuter, an
          // unsupported combo. Comet should fall back to Spark and still match.
          val df =
            sql(
              "SELECT /*+ BROADCAST(tbl_a) */ * FROM tbl_a LEFT OUTER JOIN tbl_b" +
                " ON tbl_a._1 > tbl_b._1")
          checkSparkAnswer(df)
        }
      }
    }
  }

  test("BroadcastNestedLoopJoin over an ordered streamed side falls back and keeps its order") {
    // Spark's nested loop join keeps the native range's order, and Spark drops the sort for it,
    // either directly above the join or above a hash join over it. The native nested loop join
    // emits its rows build row by build row, so it stays in Spark.
    val nestedLoop =
      "SELECT /*+ BROADCAST(n) */ p.id AS x, n.id AS z FROM range(0, 20, 1, 1) p " +
        "JOIN range(0, 2, 1, 1) n ON p.id % 2 <= n.id"
    def overHashJoin(hint: String): String =
      s"SELECT $hint j.x, j.z, b.id AS w FROM ($nestedLoop) j JOIN range(0, 2, 1, 1) b " +
        "ON j.x % 4 = b.id * 2 ORDER BY x"
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      CometConf.COMET_EXEC_RANGE_ENABLED.key -> "true") {
      for ((query, force, hashJoins) <- Seq(
          (s"SELECT * FROM ($nestedLoop) j ORDER BY x", false, 0),
          (overHashJoin(""), true, 1),
          (overHashJoin("/*+ SHUFFLE_HASH(b) */"), false, 1),
          (overHashJoin("/*+ BROADCAST(b) */"), false, 1))) {
        withSQLConf(CometConf.COMET_FORCE_SHJ.key -> force.toString) {
          val (_, plan) = checkSparkAnswerAndFallbackReason(
            sql(query),
            CometBroadcastNestedLoopJoinExec.streamedOrderReason)
          val context = s"$query\n$plan"
          assert(collect(plan) { case j: BroadcastNestedLoopJoinExec => j }.size == 1, context)
          assert(collect(plan) { case j: CometBroadcastNestedLoopJoinExec => j }.isEmpty, context)
          // The hash join over the nested loop join stays in Spark as well.
          val joins = collect(plan) {
            case j: ShuffledHashJoinExec => j
            case j: BroadcastHashJoinExec => j
          }
          assert(joins.size == hashJoins, context)
          val xs = sql(query).collect().map(_.getLong(0)).toSeq
          assert(xs == xs.sorted, s"$xs\n$context")
        }
      }
    }
  }

  test("ExistenceJoin via BroadcastHashJoin (EXISTS combined with OR)") {
    withSQLConf(
      CometConf.COMET_EXEC_EXISTENCE_JOIN_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      withParquetTable((0 until 100).map(i => (i, if (i % 3 == 0) "US" else "EU")), "tbl_a") {
        withParquetTable((0 until 30).map(i => (i, i + 1)), "tbl_b") {
          val df = sql("SELECT * FROM tbl_a a " +
            "WHERE a._2 = 'US' OR EXISTS (SELECT /*+ BROADCAST(b) */ 1 FROM tbl_b b WHERE b._1 = a._1)")
          checkSparkAnswerAndOperator(
            df,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastHashJoinExec]))
        }
      }
    }
  }

  test("ExistenceJoin via ShuffledHashJoin (EXISTS combined with OR)") {
    withSQLConf(
      CometConf.COMET_EXEC_EXISTENCE_JOIN_ENABLED.key -> "true",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "false",
      "spark.sql.join.forceApplyShuffledHashJoin" -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable((0 until 100).map(i => (i, if (i % 3 == 0) "US" else "EU")), "tbl_a") {
        withParquetTable((0 until 30).map(i => (i, i + 1)), "tbl_b") {
          val df = sql(
            "SELECT * FROM tbl_a a " +
              "WHERE a._2 = 'US' OR EXISTS (SELECT 1 FROM tbl_b b WHERE b._1 = a._1)")
          checkSparkAnswerAndOperator(df, Seq(classOf[CometHashJoinExec]))
        }
      }
    }
  }

  test("ExistenceJoin via SortMergeJoin falls back to Spark") {
    // Existence sort-merge joins are not executed natively; verify the fallback.
    withSQLConf(
      CometConf.COMET_EXEC_EXISTENCE_JOIN_ENABLED.key -> "true",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable((0 until 100).map(i => (i, if (i % 3 == 0) "US" else "EU")), "tbl_a") {
        withParquetTable((0 until 30).map(i => (i, i + 1)), "tbl_b") {
          val df = sql(
            "SELECT * FROM tbl_a a " +
              "WHERE a._2 = 'US' OR EXISTS (SELECT 1 FROM tbl_b b WHERE b._1 = a._1)")
          checkSparkAnswerAndFallbackReason(df, "Unsupported join type")
        }
      }
    }
  }

  test("ExistenceJoin with residual condition falls back to Spark") {
    // A non-equi residual predicate is evaluated over the whole candidate batch by DataFusion's
    // LeftMark join (no first-match short-circuit), so Comet keeps it on Spark; verify parity.
    // AQE is disabled because a declined broadcast join loses its fallback reason from the
    // AQE-final plan; see https://github.com/apache/datafusion-comet/issues/6442.
    withSQLConf(
      CometConf.COMET_EXEC_EXISTENCE_JOIN_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      withParquetTable((0 until 100).map(i => (i, if (i % 3 == 0) "US" else "EU")), "tbl_a") {
        withParquetTable((0 until 30).map(i => (i, i + 1)), "tbl_b") {
          val df = sql(
            "SELECT * FROM tbl_a a WHERE a._2 = 'US' OR EXISTS " +
              "(SELECT /*+ BROADCAST(b) */ 1 FROM tbl_b b WHERE b._1 = a._1 AND b._2 > a._1)")
          checkSparkAnswerAndFallbackReason(df, "residual (non-equi)")
        }
      }
    }
  }

  test("ExistenceJoin with computed join key falls back to Spark") {
    // A computed join key (not a bare column reference) is evaluated eagerly over the batch by
    // the native join, so Comet keeps it on Spark; verify parity.
    // AQE is disabled because a declined broadcast join loses its fallback reason from the
    // AQE-final plan; see https://github.com/apache/datafusion-comet/issues/6442.
    withSQLConf(
      CometConf.COMET_EXEC_EXISTENCE_JOIN_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      withParquetTable((0 until 100).map(i => (i, if (i % 3 == 0) "US" else "EU")), "tbl_a") {
        withParquetTable((0 until 30).map(i => (i, i + 1)), "tbl_b") {
          val df = sql(
            "SELECT * FROM tbl_a a WHERE a._2 = 'US' OR EXISTS " +
              "(SELECT /*+ BROADCAST(b) */ 1 FROM tbl_b b WHERE b._1 = a._1 + 1)")
          checkSparkAnswerAndFallbackReason(df, "computed (non-column)")
        }
      }
    }
  }

  test("ExistenceJoin with duplicate build keys runs natively (markers not multiplied)") {
    withSQLConf(
      CometConf.COMET_EXEC_EXISTENCE_JOIN_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      withParquetTable((0 until 100).map(i => (i, if (i % 3 == 0) "US" else "EU")), "tbl_a") {
        // Build side repeats keys 0..4 many times; the existence marker must stay "at least one
        // match" and must not multiply matching probe rows.
        withParquetTable((0 until 30).map(i => (i % 5, i)), "tbl_b") {
          val df = sql(
            "SELECT * FROM tbl_a a WHERE a._2 = 'US' OR EXISTS " +
              "(SELECT /*+ BROADCAST(b) */ 1 FROM tbl_b b WHERE b._1 = a._1)")
          checkSparkAnswerAndOperator(
            df,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastHashJoinExec]))
        }
      }
    }
  }

  test("ExistenceJoin with NOT EXISTS combined with OR runs natively") {
    withSQLConf(
      CometConf.COMET_EXEC_EXISTENCE_JOIN_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      withParquetTable((0 until 100).map(i => (i, if (i % 3 == 0) "US" else "EU")), "tbl_a") {
        withParquetTable((0 until 30).map(i => (i, i + 1)), "tbl_b") {
          val df = sql(
            "SELECT * FROM tbl_a a WHERE a._2 = 'US' OR NOT EXISTS " +
              "(SELECT /*+ BROADCAST(b) */ 1 FROM tbl_b b WHERE b._1 = a._1)")
          checkSparkAnswerAndOperator(
            df,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastHashJoinExec]))
        }
      }
    }
  }

  test("ExistenceJoin falls back to Spark when the feature flag is disabled") {
    // With the flag off, the join is declined with a toggle-specific reason rather than a generic
    // "Unsupported join type" message that reads like a permanent limitation. AQE is disabled
    // because a declined broadcast join loses its fallback reason from the AQE-final plan; see
    // https://github.com/apache/datafusion-comet/issues/6442.
    withSQLConf(
      CometConf.COMET_EXEC_EXISTENCE_JOIN_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      withParquetTable((0 until 100).map(i => (i, if (i % 3 == 0) "US" else "EU")), "tbl_a") {
        withParquetTable((0 until 30).map(i => (i, i + 1)), "tbl_b") {
          val df = sql(
            "SELECT * FROM tbl_a a WHERE a._2 = 'US' OR EXISTS " +
              "(SELECT /*+ BROADCAST(b) */ 1 FROM tbl_b b WHERE b._1 = a._1)")
          checkSparkAnswerAndFallbackReason(df, "Native ExistenceJoin is disabled")
        }
      }
    }
  }

  test("ExistenceJoin via SortMergeJoin stays on Spark even with forceShuffledHashJoin") {
    // Never a BuildRight hash join: https://github.com/apache/datafusion-comet/issues/2697
    withSQLConf(
      CometConf.COMET_EXEC_EXISTENCE_JOIN_ENABLED.key -> "true",
      CometConf.COMET_FORCE_SHJ.key -> "true",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable((0 until 100).map(i => (i, if (i % 3 == 0) "US" else "EU")), "tbl_a") {
        withParquetTable((0 until 30).map(i => (i, i + 1)), "tbl_b") {
          val df = sql(
            "SELECT * FROM tbl_a a " +
              "WHERE a._2 = 'US' OR EXISTS (SELECT 1 FROM tbl_b b WHERE b._1 = a._1)")
          val (_, cometPlan) = checkSparkAnswer(df)
          assert(
            collect(cometPlan) { case h: CometHashJoinExec => h }.isEmpty &&
              collect(cometPlan) { case s: CometSortMergeJoinExec => s }.isEmpty,
            s"Existence SMJ must not be rewritten to a native hash join:\n$cometPlan")
        }
      }
    }
  }

  test("scans of one bucketed table in a single native plan each read their own files") {
    withTable("bucketed_self", "bucketed_dim") {
      val rows = for {
        p <- 1 to 3
        i <- 0 until 20
      } yield (i % 10, s"p${p}_$i", p)
      rows
        .toDF("k", "v", "p")
        .write
        .format("parquet")
        .partitionBy("p")
        .bucketBy(4, "k")
        .saveAsTable("bucketed_self")
      Seq((1, "one"), (2, "two"), (3, "three"))
        .toDF("p", "x")
        .write
        .format("parquet")
        .saveAsTable("bucketed_dim")

      // A bucketed sort-merge self-join needs no exchange, so both scans run in one native plan
      // and must each get their own files, though they read the same table and columns.
      def assertScansShareOneNativePlan(plan: SparkPlan): Seq[CometNativeScanExec] = {
        val join = collectFirst(plan) { case j: CometSortMergeJoinExec => j }
          .getOrElse(fail(s"Expected a native sort-merge join:\n$plan"))
        val scans = collect(join) { case s: CometNativeScanExec => s }
        assert(scans.size == 2, s"Expected both scans under the join:\n$plan")
        assert(
          collect(join) { case e: ShuffleExchangeLike => e }.isEmpty,
          s"Expected no exchange between the join and its scans:\n$plan")
        scans
      }

      for (adaptive <- Seq(false, true)) {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.BUCKETING_ENABLED.key -> "true",
          SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.key -> "true") {
          // In a full outer join neither side gets an inferred isnotnull data filter, so only
          // the partition filters tell the two scans apart.
          val (_, fullOuter) = checkSparkAnswerAndOperator(
            "SELECT a.k, a.v, b.v FROM (SELECT * FROM bucketed_self WHERE p = 1) a " +
              "FULL OUTER JOIN (SELECT * FROM bucketed_self WHERE p = 2) b ON a.k = b.k")
          assertScansShareOneNativePlan(fullOuter)

          // Without filter pushdown the scans carry no data filters, and dynamic partition
          // pruning picks a different partition for each side. Spark 3.4 only runs this with
          // AQE off: CometScanRule.transformV1Scan leaves a V1 scan with AQE dynamic pruning in
          // Spark there (case 2 in CometSpark34AqeDppFallbackRule).
          if (!adaptive || isSpark35Plus) {
            withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "false") {
              val (_, pruned) = checkSparkAnswer(
                "SELECT /*+ BROADCAST(d1), BROADCAST(d2) */ a.k, a.v, b.v " +
                  "FROM bucketed_self a JOIN bucketed_self b ON a.k = b.k " +
                  "JOIN bucketed_dim d1 ON a.p = d1.p JOIN bucketed_dim d2 ON b.p = d2.p " +
                  "WHERE d1.x = 'one' AND d2.x = 'two'")
              assertScansShareOneNativePlan(pruned).foreach { scan =>
                assert(
                  scan.partitionFilters.exists(_.isInstanceOf[DynamicPruningExpression]),
                  s"Expected each scan to be pruned dynamically:\n$pruned")
              }
            }
          }
        }
      }
    }
  }

  test("outer hash joins keep a streamed side sorted outside the native plan in order") {
    // Spark drops the sort above the join because the join reports the streamed side's order,
    // and the streamed side arrives sorted through a JVM input, which declares no order to
    // DataFusion unless the planner declares it on the join's probe input; see
    // CometHashJoin.doConvert. CometInMemoryCacheSuite covers the native cache scan.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_IN_MEMORY_CACHE_ENABLED.key -> "true") {
      withParquetTable((0 until 10000).map(i => (i % 100, i)), "big") {
        withParquetTable((0 until 10).map(i => (i * 10, i)), "small") {
          try {
            val streamed =
              spark.table("big").repartition(2, $"_1").sortWithinPartitions("_1").cache()
            streamed.count()
            val build = spark.table("small")

            // A residual condition, which keeps only the build rows past 3, sends more probe rows
            // down the join's unmatched path.
            def expected(withCondition: Boolean): Seq[Row] = (0 until 10000).map { i =>
              val k = i % 100
              val matched = k % 10 == 0 && (!withCondition || k / 10 > 3)
              Row(k, i, if (matched) k / 10 else null)
            }

            // Both shapes run as DataFusion's right join: Comet swaps a join built on the right
            // and leaves one built on the left as it is.
            def outerJoin(hint: String, joinType: String, withCondition: Boolean): DataFrame = {
              val keys = streamed("_1") === build("_1")
              val on = if (withCondition) keys && build("_2") > 3 else keys
              val joined = if (joinType == "left_outer") {
                streamed.join(build.hint(hint), on, joinType)
              } else {
                build.hint(hint).join(streamed, on, joinType)
              }
              joined
                .select(streamed("_1").as("k"), streamed("_2").as("v"), build("_2").as("w"))
                .sortWithinPartitions("k")
            }

            for {
              (hint, joinClass) <- Seq(
                "shuffle_hash" -> classOf[CometHashJoinExec],
                "broadcast" -> classOf[CometBroadcastHashJoinExec])
              (joinType, buildSide) <- Seq("left_outer" -> BuildRight, "right_outer" -> BuildLeft)
              withCondition <- Seq(false, true)
            } {
              val df = outerJoin(hint, joinType, withCondition)
              val plan = df.queryExecution.executedPlan
              val joins: Seq[(Class[_], BuildSide)] = collect(plan) {
                case j: CometHashJoinExec => (j.getClass, j.buildSide)
                case j: CometBroadcastHashJoinExec => (j.getClass, j.buildSide)
              }
              assert(joins == Seq((joinClass, buildSide)), plan)
              assert(collect(plan) { case c: CometSparkToColumnarExec => c }.size == 1, plan)
              assert(
                collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty,
                plan)
              QueryTest.sameRows(expected(withCondition), df.collect().toSeq).foreach(fail(_))
              assert(partitionsInOrder(df, keyOrder(Ascending)), plan)
            }
          } finally {
            spark.catalog.clearCache()
          }
        }
      }
    }
  }

  test("an outer hash join declares the part of the streamed order Comet can express") {
    // Declaring any order keeps the probe rows in place, so a prefix of the order is enough.
    // Nothing tells whether an operator above relies on the order, so a join whose first key Comet
    // cannot express stays in Spark rather than lose it. With Add disabled, Comet cannot express
    // `_1 + 1`.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_IN_MEMORY_CACHE_ENABLED.key -> "true",
      CometConf.getExprEnabledConfigKey("Add") -> "false") {
      withParquetTable((0 until 1000).map(i => (i % 100, i)), "big") {
        withParquetTable((0 until 10).map(i => (i * 10, i)), "small") {
          def sortedCache(order: Column*): DataFrame = {
            val cached =
              spark.table("big").repartition(2, $"_1").sortWithinPartitions(order: _*).cache()
            cached.count()
            cached
          }
          val build = spark.table("small")
          try {
            val unexpressed = sortedCache($"_1" + 1)
            val (_, fallbackPlan) = checkSparkAnswerAndFallbackReason(
              unexpressed.join(
                build.hint("shuffle_hash"),
                unexpressed("_1") === build("_1"),
                "left_outer"),
              "the streamed side's sort order, which this outer join has to keep, cannot be " +
                "expressed natively")
            assert(
              collect(fallbackPlan) { case j: ShuffledHashJoinExec => j }.size == 1,
              fallbackPlan)
            assert(collect(fallbackPlan) { case j: CometHashJoinExec => j }.isEmpty, fallbackPlan)

            val prefixed = sortedCache($"_1", $"_1" + 1)
            val df = prefixed
              .join(build.hint("shuffle_hash"), prefixed("_1") === build("_1"), "left_outer")
              .select(prefixed("_1").as("k"), prefixed("_2").as("v"), build("_2").as("w"))
              .sortWithinPartitions("k")
            val plan = df.queryExecution.executedPlan
            assert(collect(plan) { case j: CometHashJoinExec => j }.size == 1, plan)
            assert(collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty, plan)
            val expected = (0 until 1000).map { i =>
              val k = i % 100
              Row(k, i, if (k % 10 == 0) k / 10 else null)
            }
            QueryTest.sameRows(expected, df.collect().toSeq).foreach(fail(_))
            assert(partitionsInOrder(df, keyOrder(Ascending)), plan)
          } finally {
            spark.catalog.clearCache()
          }
        }
      }
    }
  }

  test("an inner hash join declares a sorted cache's order for a native window above it") {
    // Spark drops the window's sort, as the join reports the cache's order, so the native window
    // needs that order declared on the join's probe input, which reads the cache from the JVM.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      CometConf.COMET_EXEC_RANGE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_IN_MEMORY_CACHE_ENABLED.key -> "true") {
      val cached = spark
        .range(2000)
        .selectExpr("id % 20 AS k", "id AS v")
        .repartition(2, $"k")
        .sortWithinPartitions("k", "v")
        .cache()
      try {
        cached.count()
        val keys = spark.range(20).toDF("id")
        for ((build, force) <- Seq(
            keys -> true,
            keys.hint("shuffle_hash") -> false,
            keys.hint("broadcast") -> false)) {
          withSQLConf(CometConf.COMET_FORCE_SHJ.key -> force.toString) {
            def df: DataFrame = cached
              .join(build, $"k" === $"id")
              .selectExpr("k", "v", "dense_rank() OVER (PARTITION BY k ORDER BY v) AS r")
            sameRowsAsSpark(df)
            val plan = df.queryExecution.executedPlan
            assert(collect(plan) { case w: CometWindowExec => w }.size == 1, plan)
            assert(collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty, plan)
            val declared = collect(plan) {
              case j: CometHashJoinExec => j.nativeOp.getHashJoin.getStreamedSortOrdersCount
              case j: CometBroadcastHashJoinExec =>
                j.nativeOp.getHashJoin.getStreamedSortOrdersCount
            }
            assert(declared == Seq(2), plan)
          }
        }
      } finally {
        cached.unpersist()
      }
    }
  }

  test("an inner hash join declares the part of the streamed order Comet can express") {
    // An inner join keeps its probe rows in place without a declared order, so one whose first
    // key Comet cannot express declares nothing and still runs natively. With Add disabled, Comet
    // cannot express `_1 + 1`.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_IN_MEMORY_CACHE_ENABLED.key -> "true",
      CometConf.getExprEnabledConfigKey("Add") -> "false") {
      withParquetTable((0 until 1000).map(i => (i % 100, i)), "big") {
        withParquetTable((0 until 10).map(i => (i * 10, i)), "small") {
          val build = spark.table("small")
          try {
            for ((order, declared) <- Seq(Seq($"_1" + 1) -> 0, Seq($"_1", $"_1" + 1) -> 1)) {
              val cached =
                spark.table("big").repartition(2, $"_1").sortWithinPartitions(order: _*).cache()
              cached.count()
              val (_, plan) = checkSparkAnswer(
                cached.join(build.hint("shuffle_hash"), cached("_1") === build("_1")))
              val joins = collect(plan) { case j: CometHashJoinExec => j }
              assert(joins.size == 1, plan)
              assert(joins.head.nativeOp.getHashJoin.getStreamedSortOrdersCount == declared, plan)
              cached.unpersist()
            }
          } finally {
            spark.catalog.clearCache()
          }
        }
      }
    }
  }

  test("outer hash joins keep a descending, nulls last streamed order with null keys") {
    // Null keys match nothing, so all of them take the unmatched path, at the end of each
    // partition.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_IN_MEMORY_CACHE_ENABLED.key -> "true") {
      val data = (0 until 10000).map(i => (if (i % 7 == 0) None else Some(i % 100), i))
      withParquetTable(data, "big") {
        withParquetTable((0 until 10).map(i => (i * 10, i)), "small") {
          try {
            val streamed = spark
              .table("big")
              .repartition(2, $"_1")
              .sortWithinPartitions($"_1".desc_nulls_last)
              .cache()
            streamed.count()
            val build = spark.table("small").hint("shuffle_hash")
            val expected = data.map { case (k, i) =>
              Row(k.getOrElse(null), i, k.filter(_ % 10 == 0).map(_ / 10).getOrElse(null))
            }
            Seq("left_outer", "right_outer").foreach { joinType =>
              val joined = if (joinType == "left_outer") {
                streamed.join(build, streamed("_1") === build("_1"), joinType)
              } else {
                build.join(streamed, build("_1") === streamed("_1"), joinType)
              }
              val df = joined
                .select(streamed("_1").as("k"), streamed("_2").as("v"), build("_2").as("w"))
                .sortWithinPartitions($"k".desc_nulls_last)
              val plan = df.queryExecution.executedPlan
              assert(collect(plan) { case j: CometHashJoinExec => j }.size == 1, plan)
              assert(
                collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty,
                plan)
              QueryTest.sameRows(expected, df.collect().toSeq).foreach(fail(_))
              assert(partitionsInOrder(df, keyOrder(Descending)), plan)
            }
          } finally {
            spark.catalog.clearCache()
          }
        }
      }
    }
  }

  // Takes the checkpoint with AQE off, as an adaptive plan reports no order.
  private def withOrderedCheckpoint(f: DataFrame => Unit): Unit =
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      CometConf.COMET_CONVERT_FROM_RDD_ENABLED.key -> "true",
      CometConf.COMET_EXEC_RANGE_ENABLED.key -> "true") {
      f(
        spark
          .range(2000)
          .selectExpr("id % 100 AS k", "id AS v")
          .repartition(2, $"k")
          .sortWithinPartitions("k", "v")
          .localCheckpoint(eager = true))
    }

  /**
   * Build sides for an inner join with a checkpoint, each with whether to force a shuffled hash
   * join: unhinted, then hinted for a shuffled hash join, then for a broadcast.
   */
  private def checkpointJoinBuildSides: Seq[(DataFrame, Boolean)] = {
    val keys = spark.range(100).toDF("id")
    Seq(keys -> true, keys.hint("shuffle_hash") -> false, keys.hint("broadcast") -> false)
  }

  test("outer hash joins over a checkpointed streamed side fall back and keep its order") {
    // Spark drops the sort above the join for the checkpoint's order, which Comet cannot check.
    withOrderedCheckpoint { streamed =>
      val build = spark.range(10).selectExpr("id * 10 AS k", "id AS w")
      val order =
        (0 to 1).map(i => SortOrder(BoundReference(i, LongType, nullable = true), Ascending))
      for (adaptive <- Seq(false, true))
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
          for {
            (hint, sparkJoin) <- Seq(
              "shuffle_hash" -> classOf[ShuffledHashJoinExec],
              "broadcast" -> classOf[BroadcastHashJoinExec])
            joinType <- Seq("left_outer", "right_outer")
          } {
            def df: DataFrame = {
              val on = streamed("k") === build("k")
              val joined = if (joinType == "left_outer") {
                streamed.join(build.hint(hint), on, joinType)
              } else {
                build.hint(hint).join(streamed, on, joinType)
              }
              joined
                .select(streamed("k"), streamed("v"), build("w"))
                .sortWithinPartitions("k", "v")
            }
            val (_, plan) =
              checkSparkAnswerAndFallbackReason(df, CometHashJoin.uncheckedStreamedOrderReason)
            val context = s"$hint $joinType AQE=$adaptive\n$plan"
            assert(collect(plan) { case j if j.getClass == sparkJoin => j }.size == 1, context)
            assert(
              collect(plan) {
                case j: CometHashJoinExec => j
                case j: CometBroadcastHashJoinExec => j
              }.isEmpty,
              context)
            assert(
              collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty,
              context)
            assert(partitionsInOrder(df, order: _*), context)
          }
        }
    }
  }

  test("an outer hash join over a native inner join over a checkpoint falls back") {
    // The order walk reaches the checkpoint through the inner join's streamed side.
    withOrderedCheckpoint { streamed =>
      val d1 = spark.range(100).where("id % 4 != 3").selectExpr("id AS k", "id AS a")
      val d2 = spark.range(10).selectExpr("id * 10 AS k", "id AS w")
      val order =
        (0 to 1).map(i => SortOrder(BoundReference(i, LongType, nullable = true), Ascending))
      def df: DataFrame = streamed
        .join(d1.hint("broadcast"), streamed("k") === d1("k"), "inner")
        .join(d2.hint("broadcast"), streamed("k") === d2("k"), "left_outer")
        .select(streamed("k"), streamed("v"), d1("a"), d2("w"))
        .sortWithinPartitions("k", "v")
      val (_, plan) =
        checkSparkAnswerAndFallbackReason(df, CometHashJoin.uncheckedStreamedOrderReason)
      val inner = collect(plan) { case j: CometBroadcastHashJoinExec => j.joinType }
      assert(inner == Seq(Inner), plan)
      val outer = collect(plan) { case j: BroadcastHashJoinExec => j.joinType }
      assert(outer == Seq(LeftOuter), plan)
      assert(collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty, plan)
      assert(partitionsInOrder(df, order: _*), plan)
    }
  }

  test("an inner hash join over a checkpointed streamed side declares no order") {
    // Comet cannot check the checkpoint's order, so it declares none, and the inner join keeps its
    // probe rows in place without one, so it stays native.
    withOrderedCheckpoint { streamed =>
      val build = spark.range(10).selectExpr("id * 10 AS k", "id AS w")
      val order =
        (0 to 1).map(i => SortOrder(BoundReference(i, LongType, nullable = true), Ascending))
      for (hint <- Seq("shuffle_hash", "broadcast")) {
        def df: DataFrame = streamed
          .join(build.hint(hint), streamed("k") === build("k"))
          .select(streamed("k"), streamed("v"), build("w"))
          .sortWithinPartitions("k", "v")
        val (_, plan) = checkSparkAnswer(df)
        val declared = collect(plan) {
          case j: CometHashJoinExec => j.nativeOp.getHashJoin.getStreamedSortOrdersCount
          case j: CometBroadcastHashJoinExec => j.nativeOp.getHashJoin.getStreamedSortOrdersCount
        }
        assert(declared == Seq(0), s"$hint\n$plan")
        assert(
          collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty,
          s"$hint\n$plan")
        assert(partitionsInOrder(df, order: _*), s"$hint\n$plan")
      }
    }
  }

  test("a native window above an inner hash join over a checkpointed streamed side runs") {
    // Spark drops the window's sort for the checkpoint's order, which the join reports, but Comet
    // cannot check that order, so the join declares none and the native window sorts its input.
    withOrderedCheckpoint { streamed =>
      for {
        (build, force) <- checkpointJoinBuildSides
        window <- Seq(
          "dense_rank() OVER (PARTITION BY k ORDER BY v)",
          "sum(v) OVER (PARTITION BY k)")
      } {
        withSQLConf(CometConf.COMET_FORCE_SHJ.key -> force.toString) {
          def df: DataFrame = streamed
            .join(build, $"k" === $"id")
            .selectExpr("k", "v", s"$window AS r")
          val plan = df.queryExecution.executedPlan
          val context = s"force=$force $window\n$plan"
          val joins = collect(plan) {
            case j: CometHashJoinExec => j
            case j: CometBroadcastHashJoinExec => j
          }
          assert(joins.size == 1, context)
          assert(collect(plan) { case w: CometWindowExec => w }.size == 1, context)
          assert(
            collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty,
            context)
          sameRowsAsSpark(df)
        }
      }
    }
  }

  test("a native window group limit above an inner hash join over a checkpoint ranks it") {
    assume(isSpark35Plus, "WindowGroupLimit was added in Spark 3.5")
    // The native limit declares the order Spark requires of its input without checking it, which
    // holds for the checkpoint's rows.
    withOrderedCheckpoint { streamed =>
      for ((build, force) <- checkpointJoinBuildSides) {
        withSQLConf(CometConf.COMET_FORCE_SHJ.key -> force.toString) {
          def df: DataFrame = streamed
            .join(build, $"k" === $"id")
            .selectExpr("k", "v", "rank() OVER (PARTITION BY k ORDER BY v) AS rk")
            .where("rk <= 2")
          val plan = df.queryExecution.executedPlan
          val context = s"force=$force\n$plan"
          assert(collect(plan) { case l: CometWindowGroupLimitExec => l }.nonEmpty, context)
          assert(collect(plan) { case w: CometWindowExec => w }.size == 1, context)
          assert(
            collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty,
            context)
          sameRowsAsSpark(df)
        }
      }
    }
  }

  test("chained outer hash joins over a native window keep its order natively") {
    // The window and both joins share one native plan, where DataFusion sees the order itself.
    withParquetTable((0 until 2000).map(i => (i % 40, i)), "t") {
      withParquetTable((0 until 40 by 2).map(k => (k, k * 10)), "d1") {
        withParquetTable((0 until 40 by 3).map(k => (k, k * 100)), "d2") {
          val order = (0 to 1).map { i =>
            SortOrder(BoundReference(i, IntegerType, nullable = true), Ascending)
          }
          for (adaptive <- Seq(false, true)) {
            withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
              def df: DataFrame = sql("""SELECT w.k, w.v, w.rn, d1._2 AS x, d2._2 AS y
                  |FROM (SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn
                  |      FROM (SELECT _1 AS k, _2 AS v FROM t)) w
                  |LEFT JOIN d1 ON w.k = d1._1
                  |LEFT JOIN d2 ON w.k = d2._1""".stripMargin)
                .sortWithinPartitions("k", "v")
              val (_, plan) = checkSparkAnswer(df)
              val context = s"AQE=$adaptive\n$plan"
              assert(new ExtendedExplainInfo().getFallbackReasons(plan).isEmpty, context)
              val joins = collect(plan) { case j: CometBroadcastHashJoinExec => j }
              assert(joins.size == 2 && joins.forall(_.joinType == LeftOuter), context)
              assert(collect(plan) { case w: CometWindowExec => w }.size == 1, context)
              assert(
                collect(plan) {
                  case s @ (_: CometSortExec | _: SortExec)
                      if s.find(_.isInstanceOf[CometBroadcastHashJoinExec]).isDefined =>
                    s
                }.isEmpty,
                context)
              assert(partitionsInOrder(df, order: _*), context)
            }
          }
        }
      }
    }
  }

  // Compares the rows of a fresh `df` with the ones Spark returns for it, as a multiset unless
  // `isSorted`. The rows of a key tie under a sort, so their order within it can differ between
  // the two.
  private def sameRowsAsSpark(df: => DataFrame, isSorted: Boolean = false): Unit = {
    // withSQLConf returns Unit on Spark 3.x, so Spark's rows are assigned inside it.
    var sparkRows = Seq.empty[Row]
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      sparkRows = df.collect().toSeq
    }
    QueryTest.sameRows(sparkRows, df.collect().toSeq, isSorted).foreach(fail(_))
  }

  test("outer hash joins keep the order of a native range") {
    // Spark's RangeExec reports id ascending, or descending for a negative step, and the sort above
    // the join is dropped on that basis. The range is a native leaf, not a JVM input, so the
    // planner has to declare its order itself or the join moves its unmatched rows. A broadcast
    // join, since a shuffled one would put an exchange between the range and the join.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      CometConf.COMET_EXEC_RANGE_ENABLED.key -> "true") {
      withParquetTable((0 until 40).map(k => (k.toLong, k)), "small") {
        val build = spark.table("small")
        for {
          (start, end, step) <- Seq((0L, 2000L, 1L), (1999L, -1L, -1L))
          joinType <- Seq("left_outer", "right_outer")
        } {
          val streamed = spark.range(start, end, step, 1)
          val joined = if (joinType == "left_outer") {
            streamed.join(build.hint("broadcast"), streamed("id") === build("_1"), joinType)
          } else {
            build.hint("broadcast").join(streamed, streamed("id") === build("_1"), joinType)
          }
          val direction = if (step > 0) Ascending else Descending
          val df = joined
            .select(streamed("id").as("k"), build("_2").as("w"))
            .sortWithinPartitions(if (step > 0) $"k" else $"k".desc)
          val plan = df.queryExecution.executedPlan
          val context = s"$joinType step $step\n$plan"
          assert(collect(plan) { case r: CometRangeExec => r }.size == 1, context)
          val joins = collect(plan) { case j: CometBroadcastHashJoinExec => j.nativeOp }
          assert(joins.size == 1, context)
          assert(joins.head.getHashJoin.getStreamedSortOrdersCount > 0, context)
          assert(
            collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty,
            context)
          val expected =
            (start until end by step).map(id => Row(id, if (id < 40) id.toInt else null))
          QueryTest.sameRows(expected, df.collect().toSeq).foreach(fail(_))
          assert(
            partitionsInOrder(
              df,
              SortOrder(BoundReference(0, LongType, nullable = true), direction)),
            context)
        }
      }
    }
  }

  test("outer hash joins keep the order of a sorted bucketed table read by the native scan") {
    // With spark.sql.legacy.bucketedTableScan.outputOrdering on, Spark reports a bucketed table's
    // sort columns as the scan's order when every bucket has one file, and the sort above the
    // join is dropped on that basis. The native scan reads each bucket's file in order, but it is
    // a native leaf, not a JVM input, so the planner has to count it as a source whose order
    // Spark reports, or the join moves its unmatched rows. A broadcast join, since a shuffled one
    // would put an exchange between the scan and the join.
    withTable("sorted_bucketed") {
      // A single writer task writes one file per bucket.
      (0 until 2000)
        .map(i => (i % 40, i))
        .toDF("k", "v")
        .coalesce(1)
        .write
        .format("parquet")
        .bucketBy(2, "k")
        .sortBy("k")
        .saveAsTable("sorted_bucketed")
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.BUCKETING_ENABLED.key -> "true",
        SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "false",
        SQLConf.LEGACY_BUCKETED_TABLE_SCAN_OUTPUT_ORDERING.key -> "true") {
        withParquetTable((0 until 40 by 2).map(k => (k, k / 2)), "small") {
          val streamed = spark.table("sorted_bucketed")
          val build = spark.table("small")
          val scanOrder = collectFirst(streamed.queryExecution.executedPlan) {
            case scan: CometNativeScanExec if scan.bucketedScan => scan.outputOrdering
          }
          assert(scanOrder.exists(_.nonEmpty), streamed.queryExecution.executedPlan)
          Seq("left_outer", "right_outer").foreach { joinType =>
            val joined = if (joinType == "left_outer") {
              streamed.join(build.hint("broadcast"), streamed("k") === build("_1"), joinType)
            } else {
              build.hint("broadcast").join(streamed, streamed("k") === build("_1"), joinType)
            }
            val df = joined
              .select(streamed("k"), streamed("v"), build("_2").as("w"))
              .sortWithinPartitions("k")
            val plan = df.queryExecution.executedPlan
            val context = s"$joinType\n$plan"
            val joins = collect(plan) { case j: CometBroadcastHashJoinExec => j }
            assert(joins.size == 1, context)
            assert(
              collect(joins.head) {
                case s: CometNativeScanExec if s.bucketedScan => s
              }.size == 1,
              context)
            assert(joins.head.nativeOp.getHashJoin.getStreamedSortOrdersCount > 0, context)
            assert(
              collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty,
              context)
            val expected = (0 until 2000).map { i =>
              val k = i % 40
              Row(k, i, if (k % 2 == 0) k / 2 else null)
            }
            QueryTest.sameRows(expected, df.collect().toSeq).foreach(fail(_))
            assert(partitionsInOrder(df, keyOrder(Ascending)), context)
          }
        }
      }
    }
  }

  test("outer hash joins keep the streamed order for decimal, string and timestamp keys") {
    // SortedInputExec passes the rows through, so the order is physically Spark's, but the serde
    // and the native sort expression handle the key, and the sorted aggregate above the join finds
    // its group boundaries by comparing it. The timestamps run in a session time zone that is not
    // UTC and include values before 1970. Double, array and struct keys have tests of their own,
    // and Spark cannot sort by a map.
    val keyTypes = Seq(
      "decimal(20,2)" -> Seq(
        "-999999999999999999.99",
        "-0.01",
        "0.00",
        "0.01",
        "999999999999999999.99"),
      "string" -> Seq("apple", "Äpfel", "naïve", "日本語", "中文", "한국어", "😀"),
      "timestamp" -> Seq(
        "1900-01-01 00:00:00",
        "1969-12-31 23:59:59.999999",
        "1970-01-01 00:00:00",
        "2026-11-01 01:30:00",
        "2030-06-15 12:00:00"))
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_IN_MEMORY_CACHE_ENABLED.key -> "true") {
      keyTypes.foreach { case (typeName, keys) =>
        val data = (0 until 2000).map(i => (keys(i % keys.size), i))
        val buildData = keys.zipWithIndex.collect { case (k, i) if i % 2 == 0 => (k, i) }
        withParquetTable(data, "big") {
          withParquetTable(buildData, "small") {
            try {
              val streamed = spark
                .table("big")
                .select($"_1".cast(typeName).as("_1"), $"_2")
                .repartition(2, $"_1")
                .sortWithinPartitions("_1")
                .cache()
              streamed.count()
              val build = spark.table("small").select($"_1".cast(typeName).as("_1"), $"_2")
              val order = SortOrder(
                BoundReference(0, streamed.schema("_1").dataType, nullable = true),
                Ascending)

              Seq("left_outer", "right_outer").foreach { joinType =>
                def joined: DataFrame = {
                  val hinted = build.hint("shuffle_hash")
                  if (joinType == "left_outer") {
                    streamed.join(hinted, streamed("_1") === build("_1"), joinType)
                  } else {
                    hinted.join(streamed, build("_1") === streamed("_1"), joinType)
                  }
                }
                def sorted: DataFrame = joined
                  .select(streamed("_1").as("k"), streamed("_2").as("v"), build("_2").as("w"))
                  .sortWithinPartitions("k")
                val df = sorted
                val plan = df.queryExecution.executedPlan
                val joins = collect(plan) { case j: CometHashJoinExec => j }
                assert(joins.size == 1, s"$typeName $joinType\n$plan")
                assert(
                  joins.head.nativeOp.getHashJoin.getStreamedSortOrdersCount > 0,
                  s"$typeName $joinType\n$plan")
                assert(
                  collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty,
                  s"$typeName $joinType\n$plan")
                sameRowsAsSpark(sorted)
                assert(partitionsInOrder(df, order), s"$typeName $joinType\n$plan")

                // The aggregate runs in sorted mode over the declared order, and groups a key once
                // only if the join keeps it.
                def agg: DataFrame = joined
                  .groupBy(streamed("_1").as("k"))
                  .agg(count(lit(1)), sum(streamed("_2")), count(build("_2")))
                sameRowsAsSpark(agg)
                assert(agg.count() == keys.size, s"$typeName $joinType groups a key twice")
              }
            } finally {
              spark.catalog.clearCache()
            }
          }
        }
      }
    }
  }

  test("outer hash joins keep a streamed double order with signed zeros and NaN") {
    // Spark sorts -0.0 and 0.0 as equal and NaN above every other value. The planner declares the
    // order on the key normalized as the native sort normalizes it, so an operator above the join
    // compares keys as Spark does. A top-k above the join runs in a native plan of its own, over
    // the join's output, so it cannot stop early on the declared order, and one sorting by the key
    // alone reads only the first rows of each partition, which holds only if the join keeps them in
    // order. A broadcast join, since Spark joins on the normalized key and a shuffled join would
    // shuffle the streamed side again.
    val keys =
      Seq(Double.NegativeInfinity, -1.5, -0.0, 0.0, 1.5, Double.PositiveInfinity, Double.NaN)
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_IN_MEMORY_CACHE_ENABLED.key -> "true") {
      // The build side holds -0.0 and not 0.0, so a streamed 0.0 matches only on the normalized
      // key.
      val buildData = Seq((Double.NegativeInfinity, 0), (-0.0, 1), (1.5, 2), (Double.NaN, 3))
      withParquetTable((0 until 2000).map(i => (keys(i % keys.size), i)), "big") {
        withParquetTable(buildData, "small") {
          try {
            val streamed =
              spark.table("big").repartition(2, $"_2").sortWithinPartitions("_1").cache()
            streamed.count()
            val build = spark.table("small")
            val order = SortOrder(BoundReference(0, DoubleType, nullable = true), Ascending)
            Seq("left_outer", "right_outer").foreach { joinType =>
              def joined: DataFrame = {
                val hinted = build.hint("broadcast")
                if (joinType == "left_outer") {
                  streamed.join(hinted, streamed("_1") === build("_1"), joinType)
                } else {
                  hinted.join(streamed, build("_1") === streamed("_1"), joinType)
                }
              }
              def rows: DataFrame =
                joined.select(streamed("_1").as("k"), streamed("_2").as("v"), build("_2").as("w"))
              val df = rows.sortWithinPartitions("k")
              val plan = df.queryExecution.executedPlan
              val context = s"$joinType\n$plan"
              val joins = collect(plan) { case j: CometBroadcastHashJoinExec => j.nativeOp }
              assert(joins.size == 1, context)
              assert(joins.head.getHashJoin.getStreamedSortOrdersCount > 0, context)
              assert(
                collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty,
                context)
              sameRowsAsSpark(rows.sortWithinPartitions("k"))
              assert(partitionsInOrder(df, order), context)

              def agg: DataFrame = joined
                .groupBy(streamed("_1").as("k"))
                .agg(count(lit(1)), sum(streamed("_2")), count(build("_2")))
              sameRowsAsSpark(agg)
              val groupKeys = agg.collect().map(_.getDouble(0)).toSeq
              assert(groupKeys.distinct.size == groupKeys.size, s"$joinType groups a key twice")

              // The first 600 rows by the key end among the zeros, which tie, so which of them
              // are taken depends on v alone. The first 300 descending take every NaN, which tie
              // too, and the rest from the infinities. A limit on the key alone ends where the
              // zeros start, since which of them it took would be arbitrary.
              sameRowsAsSpark(rows.orderBy("k", "v").limit(600), isSorted = true)
              sameRowsAsSpark(rows.orderBy($"k".desc, $"v").limit(300), isSorted = true)
              val belowZero = (0 until 2000).count(i => keys(i % keys.size) < -1.0)
              sameRowsAsSpark(rows.select("k").orderBy("k").limit(belowZero), isSorted = true)
            }
          } finally {
            spark.catalog.clearCache()
          }
        }
      }
    }
  }

  test("outer hash joins keep a streamed order that starts with an array or a struct") {
    // exprToProto takes an array or a struct sort key, so a join whose streamed order starts with
    // one sends it, stays native, and the planner declares it on the JVM input. Spark drops a sort
    // right below a join, so the sorted rows come from a cache.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_IN_MEMORY_CACHE_ENABLED.key -> "true") {
      withParquetTable((0 until 2000).map(i => (i % 40, i)), "big") {
        withParquetTable((0 until 40 by 2).map(k => (k, k / 2)), "small") {
          val build = spark.table("small")
          // An array of strings, the one array type that Comet reads from Spark's cache.
          Seq(
            "array" -> array(($"_1" % 7).cast("string"), $"_1".cast("string")),
            "struct" -> struct($"_1" % 7, $"_1")).foreach { case (keyName, key) =>
            try {
              val streamed = spark
                .table("big")
                .select(key.as("s"), $"_1", $"_2")
                .repartition(2, $"_1")
                .sortWithinPartitions("s")
                .cache()
              streamed.count()
              val order = SortOrder(
                BoundReference(0, streamed.schema("s").dataType, nullable = true),
                Ascending)
              Seq("left_outer", "right_outer").foreach { joinType =>
                def joined: DataFrame = {
                  val hinted = build.hint("broadcast")
                  if (joinType == "left_outer") {
                    streamed.join(hinted, streamed("_1") === build("_1"), joinType)
                  } else {
                    hinted.join(streamed, build("_1") === streamed("_1"), joinType)
                  }
                }
                def df: DataFrame = joined
                  .select(streamed("s"), streamed("_2").as("v"), build("_2").as("w"))
                  .sortWithinPartitions("s")
                val plan = df.queryExecution.executedPlan
                val context = s"$keyName $joinType\n$plan"
                val joins = collect(plan) { case j: CometBroadcastHashJoinExec => j.nativeOp }
                assert(joins.size == 1, context)
                assert(joins.head.getHashJoin.getStreamedSortOrdersCount > 0, context)
                assert(
                  collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty,
                  context)
                sameRowsAsSpark(df)
                assert(partitionsInOrder(df, order), context)
                sameRowsAsSpark(
                  joined
                    .groupBy(streamed("s"))
                    .agg(count(lit(1)), sum(streamed("_2")), count(build("_2"))))
              }
            } finally {
              spark.catalog.clearCache()
            }
          }
        }
      }
    }
  }

  // The setup several cache tests share: `big(_1, _2)` with 2000 rows over 40 sparse keys and
  // `small` with a row for every even key, and `big` as `cached(k, v)`, partitioned on the key,
  // sorted within each partition and cached, with 8-row batches and the native cache on. `views`
  // names the temporary views that `f` creates, which are dropped afterwards.
  private def withSortedCache(views: String*)(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      SQLConf.COLUMN_BATCH_SIZE.key -> "8",
      CometConf.COMET_BATCH_SIZE.key -> "8",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {
      withParquetTable((0 until 2000).map(i => ((i % 40) * 1000, i)), "big") {
        withParquetTable((0 until 40 by 2).map(k => (k * 1000, k)), "small") {
          try {
            spark
              .table("big")
              .toDF("k", "v")
              .repartition(2, $"k")
              .sortWithinPartitions("k")
              .cache()
              .createOrReplaceTempView("cached")
            spark.table("cached").count()
            f
          } finally {
            spark.catalog.clearCache()
            (views :+ "cached").foreach(spark.catalog.dropTempView)
          }
        }
      }
    }
  }

  test("NOT IN below an outer hash join over a sorted cache keeps each group once") {
    // Spark reports the streamed side's order for a null-aware anti join, but a native one does
    // not keep it, so Comet leaves a NOT IN over an ordered side in Spark. Its order then holds
    // and the outer join above may declare it. Declaring it over a native NOT IN would put the
    // aggregates in sorted mode over unsorted rows and split groups. Sparse keys keep DataFusion
    // off its perfect hash build path, which keeps the build batches in order and would hide
    // that, and small batches spread each partition over many.
    withSortedCache() {
      withParquetTable(Seq(1000, 10000, 20000, 100000).map(x => Tuple1(Option(x))), "excl") {
        val query =
          """SELECT /*+ BROADCAST(b) */ a.k, count(*), sum(a.v), count(b._2)
                  |FROM (SELECT * FROM cached WHERE k NOT IN (SELECT _1 FROM excl)) a
                  |LEFT JOIN small b ON a.k = b._1
                  |GROUP BY a.k""".stripMargin
        checkEachKeyGroupedOnce(query)
      }
    }
  }

  test("a cache of an outer hash join over a native sample reports no order, as Spark does") {
    // Spark's SampleExec reports no output ordering, so the sort above a join over a sample stays,
    // and CometSampleExec reports none either. The native sample does not say it keeps its input's
    // order, so the planner could not declare an order through it, and a cache that reported the
    // order would carry a false claim to whatever reads it next.
    withSortedCache("joined") {
      sql("""SELECT /*+ BROADCAST(b) */ a.k, a.v, b._2 AS w
                  |FROM (SELECT * FROM cached TABLESAMPLE (90 PERCENT) REPEATABLE (7)) a
                  |LEFT JOIN small b ON a.k = b._1""".stripMargin)
        .cache()
        .createOrReplaceTempView("joined")
      spark.table("joined").count()
      val cachedPlan = cachedPlanOf(spark.table("joined"))
      assert(
        find(cachedPlan) {
          case join: CometBroadcastHashJoinExec =>
            join.joinType == LeftOuter && join.nativeOp.getHashJoin.getStreamedSortOrdersCount == 0
          case _ => false
        }.isDefined,
        s"the cache was not written by a native outer join that sent no order:\n$cachedPlan")
      assert(
        find(cachedPlan)(_.isInstanceOf[CometSampleExec]).isDefined,
        s"the outer join does not read a native sample:\n$cachedPlan")
      assert(
        cachedPlan.outputOrdering.isEmpty,
        s"the cache reports an order that nothing keeps:\n$cachedPlan")
      // The sort above the cache stays, so the partitions come back sorted.
      assert(
        partitionsInOrder(spark.table("joined").sortWithinPartitions("k"), keyOrder(Ascending)))
      val query =
        """SELECT /*+ BROADCAST(c) */ j.k, count(*), sum(j.v), count(c._2)
                |FROM joined j LEFT JOIN small c ON j.k = c._1
                |GROUP BY j.k""".stripMargin
      checkEachKeyGroupedOnce(query)
    }
  }

  test("an outer hash join over a cache of a declared outer hash join declares the order too") {
    // The first outer join's probe input is the sorted cache, a JVM input that the planner
    // declares the order on, so the join keeps its unmatched rows in place and the cache it
    // writes is in the order it reports. The second outer join can rely on that.
    withSortedCache("joined") {
      sql("""SELECT /*+ BROADCAST(b) */ a.k, a.v, b._2 AS w
                  |FROM cached a LEFT JOIN small b ON a.k = b._1""".stripMargin)
        .cache()
        .createOrReplaceTempView("joined")
      spark.table("joined").count()
      val cachedPlan = cachedPlanOf(spark.table("joined"))
      assert(
        find(cachedPlan) {
          case join: CometBroadcastHashJoinExec =>
            join.joinType == LeftOuter &&
            join.nativeOp.getHashJoin.getStreamedSortOrdersCount > 0
          case _ => false
        }.isDefined,
        s"the cache was not written by a native outer join that sent the order:\n$cachedPlan")
      assert(
        partitionsInOrder(spark.table("joined"), keyOrder(Ascending)),
        "the cached outer join rows are out of order, so the first join did not keep them")
      val df = sql("""SELECT /*+ BROADCAST(c) */ j.k, j.v, c._2
                           |FROM joined j LEFT JOIN small c ON j.k = c._1""".stripMargin)
        .sortWithinPartitions("k")
      val plan = df.queryExecution.executedPlan
      val joins = collect(plan) { case join: CometBroadcastHashJoinExec => join }
      assert(joins.size == 1, plan)
      assert(joins.head.nativeOp.getHashJoin.getStreamedSortOrdersCount > 0, plan)
      assert(collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty, plan)
      val expected = (0 until 2000).map { i =>
        val m = i % 40
        Row(m * 1000, i, if (m % 2 == 0) m else null)
      }
      QueryTest.sameRows(expected, df.collect().toSeq).foreach(fail(_))
      assert(partitionsInOrder(df, keyOrder(Ascending)), plan)
    }
  }

  test("chained outer hash joins over a native filter over a sorted cache stay native") {
    // DataFusion sees the first join's order, in the same native plan, so the second sends none.
    withSortedCache() {
      def df: DataFrame = sql("""SELECT /*+ BROADCAST(b), BROADCAST(c) */ a.k, a.v, b._2, c._2
                                 |FROM (SELECT * FROM cached WHERE v >= 0) a
                                 |LEFT JOIN small b ON a.k = b._1
                                 |LEFT JOIN small c ON a.k = c._1""".stripMargin)
        .sortWithinPartitions("k")
      val (_, plan) = checkSparkAnswer(df)
      // Spark's cache format adds a reason of its own to the cache scan.
      assert(
        !new ExtendedExplainInfo().getFallbackReasons(plan).exists(_.contains("sort order")),
        plan)
      val joins = collect(plan) { case j: CometBroadcastHashJoinExec => j }
      assert(joins.size == 2 && joins.forall(_.joinType == LeftOuter), plan)
      val (first, second) =
        joins.partition(_.left.find(_.isInstanceOf[CometBroadcastHashJoinExec]).isEmpty)
      assert(first.size == 1 && second.size == 1, plan)
      assert(collect(first.head.left) { case f: CometFilterExec => f }.size == 1, plan)
      assert(first.head.nativeOp.getHashJoin.getStreamedSortOrdersCount > 0, plan)
      assert(second.head.nativeOp.getHashJoin.getStreamedSortOrdersCount == 0, plan)
      assert(collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty, plan)
      assert(partitionsInOrder(df, keyOrder(Ascending)), plan)
    }
  }

  test("an outer hash join over a nested loop join over a sorted cache keeps its order") {
    // The nested loop join stays in Spark, which keeps the cache's order over any number of build
    // rows, and so does the outer join over it, so the order holds without the sort Spark drops.
    withSortedCache() {
      withParquetTable(Seq(Tuple1(0), Tuple1(1)), "two") {
        def df: DataFrame =
          sql("""SELECT /*+ BROADCAST(n), BROADCAST(b) */ a.k, a.v, n._1 AS z, b._2 AS w
                |FROM cached a JOIN two n ON a.v % 2 <= n._1
                |LEFT JOIN small b ON a.k = b._1""".stripMargin)
            .sortWithinPartitions("k")
        sameRowsAsSpark(df)
        val plan = df.queryExecution.executedPlan
        val reasons = new ExtendedExplainInfo().getFallbackReasons(plan)
        assert(
          reasons.exists(_.contains(CometBroadcastNestedLoopJoinExec.streamedOrderReason)),
          plan)
        assert(collect(plan) { case j: BroadcastNestedLoopJoinExec => j }.size == 1, plan)
        val outer = collect(plan) { case j: BroadcastHashJoinExec => j.joinType }
        assert(outer == Seq(LeftOuter), plan)
        assert(collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty, plan)
        assert(partitionsInOrder(df, keyOrder(Ascending)), plan)
      }
    }
  }

  test("an outer hash join declares the streamed order over a semi join") {
    // `k IN (SELECT ...)` runs as a left semi join, which emits its probe rows in ascending
    // order whether or not the probe input declares an order. The outer join above it therefore
    // declares the cache's order on the semi join's output, and keeps it.
    withSortedCache() {
      withParquetTable((0 until 40).filter(_ % 7 != 3).map(k => Tuple1(k * 1000)), "incl") {
        val df = sql("""SELECT /*+ BROADCAST(b) */ a.k, a.v, b._2 AS w
                             |FROM (SELECT * FROM cached WHERE k IN (SELECT _1 FROM incl)) a
                             |LEFT JOIN small b ON a.k = b._1""".stripMargin)
          .sortWithinPartitions("k")
        val plan = df.queryExecution.executedPlan
        // Spark also pushes the IN to the other input of the outer join, so the outer join
        // is found by its type and has to read the semi join over the cache.
        val outer = collect(plan) {
          case join: CometBroadcastHashJoinExec if join.joinType == LeftOuter => join
        }
        assert(outer.size == 1, plan)
        assert(
          outer.head.left match {
            case semi: CometBroadcastHashJoinExec => semi.joinType == LeftSemi
            case _ => false
          },
          plan)
        assert(outer.head.nativeOp.getHashJoin.getStreamedSortOrdersCount > 0, plan)
        assert(collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty, plan)
        val expected = (0 until 2000).filter(i => (i % 40) % 7 != 3).map { i =>
          val m = i % 40
          Row(m * 1000, i, if (m % 2 == 0) m else null)
        }
        QueryTest.sameRows(expected, df.collect().toSeq).foreach(fail(_))
        assert(partitionsInOrder(df, keyOrder(Ascending)), plan)
      }
    }
  }
}
