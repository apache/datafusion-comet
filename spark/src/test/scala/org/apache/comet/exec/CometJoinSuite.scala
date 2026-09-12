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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, IsNotNull}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometBroadcastHashJoinExec, CometBroadcastNestedLoopJoinExec, CometFilterExec, CometHashJoinExec, CometNativeScanExec, CometSortMergeJoinExec, CometUnionExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, IntegerType, MetadataBuilder, StructField, StructType}

import org.apache.comet.CometConf

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
                  val evaluated = join.metrics("dynamic_filter_rows_evaluated").value
                  val pruned = join.metrics("dynamic_filter_rows_pruned").value
                  val bypassed = join.metrics("dynamic_filter_rows_bypassed").value
                  assert(evaluated > 0L && pruned > 0L)
                  assert(probeRows + pruned == evaluated + bypassed)
                  assert(probeRows < unfilteredProbeRows)
                  assert(evaluated + bypassed <= unfilteredProbeRows)
                  assert(join.metrics("dynamic_filter_eval_time").value > 0L)
                } else {
                  assert(!join.metrics.contains("dynamic_filter_rows_pruned"))
                  unfilteredProbeRows = probeRows
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
                    assert(
                      joins.head.metrics("dynamic_filter_reader_filters_attached").value > 0L)
                    assert(
                      joins.head.metrics("dynamic_filter_reader_filters_skipped").value == 0L)
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

  test("join dynamic filter preserves seeded rand probe order") {
    withTempPath { probePath =>
      withSQLConf(
        CometConf.COMET_BATCH_SIZE.key -> "16",
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
                  assert(joins.head.metrics("dynamic_filter_reader_filters_attached").value == 0L)
                  assert(joins.head.metrics("dynamic_filter_reader_filters_skipped").value == 1L)
                  assert(joins.head.metrics("dynamic_filter_rows_evaluated").value == 1L)
                  assert(joins.head.metrics("dynamic_filter_rows_pruned").value == 0L)
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
        (0 until 100).map(i => (Some(i), i.toLong)) :+ (None, -1L),
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
              assert(joins.head.metrics("dynamic_filter_rows_evaluated").value > 0L)
              assert(joins.head.metrics("dynamic_filter_rows_pruned").value == 0L)
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
            assert(native.head.metrics("dynamic_filter_rows_evaluated").value == 0L)
            assert(native.head.metrics("dynamic_filter_rows_pruned").value == 0L)
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
            assert(native.head.metrics("dynamic_filter_rows_evaluated").value == 0L)
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
          assert(join.metrics("dynamic_filter_rows_evaluated").value > 0L)
          assert(join.metrics("dynamic_filter_reader_filters_attached").value > 0L)
          val probeScans = collect(plan) {
            case scan: CometNativeScanExec if scan.output.size == 1 => scan
          }
          assert(probeScans.size == 1, s"Expected one native byte probe scan:\n$plan")
          val probeRows = probeScans.head.metrics("output_rows").value
          val residualPruned = join.metrics("dynamic_filter_rows_pruned").value
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
}
