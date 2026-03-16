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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometBroadcastHashJoinExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class CometJoinSuite extends CometTestBase {

  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
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

  test("SortMergeJoin with unsupported key type should fall back to Spark") {
    withSQLConf(
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Kathmandu",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("t1", "t2") {
        sql("CREATE TABLE t1(name STRING, time TIMESTAMP) USING PARQUET")
        sql("INSERT OVERWRITE t1 VALUES('a', timestamp'2019-01-01 11:11:11')")

        sql("CREATE TABLE t2(name STRING, time TIMESTAMP) USING PARQUET")
        sql("INSERT OVERWRITE t2 VALUES('a', timestamp'2019-01-01 11:11:11')")

        val df = sql("SELECT * FROM t1 JOIN t2 ON t1.time = t2.time")
        val (sparkPlan, cometPlan) = checkSparkAnswer(df)
        assert(sparkPlan.canonicalized === cometPlan.canonicalized)
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

          // DataFusion HashJoin LeftAnti has bugs in handling nulls and is disabled for now.
          // left.join(right, left("_2") === right("_1"), "leftanti")
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

          // First verify correctness
          val df = sql(query)
          checkSparkAnswerAndOperator(
            df,
            Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastHashJoinExec]))

          // Run again and check metrics on the executed plan
          val df2 = sql(query)
          df2.collect()

          val joins = collect(df2.queryExecution.executedPlan) {
            case j: CometBroadcastHashJoinExec => j
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

          val broadcasts = collect(df2.queryExecution.executedPlan) {
            case b: CometBroadcastExchangeExec => b
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
}
