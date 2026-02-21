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

import scala.util.Random

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometBroadcastHashJoinExec, CometGraceHashJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, Decimal, StructField, StructType}

import org.apache.comet.CometConf
import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}

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

  test("HashJoin struct key") {
    // https://github.com/apache/datafusion-comet/issues/1441
    assume(usingLegacyNativeCometScan)
    withSQLConf(
      "spark.sql.join.forceApplyShuffledHashJoin" -> "true",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "false",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {

      def manyTypes(idx: Int, v: Int) =
        (
          idx,
          v,
          v.toLong,
          v.toFloat,
          v.toDouble,
          v.toString,
          v % 2 == 0,
          v.toString.getBytes,
          Decimal(v))

      withParquetTable((0 until 10).map(i => manyTypes(i, i % 5)), "tbl_a") {
        withParquetTable((0 until 10).map(i => manyTypes(i, i % 10)), "tbl_b") {
          // Full join: struct key
          val df1 =
            sql(
              "SELECT /*+ SHUFFLE_HASH(tbl_b) */ * FROM tbl_a FULL JOIN tbl_b " +
                "ON named_struct('1', tbl_a._2) = named_struct('1', tbl_b._1)")
          checkSparkAnswerAndOperator(df1)

          // Full join: struct key with nulls
          val df2 =
            sql("SELECT /*+ SHUFFLE_HASH(tbl_b) */ * FROM tbl_a FULL JOIN tbl_b " +
              "ON IF(tbl_a._1 > 5, named_struct('2', tbl_a._2), NULL) = IF(tbl_b._2 > 5, named_struct('2', tbl_b._1), NULL)")
          checkSparkAnswerAndOperator(df2)

          // Full join: struct key with nulls in the struct
          val df3 =
            sql("SELECT /*+ SHUFFLE_HASH(tbl_b) */ * FROM tbl_a FULL JOIN tbl_b " +
              "ON named_struct('2', IF(tbl_a._1 > 5, tbl_a._2, NULL)) = named_struct('2', IF(tbl_b._2 > 5, tbl_b._1, NULL))")
          checkSparkAnswerAndOperator(df3)

          // Full join: nested structs
          val df4 =
            sql("SELECT /*+ SHUFFLE_HASH(tbl_b) */ * FROM tbl_a FULL JOIN tbl_b " +
              "ON named_struct('1', named_struct('2', tbl_a._2)) = named_struct('1', named_struct('2',  tbl_b._1))")
          checkSparkAnswerAndOperator(df4)

          val columnCount = manyTypes(0, 0).productArity
          def key(tbl: String) =
            (1 to columnCount).map(i => s"${tbl}._$i").mkString("struct(", ", ", ")")
          // Using several different types in the struct key
          val df5 =
            sql(
              "SELECT /*+ SHUFFLE_HASH(tbl_b) */ * FROM tbl_a FULL JOIN tbl_b " +
                s"ON ${key("tbl_a")} = ${key("tbl_b")}")
          checkSparkAnswerAndOperator(df5)
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

  // Common SQL config for Grace Hash Join tests
  private val graceHashJoinConf: Seq[(String, String)] = Seq(
    CometConf.COMET_EXEC_GRACE_HASH_JOIN_ENABLED.key -> "true",
    CometConf.COMET_EXEC_GRACE_HASH_JOIN_NUM_PARTITIONS.key -> "4",
    "spark.sql.join.forceApplyShuffledHashJoin" -> "true",
    SQLConf.PREFER_SORTMERGEJOIN.key -> "false",
    SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1")

  test("Grace HashJoin - all join types") {
    withSQLConf(graceHashJoinConf: _*) {
      withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 100).map(i => (i % 10, i + 2)), "tbl_b") {
          // Inner join
          checkSparkAnswer(
            sql(
              "SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a JOIN tbl_b ON tbl_a._2 = tbl_b._1"))

          // Left join
          checkSparkAnswer(sql(
            "SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a LEFT JOIN tbl_b ON tbl_a._2 = tbl_b._1"))

          // Right join
          checkSparkAnswer(sql(
            "SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a RIGHT JOIN tbl_b ON tbl_a._2 = tbl_b._1"))

          // Full outer join
          checkSparkAnswer(sql(
            "SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a FULL JOIN tbl_b ON tbl_a._2 = tbl_b._1"))

          // Left semi join
          checkSparkAnswer(sql(
            "SELECT /*+ SHUFFLE_HASH(tbl_b) */ * FROM tbl_a LEFT SEMI JOIN tbl_b ON tbl_a._2 = tbl_b._1"))

          // Left anti join
          checkSparkAnswer(sql(
            "SELECT /*+ SHUFFLE_HASH(tbl_b) */ * FROM tbl_a LEFT ANTI JOIN tbl_b ON tbl_a._2 = tbl_b._1"))
        }
      }
    }
  }

  test("Grace HashJoin - with filter condition") {
    withSQLConf(graceHashJoinConf: _*) {
      withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 100).map(i => (i % 10, i + 2)), "tbl_b") {
          checkSparkAnswer(
            sql("SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a JOIN tbl_b " +
              "ON tbl_a._2 = tbl_b._1 AND tbl_a._1 > tbl_b._2"))

          checkSparkAnswer(
            sql("SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a LEFT JOIN tbl_b " +
              "ON tbl_a._2 = tbl_b._1 AND tbl_a._1 > tbl_b._2"))

          checkSparkAnswer(
            sql("SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a FULL JOIN tbl_b " +
              "ON tbl_a._2 = tbl_b._1 AND tbl_a._1 > tbl_b._2"))
        }
      }
    }
  }

  test("Grace HashJoin - various data types") {
    withSQLConf(graceHashJoinConf: _*) {
      // String keys
      withParquetTable((0 until 50).map(i => (s"key_${i % 10}", i)), "str_a") {
        withParquetTable((0 until 50).map(i => (s"key_${i % 5}", i * 2)), "str_b") {
          checkSparkAnswer(
            sql(
              "SELECT /*+ SHUFFLE_HASH(str_a) */ * FROM str_a JOIN str_b ON str_a._1 = str_b._1"))
        }
      }

      // Decimal keys
      withParquetTable((0 until 50).map(i => (Decimal(i % 10), i)), "dec_a") {
        withParquetTable((0 until 50).map(i => (Decimal(i % 5), i * 2)), "dec_b") {
          checkSparkAnswer(
            sql(
              "SELECT /*+ SHUFFLE_HASH(dec_a) */ * FROM dec_a JOIN dec_b ON dec_a._1 = dec_b._1"))
        }
      }
    }
  }

  test("Grace HashJoin - empty tables") {
    withSQLConf(graceHashJoinConf: _*) {
      withParquetTable(Seq.empty[(Int, Int)], "empty_a") {
        withParquetTable((0 until 10).map(i => (i, i)), "nonempty_b") {
          // Empty left side
          checkSparkAnswer(sql(
            "SELECT /*+ SHUFFLE_HASH(empty_a) */ * FROM empty_a JOIN nonempty_b ON empty_a._1 = nonempty_b._1"))

          // Empty left with left join
          checkSparkAnswer(sql(
            "SELECT /*+ SHUFFLE_HASH(empty_a) */ * FROM empty_a LEFT JOIN nonempty_b ON empty_a._1 = nonempty_b._1"))

          // Empty right side
          checkSparkAnswer(sql(
            "SELECT /*+ SHUFFLE_HASH(nonempty_b) */ * FROM nonempty_b JOIN empty_a ON nonempty_b._1 = empty_a._1"))

          // Empty right with right join
          checkSparkAnswer(sql(
            "SELECT /*+ SHUFFLE_HASH(nonempty_b) */ * FROM nonempty_b RIGHT JOIN empty_a ON nonempty_b._1 = empty_a._1"))
        }
      }
    }
  }

  test("Grace HashJoin - self join") {
    withSQLConf(graceHashJoinConf: _*) {
      withParquetTable((0 until 50).map(i => (i, i % 10)), "self_tbl") {
        checkSparkAnswer(
          sql("SELECT /*+ SHUFFLE_HASH(a) */ * FROM self_tbl a JOIN self_tbl b ON a._2 = b._2"))
      }
    }
  }

  test("Grace HashJoin - build side selection") {
    withSQLConf(graceHashJoinConf: _*) {
      withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 100).map(i => (i % 10, i + 2)), "tbl_b") {
          // Build left (hint on left table)
          checkSparkAnswer(
            sql(
              "SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a JOIN tbl_b ON tbl_a._2 = tbl_b._1"))

          // Build right (hint on right table)
          checkSparkAnswer(
            sql(
              "SELECT /*+ SHUFFLE_HASH(tbl_b) */ * FROM tbl_a JOIN tbl_b ON tbl_a._2 = tbl_b._1"))

          // Left join build right
          checkSparkAnswer(sql(
            "SELECT /*+ SHUFFLE_HASH(tbl_b) */ * FROM tbl_a LEFT JOIN tbl_b ON tbl_a._2 = tbl_b._1"))

          // Right join build left
          checkSparkAnswer(sql(
            "SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a RIGHT JOIN tbl_b ON tbl_a._2 = tbl_b._1"))
        }
      }
    }
  }

  test("Grace HashJoin - plan shows CometGraceHashJoinExec") {
    withSQLConf(graceHashJoinConf: _*) {
      withParquetTable((0 until 50).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 50).map(i => (i % 10, i + 2)), "tbl_b") {
          val df = sql(
            "SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df, Seq(classOf[CometGraceHashJoinExec]))
        }
      }
    }
  }

  test("Grace HashJoin - multiple key columns") {
    withSQLConf(graceHashJoinConf: _*) {
      withParquetTable((0 until 50).map(i => (i, i % 5, i % 3)), "multi_a") {
        withParquetTable((0 until 50).map(i => (i % 10, i % 5, i % 3)), "multi_b") {
          checkSparkAnswer(
            sql("SELECT /*+ SHUFFLE_HASH(multi_a) */ * FROM multi_a JOIN multi_b " +
              "ON multi_a._2 = multi_b._2 AND multi_a._3 = multi_b._3"))
        }
      }
    }
  }

  // Schema with types that work well as join keys (no NaN/float issues)
  private val fuzzJoinSchema = StructType(
    Seq(
      StructField("c_int", DataTypes.IntegerType),
      StructField("c_long", DataTypes.LongType),
      StructField("c_str", DataTypes.StringType),
      StructField("c_date", DataTypes.DateType),
      StructField("c_dec", DataTypes.createDecimalType(10, 2)),
      StructField("c_short", DataTypes.ShortType),
      StructField("c_bool", DataTypes.BooleanType)))

  private val joinTypes =
    Seq("JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "LEFT SEMI JOIN", "LEFT ANTI JOIN")

  test("Grace HashJoin fuzz - all join types with generated data") {
    val dataGenOptions =
      DataGenOptions(allowNull = true, generateNegativeZero = false, generateNaN = false)

    withSQLConf(graceHashJoinConf: _*) {
      withTempPath { dir =>
        val path1 = s"${dir.getAbsolutePath}/fuzz_left"
        val path2 = s"${dir.getAbsolutePath}/fuzz_right"
        val random = new Random(42)

        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          ParquetGenerator
            .makeParquetFile(random, spark, path1, fuzzJoinSchema, 200, dataGenOptions)
          ParquetGenerator
            .makeParquetFile(random, spark, path2, fuzzJoinSchema, 200, dataGenOptions)
        }

        spark.read.parquet(path1).createOrReplaceTempView("fuzz_l")
        spark.read.parquet(path2).createOrReplaceTempView("fuzz_r")

        for (jt <- joinTypes) {
          // Join on int column
          checkSparkAnswer(sql(
            s"SELECT /*+ SHUFFLE_HASH(fuzz_l) */ * FROM fuzz_l $jt fuzz_r ON fuzz_l.c_int = fuzz_r.c_int"))

          // Join on string column
          checkSparkAnswer(sql(
            s"SELECT /*+ SHUFFLE_HASH(fuzz_l) */ * FROM fuzz_l $jt fuzz_r ON fuzz_l.c_str = fuzz_r.c_str"))

          // Join on decimal column
          checkSparkAnswer(sql(
            s"SELECT /*+ SHUFFLE_HASH(fuzz_l) */ * FROM fuzz_l $jt fuzz_r ON fuzz_l.c_dec = fuzz_r.c_dec"))
        }
      }
    }
  }

  test("Grace HashJoin fuzz - with spilling") {
    val dataGenOptions =
      DataGenOptions(allowNull = true, generateNegativeZero = false, generateNaN = false)

    // Use very small memory pool to force spilling
    withSQLConf(
      (graceHashJoinConf ++ Seq(
        CometConf.COMET_ONHEAP_MEMORY_OVERHEAD.key -> "10000000",
        CometConf.COMET_EXEC_GRACE_HASH_JOIN_NUM_PARTITIONS.key -> "8")): _*) {
      withTempPath { dir =>
        val path1 = s"${dir.getAbsolutePath}/spill_left"
        val path2 = s"${dir.getAbsolutePath}/spill_right"
        val random = new Random(99)

        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          ParquetGenerator
            .makeParquetFile(random, spark, path1, fuzzJoinSchema, 500, dataGenOptions)
          ParquetGenerator
            .makeParquetFile(random, spark, path2, fuzzJoinSchema, 500, dataGenOptions)
        }

        spark.read.parquet(path1).createOrReplaceTempView("spill_l")
        spark.read.parquet(path2).createOrReplaceTempView("spill_r")

        for (jt <- joinTypes) {
          checkSparkAnswer(sql(
            s"SELECT /*+ SHUFFLE_HASH(spill_l) */ * FROM spill_l $jt spill_r ON spill_l.c_int = spill_r.c_int"))
        }
      }
    }
  }
}
