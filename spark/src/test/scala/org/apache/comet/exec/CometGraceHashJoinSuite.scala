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
import org.apache.spark.sql.comet.CometHashJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, Decimal, StructField, StructType}

import org.apache.comet.CometConf
import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}

class CometGraceHashJoinSuite extends CometTestBase {

  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
        testFun
      }
    }
  }

  // Common SQL config for Grace Hash Join tests
  private val graceHashJoinConf: Seq[(String, String)] = Seq(
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

  test("Grace HashJoin - plan shows CometHashJoinExec") {
    withSQLConf(graceHashJoinConf: _*) {
      withParquetTable((0 until 50).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 50).map(i => (i % 10, i + 2)), "tbl_b") {
          val df = sql(
            "SELECT /*+ SHUFFLE_HASH(tbl_a) */ * FROM tbl_a JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df, Seq(classOf[CometHashJoinExec]))
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
