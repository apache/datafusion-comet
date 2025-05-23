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

package org.apache.comet

import scala.util.Random

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}

class CometBitwiseExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("bitwise expressions") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col1 int, col2 int) using parquet")
          sql(s"insert into $table values(1111, 2)")
          sql(s"insert into $table values(1111, 2)")
          sql(s"insert into $table values(3333, 4)")
          sql(s"insert into $table values(5555, 6)")

          checkSparkAnswerAndOperator(
            s"SELECT col1 & col2,  col1 | col2, col1 ^ col2 FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT col1 & 1234,  col1 | 1234, col1 ^ 1234 FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT shiftright(col1, 2), shiftright(col1, col2) FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT shiftleft(col1, 2), shiftleft(col1, col2) FROM $table")
          checkSparkAnswerAndOperator(s"SELECT ~(11), ~col1, ~col2 FROM $table")
        }
      }
    }
  }

  test("bitwise shift with different left/right types") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col1 long, col2 int) using parquet")
          sql(s"insert into $table values(1111, 2)")
          sql(s"insert into $table values(1111, 2)")
          sql(s"insert into $table values(3333, 4)")
          sql(s"insert into $table values(5555, 6)")

          checkSparkAnswerAndOperator(
            s"SELECT shiftright(col1, 2), shiftright(col1, col2) FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT shiftleft(col1, 2), shiftleft(col1, col2) FROM $table")
        }
      }
    }
  }

  test("bitwise_get - throws exceptions") {
    def checkSparkAndCometEqualThrows(query: String): Unit = {
      checkSparkMaybeThrows(sql(query)) match {
        case (Some(sparkExc), Some(cometExc)) =>
          assert(sparkExc.getMessage == cometExc.getMessage)
        case _ => fail("Exception should be thrown")
      }
    }
    checkSparkAndCometEqualThrows("select bit_get(1000, -30)")
    checkSparkAndCometEqualThrows("select bit_get(cast(1000 as byte), 9)")
    checkSparkAndCometEqualThrows("select bit_count(cast(null as byte), 4)")
    checkSparkAndCometEqualThrows("select bit_count(1000, cast(null as int))")
  }

  test("bitwise_get - random values (spark parquet gen)") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        ParquetGenerator.makeParquetFile(
          random,
          spark,
          filename,
          100,
          DataGenOptions(
            allowNull = true,
            generateNegativeZero = true,
            generateArray = false,
            generateStruct = false,
            generateMap = false))
      }
      val table = spark.read.parquet(filename)
      checkSparkAnswerAndOperator(
        table
          .selectExpr("bit_get(c1, 7)", "bit_get(c2, 10)", "bit_get(c3, 12)", "bit_get(c4, 16)"))
    }
  }

  test("bitwise_get - random values (native parquet gen)") {
    def randomBitPosition(maxBitPosition: Int): Int = {
      Random.nextInt(maxBitPosition)
    }
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllTypes(path, dictionaryEnabled, 0, 10000, nullEnabled = false)
        val table = spark.read.parquet(path.toString)
        (0 to 10).foreach { _ =>
          val byteBitPosition = randomBitPosition(java.lang.Byte.SIZE)
          val shortBitPosition = randomBitPosition(java.lang.Short.SIZE)
          val intBitPosition = randomBitPosition(java.lang.Integer.SIZE)
          val longBitPosition = randomBitPosition(java.lang.Long.SIZE)
          checkSparkAnswerAndOperator(
            table
              .selectExpr(
                s"bit_get(_2, $byteBitPosition)",
                s"bit_get(_3, $shortBitPosition)",
                s"bit_get(_4, $intBitPosition)",
                s"bit_get(_5, $longBitPosition)",
                s"bit_get(_11, $longBitPosition)"))
        }
      }
    }
  }
}
