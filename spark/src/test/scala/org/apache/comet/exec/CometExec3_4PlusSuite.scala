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

import java.io.ByteArrayOutputStream

import scala.util.Random

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.{Column, CometTestBase, DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, BloomFilterMightContain, Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.util.sketch.BloomFilter

import org.apache.comet.CometConf

/**
 * This test suite contains tests for only Spark 3.4+.
 */
class CometExec3_4PlusSuite extends CometTestBase {
  import testImplicits._

  val func_might_contain = new FunctionIdentifier("might_contain")

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Register 'might_contain' to builtin.
    spark.sessionState.functionRegistry.registerFunction(
      func_might_contain,
      new ExpressionInfo(classOf[BloomFilterMightContain].getName, "might_contain"),
      (children: Seq[Expression]) => BloomFilterMightContain(children.head, children(1)))
  }

  override def afterAll(): Unit = {
    spark.sessionState.functionRegistry.dropFunction(func_might_contain)
    super.afterAll()
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
        testFun
      }
    }
  }

  // The syntax is only supported by Spark 3.4+.
  test("subquery limit: limit with offset should return correct results") {
    withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      withTable("t1", "t2") {
        val table1 =
          """create temporary view t1 as select * from values
            |  ("val1a", 6S, 8, 10L, float(15.0), 20D, 20E2BD, timestamp '2014-04-04 01:00:00.000', date '2014-04-04'),
            |  ("val1b", 8S, 16, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
            |  ("val1a", 16S, 12, 21L, float(15.0), 20D, 20E2BD, timestamp '2014-06-04 01:02:00.001', date '2014-06-04'),
            |  ("val1a", 16S, 12, 10L, float(15.0), 20D, 20E2BD, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
            |  ("val1c", 8S, 16, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.001', date '2014-05-05'),
            |  ("val1d", null, 16, 22L, float(17.0), 25D, 26E2BD, timestamp '2014-06-04 01:01:00.000', null),
            |  ("val1d", null, 16, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-07-04 01:02:00.001', null),
            |  ("val1e", 10S, null, 25L, float(17.0), 25D, 26E2BD, timestamp '2014-08-04 01:01:00.000', date '2014-08-04'),
            |  ("val1e", 10S, null, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-09-04 01:02:00.001', date '2014-09-04'),
            |  ("val1d", 10S, null, 12L, float(17.0), 25D, 26E2BD, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
            |  ("val1a", 6S, 8, 10L, float(15.0), 20D, 20E2BD, timestamp '2014-04-04 01:02:00.001', date '2014-04-04'),
            |  ("val1e", 10S, null, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04')
            |  as t1(t1a, t1b, t1c, t1d, t1e, t1f, t1g, t1h, t1i);""".stripMargin
        val table2 =
          """create temporary view t2 as select * from values
            |  ("val2a", 6S, 12, 14L, float(15), 20D, 20E2BD, timestamp '2014-04-04 01:01:00.000', date '2014-04-04'),
            |  ("val1b", 10S, 12, 19L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
            |  ("val1b", 8S, 16, 119L, float(17), 25D, 26E2BD, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
            |  ("val1c", 12S, 16, 219L, float(17), 25D, 26E2BD, timestamp '2016-05-04 01:01:00.000', date '2016-05-04'),
            |  ("val1b", null, 16, 319L, float(17), 25D, 26E2BD, timestamp '2017-05-04 01:01:00.000', null),
            |  ("val2e", 8S, null, 419L, float(17), 25D, 26E2BD, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
            |  ("val1f", 19S, null, 519L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
            |  ("val1b", 10S, 12, 19L, float(17), 25D, 26E2BD, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
            |  ("val1b", 8S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
            |  ("val1c", 12S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-08-04 01:01:00.000', date '2014-08-05'),
            |  ("val1e", 8S, null, 19L, float(17), 25D, 26E2BD, timestamp '2014-09-04 01:01:00.000', date '2014-09-04'),
            |  ("val1f", 19S, null, 19L, float(17), 25D, 26E2BD, timestamp '2014-10-04 01:01:00.000', date '2014-10-04'),
            |  ("val1b", null, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', null)
            |  as t2(t2a, t2b, t2c, t2d, t2e, t2f, t2g, t2h, t2i);""".stripMargin
        sql(table1)
        sql(table2)

        val df = sql("""SELECT *
                       |FROM   t1
                       |WHERE  t1c IN (SELECT t2c
                       |               FROM   t2
                       |               WHERE  t2b >= 8
                       |               LIMIT  2
                       |               OFFSET 2)
                       |LIMIT 4
                       |OFFSET 2;""".stripMargin)
        checkSparkAnswer(df)
      }
    }
  }

  // Dataset.offset API is not available before Spark 3.4
  test("offset") {
    withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      checkSparkAnswer(testData.offset(90))
      checkSparkAnswer(arrayData.toDF().offset(99))
      checkSparkAnswer(mapData.toDF().offset(99))
    }
  }

  test("test BloomFilterMightContain can take a constant value input") {
    val table = "test"

    withTable(table) {
      sql(s"create table $table(col1 long, col2 int) using parquet")
      sql(s"insert into $table values (201, 1)")
      checkSparkAnswerAndOperator(s"""
           |SELECT might_contain(
           |X'00000001000000050000000343A2EC6EA8C117E2D3CDB767296B144FC5BFBCED9737F267', col1) FROM $table
           |""".stripMargin)
    }
  }

  test("test NULL inputs for BloomFilterMightContain") {
    val table = "test"

    withTable(table) {
      sql(s"create table $table(col1 long, col2 int) using parquet")
      sql(s"insert into $table values (201, 1), (null, 2)")
      checkSparkAnswerAndOperator(s"""
           |SELECT might_contain(null, null) both_null,
           |       might_contain(null, 1L) null_bf,
           |       might_contain(
           |         X'00000001000000050000000343A2EC6EA8C117E2D3CDB767296B144FC5BFBCED9737F267', col1) null_value
           |       FROM $table
           |""".stripMargin)
    }
  }

  test("test BloomFilterMightContain from random input") {
    val (longs, bfBytes) = bloomFilterFromRandomInput(10000, 10000)
    val table = "test"

    withTable(table) {
      sql(s"create table $table(col1 long, col2 binary) using parquet")
      spark
        .createDataset(longs)
        .map(x => (x, bfBytes))
        .toDF("col1", "col2")
        .write
        .insertInto(table)
      val bfExpr: Expression =
        BloomFilterMightContain(Literal(bfBytes), UnresolvedAttribute("col1"))
      val aliasExpr = Alias(bfExpr, "might_contain")()
      val plan = spark.table(table).toDF().queryExecution.analyzed
      val newPlan = Project(Seq(aliasExpr), plan)

      val df = fromLogicalPlan(newPlan)
      checkSparkAnswerAndOperator(df)
      // check with scalar subquery
      checkSparkAnswerAndOperator(s"""
           |SELECT might_contain((select first(col2) as col2 from $table), col1) FROM $table
           |""".stripMargin)
    }
  }

  private def bloomFilterFromRandomInput(
      expectedItems: Long,
      expectedBits: Long): (Seq[Long], Array[Byte]) = {
    val bf = BloomFilter.create(expectedItems, expectedBits)
    val longs = (0 until expectedItems.toInt).map(_ => Random.nextLong())
    longs.foreach(bf.put)
    val os = new ByteArrayOutputStream()
    bf.writeTo(os)
    (longs, os.toByteArray)
  }

  private def fromLogicalPlan(plan: LogicalPlan): DataFrame = {
    val method = spark.getClass.getMethod("executionQuery", classOf[LogicalPlan])
    method.invoke(spark, plan).asInstanceOf[DataFrame]
  }
}
