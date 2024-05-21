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

import org.apache.spark.sql.{Column, CometTestBase}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Expression, ExpressionInfo}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.util.sketch.BloomFilter
import java.io.ByteArrayOutputStream
import scala.util.Random

class CometExpression3_3PlusSuite extends CometTestBase with AdaptiveSparkPlanHelper {
  import testImplicits._

  val func_might_contain = new FunctionIdentifier("might_contain")

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Register 'might_contain' to builtin.
    spark.sessionState.functionRegistry.registerFunction(func_might_contain,
      new ExpressionInfo(classOf[BloomFilterMightContain].getName, "might_contain"),
      (children: Seq[Expression]) => BloomFilterMightContain(children.head, children(1)))
  }

  override def afterAll(): Unit = {
    spark.sessionState.functionRegistry.dropFunction(func_might_contain)
    super.afterAll()
  }

  test("test BloomFilterMightContain can take a constant value input") {
    val table = "test"

    withTable(table) {
      sql(s"create table $table(col1 long, col2 int) using parquet")
      sql(s"insert into $table values (201, 1)")
      checkSparkAnswerAndOperator(
        s"""
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
      checkSparkAnswerAndOperator(
        s"""
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
      spark.createDataset(longs).map(x => (x, bfBytes)).toDF("col1", "col2").write.insertInto(table)
      val df = spark.table(table).select(new Column(BloomFilterMightContain(lit(bfBytes).expr, col("col1").expr)))
      checkSparkAnswerAndOperator(df)
      // check with scalar subquery
      checkSparkAnswerAndOperator(
        s"""
          |SELECT might_contain((select first(col2) as col2 from $table), col1) FROM $table
          |""".stripMargin)
    }
  }

  private def bloomFilterFromRandomInput(expectedItems: Long, expectedBits: Long): (Seq[Long], Array[Byte]) = {
    val bf = BloomFilter.create(expectedItems, expectedBits)
    val longs = (0 until expectedItems.toInt).map(_ => Random.nextLong())
    longs.foreach(bf.put)
    val os = new ByteArrayOutputStream()
    bf.writeTo(os)
    (longs, os.toByteArray)
  }
}
