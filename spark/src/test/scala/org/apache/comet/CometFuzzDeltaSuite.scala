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

import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.types._

import org.apache.comet.DataTypeSupport.isComplexType

/**
 * Fuzz/property-based testing suite for the native Delta scan path. Generates a randomized
 * schema, writes a Delta table, then runs a battery of SQL queries to ensure Comet's native path
 * produces results identical to vanilla Spark+Delta.
 *
 * Mirrors CometFuzzIcebergSuite.
 */
class CometFuzzDeltaSuite extends CometFuzzDeltaBase {

  test("select *") {
    val sql = s"SELECT * FROM $deltaTableName"
    val (_, cometPlan) = checkSparkAnswer(sql)
    assert(1 == collectDeltaNativeScans(cometPlan).length)
  }

  test("select * with limit") {
    val sql = s"SELECT * FROM $deltaTableName LIMIT 500"
    val (_, cometPlan) = checkSparkAnswer(sql)
    assert(1 == collectDeltaNativeScans(cometPlan).length)
  }

  test("order by single column") {
    val df = spark.table(deltaTableName)
    for (col <- df.columns) {
      val sql = s"SELECT $col FROM $deltaTableName ORDER BY $col"
      val (_, cometPlan) = checkSparkAnswer(sql)
      assert(1 == collectDeltaNativeScans(cometPlan).length)
    }
  }

  test("order by multiple columns") {
    val df = spark.table(deltaTableName)
    val allCols = df.columns.mkString(",")
    val sql = s"SELECT $allCols FROM $deltaTableName ORDER BY $allCols"
    val (_, cometPlan) = checkSparkAnswer(sql)
    assert(1 == collectDeltaNativeScans(cometPlan).length)
  }

  test("order by random columns") {
    val df = spark.table(deltaTableName)
    for (_ <- 1 to 10) {
      val primitiveColumns =
        df.schema.fields.filterNot(f => isComplexType(f.dataType)).map(_.name)
      val shuffledPrimitiveCols = Random.shuffle(primitiveColumns.toList)
      val randomSize = Random.nextInt(shuffledPrimitiveCols.length) + 1
      val randomColsSubset = shuffledPrimitiveCols.take(randomSize).toArray.mkString(",")
      val sql = s"SELECT $randomColsSubset FROM $deltaTableName ORDER BY $randomColsSubset"
      checkSparkAnswerAndOperator(sql)
    }
  }

  test("shuffle supports all types") {
    val df = spark.table(deltaTableName)
    // Sort by all primitive columns to make the ordering fully deterministic.
    // Sorting by c1 alone is non-deterministic for rows with equal c1 values
    // (1000 rows with only 256 unique tinyint values = many ties).
    val sortCols = df.schema.fields
      .filterNot(f => DataTypeSupport.isComplexType(f.dataType))
      .map(f => df.col(f.name))
    val df2 = df.repartition(8, df.col("c0")).sort(sortCols: _*)
    checkSparkAnswer(df2)
  }

  test("join with filter on primitives") {
    val df = spark.table(deltaTableName)
    val primitiveColumns =
      df.schema.fields.filterNot(f => isComplexType(f.dataType)).map(_.name)
    if (primitiveColumns.nonEmpty) {
      val joinCol = primitiveColumns.head
      val sql = s"""
        SELECT a.$joinCol FROM $deltaTableName a
        JOIN $deltaTableName b ON a.$joinCol = b.$joinCol
      """
      checkSparkAnswer(sql)
    }
  }
}
