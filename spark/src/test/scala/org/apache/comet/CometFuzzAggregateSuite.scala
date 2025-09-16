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

class CometFuzzAggregateSuite extends CometFuzzTestBase {

  test("count distinct") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.columns) {
      val sql = s"SELECT count(distinct $col) FROM t1"
      val (_, cometPlan) = checkSparkAnswer(sql)
      if (usingDataSourceExec) {
        assert(1 == collectNativeScans(cometPlan).length)
      }
    }
  }

  test("count(*) group by single column") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.columns) {
      // cannot run fully natively due to range partitioning and sort
      val sql = s"SELECT $col, count(*) FROM t1 GROUP BY $col ORDER BY $col"
      val (_, cometPlan) = checkSparkAnswer(sql)
      if (usingDataSourceExec) {
        assert(1 == collectNativeScans(cometPlan).length)
      }
    }
  }

  test("count(col) group by single column") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    val groupCol = df.columns.head
    for (col <- df.columns.drop(1)) {
      // cannot run fully natively due to range partitioning and sort
      val sql = s"SELECT $groupCol, count($col) FROM t1 GROUP BY $groupCol ORDER BY $groupCol"
      val (_, cometPlan) = checkSparkAnswer(sql)
      if (usingDataSourceExec) {
        assert(1 == collectNativeScans(cometPlan).length)
      }
    }
  }

  test("count(col1, col2, ..) group by single column") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    val groupCol = df.columns.head
    val otherCol = df.columns.drop(1)
    // cannot run fully natively due to range partitioning and sort
    val sql = s"SELECT $groupCol, count(${otherCol.mkString(", ")}) FROM t1 " +
      s"GROUP BY $groupCol ORDER BY $groupCol"
    val (_, cometPlan) = checkSparkAnswer(sql)
    if (usingDataSourceExec) {
      assert(1 == collectNativeScans(cometPlan).length)
    }
  }

  test("min/max aggregate") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.columns) {
      // cannot run fully native due to HashAggregate
      val sql = s"SELECT min($col), max($col) FROM t1"
      val (_, cometPlan) = checkSparkAnswer(sql)
      if (usingDataSourceExec) {
        assert(1 == collectNativeScans(cometPlan).length)
      }
    }
  }

}
