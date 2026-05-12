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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.LongType

import org.apache.comet.udf.CometUdfRegistry
import org.apache.comet.udf.testing.DoubleIntUdf

class CometUserUdfSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override def afterEach(): Unit = {
    CometUdfRegistry.clear()
    super.afterEach()
  }

  private def registerDoubleInt(): Unit = {
    CometUdfRegistry.register(spark, classOf[DoubleIntUdf], (x: Int) => x.toLong * 2L)
  }

  test("user CometUDF - basic integer doubling") {
    registerDoubleInt()
    withTable("t") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (2), (3), (NULL), (100)")
      checkSparkAnswerAndOperator(sql("SELECT double_int(x) FROM t"))
    }
  }

  test("user CometUDF - unregistered UDF falls back to Spark") {
    spark.udf.register("triple_int", (x: Int) => x * 3)

    withTable("t") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (2), (3)")
      checkSparkAnswerAndFallbackReason(
        sql("SELECT triple_int(x) FROM t"),
        "ScalaUDF 'triple_int' is not registered in CometUdfRegistry")
    }
  }

  test("user CometUDF - multiple arguments") {
    registerDoubleInt()
    withTable("t") {
      sql("CREATE TABLE t (x INT, y INT) USING parquet")
      sql("INSERT INTO t VALUES (10, 20), (NULL, 5), (3, NULL)")
      checkSparkAnswerAndOperator(sql("SELECT double_int(x), double_int(y) FROM t"))
    }
  }

  test("user CometUDF - with filter") {
    registerDoubleInt()
    withTable("t") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
      checkSparkAnswerAndOperator(sql("SELECT double_int(x) FROM t WHERE x > 2"))
    }
  }

  test("user CometUDF - columnar-only registration runs natively") {
    CometUdfRegistry.registerColumnarOnly(spark, classOf[DoubleIntUdf])
    withTable("t") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (2), (3), (NULL), (100)")
      // No Spark-side comparison: the synthesized stub intentionally throws when invoked
      // row-at-a-time. With Comet enabled, the query routes to the vectorized implementation.
      val rows = sql("SELECT double_int(x) FROM t ORDER BY x").collect().toSeq.map(_.get(0))
      assert(rows == Seq(null, 2L, 4L, 6L, 200L))
    }
  }

  test("user CometUDF - columnar-only stub raises when Comet is disabled") {
    CometUdfRegistry.registerColumnarOnly(spark, classOf[DoubleIntUdf])
    withTable("t") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (1)")
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val ex = intercept[org.apache.spark.SparkException] {
          sql("SELECT double_int(x) FROM t").collect()
        }
        assert(
          ex.getMessage.contains("columnar-only") ||
            Option(ex.getCause).exists(_.getMessage.contains("columnar-only")))
      }
    }
  }

  test("CometUdfRegistry - register from class") {
    assert(!CometUdfRegistry.isRegistered("double_int"))
    CometUdfRegistry.register(classOf[DoubleIntUdf])
    assert(CometUdfRegistry.isRegistered("double_int"))
    val entry = CometUdfRegistry.get("double_int")
    assert(entry.isDefined)
    assert(entry.get.className == "org.apache.comet.udf.testing.DoubleIntUdf")
    assert(entry.get.returnType == LongType)
    assert(entry.get.nullable)
    CometUdfRegistry.remove("double_int")
    assert(!CometUdfRegistry.isRegistered("double_int"))
  }
}
