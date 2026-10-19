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

import org.scalatest.exceptions.TestFailedException

import org.apache.spark.sql.CometTestBase

/**
 * The part of the `-0.0` and NaN sweep (#6385) that the SQL file tests cannot express.
 *
 * The sweep itself lives in the `float_semantics` directory of `sql-tests`, where every case must
 * match Spark.
 *
 * A SQL file test has no mode that expects a difference, so it cannot hold a case that is a known
 * gap. This suite holds the one that remains, `collect_set` before Spark 4.2 (#5312), and expects
 * it to return a different answer from Spark. The test fails once the answers match, so that a
 * fix, or a DataFusion upgrade that changes float behavior, removes the test instead of leaving
 * it stale. `aggregates_collect_set_4_2.sql` covers Spark 4.2, where the answers match.
 *
 * The edge values are `0.0`, `-0.0`, a canonical NaN and a NaN with the sign bit set, plus `1.0`,
 * `-1.0`, both infinities and `NULL`. A Parquet round trip through Spark's writer canonicalizes
 * NaN, so the sign-bit NaN is made at query time by negating a stored NaN.
 */
class CometFloatSemanticsSuite extends CometTestBase {

  import CometFloatSemanticsSuite._

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql("CREATE TABLE fs_edge_raw(id INT, d DOUBLE, f FLOAT, neg BOOLEAN) USING parquet")
    sql(s"INSERT INTO fs_edge_raw VALUES ${edgeRows.mkString(", ")}")
    sql("""CREATE TABLE fs_pair_raw USING parquet AS
        |SELECT a.id * 10 + b.id AS id, a.d, a.f, a.neg AS dneg, b.d AS e, b.f AS h, b.neg AS eneg
        |FROM fs_edge_raw a CROSS JOIN fs_edge_raw b""".stripMargin)
  }

  override def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS fs_edge_raw")
      sql("DROP TABLE IF EXISTS fs_pair_raw")
    } finally {
      super.afterAll()
    }
  }

  // Spark 4.2 normalizes the input of collect_set, so Comet matches it there.
  if (!CometSparkSessionExtensions.isSpark42Plus) {
    for ((name, l, r) <- Seq(("double", "d", "e"), ("float", "f", "h"))) {
      test(s"known gap #5312: collect_set before Spark 4.2 [$name]") {
        // Each pair is expanded from one input row, so its two values reach the same partial
        // aggregate in array order.
        val query =
          s"""WITH p AS (
             |  SELECT id, IF(dneg, -$l, $l) AS l, IF(eneg, -$r, $r) AS r FROM fs_pair_raw)
             |SELECT id, size(collect_set(x))
             |FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x)
             |GROUP BY id""".stripMargin
        val matched =
          try {
            checkSparkAnswer(query)
            true
          } catch {
            case e: TestFailedException if e.getMessage.startsWith("Results do not match") =>
              false
          }
        assert(
          !matched,
          "This case now matches Spark. Remove this test, and the MinSparkVersion line in " +
            "aggregates_collect_set_4_2.sql, for https://github.com/apache/datafusion-comet/issues/5312.")
      }
    }
  }
}

object CometFloatSemanticsSuite {

  private val edgeRows = Seq(
    "(1, 0.0D, float(0.0), false)",
    "(2, double('-0.0'), float('-0.0'), false)",
    "(3, double('NaN'), float('NaN'), false)",
    "(4, double('NaN'), float('NaN'), true)",
    "(5, 1.0D, float(1.0), false)",
    "(6, -1.0D, float(-1.0), false)",
    "(7, double('Infinity'), float('Infinity'), false)",
    "(8, double('-Infinity'), float('-Infinity'), false)",
    "(9, NULL, NULL, false)")
}
