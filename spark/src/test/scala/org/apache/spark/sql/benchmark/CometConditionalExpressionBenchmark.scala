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

package org.apache.spark.sql.benchmark

/**
 * Benchmark to measure Comet execution performance for conditional expressions. To run this
 * benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometConditionalExpressionBenchmark
 * }}}
 * Results will be written to
 * "spark/benchmarks/CometConditionalExpressionBenchmark-**results.txt".
 */
object CometConditionalExpressionBenchmark extends CometBenchmarkBase {

  private def prepareTestTable(values: Int)(f: => Unit): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        // Create table with multiple columns for richer test scenarios:
        // - c1: random long values (full range)
        // - c2: values 0-99 for multi-branch testing
        // - c3: secondary column for non-literal result expressions
        // - c4: string column for string result expressions
        prepareTable(
          dir,
          spark.sql(s"""
            SELECT
              value AS c1,
              CAST(ABS(value % 100) AS INT) AS c2,
              CAST(value * 2 AS LONG) AS c3,
              CAST(value AS STRING) AS c4
            FROM $tbl
          """))
        f
      }
    }
  }

  def caseWhenLiteralBenchmark(values: Int): Unit = {
    prepareTestTable(values) {
      val query =
        "SELECT CASE WHEN c1 < 0 THEN '<0' WHEN c1 = 0 THEN '=0' ELSE '>0' END FROM parquetV1Table"
      runExpressionBenchmark("Case When Literal (3 branches)", values, query)
    }
  }

  def caseWhenManyBranchesLiteralBenchmark(values: Int): Unit = {
    prepareTestTable(values) {
      // 10 branches using c2 (values 0-99)
      val query = """
        SELECT CASE
          WHEN c2 < 10 THEN 'a'
          WHEN c2 < 20 THEN 'b'
          WHEN c2 < 30 THEN 'c'
          WHEN c2 < 40 THEN 'd'
          WHEN c2 < 50 THEN 'e'
          WHEN c2 < 60 THEN 'f'
          WHEN c2 < 70 THEN 'g'
          WHEN c2 < 80 THEN 'h'
          WHEN c2 < 90 THEN 'i'
          ELSE 'j'
        END FROM parquetV1Table
      """
      runExpressionBenchmark("Case When Literal (10 branches)", values, query)
    }
  }

  def caseWhenColumnResultBenchmark(values: Int): Unit = {
    prepareTestTable(values) {
      // Result expressions are column references, not literals
      val query =
        "SELECT CASE WHEN c1 < 0 THEN c3 WHEN c1 = 0 THEN c1 ELSE c3 + c1 END FROM parquetV1Table"
      runExpressionBenchmark("Case When Column Result (3 branches)", values, query)
    }
  }

  def caseWhenManyBranchesColumnResultBenchmark(values: Int): Unit = {
    prepareTestTable(values) {
      // 10 branches with column expressions as results
      val query = """
        SELECT CASE
          WHEN c2 < 10 THEN c1
          WHEN c2 < 20 THEN c3
          WHEN c2 < 30 THEN c1 + c3
          WHEN c2 < 40 THEN c1 - c2
          WHEN c2 < 50 THEN c3 * 2
          WHEN c2 < 60 THEN c1 / 2
          WHEN c2 < 70 THEN c2 + c3
          WHEN c2 < 80 THEN c1 * c2
          WHEN c2 < 90 THEN c3 - c1
          ELSE c1 + c2 + c3
        END FROM parquetV1Table
      """
      runExpressionBenchmark("Case When Column Result (10 branches)", values, query)
    }
  }

  def ifLiteralBenchmark(values: Int): Unit = {
    prepareTestTable(values) {
      val query = "SELECT IF(c1 < 0, '<0', '>=0') FROM parquetV1Table"
      runExpressionBenchmark("If Literal", values, query)
    }
  }

  def ifColumnResultBenchmark(values: Int): Unit = {
    prepareTestTable(values) {
      // Result expressions are column references
      val query = "SELECT IF(c1 < 0, c3, c1 + c3) FROM parquetV1Table"
      runExpressionBenchmark("If Column Result", values, query)
    }
  }

  def nestedIfBenchmark(values: Int): Unit = {
    prepareTestTable(values) {
      // Nested IF expressions (equivalent to CASE WHEN with multiple branches)
      val query = """
        SELECT IF(c2 < 25, 'a',
                 IF(c2 < 50, 'b',
                   IF(c2 < 75, 'c', 'd')))
        FROM parquetV1Table
      """
      runExpressionBenchmark("Nested If Literal (4 outcomes)", values, query)
    }
  }

  def nestedIfColumnResultBenchmark(values: Int): Unit = {
    prepareTestTable(values) {
      val query = """
        SELECT IF(c2 < 25, c1,
                 IF(c2 < 50, c3,
                   IF(c2 < 75, c1 + c3, c3 * 2)))
        FROM parquetV1Table
      """
      runExpressionBenchmark("Nested If Column Result (4 outcomes)", values, query)
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024

    // CASE WHEN with literal results
    runBenchmarkWithTable("caseWhenLiteral", values) { v =>
      caseWhenLiteralBenchmark(v)
    }

    runBenchmarkWithTable("caseWhenManyBranchesLiteral", values) { v =>
      caseWhenManyBranchesLiteralBenchmark(v)
    }

    // CASE WHEN with column/expression results
    runBenchmarkWithTable("caseWhenColumnResult", values) { v =>
      caseWhenColumnResultBenchmark(v)
    }

    runBenchmarkWithTable("caseWhenManyBranchesColumnResult", values) { v =>
      caseWhenManyBranchesColumnResultBenchmark(v)
    }

    // IF with literal results
    runBenchmarkWithTable("ifLiteral", values) { v =>
      ifLiteralBenchmark(v)
    }

    // IF with column/expression results
    runBenchmarkWithTable("ifColumnResult", values) { v =>
      ifColumnResultBenchmark(v)
    }

    // Nested IF expressions
    runBenchmarkWithTable("nestedIfLiteral", values) { v =>
      nestedIfBenchmark(v)
    }

    runBenchmarkWithTable("nestedIfColumnResult", values) { v =>
      nestedIfColumnResultBenchmark(v)
    }
  }
}
