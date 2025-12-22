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
 * Benchmark to measure Comet execution performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometPredicateExpressionBenchmark` Results will be
 * written to "spark/benchmarks/CometPredicateExpressionBenchmark -**results.txt".
 */
object CometPredicateExpressionBenchmark extends CometBenchmarkBase {

  def inExprBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            "select CASE WHEN value < 0 THEN 'negative'" +
              s" WHEN value = 0 THEN 'zero' ELSE 'positive' END c1 from $tbl"))

        val query = "select * from parquetV1Table where c1 in ('positive', 'zero')"

        runExpressionBenchmark("in Expr", values, query)
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024;

    runBenchmarkWithTable("inExpr", values) { v =>
      inExprBenchmark(v)
    }
  }
}
