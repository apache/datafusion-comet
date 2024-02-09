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

import org.apache.spark.benchmark.Benchmark

import org.apache.comet.CometConf

/**
 * Benchmark to measure Comet execution performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometConditionalExpressionBenchmark` Results will be
 * written to "spark/benchmarks/CometConditionalExpressionBenchmark-**results.txt".
 */
object CometConditionalExpressionBenchmark extends CometBenchmarkBase {

  def caseWhenExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Case When Expr", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT value AS c1 FROM $tbl"))

        val query =
          "select CASE WHEN c1 < 0 THEN '<0' WHEN c1 = 0 THEN '=0' ELSE '>0' END from parquetV1Table"

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql(query).noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql(query).noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def ifExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("If Expr", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT value AS c1 FROM $tbl"))
        val query = "select IF (c1 < 0, '<0', '>=0') from parquetV1Table"

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql(query).noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql(query).noop()
          }
        }

        benchmark.run()
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024;

    runBenchmarkWithTable("caseWhenExpr", values) { v =>
      caseWhenExprBenchmark(v)
    }

    runBenchmarkWithTable("ifExpr", values) { v =>
      ifExprBenchmark(v)
    }
  }
}
