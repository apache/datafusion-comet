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
import org.apache.comet.CometConf.{SCAN_NATIVE_COMET, SCAN_NATIVE_ICEBERG_COMPAT}

/**
 * Benchmark to measure partition column scan performance. This exercises the CometConstantVector
 * path where constant columns are exported as 1-element Arrow arrays and expanded on the native
 * side.
 *
 * To run this benchmark:
 * {{{
 * SPARK_GENERATE_BENCHMARK_FILES=1 make \
 *   benchmark-org.apache.spark.sql.benchmark.CometPartitionColumnBenchmark
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometPartitionColumnBenchmark-**results.txt".
 */
object CometPartitionColumnBenchmark extends CometBenchmarkBase {

  def partitionColumnScanBenchmark(values: Int, numPartitionCols: Int): Unit = {
    val sqlBenchmark = new Benchmark(
      s"Partitioned Scan with $numPartitionCols partition column(s)",
      values,
      output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val partCols =
          (1 to numPartitionCols).map(i => s"'part$i' as p$i").mkString(", ")
        val partNames = (1 to numPartitionCols).map(i => s"p$i").mkString(", ")
        prepareTable(dir, spark.sql(s"SELECT value as id, $partCols FROM $tbl"), Some(partNames))

        sqlBenchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select sum(id) from parquetV1Table").noop()
        }

        sqlBenchmark.addCase("SQL Parquet - Comet (Scan Only)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_COMET) {
            spark.sql("select sum(id) from parquetV1Table").noop()
          }
        }

        sqlBenchmark.addCase("SQL Parquet - Comet (Scan + Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_ICEBERG_COMPAT) {
            spark.sql("select sum(id) from parquetV1Table").noop()
          }
        }

        // Also benchmark reading partition columns themselves
        val partSumExpr =
          (1 to numPartitionCols).map(i => s"sum(length(p$i))").mkString(", ")

        sqlBenchmark.addCase("SQL Parquet - Spark (read partition cols)") { _ =>
          spark.sql(s"select $partSumExpr from parquetV1Table").noop()
        }

        sqlBenchmark.addCase("SQL Parquet - Comet (read partition cols)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_ICEBERG_COMPAT) {
            spark.sql(s"select $partSumExpr from parquetV1Table").noop()
          }
        }

        sqlBenchmark.run()
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("Partitioned Column Scan", 1024 * 1024 * 15) { v =>
      for (numPartCols <- List(1, 5)) {
        partitionColumnScanBenchmark(v, numPartCols)
      }
    }
  }
}
