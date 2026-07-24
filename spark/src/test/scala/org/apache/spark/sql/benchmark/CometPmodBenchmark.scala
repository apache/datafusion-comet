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

import org.apache.spark.sql.types._

/**
 * Benchmark to measure the cost of evaluating `pmod` as a filter predicate.
 *
 * The queries take the form `SELECT count(*) FROM t WHERE pmod(c1, k) > n`. Aggregating to a
 * single row keeps the result tiny, so the benchmark measures the scan and predicate evaluation
 * rather than the cost of transferring a large result set back to the JVM over Arrow FFI.
 *
 * To run this benchmark: `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometPmodBenchmark` Results will be written to
 * "spark/benchmarks/CometPmodBenchmark-**results.txt".
 */
object CometPmodBenchmark extends CometBenchmarkBase {
  private val table = "parquetV1Table"

  // A non-zero literal divisor and a threshold that selects part of the [0, divisor) output range.
  private val divisor = 7
  private val threshold = 3

  private def integerPmodBenchmark(
      values: Int,
      dataType: DataType,
      useDictionary: Boolean): Unit = {
    import spark.implicits._
    withTempPath { dir =>
      withTempTable(table) {
        // A small distinct set encourages Parquet dictionary encoding; otherwise use the raw id.
        val col = if (useDictionary) ($"id" % 8) else $"id"
        prepareTable(dir, spark.range(values).select(col.cast(dataType).as("c1")))

        val name = s"pmod(${dataType.sql}), dictionary = $useDictionary"
        val query = s"SELECT count(*) FROM $table WHERE pmod(c1, $divisor) > $threshold"
        runExpressionBenchmark(name, values, query)
      }
    }
  }

  private def decimalPmodBenchmark(
      values: Int,
      dataType: DecimalType,
      useDictionary: Boolean): Unit = {
    import spark.implicits._
    withTempPath { dir =>
      withTempTable(table) {
        // Bounded so the values fit the target precision and scale (no overflow-to-NULL).
        val raw = if (useDictionary) ($"id" % 8) else ($"id" % 100000)
        val col = (raw / 100.0).cast(dataType)
        prepareTable(dir, spark.range(values).select(col.as("c1")))

        val name = s"pmod(${dataType.sql}), dictionary = $useDictionary"
        val query =
          s"SELECT count(*) FROM $table " +
            s"WHERE pmod(c1, CAST($divisor AS ${dataType.sql})) > CAST($threshold AS ${dataType.sql})"
        runExpressionBenchmark(name, values, query)
      }
    }
  }

  private val TOTAL: Int = 1024 * 1024 * 10

  override def runCometBenchmark(args: Array[String]): Unit = {
    Seq(true, false).foreach { useDictionary =>
      runBenchmark("pmod integer filter") {
        integerPmodBenchmark(TOTAL, IntegerType, useDictionary)
      }
      runBenchmark("pmod long filter") {
        integerPmodBenchmark(TOTAL, LongType, useDictionary)
      }
      runBenchmark("pmod double filter") {
        integerPmodBenchmark(TOTAL, DoubleType, useDictionary)
      }
      for ((precision, scale) <- Seq((18, 2), (38, 10))) {
        runBenchmark("pmod decimal filter") {
          decimalPmodBenchmark(TOTAL, DecimalType(precision, scale), useDictionary)
        }
      }
    }
  }
}
