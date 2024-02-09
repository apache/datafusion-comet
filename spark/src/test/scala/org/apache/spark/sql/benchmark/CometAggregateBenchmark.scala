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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DecimalType

import org.apache.comet.CometConf

/**
 * Benchmark to measure Comet execution performance. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometAggregateBenchmark
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometAggregateBenchmark-**results.txt".
 */
object CometAggregateBenchmark extends CometBenchmarkBase {
  override def getSparkSession: SparkSession = {
    val session = super.getSparkSession
    session.conf.set("parquet.enable.dictionary", "false")
    session.conf.set("spark.sql.shuffle.partitions", "2")
    session
  }

  def singleGroupAndAggregate(values: Int, groupingKeyCardinality: Int): Unit = {
    val benchmark =
      new Benchmark(
        s"Grouped HashAgg Exec: single group key (cardinality $groupingKeyCardinality), single aggregate",
        values,
        output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(s"SELECT value, floor(rand() * $groupingKeyCardinality) as key FROM $tbl"))

        val query = "SELECT key, SUM(value) FROM parquetV1Table GROUP BY key"

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

  def singleGroupAndAggregateDecimal(
      values: Int,
      dataType: DecimalType,
      groupingKeyCardinality: Int): Unit = {
    val benchmark =
      new Benchmark(
        s"Grouped HashAgg Exec: single group key (cardinality $groupingKeyCardinality), single aggregate on decimal",
        values,
        output = output)

    val df = makeDecimalDataFrame(values, dataType, false);

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        df.createOrReplaceTempView(tbl)
        prepareTable(
          dir,
          spark.sql(
            s"SELECT dec as value, floor(rand() * $groupingKeyCardinality) as key FROM $tbl"))

        val query = "SELECT key, SUM(value) FROM parquetV1Table GROUP BY key"

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

  def multiGroupKeys(values: Int, groupingKeyCard: Int): Unit = {
    val benchmark =
      new Benchmark(
        s"Grouped HashAgg Exec: multiple group keys (cardinality $groupingKeyCard), single aggregate",
        values,
        output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            s"SELECT value, floor(rand() * $groupingKeyCard) as key1, " +
              s"floor(rand() * $groupingKeyCard) as key2 FROM $tbl"))

        val query = "SELECT key1, key2, SUM(value) FROM parquetV1Table GROUP BY key1, key2"

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql(query).noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_MEMORY_OVERHEAD.key -> "1G") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true",
            CometConf.COMET_MEMORY_OVERHEAD.key -> "1G") {
            spark.sql(query).noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def multiAggregates(values: Int, groupingKeyCard: Int): Unit = {
    val benchmark =
      new Benchmark(
        s"Grouped HashAgg Exec: single group key (cardinality $groupingKeyCard), multiple aggregates",
        values,
        output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            s"SELECT value as value1, value as value2, floor(rand() * $groupingKeyCard) as key " +
              s"FROM $tbl"))

        val query = "SELECT key, SUM(value1), SUM(value2) FROM parquetV1Table GROUP BY key"

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
    val total = 1024 * 1024 * 10
    val combinations = List(100, 1024, 1024 * 1024) // number of distinct groups

    runBenchmarkWithTable("Grouped Aggregate (single group key + single aggregate)", total) { v =>
      for (card <- combinations) {
        singleGroupAndAggregate(v, card)
      }
    }

    runBenchmarkWithTable("Grouped Aggregate (multiple group keys + single aggregate)", total) {
      v =>
        for (card <- combinations) {
          multiGroupKeys(v, card)
        }
    }

    runBenchmarkWithTable("Grouped Aggregate (single group key + multiple aggregates)", total) {
      v =>
        for (card <- combinations) {
          multiAggregates(v, card)
        }
    }

    runBenchmarkWithTable(
      "Grouped Aggregate (single group key + single aggregate on decimal)",
      total) { v =>
      for (card <- combinations) {
        singleGroupAndAggregateDecimal(v, DecimalType(18, 10), card)
      }
    }
  }
}
