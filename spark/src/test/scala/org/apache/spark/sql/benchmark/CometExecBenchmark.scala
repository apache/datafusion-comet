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

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Benchmark to measure Comet execution performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometExecBenchmark` Results will be written to
 * "spark/benchmarks/CometExecBenchmark-**results.txt".
 */
object CometExecBenchmark extends CometBenchmarkBase {
  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometExecBenchmark")
      .set("spark.master", "local[5]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set("spark.executor.memoryOverhead", "10g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .set("spark.comet.columnar.shuffle.async.thread.num", "7")
      .set("spark.comet.columnar.shuffle.spill.threshold", "30000")

    val sparkSession = SparkSession.builder
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    // Set default configs. Individual cases will change them if necessary.
    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_MEMORY_OVERHEAD.key, "10g")
    sparkSession.conf.set(CometConf.COMET_COLUMNAR_SHUFFLE_MEMORY_SIZE.key, "10g")
    // TODO: support dictionary encoding in vectorized execution
    sparkSession.conf.set("parquet.enable.dictionary", "false")
    sparkSession.conf.set("spark.sql.shuffle.partitions", "2")

    sparkSession
  }

  def numericFilterExecBenchmark(values: Int, fractionOfZeros: Double): Unit = {
    val percentageOfZeros = fractionOfZeros * 100
    val benchmark =
      new Benchmark(s"Project + Filter Exec ($percentageOfZeros% zeros)", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            s"SELECT IF(RAND(1) < $fractionOfZeros, -1, value) AS c1, value AS c2 FROM " +
              s"$tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select c2 + 1, c1 + 2 from parquetV1Table where c1 + 1 > 0").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select c2 + 1, c1 + 2 from parquetV1Table where c1 + 1 > 0").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true") {
            spark.sql("select c2 + 1, c1 + 2 from parquetV1Table where c1 + 1 > 0").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Spark (Scan), Comet (Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
            spark.sql("select c2 + 1, c1 + 2 from parquetV1Table where c1 + 1 > 0").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def subqueryExecBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Subquery", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(s"SELECT value as col1, value + 100 as col2, value + 10 as col3 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql(
            "SELECT (SELECT max(col1) AS parquetV1Table FROM parquetV1Table) AS a, " +
              "col2, col3 FROM parquetV1Table")
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql(
              "SELECT (SELECT max(col1) AS parquetV1Table FROM parquetV1Table) AS a, " +
                "col2, col3 FROM parquetV1Table")
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
            spark.sql(
              "SELECT (SELECT max(col1) AS parquetV1Table FROM parquetV1Table) AS a, " +
                "col2, col3 FROM parquetV1Table")
          }
        }

        benchmark.run()
      }
    }
  }

  def sortExecBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Sort Exec", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT * FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select * from parquetV1Table").sortWithinPartitions("value").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select * from parquetV1Table").sortWithinPartitions("value").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true") {
            spark.sql("select * from parquetV1Table").sortWithinPartitions("value").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def expandExecBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expand Exec", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(s"SELECT value as col1, value + 100 as col2, value + 10 as col3 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark
            .sql("SELECT col1, col2, SUM(col3) FROM parquetV1Table " +
              "GROUP BY col1, col2 GROUPING SETS ((col1), (col2))")
            .noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark
              .sql("SELECT col1, col2, SUM(col3) FROM parquetV1Table " +
                "GROUP BY col1, col2 GROUPING SETS ((col1), (col2))")
              .noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true") {
            spark
              .sql("SELECT col1, col2, SUM(col3) FROM parquetV1Table " +
                "GROUP BY col1, col2 GROUPING SETS ((col1), (col2))")
              .noop()
          }
        }

        benchmark.run()
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("Subquery", 1024 * 1024 * 10) { v =>
      subqueryExecBenchmark(v)
    }

    runBenchmarkWithTable("Expand", 1024 * 1024 * 10) { v =>
      expandExecBenchmark(v)
    }

    runBenchmarkWithTable("Project + Filter", 1024 * 1024 * 10) { v =>
      for (fractionOfZeros <- List(0.0, 0.50, 0.95)) {
        numericFilterExecBenchmark(v, fractionOfZeros)
      }
    }

    runBenchmarkWithTable("Sort", 1024 * 1024 * 10) { v =>
      sortExecBenchmark(v)
    }
  }
}
