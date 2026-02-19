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

import java.io.File

import org.apache.spark.benchmark.Benchmark

import org.apache.comet.CometConf

/**
 * Benchmark to measure per-file overhead in Comet Iceberg reads. Creates tables with many small
 * files to amplify operator construction cost. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometIcebergManyFilesBenchmark` Results will be
 * written to "spark/benchmarks/CometIcebergManyFilesBenchmark-**results.txt".
 */
object CometIcebergManyFilesBenchmark extends CometBenchmarkBase {

  private def manyFilesBenchmark(numFiles: Int, rowsPerFile: Int): Unit = {
    val numRows = numFiles * rowsPerFile
    val benchmark = new Benchmark(
      s"Iceberg scan: $numFiles files ($numRows rows, $rowsPerFile rows/file)",
      numRows,
      output = output)

    withTempPath { dir =>
      val warehouseDir = new File(dir, "iceberg-warehouse")
      spark.conf.set("spark.sql.catalog.benchmark_cat", "org.apache.iceberg.spark.SparkCatalog")
      spark.conf.set("spark.sql.catalog.benchmark_cat.type", "hadoop")
      spark.conf.set("spark.sql.catalog.benchmark_cat.warehouse", warehouseDir.getAbsolutePath)

      val tableName = "benchmark_cat.db.many_small"
      spark.sql(s"DROP TABLE IF EXISTS $tableName")

      spark.sql(s"""
        CREATE TABLE $tableName (
          id BIGINT,
          value DOUBLE
        ) USING iceberg
        TBLPROPERTIES (
          'format-version'='2',
          'write.parquet.compression-codec' = 'snappy'
        )
      """)

      // Each repartitioned Spark task writes one file.
      spark
        .range(numRows)
        .selectExpr("id", "CAST(id * 1.5 AS DOUBLE) as value")
        .repartition(numFiles)
        .writeTo(tableName)
        .append()

      val actualFiles = spark
        .sql(s"SELECT COUNT(*) FROM $tableName.files")
        .collect()(0)
        .getLong(0)
      assert(
        actualFiles >= numFiles,
        s"Expected at least $numFiles data files but found $actualFiles")
      // scalastyle:off println
      println(s"Table has $actualFiles data files")
      // scalastyle:on println

      val query = s"SELECT SUM(value) FROM $tableName"

      benchmark.addCase("Spark") { _ =>
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "false",
          "spark.memory.offHeap.enabled" -> "true",
          "spark.memory.offHeap.size" -> "10g") {
          spark.sql(query).noop()
        }
      }

      benchmark.addCase("Comet (Iceberg-Rust)") { _ =>
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
          "spark.memory.offHeap.enabled" -> "true",
          "spark.memory.offHeap.size" -> "10g") {
          spark.sql(query).noop()
        }
      }

      benchmark.run()

      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmark("Iceberg Many Files Scan") {
      manyFilesBenchmark(numFiles = 10000, rowsPerFile = 10)
    }
  }
}
