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

import org.apache.comet.iceberg.IcebergReflection

/**
 * Benchmark of Iceberg's system functions (`bucket`, `truncate`, `years`, `months`, `days`,
 * `hours`) with Comet on and off. The Spark case is Iceberg's own JVM implementation: Spark binds
 * each function as a `StaticInvoke` of the matching class under
 * `org.apache.iceberg.spark.functions` and whole-stage codegen calls it once per row.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometIcebergSystemFunctionBenchmark
 * }}}
 * Results will be written to
 * "spark/benchmarks/CometIcebergSystemFunctionBenchmark-**results.txt".
 */
object CometIcebergSystemFunctionBenchmark extends CometBenchmarkBase {

  private val catalog = "benchmark_cat"

  /**
   * One case per (transform, input type). `c_str_dict` holds eight distinct values so Parquet
   * dictionary-encodes it, which is the shape a string partition column normally arrives in;
   * `c_str` is distinct per row.
   */
  private def cases: Seq[(String, String)] = {
    val bucket = Seq("c_int", "c_long", "c_dec", "c_str_dict", "c_str", "c_bin", "c_date", "c_ts")
      .map(column => s"bucket($column)" -> s"select $catalog.system.bucket(16, $column)")
    val truncate = Seq("c_int", "c_long", "c_dec", "c_str_dict", "c_str", "c_bin")
      .map(column => s"truncate($column)" -> s"select $catalog.system.truncate(4, $column)")
    val temporal = Seq("years", "months", "days").flatMap { fn =>
      Seq("c_date", "c_ts").map(column =>
        s"$fn($column)" -> s"select $catalog.system.$fn($column)")
    } :+ ("hours(c_ts)" -> s"select $catalog.system.hours(c_ts)")
    (bucket ++ truncate ++ temporal).map { case (name, select) =>
      name -> s"$select from parquetV1Table"
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    if (!icebergOnClasspath) {
      // scalastyle:off println
      println("Iceberg is not on the classpath; skipping. Build with an Iceberg-enabled profile.")
      // scalastyle:on println
      return
    }
    // The Iceberg system functions are resolved through a v2 catalog, so one has to be
    // registered. No Iceberg table is read: the data stays in Parquet so both cases scan
    // identically and the only difference is who evaluates the transform.
    withTempPath { warehouse =>
      spark.conf.set(s"spark.sql.catalog.$catalog", "org.apache.iceberg.spark.SparkCatalog")
      spark.conf.set(s"spark.sql.catalog.$catalog.type", "hadoop")
      spark.conf.set(s"spark.sql.catalog.$catalog.warehouse", warehouse.getAbsolutePath)

      runBenchmarkWithTable("Iceberg system functions", 1024 * 1024) { v =>
        withTempPath { dir =>
          withTempTable("parquetV1Table") {
            prepareTable(
              dir,
              spark.sql(s"""
                SELECT CAST(value AS INT) AS c_int,
                       value AS c_long,
                       CAST(value AS DECIMAL(38,10)) AS c_dec,
                       CAST(PMOD(value, 8) AS STRING) AS c_str_dict,
                       REPEAT(CAST(value AS STRING), 3) AS c_str,
                       CAST(CAST(value AS STRING) AS BINARY) AS c_bin,
                       DATE_ADD(DATE '1970-01-01', CAST(PMOD(value, 40000) AS INT)) AS c_date,
                       TIMESTAMP_SECONDS(PMOD(value, 4000000000)) AS c_ts
                FROM $tbl"""))

            cases.foreach { case (name, query) =>
              runBenchmark(name) {
                runExpressionBenchmark(name, v, query)
              }
            }
          }
        }
      }
    }
  }

  private def icebergOnClasspath: Boolean =
    try {
      IcebergReflection.loadClass("org.apache.iceberg.spark.functions.BucketFunction")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
}
