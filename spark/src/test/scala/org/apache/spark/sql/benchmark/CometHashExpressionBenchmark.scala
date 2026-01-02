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

case class HashExprConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Comprehensive benchmark for Comet hash expressions. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometHashExpressionBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometHashExpressionBenchmark-**results.txt".
 */
object CometHashExpressionBenchmark extends CometBenchmarkBase {

  private val hashExpressions = List(
    HashExprConfig("xxhash64_single", "SELECT xxhash64(c_str) FROM parquetV1Table"),
    HashExprConfig("xxhash64_multi", "SELECT xxhash64(c_str, c_int, c_long) FROM parquetV1Table"),
    HashExprConfig("murmur3_hash_single", "SELECT hash(c_str) FROM parquetV1Table"),
    HashExprConfig("murmur3_hash_multi", "SELECT hash(c_str, c_int, c_long) FROM parquetV1Table"),
    HashExprConfig("sha1", "SELECT sha1(c_str) FROM parquetV1Table"),
    HashExprConfig("sha2_224", "SELECT sha2(c_str, 224) FROM parquetV1Table"),
    HashExprConfig("sha2_256", "SELECT sha2(c_str, 256) FROM parquetV1Table"),
    HashExprConfig("sha2_384", "SELECT sha2(c_str, 384) FROM parquetV1Table"),
    HashExprConfig("sha2_512", "SELECT sha2(c_str, 512) FROM parquetV1Table"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024

    runBenchmarkWithTable("Hash expression benchmarks", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT
                CASE WHEN value % 100 = 0 THEN NULL ELSE CONCAT('string_', CAST(value AS STRING)) END AS c_str,
                CASE WHEN value % 100 = 1 THEN NULL ELSE CAST(value % 1000000 AS INT) END AS c_int,
                CASE WHEN value % 100 = 2 THEN NULL ELSE CAST(value * 1000 AS LONG) END AS c_long
              FROM $tbl
            """))

          hashExpressions.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
