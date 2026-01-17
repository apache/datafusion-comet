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
 * Configuration for an encryption expression benchmark.
 * @param name
 *   Name for the benchmark
 * @param query
 *   SQL query to benchmark
 * @param extraCometConfigs
 *   Additional Comet configurations for the scan+exec case
 */
case class EncryptionExprConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Benchmark to measure performance of Comet encryption expressions. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometEncryptionBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometEncryptionBenchmark-**results.txt".
 */
object CometEncryptionBenchmark extends CometBenchmarkBase {

  private val encryptionExpressions = List(
    EncryptionExprConfig(
      "aes_encrypt_gcm_basic",
      "select hex(aes_encrypt(data, key)) from parquetV1Table"),
    EncryptionExprConfig(
      "aes_encrypt_gcm_with_mode",
      "select hex(aes_encrypt(data, key, 'GCM')) from parquetV1Table"),
    EncryptionExprConfig(
      "aes_encrypt_cbc",
      "select hex(aes_encrypt(data, key, 'CBC', 'PKCS')) from parquetV1Table"),
    EncryptionExprConfig(
      "aes_encrypt_ecb",
      "select hex(aes_encrypt(data, key, 'ECB', 'PKCS')) from parquetV1Table"),
    EncryptionExprConfig(
      "aes_encrypt_gcm_with_iv",
      "select hex(aes_encrypt(data, key, 'GCM', 'DEFAULT', iv)) from parquetV1Table"),
    EncryptionExprConfig(
      "aes_encrypt_gcm_with_aad",
      "select hex(aes_encrypt(data, key, 'GCM', 'DEFAULT', iv, aad)) from parquetV1Table"),
    EncryptionExprConfig(
      "aes_encrypt_with_base64",
      "select base64(aes_encrypt(data, key)) from parquetV1Table"),
    EncryptionExprConfig(
      "aes_encrypt_long_data",
      "select hex(aes_encrypt(long_data, key)) from parquetV1Table"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("Encryption expressions", 100000) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT
                CAST(REPEAT(CAST(value AS STRING), 2) AS BINARY) AS data,
                CAST('0000111122223333' AS BINARY) AS key,
                CAST(unhex('000000000000000000000000') AS BINARY) AS iv,
                CAST('This is AAD data' AS BINARY) AS aad,
                CAST(REPEAT(CAST(value AS STRING), 100) AS BINARY) AS long_data
              FROM $tbl
            """))

          encryptionExpressions.foreach { config =>
            runBenchmark(config.name) {
              runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
            }
          }
        }
      }
    }
  }
}
