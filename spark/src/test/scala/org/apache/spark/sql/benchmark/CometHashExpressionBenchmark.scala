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
          // Data distribution: 1% NULL per column
          // - c_str: unique strings "string_0" through "string_N"
          // - c_int: integers 0-999,999 (cycling)
          // - c_long: large values 0 to ~1 billion
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

    runMurmur3HashBenchmarks(values)
  }

  /**
   * Comprehensive benchmarks for murmur3 hash function across different data types. These
   * benchmarks cover primitive types, complex types (arrays, structs), and nested structures to
   * measure hash performance comprehensively.
   */
  private def runMurmur3HashBenchmarks(values: Int): Unit = {
    // Primitive type benchmarks
    runPrimitiveTypeBenchmarks(values)
    // Complex type benchmarks (arrays, structs)
    runComplexTypeBenchmarks(values)
    // Nested structure benchmarks
    runNestedTypeBenchmarks(values)
  }

  private def runPrimitiveTypeBenchmarks(values: Int): Unit = {
    runBenchmarkWithTable("Murmur3 hash - primitive types", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT
                CASE WHEN value % 100 = 0 THEN NULL ELSE CAST(value % 2 = 0 AS BOOLEAN) END AS c_bool,
                CASE WHEN value % 100 = 1 THEN NULL ELSE CAST(value % 128 AS TINYINT) END AS c_byte,
                CASE WHEN value % 100 = 2 THEN NULL ELSE CAST(value % 32768 AS SMALLINT) END AS c_short,
                CASE WHEN value % 100 = 3 THEN NULL ELSE CAST(value AS INT) END AS c_int,
                CASE WHEN value % 100 = 4 THEN NULL ELSE CAST(value * 1000 AS BIGINT) END AS c_long,
                CASE WHEN value % 100 = 5 THEN NULL ELSE CAST(value * 1.5 AS FLOAT) END AS c_float,
                CASE WHEN value % 100 = 6 THEN NULL ELSE CAST(value * 1.5 AS DOUBLE) END AS c_double,
                CASE WHEN value % 100 = 7 THEN NULL ELSE CONCAT('str_', CAST(value AS STRING)) END AS c_string,
                CASE WHEN value % 100 = 8 THEN NULL ELSE CAST(CONCAT('bin_', CAST(value AS STRING)) AS BINARY) END AS c_binary,
                CASE WHEN value % 100 = 9 THEN NULL ELSE DATE_ADD(DATE '2020-01-01', CAST(value % 1000 AS INT)) END AS c_date,
                CASE WHEN value % 100 = 10 THEN NULL ELSE CAST(value AS DECIMAL(10, 2)) END AS c_decimal
              FROM $tbl
            """))

          val primitiveHashBenchmarks = List(
            HashExprConfig("hash_boolean", "SELECT hash(c_bool) FROM parquetV1Table"),
            HashExprConfig("hash_byte", "SELECT hash(c_byte) FROM parquetV1Table"),
            HashExprConfig("hash_short", "SELECT hash(c_short) FROM parquetV1Table"),
            HashExprConfig("hash_int", "SELECT hash(c_int) FROM parquetV1Table"),
            HashExprConfig("hash_long", "SELECT hash(c_long) FROM parquetV1Table"),
            HashExprConfig("hash_float", "SELECT hash(c_float) FROM parquetV1Table"),
            HashExprConfig("hash_double", "SELECT hash(c_double) FROM parquetV1Table"),
            HashExprConfig("hash_string", "SELECT hash(c_string) FROM parquetV1Table"),
            HashExprConfig("hash_binary", "SELECT hash(c_binary) FROM parquetV1Table"),
            HashExprConfig("hash_date", "SELECT hash(c_date) FROM parquetV1Table"),
            HashExprConfig("hash_decimal", "SELECT hash(c_decimal) FROM parquetV1Table"))

          primitiveHashBenchmarks.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }

  private def runComplexTypeBenchmarks(values: Int): Unit = {
    runBenchmarkWithTable("Murmur3 hash - complex types", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT
                CASE WHEN value % 100 = 0 THEN NULL
                     ELSE array(CAST(value AS INT), CAST(value + 1 AS INT), CAST(value + 2 AS INT))
                END AS c_array_int,
                CASE WHEN value % 100 = 1 THEN NULL
                     ELSE array(CONCAT('s', CAST(value AS STRING)), CONCAT('t', CAST(value AS STRING)))
                END AS c_array_string,
                CASE WHEN value % 100 = 2 THEN NULL
                     ELSE array(CAST(value * 1.1 AS DOUBLE), CAST(value * 2.2 AS DOUBLE))
                END AS c_array_double,
                CASE WHEN value % 100 = 3 THEN NULL
                     ELSE named_struct('a', CAST(value AS INT), 'b', CONCAT('str_', CAST(value AS STRING)))
                END AS c_struct,
                CASE WHEN value % 100 = 4 THEN NULL
                     ELSE named_struct(
                       'x', CAST(value AS INT),
                       'y', CAST(value * 1.5 AS DOUBLE),
                       'z', CAST(value % 2 = 0 AS BOOLEAN)
                     )
                END AS c_struct_multi
              FROM $tbl
            """))

          val complexHashBenchmarks = List(
            HashExprConfig("hash_array_int", "SELECT hash(c_array_int) FROM parquetV1Table"),
            HashExprConfig(
              "hash_array_string",
              "SELECT hash(c_array_string) FROM parquetV1Table"),
            HashExprConfig(
              "hash_array_double",
              "SELECT hash(c_array_double) FROM parquetV1Table"),
            HashExprConfig("hash_struct", "SELECT hash(c_struct) FROM parquetV1Table"),
            HashExprConfig(
              "hash_struct_multi_fields",
              "SELECT hash(c_struct_multi) FROM parquetV1Table"))

          complexHashBenchmarks.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }

  private def runNestedTypeBenchmarks(values: Int): Unit = {
    runBenchmarkWithTable("Murmur3 hash - nested types", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT
                CASE WHEN value % 100 = 0 THEN NULL
                     ELSE array(
                       array(CAST(value AS INT), CAST(value + 1 AS INT)),
                       array(CAST(value + 2 AS INT))
                     )
                END AS c_nested_array,
                CASE WHEN value % 100 = 1 THEN NULL
                     ELSE named_struct(
                       'a', CAST(value AS INT),
                       'b', named_struct('x', CONCAT('s', CAST(value AS STRING)), 'y', CAST(value * 1.5 AS DOUBLE))
                     )
                END AS c_nested_struct,
                CASE WHEN value % 100 = 2 THEN NULL
                     ELSE named_struct(
                       'id', CAST(value AS INT),
                       'items', array(CONCAT('item_', CAST(value AS STRING)), CONCAT('item_', CAST(value + 1 AS STRING)))
                     )
                END AS c_struct_with_array,
                CASE WHEN value % 100 = 3 THEN NULL
                     ELSE array(
                       named_struct('k', CAST(value AS INT), 'v', CONCAT('val_', CAST(value AS STRING))),
                       named_struct('k', CAST(value + 1 AS INT), 'v', CONCAT('val_', CAST(value + 1 AS STRING)))
                     )
                END AS c_array_of_struct
              FROM $tbl
            """))

          val nestedHashBenchmarks = List(
            HashExprConfig(
              "hash_nested_array",
              "SELECT hash(c_nested_array) FROM parquetV1Table"),
            HashExprConfig(
              "hash_nested_struct",
              "SELECT hash(c_nested_struct) FROM parquetV1Table"),
            HashExprConfig(
              "hash_struct_with_array",
              "SELECT hash(c_struct_with_array) FROM parquetV1Table"),
            HashExprConfig(
              "hash_array_of_struct",
              "SELECT hash(c_array_of_struct) FROM parquetV1Table"))

          nestedHashBenchmarks.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
