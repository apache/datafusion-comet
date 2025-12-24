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

case class ArrayExprConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

// spotless:off
/**
 * Comprehensive benchmark for Comet array expressions. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometArrayExpressionBenchmark`
 * Results will be written to "spark/benchmarks/CometArrayExpressionBenchmark-**results.txt".
 */
// spotless:on
object CometArrayExpressionBenchmark extends CometBenchmarkBase {

  private val arrayExpressions = List(
    ArrayExprConfig("array_contains", "SELECT array_contains(arr_int, 5) FROM parquetV1Table"),
    ArrayExprConfig("array_max", "SELECT array_max(arr_int) FROM parquetV1Table"),
    ArrayExprConfig("array_min", "SELECT array_min(arr_int) FROM parquetV1Table"),
    ArrayExprConfig("array_distinct", "SELECT array_distinct(arr_int) FROM parquetV1Table"),
    ArrayExprConfig("array_remove", "SELECT array_remove(arr_int, 5) FROM parquetV1Table"),
    ArrayExprConfig("array_append", "SELECT array_append(arr_int, 100) FROM parquetV1Table"),
    ArrayExprConfig("array_compact", "SELECT array_compact(arr_nullable) FROM parquetV1Table"),
    ArrayExprConfig(
      "array_intersect",
      "SELECT array_intersect(arr_int, arr_int2) FROM parquetV1Table"),
    ArrayExprConfig("array_except", "SELECT array_except(arr_int, arr_int2) FROM parquetV1Table"),
    ArrayExprConfig("array_union", "SELECT array_union(arr_int, arr_int2) FROM parquetV1Table"),
    ArrayExprConfig(
      "arrays_overlap",
      "SELECT arrays_overlap(arr_int, arr_int2) FROM parquetV1Table"),
    ArrayExprConfig("array_insert", "SELECT array_insert(arr_int, 2, 999) FROM parquetV1Table"),
    ArrayExprConfig("array_join", "SELECT array_join(arr_str, ',') FROM parquetV1Table"),
    ArrayExprConfig("array_repeat", "SELECT array_repeat(elem, 5) FROM parquetV1Table"),
    ArrayExprConfig("get_array_item", "SELECT arr_int[0] FROM parquetV1Table"),
    ArrayExprConfig("element_at", "SELECT element_at(arr_int, 1) FROM parquetV1Table"),
    ArrayExprConfig("reverse", "SELECT reverse(arr_int) FROM parquetV1Table"),
    ArrayExprConfig("flatten", "SELECT flatten(nested_arr) FROM parquetV1Table"),
    ArrayExprConfig("size", "SELECT size(arr_int) FROM parquetV1Table"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024 * 2 // 2M rows (arrays are larger)

    runBenchmarkWithTable("Array expression benchmarks", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT
                CASE WHEN value % 50 = 0 THEN NULL
                  ELSE array(
                    cast((value % 10) as int),
                    cast((value % 20) as int),
                    cast((value % 30) as int),
                    cast((value % 10) as int),
                    5, 10, 15)
                END AS arr_int,
                CASE WHEN value % 50 = 1 THEN NULL
                  ELSE array(
                    cast((value % 15) as int),
                    cast((value % 25) as int),
                    cast((value % 35) as int))
                END AS arr_int2,
                CASE WHEN value % 50 = 2 THEN NULL
                  ELSE array(
                    CASE WHEN value % 7 = 0 THEN NULL ELSE cast((value % 10) as int) END,
                    CASE WHEN value % 7 = 1 THEN NULL ELSE cast((value % 20) as int) END,
                    CASE WHEN value % 7 = 2 THEN NULL ELSE cast((value % 30) as int) END)
                END AS arr_nullable,
                CASE WHEN value % 50 = 3 THEN NULL
                  ELSE array(
                    concat('str_', cast(value % 10 as string)),
                    concat('val_', cast(value % 20 as string)),
                    concat('item_', cast(value % 5 as string)))
                END AS arr_str,
                CASE WHEN value % 50 = 4 THEN NULL
                  ELSE array(
                    array(cast((value % 5) as int), cast((value % 10) as int)),
                    array(cast((value % 15) as int), cast((value % 20) as int)))
                END AS nested_arr,
                CASE WHEN value % 50 = 5 THEN NULL ELSE cast((value % 100) as int) END AS elem
              FROM $tbl
            """))

          arrayExpressions.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
