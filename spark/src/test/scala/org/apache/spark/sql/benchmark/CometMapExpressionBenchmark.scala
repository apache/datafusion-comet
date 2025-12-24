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

case class MapExprConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

// spotless:off
/**
 * Comprehensive benchmark for Comet map expressions. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometMapExpressionBenchmark`
 * Results will be written to "spark/benchmarks/CometMapExpressionBenchmark-**results.txt".
 */
// spotless:on
object CometMapExpressionBenchmark extends CometBenchmarkBase {

  private val mapExpressions = List(
    MapExprConfig("map_keys", "SELECT map_keys(m) FROM parquetV1Table"),
    MapExprConfig("map_values", "SELECT map_values(m) FROM parquetV1Table"),
    MapExprConfig("map_entries", "SELECT map_entries(m) FROM parquetV1Table"),
    MapExprConfig("map_from_arrays", "SELECT map_from_arrays(keys, values) FROM parquetV1Table"),
    MapExprConfig("element_at_map", "SELECT element_at(m, 'key1') FROM parquetV1Table"),
    MapExprConfig("map_subscript", "SELECT m['key2'] FROM parquetV1Table"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = getBenchmarkRows(1024 * 1024 * 2) // 2M rows default (maps are larger)

    runBenchmarkWithTable("Map expression benchmarks", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT
                CASE WHEN value % 50 = 0 THEN NULL
                  ELSE map(
                    'key1', cast((value % 100) as int),
                    'key2', cast((value % 200) as int),
                    'key3', cast((value % 300) as int))
                END AS m,
                CASE WHEN value % 50 = 1 THEN NULL
                  ELSE array('key1', 'key2', 'key3', 'key4')
                END AS keys,
                CASE WHEN value % 50 = 2 THEN NULL
                  ELSE array(
                    cast((value % 100) as int),
                    cast((value % 200) as int),
                    cast((value % 300) as int),
                    cast((value % 400) as int))
                END AS values
              FROM $tbl
            """))

          mapExpressions.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
