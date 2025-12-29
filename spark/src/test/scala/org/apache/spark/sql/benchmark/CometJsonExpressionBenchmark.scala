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

import org.apache.spark.sql.catalyst.expressions.JsonToStructs

import org.apache.comet.CometConf

/**
 * Configuration for a JSON expression benchmark.
 * @param name
 *   Name for the benchmark
 * @param schema
 *   Target schema for from_json
 * @param query
 *   SQL query to benchmark
 * @param extraCometConfigs
 *   Additional Comet configurations for the scan+exec case
 */
case class JsonExprConfig(
    name: String,
    schema: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

// spotless:off
/**
 * Benchmark to measure performance of Comet JSON expressions. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometJsonExpressionBenchmark`
 * Results will be written to "spark/benchmarks/CometJsonExpressionBenchmark-**results.txt".
 */
// spotless:on
object CometJsonExpressionBenchmark extends CometBenchmarkBase {

  /**
   * Generic method to run a JSON expression benchmark with the given configuration.
   */
  def runJsonExprBenchmark(config: JsonExprConfig, values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        // Generate data with specified JSON patterns
        val jsonData = config.name match {
          case "from_json - simple primitives" =>
            spark.sql(s"""
              SELECT
                concat('{"a":', CAST(value AS STRING), ',"b":"str_', CAST(value AS STRING), '"}') AS json_str
              FROM $tbl
            """)

          case "from_json - all primitive types" =>
            spark.sql(s"""
              SELECT
                concat(
                  '{"i32":', CAST(value % 1000 AS STRING),
                  ',"i64":', CAST(value * 1000000000L AS STRING),
                  ',"f32":', CAST(value * 1.5 AS STRING),
                  ',"f64":', CAST(value * 2.5 AS STRING),
                  ',"bool":', CASE WHEN value % 2 = 0 THEN 'true' ELSE 'false' END,
                  ',"str":"value_', CAST(value AS STRING), '"}'
                ) AS json_str
              FROM $tbl
            """)

          case "from_json - with nulls" =>
            spark.sql(s"""
              SELECT
                CASE
                  WHEN value % 10 = 0 THEN NULL
                  WHEN value % 5 = 0 THEN '{"a":null,"b":"test"}'
                  WHEN value % 3 = 0 THEN '{"a":123}'
                  ELSE concat('{"a":', CAST(value AS STRING), ',"b":"str_', CAST(value AS STRING), '"}')
                END AS json_str
              FROM $tbl
            """)

          case "from_json - nested struct" =>
            spark.sql(s"""
              SELECT
                concat(
                  '{"outer":{"inner_a":', CAST(value AS STRING),
                  ',"inner_b":"nested_', CAST(value AS STRING), '"}}') AS json_str
              FROM $tbl
            """)

          case "from_json - field access" =>
            spark.sql(s"""
              SELECT
                concat('{"a":', CAST(value AS STRING), ',"b":"str_', CAST(value AS STRING), '"}') AS json_str
              FROM $tbl
            """)

          case _ =>
            spark.sql(s"""
              SELECT
                concat('{"a":', CAST(value AS STRING), ',"b":"str_', CAST(value AS STRING), '"}') AS json_str
              FROM $tbl
            """)
        }

        prepareTable(dir, jsonData)

        val extraConfigs = Map(
          CometConf.getExprAllowIncompatConfigKey(
            classOf[JsonToStructs]) -> "true") ++ config.extraCometConfigs

        runExpressionBenchmark(
          config.name,
          values,
          config.query,
          extraConfigs,
          isANSIEnabled = false)
      }
    }
  }

  // Configuration for all JSON expression benchmarks
  private val jsonExpressions = List(
    JsonExprConfig(
      "from_json - simple primitives",
      "a INT, b STRING",
      "SELECT from_json(json_str, 'a INT, b STRING') FROM parquetV1Table"),
    JsonExprConfig(
      "from_json - all primitive types",
      "i32 INT, i64 BIGINT, f32 FLOAT, f64 DOUBLE, bool BOOLEAN, str STRING",
      "SELECT from_json(json_str, 'i32 INT, i64 BIGINT, f32 FLOAT, f64 DOUBLE, bool BOOLEAN, str STRING') FROM parquetV1Table"),
    JsonExprConfig(
      "from_json - with nulls",
      "a INT, b STRING",
      "SELECT from_json(json_str, 'a INT, b STRING') FROM parquetV1Table"),
    JsonExprConfig(
      "from_json - nested struct",
      "outer STRUCT<inner_a: INT, inner_b: STRING>",
      "SELECT from_json(json_str, 'outer STRUCT<inner_a: INT, inner_b: STRING>') FROM parquetV1Table"),
    JsonExprConfig(
      "from_json - field access",
      "a INT, b STRING",
      "SELECT from_json(json_str, 'a INT, b STRING').a FROM parquetV1Table"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024

    jsonExpressions.foreach { config =>
      runBenchmarkWithTable(config.name, values) { v =>
        runJsonExprBenchmark(config, v)
      }
    }
  }
}
