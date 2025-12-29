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

import org.apache.spark.sql.catalyst.expressions.{JsonToStructs, StructsToJson}

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

          case "to_json - simple primitives" =>
            spark.sql(
              s"""SELECT named_struct("a", CAST(value AS INT), "b", concat("str_", CAST(value AS STRING))) AS json_struct FROM $tbl""")

          case "to_json - all primitive types" =>
            spark.sql(s"""
              SELECT named_struct(
                "i32", CAST(value % 1000 AS INT),
                "i64", CAST(value * 1000000000L AS LONG),
                "f32", CAST(value * 1.5 AS FLOAT),
                "f64", CAST(value * 2.5 AS DOUBLE),
                "bool", CASE WHEN value % 2 = 0 THEN true ELSE false END,
                "str", concat("value_", CAST(value AS STRING))
              ) AS json_struct FROM $tbl
            """)

          case "to_json - with nulls" =>
            spark.sql(s"""
              SELECT
                CASE
                  WHEN value % 10 = 0 THEN CAST(NULL AS STRUCT<a: INT, b: STRING>)
                  WHEN value % 5 = 0 THEN named_struct("a", CAST(NULL AS INT), "b", "test")
                  WHEN value % 3 = 0 THEN named_struct("a", CAST(123 AS INT), "b", CAST(NULL AS STRING))
                  ELSE named_struct("a", CAST(value AS INT), "b", concat("str_", CAST(value AS STRING)))
                END AS json_struct
              FROM $tbl
            """)

          case "to_json - nested struct" =>
            spark.sql(s"""
              SELECT named_struct(
                "outer", named_struct(
                  "inner_a", CAST(value AS INT),
                  "inner_b", concat("nested_", CAST(value AS STRING))
                )
              ) AS json_struct FROM $tbl
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
          CometConf.getExprAllowIncompatConfigKey(classOf[JsonToStructs]) -> "true",
          CometConf.getExprAllowIncompatConfigKey(
            classOf[StructsToJson]) -> "true") ++ config.extraCometConfigs

        runExpressionBenchmark(config.name, values, config.query, extraConfigs)
      }
    }
  }

  // Configuration for all JSON expression benchmarks
  private val jsonExpressions = List(
    // from_json tests
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
      "SELECT from_json(json_str, 'a INT, b STRING').a FROM parquetV1Table"),

    // to_json tests
    JsonExprConfig(
      "to_json - simple primitives",
      "a INT, b STRING",
      "SELECT to_json(json_struct) FROM parquetV1Table"),
    JsonExprConfig(
      "to_json - all primitive types",
      "i32 INT, i64 BIGINT, f32 FLOAT, f64 DOUBLE, bool BOOLEAN, str STRING",
      "SELECT to_json(json_struct) FROM parquetV1Table"),
    JsonExprConfig(
      "to_json - with nulls",
      "a INT, b STRING",
      "SELECT to_json(json_struct) FROM parquetV1Table"),
    JsonExprConfig(
      "to_json - nested struct",
      "outer STRUCT<inner_a: INT, inner_b: STRING>",
      "SELECT to_json(json_struct) FROM parquetV1Table"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024

    jsonExpressions.foreach { config =>
      runBenchmarkWithTable(config.name, values) { v =>
        runJsonExprBenchmark(config, v)
      }
    }
  }
}
