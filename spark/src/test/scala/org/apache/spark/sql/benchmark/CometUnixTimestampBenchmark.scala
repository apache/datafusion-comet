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
 * Compares string parsing through codegen dispatch with Spark and native timestamp input.
 * Run with:
 * {{{
 * make benchmark-org.apache.spark.sql.benchmark.CometUnixTimestampBenchmark
 * }}}
 */
object CometUnixTimestampBenchmark extends CometBenchmarkBase {
  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val rows = 1024 * 1024
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark
            .range(rows)
            .selectExpr(
              "timestamp_seconds(id) AS ts",
              "date_format(timestamp_seconds(id), 'yyyy-MM-dd HH:mm:ss') AS s",
              "CASE WHEN id % 2 = 0 THEN 'yyyy-MM-dd HH:mm:ss' " +
                "ELSE 'yyyy-MM-dd H:m:s' END AS fmt"))
        for ((shape, arguments) <- Seq(
            "default format" -> "s",
            "column format" -> "s, fmt",
            "native timestamp" -> "ts")) {
          val name = s"unix_timestamp ($shape)"
          runBenchmark(name) {
            runExpressionBenchmark(
              name,
              rows,
              s"SELECT unix_timestamp($arguments) FROM parquetV1Table")
          }
        }
      }
    }
  }
}
