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
 * Compares padding through codegen dispatch with Spark and the existing native argument shape.
 * Run with:
 * {{{
 * make benchmark-org.apache.spark.sql.benchmark.CometPaddingExpressionBenchmark
 * }}}
 */
object CometPaddingExpressionBenchmark extends CometBenchmarkBase {
  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val rows = 1024 * 1024
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.range(rows).selectExpr(
            "CAST(id AS STRING) AS s",
            "CAST(id % 32 + 8 AS INT) AS len",
            "CASE WHEN id % 2 = 0 THEN 'xy' ELSE 'z' END AS pad"))
        for (function <- Seq("lpad", "rpad")) {
          for ((shape, arguments) <- Seq(
              "column padding" -> "s, len, pad",
              "literal string" -> "'hi', len, 'xy'",
              "native" -> "s, len, 'xy'")) {
            val name = s"$function ($shape)"
            runBenchmark(name) {
              runExpressionBenchmark(
                name,
                rows,
                s"SELECT $function($arguments) FROM parquetV1Table")
            }
          }
        }
      }
    }
  }
}
