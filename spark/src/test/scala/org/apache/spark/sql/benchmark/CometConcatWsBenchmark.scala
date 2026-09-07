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

import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Matched Parquet inputs for native concat_ws versus Spark whole-stage codegen. Pass `strings` to
 * run only the previously supported string cases against an older native library.
 */
object CometConcatWsBenchmark extends CometBenchmarkBase {
  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val rows = 65536
    val stringsOnly = mainArgs.contains("strings")
    val nativeConfigs = Map(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false")
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE.key -> "8192",
      CometConf.COMET_BATCH_SIZE.key -> "8192") {
      for (width <- Seq(8, 128); arrayLength <- Seq(2, 8, 32)
        if !stringsOnly || arrayLength == 8) {
        withTempPath { dir =>
          withTempTable("concat_inputs") {
            withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
              spark
                .sql(s"""WITH strings AS (
                  | SELECT id, CASE WHEN id % 13 = 0 THEN NULL
                  | ELSE lpad(CAST(id AS STRING), $width, 'x') END AS c1
                  | FROM range($rows)), arrays AS (
                  | SELECT *, CASE WHEN id % 11 = 0 THEN NULL ELSE
                  | transform(sequence(1, CAST(id % $arrayLength + 1 AS INT)),
                  | j -> CASE WHEN j % 5 = 0 THEN NULL
                  | ELSE concat(c1, CAST(j AS STRING)) END) END AS a1
                  | FROM strings)
                  | SELECT c1, a1, reverse(a1) AS a2,
                  | CASE WHEN id % 17 = 0 THEN NULL WHEN id % 2 = 0 THEN '|'
                  | ELSE '--' END AS sep FROM arrays""".stripMargin)
                .coalesce(1)
                .write
                .parquet(dir.getCanonicalPath)
            }
            spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("concat_inputs")
            val expressions =
              (if (arrayLength == 8) Seq("strings" -> "c1, c1") else Seq.empty) ++
                (if (stringsOnly) Seq.empty else Seq("mixed" -> "a1, c1, a2, 'tail'"))
            for ((shape, args) <- expressions; separator <- Seq("' '", "sep")) {
              val query = s"SELECT concat_ws($separator, $args) FROM concat_inputs"
              val name = s"concat_ws $shape width=$width arrayLength=$arrayLength sep=$separator"
              withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
                val plan = spark.sql(query).queryExecution.executedPlan
                require(plan.find(_.isInstanceOf[WholeStageCodegenExec]).nonEmpty)
                require(plan.toString.contains("concat_ws"))
                // scalastyle:off println
                println(s"$name Spark plan:\n$plan")
                // scalastyle:on println
              }
              withSQLConf(nativeConfigs.toSeq: _*) {
                val df = spark.sql(query)
                df.noop()
                val plan = df.queryExecution.executedPlan
                require(findFirstNonCometOperator(plan).isEmpty, plan.toString)
                // scalastyle:off println
                println(s"$name Comet plan (codegen dispatch disabled):\n$plan")
                // scalastyle:on println
              }
              runBenchmark(name) {
                runExpressionBenchmark(name, rows, query, nativeConfigs)
              }
            }
          }
        }
      }
    }
  }
}
