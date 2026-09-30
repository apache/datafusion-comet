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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.Row
import org.apache.spark.sql.comet.CometWindowExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/** Compare native sliding SUM with a Spark window over the same Comet input. */
object CometSlidingSumBenchmark extends CometBenchmarkBase {
  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val rows = mainArgs.headOption.map(_.toLong).getOrElse(65536L)
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.ANSI_ENABLED.key -> "true",
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      "spark.comet.operator.WindowExec.allowIncompatible" -> "true") {
      for (nullPercent <- Seq(0, 50, 100)) {
        withTempPath { dir =>
          spark
            .range(rows)
            .selectExpr(
              "id",
              s"CASE WHEN pmod(id, 100) < $nullPercent THEN NULL ELSE id % 17 - 8 END AS v")
            .write
            .parquet(dir.getCanonicalPath)
          withTempTable("sliding_sum_benchmark") {
            spark.read
              .parquet(dir.getCanonicalPath)
              .createOrReplaceTempView("sliding_sum_benchmark")
            for (width <- Seq(16, 1024); function <- Seq("sum", "try_sum")) {
              val query = s"SELECT sum(s), count(s) FROM (SELECT $function(v) OVER " +
                s"(ORDER BY id ROWS BETWEEN ${width - 1} PRECEDING AND CURRENT ROW) AS s " +
                "FROM sliding_sum_benchmark)"
              var expected: Option[Seq[Row]] = None
              def run(native: Boolean, verify: Boolean): Unit = {
                withSQLConf(CometConf.COMET_EXEC_WINDOW_ENABLED.key -> native.toString) {
                  val df = spark.sql(query)
                  if (verify) {
                    assert(
                      df.queryExecution.executedPlan.exists(
                        _.isInstanceOf[CometWindowExec]) == native,
                      df.queryExecution.executedPlan.toString)
                  }
                  val result = df.collect().toSeq
                  if (verify) {
                    expected.foreach(answer => assert(result == answer))
                    expected = Some(result)
                  }
                }
              }
              run(native = false, verify = true)
              run(native = true, verify = true)
              val benchmark = new Benchmark(
                s"$function: frame=$width, NULL=$nullPercent%",
                rows,
                output = output)
              benchmark.addCase("Spark window fallback") { _ =>
                run(native = false, verify = false)
              }
              benchmark.addCase("Comet native window") { _ => run(native = true, verify = false) }
              benchmark.run()
            }
          }
        }
      }
    }
  }
}
