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

import org.apache.comet.{CometConf, ExtendedExplainInfo}

/**
 * Benchmark to measure Comet execution performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometPredicateExpressionBenchmark` Results will be
 * written to "spark/benchmarks/CometPredicateExpressionBenchmark -**results.txt". Pass `--
 * atleastnnonnulls` to benchmark `DataFrame.na.drop`. Add `--focus-double` for the 32-column,
 * half-threshold, full-consumer 0/5% NaN control, and `--reverse-modes` to reverse mode order in
 * an independent run.
 */
object CometPredicateExpressionBenchmark extends CometBenchmarkBase {

  def inExprBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            "select CASE WHEN value < 0 THEN 'negative'" +
              s" WHEN value = 0 THEN 'zero' ELSE 'positive' END c1 from $tbl"))

        val query = "select * from parquetV1Table where c1 in ('positive', 'zero')"

        runExpressionBenchmark("in Expr", values.toLong, query)
      }
    }
  }

  def atLeastNNonNullsBenchmark(rows: Int, focusDouble: Boolean, reverseModes: Boolean): Unit = {
    for (kind <- (if (focusDouble) Seq("double") else Seq("double", "string"));
      width <- (if (focusDouble) Seq(32) else Seq(4, 32));
      missing <- (if (focusDouble) Seq(0) else Seq(0, 50));
      nanPercent <- (if (kind == "double") Seq(0, 5) else Seq(0))) {
      withTempPath { dir =>
        val fields = (0 until width).map { i =>
          val bucket = s"pmod(xxhash64(id, $i), 100)"
          val nan = if (nanPercent > 0) {
            s"WHEN $bucket < ${missing + nanPercent} THEN cast('NaN' AS DOUBLE) "
          } else ""
          s"CASE WHEN $bucket < $missing THEN NULL " + nan +
            s"ELSE cast(id + $i AS $kind) END AS c$i"
        }
        spark.range(rows.toLong).selectExpr(fields: _*).write.parquet(dir.getCanonicalPath)
        val key = CometConf.getExprEnabledConfigKey("AtLeastNNonNulls")
        val defaultModes = Seq(
          ("Comet native", true, true),
          ("Comet fallback", true, false),
          ("Spark", false, true))
        val modes = if (reverseModes) defaultModes.reverse else defaultModes
        for (threshold <- (if (focusDouble) Seq(width / 2) else Seq(1, width / 2, width));
          consumed <-
            (if (focusDouble) Seq(width)
             else Seq(0, 1, math.min(4, width), width).distinct)) {
          val benchmark = new Benchmark(
            s"na.drop: $width $kind columns, $missing% NULL, $nanPercent% NaN, " +
              s"n=$threshold, consumed=$consumed",
            rows.toLong,
            output = output)
          var expected: Option[Row] = None
          modes.foreach { case (name, comet, native) =>
            def run(verify: Boolean = false): Row = {
              var result = Row.empty
              withSQLConf(
                CometConf.COMET_ENABLED.key -> comet.toString,
                CometConf.COMET_EXEC_ENABLED.key -> comet.toString,
                CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false",
                key -> native.toString) {
                // Compare count-only, narrow and full consumers of the filtered output.
                val sums = (0 until consumed).map { i =>
                  val value =
                    if (kind == "string") s"length(c$i)" else s"if(isnan(c$i), 0D, c$i)"
                  s"sum($value)"
                }
                val df = spark.read
                  .parquet(dir.getCanonicalPath)
                  .na
                  .drop(threshold)
                  .selectExpr((Seq("count(*)") ++ sums): _*)
                result = df.collect().head
                if (verify) {
                  val plan = df.queryExecution.executedPlan
                  val info = new ExtendedExplainInfo()
                  require(
                    info
                      .getNativeExpressions(plan)
                      .contains("atleastnnonnulls") == (comet && native))
                  require(!info.getCodegenDispatchExpressions(plan).contains("atleastnnonnulls"))
                  if (kind == "double" && consumed > 0 && comet && native) {
                    // The cleaned sums must exercise native IF, not silently fall back.
                    require(info.getNativeExpressions(plan).contains("if"))
                    require(!info.getCodegenDispatchExpressions(plan).contains("if"))
                  }
                }
              }
              result
            }
            val result = run(verify = true)
            expected.foreach(value => require(value == result, s"$name: $result != $value"))
            expected = Some(result)
            benchmark.addCase(name)(_ => run())
          }
          benchmark.run()
        }
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024
    if (mainArgs.contains("atleastnnonnulls")) {
      atLeastNNonNullsBenchmark(
        values,
        mainArgs.contains("--focus-double"),
        mainArgs.contains("--reverse-modes"))
      return
    }

    runBenchmarkWithTable("inExpr", values) { v =>
      inExprBenchmark(v)
    }
  }
}
