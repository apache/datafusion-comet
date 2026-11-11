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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, PivotFirst}
import org.apache.spark.sql.comet.CometHashAggregateExec
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec

import org.apache.comet.CometConf

/**
 * Benchmark for the optimized `PIVOT` plan, whose second aggregation uses Spark's internal
 * `PivotFirst` aggregate. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometPivotBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometPivotBenchmark-**results.txt".
 *
 * The optional arguments are comma-separated lists of pivot value counts and of group counts,
 * which default to `4,32,128` and `1000,100000,1000000`:
 * {{{
 *   make benchmark-org.apache.spark.sql.benchmark.CometPivotBenchmark -- 128 1000000
 * }}}
 *
 * Each case pivots `sum(v)` on a string column with `numPivots` values, grouped by a column with
 * `numGroups` values, and compares Spark with two Comet runs: one with `PivotFirst` disabled, so
 * that the aggregates using it fall back to Spark while the rest of the plan stays native, and
 * one with `PivotFirst` native. The query selects every pivot column and writes the rows to the
 * noop sink, so no pivot column can be pruned. Before timing, each Comet run is checked for the
 * plan its name claims and for Spark's answer.
 */
object CometPivotBenchmark extends CometBenchmarkBase {

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    require(mainArgs.length <= 2, "Usage: CometPivotBenchmark [pivot-counts] [group-counts]")
    val pivotCounts = mainArgs.headOption.getOrElse("4,32,128").split(",").map(_.toInt).toSeq
    val groupCounts =
      mainArgs.lift(1).getOrElse("1000,100000,1000000").split(",").map(_.toInt).toSeq
    val numRows = 4L * 1024 * 1024
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val columns = groupCounts.map(g => s"CAST(id % $g AS INT) AS g$g") ++
          pivotCounts.map(p => s"concat('k', CAST(pmod(hash(id), $p) AS STRING)) AS k$p") :+
          "CAST(pmod(hash(id, 17), 1000) AS INT) AS v"
        prepareTable(dir, spark.range(numRows).selectExpr(columns: _*))
        for (numPivots <- pivotCounts; numGroups <- groupCounts) {
          runPivotBenchmark(numRows, numPivots, numGroups)
        }
      }
    }
  }

  private def runPivotBenchmark(numRows: Long, numPivots: Int, numGroups: Int): Unit = {
    val pivotValues = (0 until numPivots).map(i => s"'k$i'").mkString(", ")
    val query =
      s"""SELECT * FROM (SELECT g$numGroups AS grp, k$numPivots AS k, v FROM parquetV1Table)
         |PIVOT (sum(v) FOR k IN ($pivotValues))""".stripMargin
    val benchmark = new Benchmark(
      s"Pivot with $numPivots pivot values and $numGroups groups",
      numRows,
      output = output)

    val sparkConfigs = Seq(CometConf.COMET_ENABLED.key -> "false")
    val nativeConfigs =
      Seq(CometConf.COMET_ENABLED.key -> "true", CometConf.COMET_EXEC_ENABLED.key -> "true")
    val fallbackConfigs =
      nativeConfigs :+ (CometConf.getExprEnabledConfigKey("PivotFirst") -> "false")

    // `withSQLConf` returns Unit on Spark 3.x, so the result leaves the block through a var.
    var expected = 0L
    withSQLConf(sparkConfigs: _*) { expected = checksum(query)._1 }
    verify(benchmark, query, fallbackConfigs, expected, pivotFirstNative = false)
    verify(benchmark, query, nativeConfigs, expected, pivotFirstNative = true)

    benchmark.addCase("Spark") { _ =>
      withSQLConf(sparkConfigs: _*)(spark.sql(query).noop())
    }
    benchmark.addCase("Comet (PivotFirst in Spark)") { _ =>
      withSQLConf(fallbackConfigs: _*)(spark.sql(query).noop())
    }
    benchmark.addCase("Comet (PivotFirst native)") { _ =>
      withSQLConf(nativeConfigs: _*)(spark.sql(query).noop())
    }
    benchmark.run()
  }

  /**
   * An order-independent checksum of every column of the query's result, along with the final
   * plan that computed it.
   */
  private def checksum(query: String) = {
    val df = spark.sql(s"SELECT sum(hash(*)) FROM ($query)")
    // `collect` runs the Dataset's own query execution, so its plan is the final AQE plan.
    val sum = df.collect().head.getLong(0)
    (sum, stripAQEPlan(df.queryExecution.executedPlan))
  }

  private def verify(
      benchmark: Benchmark,
      query: String,
      configs: Seq[(String, String)],
      expected: Long,
      pivotFirstNative: Boolean): Unit = withSQLConf(configs: _*) {
    val (actual, plan) = checksum(query)
    require(actual == expected, s"Comet's pivot result differs from Spark's for:\n$query")
    def hasPivotFirst(aggregates: Seq[AggregateExpression]): Boolean =
      aggregates.exists(_.aggregateFunction.isInstanceOf[PivotFirst])
    val pivotFirstRunsNatively = collect(plan) {
      case agg: CometHashAggregateExec if hasPivotFirst(agg.aggregateExpressions) => true
      case agg: BaseAggregateExec if hasPivotFirst(agg.aggregateExpressions) => false
    }
    val nativeAggregates = collect(plan) { case agg: CometHashAggregateExec => agg }
    if (pivotFirstRunsNatively.isEmpty || pivotFirstRunsNatively.exists(_ != pivotFirstNative) ||
      nativeAggregates.isEmpty) {
      val where = if (pivotFirstNative) "natively" else "in Spark"
      warn(
        benchmark,
        s"""WARNING: expected PivotFirst to run $where and the other aggregates natively, so
           |this case does not measure what its name says.
           |Comet plan:""".stripMargin + "\n" + plan.treeString)
    }
  }
}
