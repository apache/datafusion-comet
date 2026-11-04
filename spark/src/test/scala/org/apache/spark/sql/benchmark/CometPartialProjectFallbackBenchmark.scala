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

import org.apache.comet.CometConf

/**
 * Benchmark to measure the effect of the JVM expression detour (partial project fallback,
 * `spark.comet.exec.jvmDetour.enabled`) on projections and filters that contain an expression
 * Comet cannot translate.
 *
 * Without the detour, one such expression makes the whole operator fall back to Spark: a
 * columnar-to-row transition converts every column the operator reads, and the other projection
 * items run in Spark as well. With the detour, the projection item or filter condition that
 * contains the expression runs with codegen dispatch (Spark's own generated code over the Arrow
 * batch, reading only the columns that item uses), and the operator and the plan around it stay
 * in the Comet pipeline. The detour takes the whole item, not just the expression: in the filter
 * case the whole condition `sqrt(x) > 1.0 AND a > 5.0` runs in the dispatcher.
 *
 * `sqrt` stands in for an expression without a Comet translation: it is named in
 * `spark.comet.testing.unsupportedExpressions` in both Comet cases. (Disabling it with
 * `spark.comet.expression.Sqrt.enabled=false` would not do: that keeps it out of the detour.)
 * Only the detour flag differs between the two Comet cases, so the difference between them
 * measures keeping the operator in the Comet pipeline, not the cost of `sqrt`, which runs in the
 * JVM either way. Using a cheap, registered expression keeps the measurement stable as Comet's
 * expression coverage changes.
 *
 * Five shapes: one detoured item next to six passthrough columns over a native scan (mostly the
 * cost of the transition), a projection of seven Rust-implemented items and one detoured item, a
 * filter whose condition contains the expression, and that projection and filter each feeding a
 * `GROUP BY` aggregate with a shuffle, where the detour also decides where the operators above
 * run. Each shape prints the executed plan's operators with the detour off and on. To run:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometPartialProjectFallbackBenchmark
 * }}}
 */
object CometPartialProjectFallbackBenchmark extends CometBenchmarkBase {

  // Treating `sqrt` as having no translation makes it reach the detour, standing in for any
  // expression without one. Present in BOTH Comet cases so only the detour flag varies.
  private val cometConfigs: Map[String, String] = Map(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true",
    CometConf.COMET_TESTING_UNSUPPORTED_EXPRESSIONS.key -> "Sqrt")

  /**
   * Print the operators of the executed plan with the detour off and on, top first, so the
   * results can say which operators run in Comet and which fall back to Spark, and a
   * misconfigured run is obvious.
   */
  private def reportPlans(query: String): Unit = {
    for (detour <- Seq(false, true)) {
      val configs =
        cometConfigs + (CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.key -> detour.toString)
      withSQLConf(configs.toSeq: _*) {
        val df = spark.sql(query)
        df.noop()
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        val operators = plan.collect { case op => op.nodeName }.mkString(" > ")
        val label = if (detour) "detour on " else "detour off"
        // scalastyle:off println
        println(s"  [$label] $operators")
        if (detour) {
          findFirstNonCometOperator(plan) match {
            case Some(op) =>
              println(s"  [detour on ] NOTE: falls back to Spark at ${op.nodeName}")
            case None =>
              println("  [detour on ] every operator is a Comet operator")
          }
        }
        // scalastyle:on println
      }
    }
  }

  private def benchmarkQuery(name: String, cardinality: Long, query: String): Unit = {
    reportPlans(query)
    val benchmark = new Benchmark(name, cardinality, output = output)

    benchmark.addCase("Spark") { _ =>
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.sql(query).noop()
      }
    }

    benchmark.addCase("Comet (detour off - operator falls back)") { _ =>
      val configs = cometConfigs + (CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.key -> "false")
      withSQLConf(configs.toSeq: _*) {
        spark.sql(query).noop()
      }
    }

    benchmark.addCase("Comet (detour on - operator stays in Comet)") { _ =>
      val configs = cometConfigs + (CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.key -> "true")
      withSQLConf(configs.toSeq: _*) {
        spark.sql(query).noop()
      }
    }

    benchmark.run()
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val rows = 16L * 1024 * 1024

    withTempPath { dir =>
      withTempTable("t") {
        // Wide numeric table: `x` feeds sqrt (treated as having no translation); a..f are
        // passthrough / arithmetic inputs that stay in Arrow batches only when the operator
        // stays in the Comet pipeline.
        spark
          .range(rows)
          .selectExpr(
            "id",
            "cast(id as double) AS x",
            "cast(id % 1000 as double) AS a",
            "cast(id % 997  as double) AS b",
            "cast(id % 991  as double) AS c",
            "cast(id % 977  as double) AS d",
            "cast(id % 100  as double) AS e",
            "cast(id % 7    as double) AS f")
          .write
          .parquet(dir.getAbsolutePath)
        spark.read.parquet(dir.getAbsolutePath).createOrReplaceTempView("t")

        runBenchmark(
          "Partial project fallback - lone unsupported expression over a native scan") {
          // Off: the whole projection (sqrt + six passthrough columns) falls back to Spark, so
          // every column read crosses a columnar-to-row transition. On: the projection stays in
          // Comet and only the `sqrt(x)` item runs in the dispatcher.
          benchmarkQuery(
            "lone unsupported expr, wide passthrough",
            rows,
            "SELECT sqrt(x) AS s, a, b, c, d, e, f FROM t")
        }

        runBenchmark("Partial project fallback - mixed projection with one unsupported item") {
          // Off: all eight items run row by row in Spark. On: the seven arithmetic items are
          // Rust-implemented and only the `sqrt(x)` item runs in the dispatcher.
          benchmarkQuery(
            "mixed projection (7 Rust-implemented + 1 detoured item)",
            rows,
            "SELECT sqrt(x) AS s, a + b AS n1, a * c AS n2, b - d AS n3, c * e AS n4, " +
              "(a + b) * (c - d) AS n5, e + f AS n6, a - e AS n7 FROM t")
        }

        runBenchmark("Partial project fallback - filter with an unsupported subexpression") {
          // Off: the filter falls back to Spark behind a columnar-to-row transition. On: the
          // filter stays in Comet and the whole condition runs in the dispatcher.
          benchmarkQuery(
            "filter predicate with unsupported subexpr",
            rows,
            "SELECT id, x FROM t WHERE sqrt(x) > 1.0 AND a > 5.0")
        }

        runBenchmark("Partial project fallback - detoured projection feeding an aggregate") {
          // What the detour is for: the operators above the projection. `s` is read twice, so
          // `CollapseProject` does not fold the projection into the aggregate, and the projection
          // with the detoured `sqrt(x)` item stays under a hash aggregate and its shuffle.
          benchmarkQuery(
            "projection with a detoured item under GROUP BY",
            rows,
            "SELECT k, sum(s) AS total, max(s) AS top, count(*) AS n FROM " +
              "(SELECT cast(f AS INT) AS k, sqrt(x) AS s FROM t) GROUP BY k")
        }

        runBenchmark("Partial project fallback - detoured filter feeding an aggregate") {
          benchmarkQuery(
            "filter with a detoured condition under GROUP BY",
            rows,
            "SELECT cast(f AS INT) AS k, sum(a) AS total, count(*) AS n FROM t " +
              "WHERE sqrt(x) > 1.0 AND a > 5.0 GROUP BY cast(f AS INT)")
        }
      }
    }
  }
}
