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

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Benchmark to measure the effect of the partial project fallback / JVM expression detour
 * (`spark.comet.exec.jvmDetour.enabled`) on projections and filters that contain a single
 * expression with no native translation.
 *
 * Without the detour, one unsupported expression falls the whole operator back to Spark, which
 * breaks the native island above the scan: a `ColumnarToRow` transition materializes every read
 * column and the supported sibling expressions run row-wise in Spark WSCG. With the detour, only
 * the unsupported subtree crosses to the JVM (its argument columns over Arrow FFI); the operator
 * — and the pipeline around it — stays native.
 *
 * The unsupported expression is simulated by disabling `sqrt`'s native serde
 * (`spark.comet.expression.Sqrt.enabled=false`) in both Comet cases, so `sqrt` reaches the
 * last-resort detour hook exactly as a genuinely-unsupported expression would. Only the detour
 * flag differs between the two Comet cases, so the delta isolates the island-preservation win
 * rather than the cost of `sqrt` itself (which runs in the JVM either way). Using a cheap,
 * registered expression keeps the measurement stable as Comet's native coverage evolves.
 *
 * Three shapes: a lone unsupported expression over a wide native scan (pure transition cost), a
 * projection mixing many native expressions with one unsupported (the compounding case — native
 * siblings stay native), and a filter whose predicate contains an unsupported subexpression. To
 * run:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometPartialProjectFallbackBenchmark
 * }}}
 */
object CometPartialProjectFallbackBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    // Mirrors CometTestBase's memory/scan setup so native execution is actually active. Without
    // an enabled Comet memory mode (on-heap here) the native runtime is disabled and the whole
    // plan — scan included — falls back to Spark, leaving no native island to preserve. The Spark
    // baseline case turns Comet off per-run via withSQLConf.
    val conf = new SparkConf()
      .setAppName("CometPartialProjectFallbackBenchmark")
      .set("spark.master", "local[5]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "2g")
      .set(CometConf.COMET_ENABLED.key, "true")
      .set(CometConf.COMET_ONHEAP_ENABLED.key, "true")
      .set(CometConf.COMET_EXEC_ENABLED.key, "true")
      .set(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key, "true")
      .set(CometConf.COMET_SPARK_TO_ARROW_ENABLED.key, "true")
      .set(CometConf.COMET_NATIVE_SCAN_ENABLED.key, "true")
      .set(CometConf.COMET_ONHEAP_MEMORY_OVERHEAD.key, "2g")

    SparkSession.builder
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()
  }

  // Disabling `sqrt`'s serde makes it reach the detour hook, standing in for any expression with
  // no native translation. Present in BOTH Comet cases so only the detour flag varies.
  private val cometConfigs: Map[String, String] = Map(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true",
    CometConf.getExprEnabledConfigKey("Sqrt") -> "false")

  /**
   * Print whether the detour-on plan is fully Comet native, so a misconfigured run is obvious.
   */
  private def reportDetourPlan(query: String): Unit = {
    val configs = cometConfigs + (CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.key -> "true")
    withSQLConf(configs.toSeq: _*) {
      val df = spark.sql(query)
      df.noop()
      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      // scalastyle:off println
      findFirstNonCometOperator(plan) match {
        case Some(op) =>
          println(
            s"  [detour on] NOTE: plan not fully Comet native (first: ${op.nodeName}) — " +
              "island not preserved for this shape")
        case None =>
          println("  [detour on] plan is fully Comet native (island preserved)")
      }
      // scalastyle:on println
    }
  }

  private def benchmarkQuery(name: String, cardinality: Long, query: String): Unit = {
    reportDetourPlan(query)
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

    benchmark.addCase("Comet (detour on - island preserved)") { _ =>
      val configs = cometConfigs + (CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.key -> "true")
      withSQLConf(configs.toSeq: _*) {
        spark.sql(query).noop()
      }
    }

    benchmark.run()
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val rows = 16 * 1024 * 1024

    withTempPath { dir =>
      withTempTable("t") {
        // Wide numeric table: `x` feeds the (disabled) sqrt; a..f are native passthrough /
        // arithmetic inputs that stay columnar only when the operator stays native.
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
          // every read column crosses a ColumnarToRow transition. On: native project, sqrt detours.
          benchmarkQuery(
            "lone unsupported expr, wide passthrough",
            rows,
            "SELECT sqrt(x) AS s, a, b, c, d, e, f FROM t")
        }

        runBenchmark("Partial project fallback - mixed native + one unsupported expression") {
          // Off: all eight expressions run row-wise in Spark. On: the seven native arithmetic
          // expressions stay native, only sqrt detours to the JVM kernel.
          benchmarkQuery(
            "mixed projection (7 native + 1 unsupported)",
            rows,
            "SELECT sqrt(x) AS s, a + b AS n1, a * c AS n2, b - d AS n3, c * e AS n4, " +
              "(a + b) * (c - d) AS n5, e + f AS n6, a - e AS n7 FROM t")
        }

        runBenchmark("Partial project fallback - filter with an unsupported subexpression") {
          // Off: the filter falls back, breaking the scan->filter island. On: the filter stays
          // native with sqrt detoured under an otherwise-native predicate.
          benchmarkQuery(
            "filter predicate with unsupported subexpr",
            rows,
            "SELECT id, x FROM t WHERE sqrt(x) > 1.0 AND a > 5.0")
        }
      }
    }
  }
}
