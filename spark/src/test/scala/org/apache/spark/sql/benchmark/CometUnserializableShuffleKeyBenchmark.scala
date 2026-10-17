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
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * What a range-partitioned exchange costs when its sort key has no Comet serde, which is the
 * shape #5971 moves from Spark's shuffle to Comet's JVM columnar shuffle.
 *
 * The operator above such an exchange serializes the same key and fails the same check, so it
 * stays on Spark. The exchange therefore sits between two Spark operators, and the columnar path
 * pulls its rows through `ColumnarToRowExec`, converts them to Arrow, and hands them back as
 * rows. This measures whether that round trip costs anything.
 *
 * Turning the codegen dispatcher off is only a stable way to obtain a key with no serde; a Hive
 * UDF behaves the same.
 *
 * The baseline case switches Comet shuffle off, which reproduces the plan this query had before
 * #5971: a plain Spark `Exchange` with the rest of the plan unchanged. Comparing the two inside
 * one build isolates the exchange, where comparing two branches would also carry every unrelated
 * commit between them.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometUnserializableShuffleKeyBenchmark
 * }}}
 * Results will be written to
 * "spark/benchmarks/CometUnserializableShuffleKeyBenchmark-**results.txt".
 */
object CometUnserializableShuffleKeyBenchmark extends CometBenchmarkBase {

  private val Rows = 8L * 1024 * 1024

  /** Range partitioning, as in the reported shape, and hash partitioning for comparison. */
  private val rangeQuery = "SELECT * FROM parquetV1Table ORDER BY bump(a), b"
  private val hashQuery =
    "SELECT max(b) FROM parquetV1Table GROUP BY bump(a) ORDER BY 1 LIMIT 10"

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    spark.udf.register("bump", (x: Int) => x + 1)

    withTempPath { dir =>
      withTempTable(tbl, "parquetV1Table") {
        spark.range(Rows).createOrReplaceTempView(tbl)
        prepareTable(
          dir,
          spark.sql(
            s"SELECT CAST(id % 1000003 AS INT) AS a, CAST(id AS INT) AS b, " +
              s"CAST(id AS STRING) AS c FROM $tbl"))

        checkPlans()
        run("range partitioning on a key with no serde", rangeQuery)
        run("hash partitioning on a key with no serde", hashQuery)
      }
    }
  }

  /**
   * `cometShuffle = false` is the pre-#5971 plan for these queries: the serde probe rejected the
   * exchange, so it ran on Spark's shuffle. `cometChild = false` covers the other child shape,
   * where no Comet operator sits on either side of the exchange and the columnar path is pure
   * overhead. The columnar path takes a child that is row-based or a Comet operator, so that case
   * turns off Spark's vectorized reader as well; both of its arms pay for the row-based scan, so
   * the difference between them is still the exchange alone.
   */
  private case class Case(label: String, cometShuffle: Boolean, cometChild: Boolean)

  private val cases = Seq(
    Case("Spark shuffle, Comet scan (pre-#5971)", cometShuffle = false, cometChild = true),
    Case("Comet columnar shuffle, Comet scan", cometShuffle = true, cometChild = true),
    Case(
      "Spark shuffle, row-based Spark scan (pre-#5971)",
      cometShuffle = false,
      cometChild = false),
    Case("Comet columnar shuffle, row-based Spark scan", cometShuffle = true, cometChild = false))

  private def configs(c: Case): Seq[(String, String)] = Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    // The point of the exercise: a partition key Comet cannot serialize.
    CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false",
    CometConf.COMET_NATIVE_SCAN_ENABLED.key -> c.cometChild.toString,
    // A Spark columnar scan is neither row-based nor a Comet operator, so the columnar path
    // declines it outright; the non-Comet child has to arrive as rows.
    SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> c.cometChild.toString,
    CometConf.COMET_SHUFFLE_ENABLED.key -> c.cometShuffle.toString,
    CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
    CometConf.COMET_SHUFFLE_CONVERT_FROM_SPARK_PLAN_ENABLED.key -> "true")

  /**
   * Fails unless each case really produces the exchange it is named for. Without this the
   * benchmark could silently time the same plan twice.
   */
  private def checkPlans(): Unit = {
    for (query <- Seq(rangeQuery, hashQuery); c <- cases) {
      withSQLConf(configs(c): _*) {
        val df = spark.sql(query)
        df.noop()
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        val stagePlans = collect(plan) { case s: QueryStageExec => s.plan }
        val exchanges = (plan +: stagePlans).flatMap { p: SparkPlan =>
          collect(p) {
            case e: CometShuffleExchangeExec => e.nodeName
            case e: ShuffleExchangeExec => e.nodeName
          }
        }
        val comet = exchanges.count(_.startsWith("CometColumnarExchange"))
        val spark0 = exchanges.count(_ == "Exchange")
        val ok = if (c.cometShuffle) comet >= 1 else comet == 0 && spark0 >= 1
        if (!ok) {
          throw new IllegalStateException(
            s"${c.label}: expected ${if (c.cometShuffle) "a Comet columnar" else "a Spark"} " +
              s"exchange but found $exchanges in:\n$plan")
        }
      }
    }
  }

  private def run(name: String, query: String): Unit = {
    val benchmark = new Benchmark(name, Rows, minNumIters = 5, output = output)
    cases.foreach { c =>
      benchmark.addCase(c.label) { _ =>
        withSQLConf(configs(c): _*) {
          spark.sql(query).noop()
        }
      }
    }
    benchmark.run()
  }
}
