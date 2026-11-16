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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.comet.{CometCartesianProductExec, CometExec, CometPlan}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.joins.CartesianProductExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Benchmark for `CartesianProductExec`. Comet runs it natively only for a condition-free product
 * of single-partition inputs, so each side is one Parquet file coalesced to one partition. The
 * cases cover a small left side, a large left side (`CrossJoinExec` buffers the left) and a
 * square product, each run with Spark, Comet with the product in Spark, and Comet. To run it:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometCartesianProductBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometCartesianProductBenchmark-**results.txt".
 */
object CometCartesianProductBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometCartesianProductBenchmark")
      .set("spark.master", "local[5]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      // Off-heap memory is disabled, so Comet needs on-heap mode enabled.
      .set("spark.comet.exec.onHeap.enabled", "true")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.ANSI_ENABLED.key, "false")
    sparkSession.conf.set("spark.sql.shuffle.partitions", "2")
    // No broadcast, so Spark plans a CartesianProductExec.
    sparkSession.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    sparkSession.conf.set(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    // Keep each one-file scan to one task.
    sparkSession.conf.set(SQLConf.FILES_MAX_PARTITION_BYTES.key, (1L << 30).toString)

    sparkSession
  }

  private case class CartesianCase(name: String, leftRows: Long, rightRows: Long)

  private val SparkCaseName = "Spark"
  private val ProductInSparkCaseName = "Comet, product in Spark"
  private val CometCaseName = "Comet"

  private val sparkConfigs = Seq(CometConf.COMET_ENABLED.key -> "false")

  private val productInSparkConfigs = Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_EXEC_CARTESIAN_PRODUCT_ENABLED.key -> "false")

  private val cometConfigs = Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_EXEC_CARTESIAN_PRODUCT_ENABLED.key -> "true")

  // A file scan reports UnknownPartitioning(0), which the native product rejects, so each side is
  // coalesced to one partition. The sum reads both sides.
  private val query =
    "SELECT sum(l.v + r.w) FROM (SELECT /*+ COALESCE(1) */ * FROM lhs) l " +
      "CROSS JOIN (SELECT /*+ COALESCE(1) */ * FROM rhs) r"

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val cases = Seq(
      CartesianCase("small left, large right", leftRows = 64, rightRows = 256 * 1024),
      CartesianCase("large left, small right", leftRows = 256 * 1024, rightRows = 64),
      CartesianCase("square", leftRows = 4096, rightRows = 4096))

    withTempPath { dir =>
      withTempTable("lhs", "rhs") {
        def writeTable(view: String, path: String, df: DataFrame): Unit = {
          df.coalesce(1).write.parquet(s"${dir.getAbsolutePath}/$path")
          spark.read.parquet(s"${dir.getAbsolutePath}/$path").createOrReplaceTempView(view)
        }

        cases.foreach { c =>
          writeTable("lhs", s"lhs_${c.leftRows}", spark.range(c.leftRows).selectExpr("id AS v"))
          writeTable("rhs", s"rhs_${c.rightRows}", spark.range(c.rightRows).selectExpr("id AS w"))

          val productRows = c.leftRows * c.rightRows
          val name = s"Cartesian product - ${c.name} (${c.leftRows} x ${c.rightRows})"
          runBenchmark(name) {
            verify(name, productRows)
            val benchmark = new Benchmark(name, productRows, output = output)
            benchmark.addCase(SparkCaseName)(_ => runQuery(sparkConfigs))
            benchmark.addCase(ProductInSparkCaseName)(_ => runQuery(productInSparkConfigs))
            benchmark.addCase(CometCaseName)(_ => runQuery(cometConfigs))
            benchmark.run()
          }
        }
      }
    }
  }

  private def runQuery(configs: Seq[(String, String)]): Unit =
    withSQLConf(configs: _*) {
      spark.sql(query).noop()
    }

  /**
   * Fails unless each arm runs the product it claims, the Comet arm is fully native, and all arms
   * return the same sum.
   */
  private def verify(name: String, productRows: Long): Unit = {
    val arms = Seq(
      SparkCaseName -> sparkConfigs,
      ProductInSparkCaseName -> productInSparkConfigs,
      CometCaseName -> cometConfigs)
    val sums = arms.map { case (arm, configs) =>
      // A local, because `withSQLConf` returns `Unit` on Spark 3.4 and 3.5.
      var sum: Any = null
      withSQLConf(configs: _*) {
        val df = spark.sql(query)
        sum = df.collect().head.get(0)
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        def fail(problem: String): Nothing =
          throw new AssertionError(s"$name, $arm: $problem\n${plan.treeString}")

        val products = collect(plan) {
          case p: CartesianProductExec => p
          case p: CometExec if p.originalPlan.isInstanceOf[CartesianProductExec] => p
        }
        val expected =
          if (arm == CometCaseName) classOf[CometCartesianProductExec]
          else classOf[CartesianProductExec]
        products match {
          case Seq(node) if expected.isInstance(node) =>
            if (arm == SparkCaseName &&
              node.metrics("numOutputRows").value != productRows) {
              fail(s"the product produced ${node.metrics("numOutputRows").value} rows")
            }
          case _ => fail(s"expected one ${expected.getSimpleName}")
        }
        if (arm == CometCaseName) {
          // Query stages are leaves to the checker, so check the plan inside each one as well.
          val stagePlans = collect(plan) { case s: QueryStageExec => s.plan }
          val wrappers = Seq(classOf[QueryStageExec], classOf[AQEShuffleReadExec])
          (plan +: stagePlans)
            .flatMap(findFirstNonCometOperator(_, wrappers: _*))
            .headOption
            .foreach(op =>
              fail(s"the plan is not fully native, first non-Comet operator: ${op.nodeName}"))
        }
        if (arm == ProductInSparkCaseName && collect(plan) { case p: CometPlan => p }.isEmpty) {
          fail("Comet runs no operator")
        }
      }
      arm -> sum
    }
    val (_, sparkSum) = sums.head
    sums.tail.foreach { case (arm, sum) =>
      if (sum != sparkSum) {
        throw new AssertionError(s"$name: Spark returned $sparkSum, $arm returned $sum")
      }
    }
  }
}
