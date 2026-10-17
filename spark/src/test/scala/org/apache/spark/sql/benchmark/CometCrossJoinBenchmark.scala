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
import org.apache.spark.sql.catalyst.plans.Cross
import org.apache.spark.sql.comet.{CometBroadcastHashJoinExec, CometBroadcastNestedLoopJoinExec, CometExec, CometHashJoinExec, CometPlan, CometSortMergeJoinExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.joins.{BaseJoinExec, BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometSparkSessionExtensions, ConfigEntry}

/**
 * Benchmark to measure performance of joins of type Cross, which Spark plans for an explicit
 * `CROSS JOIN` or `Dataset.crossJoin`. A filter on an equi key turns the cross join into a
 * sort-merge, shuffled hash or broadcast hash join of type Cross, and a broadcast side with no
 * equi key gives a broadcast nested loop join of type Cross.
 *
 * Each case runs Spark, Comet with the join left in Spark, which is how Comet ran these joins
 * before it converted the join type Cross, and Comet. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometCrossJoinBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometCrossJoinBenchmark-**results.txt".
 */
object CometCrossJoinBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometCrossJoinBenchmark")
      .set("spark.master", "local[5]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      // Off-heap memory is disabled, so Comet runs in on-heap mode, which must be enabled
      // explicitly or Comet stays disabled and every case measures Spark.
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
    // The hints choose each join's strategy, so nothing is broadcast by size.
    sparkSession.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    sparkSession.conf.set(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")

    sparkSession
  }

  /**
   * @param joinEnabled
   *   Comet's switch for the join operator. Turning it off leaves the join in Spark and the
   *   operators around it as they were, which is the plan Comet made when it rejected the join
   *   type Cross.
   */
  private case class CrossJoinCase(
      name: String,
      query: String,
      joinEnabled: ConfigEntry[Boolean],
      sparkJoin: Class[_ <: SparkPlan],
      cometJoin: Class[_ <: SparkPlan])

  private val SparkCaseName = "Spark"
  private val JoinInSparkCaseName = "Comet, join in Spark"
  private val CometCaseName = "Comet"

  private val sparkConfigs = Seq(CometConf.COMET_ENABLED.key -> "false")

  private val cometConfigs =
    Seq(CometConf.COMET_ENABLED.key -> "true", CometConf.COMET_EXEC_ENABLED.key -> "true")

  private def joinInSparkConfigs(c: CrossJoinCase): Seq[(String, String)] =
    cometConfigs :+ (c.joinEnabled.key -> "false")

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val probeRows = 1024 * 1024L
    val keys = 1024L

    // The sum reads a column from each side, so the join has to produce both, and only its one
    // row leaves the native plan.
    def equiJoin(hint: String): String =
      s"SELECT /*+ $hint(b) */ sum(p.v + b.w) FROM probe p CROSS JOIN build b WHERE p.k = b.k"

    val cases = Seq(
      CrossJoinCase(
        "SortMergeJoin",
        equiJoin("MERGE"),
        CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED,
        classOf[SortMergeJoinExec],
        classOf[CometSortMergeJoinExec]),
      CrossJoinCase(
        "ShuffledHashJoin",
        equiJoin("SHUFFLE_HASH"),
        CometConf.COMET_EXEC_HASH_JOIN_ENABLED,
        classOf[ShuffledHashJoinExec],
        classOf[CometHashJoinExec]),
      CrossJoinCase(
        "BroadcastHashJoin",
        equiJoin("BROADCAST"),
        CometConf.COMET_EXEC_BROADCAST_HASH_JOIN_ENABLED,
        classOf[BroadcastHashJoinExec],
        classOf[CometBroadcastHashJoinExec]),
      CrossJoinCase(
        "BroadcastNestedLoopJoin",
        "SELECT /*+ BROADCAST(t) */ sum(p.v + t.w) FROM probe p CROSS JOIN tiny t",
        CometConf.COMET_EXEC_BROADCAST_NESTED_LOOP_JOIN_ENABLED,
        classOf[BroadcastNestedLoopJoinExec],
        classOf[CometBroadcastNestedLoopJoinExec]))

    withTempPath { dir =>
      withTempTable("probe", "build", "tiny") {
        def writeTable(view: String, path: String, df: DataFrame): Unit = {
          df.write.parquet(s"${dir.getAbsolutePath}/$path")
          spark.read.parquet(s"${dir.getAbsolutePath}/$path").createOrReplaceTempView(view)
        }

        writeTable(
          "probe",
          "probe",
          spark.range(probeRows).selectExpr(s"id % $keys AS k", "id AS v"))

        // Every probe row meets fanOut rows on the other side, so every case joins
        // probeRows * fanOut rows. The larger fan-out leaves less of the time to the scans and
        // shuffles around the join.
        Seq(10L, 100L).foreach { fanOut =>
          val joinedRows = probeRows * fanOut
          writeTable(
            "build",
            s"build_$fanOut",
            spark.range(keys * fanOut).selectExpr(s"id % $keys AS k", "id AS w"))
          writeTable("tiny", s"tiny_$fanOut", spark.range(fanOut).selectExpr("id AS w"))

          cases.foreach { c =>
            val name = s"Cross join - ${c.name}, fan-out $fanOut"
            runBenchmark(name) {
              verify(name, c, joinedRows)
              val benchmark = new Benchmark(name, joinedRows, output = output)
              benchmark.addCase(SparkCaseName)(_ => runQuery(c.query, sparkConfigs))
              benchmark.addCase(JoinInSparkCaseName)(_ =>
                runQuery(c.query, joinInSparkConfigs(c)))
              benchmark.addCase(CometCaseName)(_ => runQuery(c.query, cometConfigs))
              benchmark.run()
            }
          }
        }
      }
    }
  }

  private def runQuery(query: String, configs: Seq[(String, String)]): Unit =
    withSQLConf(configs: _*) {
      spark.sql(query).noop()
    }

  /**
   * Fails unless every arm runs the join its name claims, with the join type Cross, the Spark
   * join produces `joinedRows` rows, and the three arms return the same sum.
   */
  private def verify(name: String, c: CrossJoinCase, joinedRows: Long): Unit = {
    val arms = Seq(
      SparkCaseName -> sparkConfigs,
      JoinInSparkCaseName -> joinInSparkConfigs(c),
      CometCaseName -> cometConfigs)
    val sums = arms.map { case (arm, configs) =>
      // Assigned to a local rather than returned from the block: Spark 3.4 and 3.5 declare
      // `SQLHelper.withSQLConf` as returning `Unit`.
      var sum: Any = null
      withSQLConf(configs: _*) {
        val df = spark.sql(c.query)
        sum = df.collect().head.get(0)
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        def fail(problem: String): Nothing =
          throw new AssertionError(s"$name, $arm: $problem\n${plan.treeString}")

        // Each join with the Spark join it is or replaced, which carries the join type.
        val joins = collect(plan) {
          case j: BaseJoinExec => j -> j
          case j: CometExec if j.originalPlan.isInstanceOf[BaseJoinExec] =>
            j -> j.originalPlan.asInstanceOf[BaseJoinExec]
        }
        val expected = if (arm == CometCaseName) c.cometJoin else c.sparkJoin
        joins match {
          case Seq((node, join)) if expected.isInstance(node) && join.joinType == Cross =>
            if (arm == SparkCaseName && join.metrics("numOutputRows").value != joinedRows) {
              fail(s"the join produced ${join.metrics("numOutputRows").value} rows")
            }
          case _ => fail(s"expected one ${expected.getSimpleName} of join type Cross")
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
        if (arm == JoinInSparkCaseName && collect(plan) { case p: CometPlan => p }.isEmpty) {
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
