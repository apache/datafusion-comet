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
import org.apache.spark.sql.comet.{CometSortExec, CometSortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Benchmark to measure performance of Comet's SortMergeJoin across common Spark join shapes
 * (Inner count, Inner projected, LEFT OUTER, LEFT SEMI, RIGHT OUTER). To run:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometSortMergeJoinBenchmark
 * }}}
 */
object CometSortMergeJoinBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometSortMergeJoinBenchmark")
      .set("spark.master", "local[5]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
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

    sparkSession
  }

  // Force SortMergeJoin: prefer SMJ, disable broadcast.
  private val cometConfigs: Map[String, String] = Map(
    SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1")

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val probeRows = 1024 * 1024
    val buildRows = 100000

    withTempPath { dir =>
      withTempTable("probe", "build", "build_repeated", "build_moderate") {
        spark
          .range(probeRows.toLong)
          .selectExpr("id AS k", "id % 100 AS v")
          .write
          .parquet(s"${dir.getAbsolutePath}/probe")
        spark
          .range(buildRows.toLong)
          .selectExpr("id AS k", "id * 10 AS w")
          .write
          .parquet(s"${dir.getAbsolutePath}/build")
        spark
          .range(buildRows.toLong)
          .selectExpr("id % 128 AS k")
          .write
          .parquet(s"${dir.getAbsolutePath}/build_repeated")
        // Each group of four rows contains three distinct keys. No key occurs more than twice.
        spark
          .range(buildRows.toLong)
          .selectExpr("id - IF(id % 4 = 3, 1, 0) AS k")
          .write
          .parquet(s"${dir.getAbsolutePath}/build_moderate")

        spark.read.parquet(s"${dir.getAbsolutePath}/probe").createOrReplaceTempView("probe")
        spark.read.parquet(s"${dir.getAbsolutePath}/build").createOrReplaceTempView("build")
        spark.read
          .parquet(s"${dir.getAbsolutePath}/build_repeated")
          .createOrReplaceTempView("build_repeated")
        spark.read
          .parquet(s"${dir.getAbsolutePath}/build_moderate")
          .createOrReplaceTempView("build_moderate")

        runBenchmark("SortMergeJoin - inner count") {
          runExpressionBenchmark(
            "inner count",
            probeRows.toLong,
            "SELECT /*+ MERGE(b) */ count(*) FROM probe p JOIN build b ON p.k = b.k",
            cometConfigs)
        }

        runBenchmark("SortMergeJoin - inner projected") {
          runExpressionBenchmark(
            "inner projected",
            probeRows.toLong,
            "SELECT /*+ MERGE(b) */ p.k, p.v, b.w FROM probe p JOIN build b ON p.k = b.k",
            cometConfigs)
        }

        runBenchmark("SortMergeJoin - left outer") {
          runExpressionBenchmark(
            "left outer",
            probeRows.toLong,
            "SELECT /*+ MERGE(b) */ count(*) FROM probe p LEFT JOIN build b ON p.k = b.k",
            cometConfigs)
        }

        runBenchmark("SortMergeJoin - left semi") {
          runExpressionBenchmark(
            "left semi",
            probeRows.toLong,
            "SELECT /*+ MERGE(b) */ count(*) FROM probe p LEFT SEMI JOIN build b ON p.k = b.k",
            cometConfigs)
        }

        runBenchmark("SortMergeJoin - right key reduction") {
          val settings = cometConfigs ++ Map(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
            CometConf.COMET_BATCH_SIZE.key -> "4096",
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> "native")
          withSQLConf(settings.toSeq: _*) {
            val flag = CometConf.COMET_EXEC_SORT_MERGE_JOIN_RIGHT_KEY_REDUCTION_ENABLED.key
            // About three batches per shuffle partition, just beyond the reducer's initial prefix.
            val shortRows = 24576
            for ((table, inputRows, distinctKeys) <- Seq(
                ("build_repeated", buildRows, 128),
                ("build_moderate", buildRows, buildRows * 3 / 4),
                ("build", buildRows, buildRows),
                ("build", shortRows, shortRows))) {
              val benchmark = new Benchmark(
                s"left semi, $distinctKeys distinct right keys in $inputRows rows",
                inputRows.toLong,
                output = output)
              val query = s"""SELECT /*+ MERGE(b) */ count(*)
                             |FROM (SELECT k FROM probe WHERE k < $inputRows) p
                             |LEFT SEMI JOIN (SELECT k FROM $table WHERE k < $inputRows) b
                             |ON p.k = b.k""".stripMargin
              // Check the result and the actual reducer work outside the timed section.
              for (enabled <- Seq(false, true)) {
                withSQLConf(flag -> enabled.toString) {
                  val df = spark.sql(query)
                  require(df.collect().head.getLong(0) == distinctKeys)
                  val plan = df.queryExecution.executedPlan
                  val join = collectFirst(plan) { case j: CometSortMergeJoinExec => j }
                    .getOrElse(throw new IllegalStateException(plan.treeString))
                  require(join.nativeOp.getSortMergeJoin.getRightKeyReductionEnabled == enabled)
                  val sortRows = collectFirst(join.right) { case s: CometSortExec =>
                    s.longMetric("output_rows").value
                  }.getOrElse(throw new IllegalStateException(plan.treeString))
                  if (enabled) {
                    val in = join.longMetric("right_key_reduction_input_rows").value
                    val out = join.longMetric("right_key_reduction_output_rows").value
                    val bypass = join.longMetric("right_key_reduction_bypass_rows").value
                    require(in == inputRows && out == sortRows)
                    if (table == "build_repeated") {
                      require(out > 0 && out < in)
                    } else {
                      require(out == in && bypass == in)
                    }
                  } else {
                    require(sortRows == inputRows)
                  }
                }
                benchmark.addCase(s"right key reduction=$enabled") { _ =>
                  withSQLConf(flag -> enabled.toString) { spark.sql(query).collect() }
                }
              }
              benchmark.run()
            }
          }
        }

        runBenchmark("SortMergeJoin - right outer") {
          runExpressionBenchmark(
            "right outer",
            probeRows.toLong,
            "SELECT /*+ MERGE(p) */ count(*) FROM probe p RIGHT JOIN build b ON p.k = b.k",
            cometConfigs)
        }
      }
    }
  }
}
