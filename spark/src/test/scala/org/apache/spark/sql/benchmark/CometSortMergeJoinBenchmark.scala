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
import org.apache.spark.sql.SparkSession
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

    val sparkSession = SparkSession.builder
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
      withTempTable("probe", "build") {
        spark
          .range(probeRows)
          .selectExpr("id AS k", "id % 100 AS v")
          .write
          .parquet(s"${dir.getAbsolutePath}/probe")
        spark
          .range(buildRows)
          .selectExpr("id AS k", "id * 10 AS w")
          .write
          .parquet(s"${dir.getAbsolutePath}/build")

        spark.read.parquet(s"${dir.getAbsolutePath}/probe").createOrReplaceTempView("probe")
        spark.read.parquet(s"${dir.getAbsolutePath}/build").createOrReplaceTempView("build")

        runBenchmark("SortMergeJoin - inner count") {
          runExpressionBenchmark(
            "inner count",
            probeRows,
            "SELECT /*+ MERGE(b) */ count(*) FROM probe p JOIN build b ON p.k = b.k",
            cometConfigs)
        }

        runBenchmark("SortMergeJoin - inner projected") {
          runExpressionBenchmark(
            "inner projected",
            probeRows,
            "SELECT /*+ MERGE(b) */ p.k, p.v, b.w FROM probe p JOIN build b ON p.k = b.k",
            cometConfigs)
        }

        runBenchmark("SortMergeJoin - left outer") {
          runExpressionBenchmark(
            "left outer",
            probeRows,
            "SELECT /*+ MERGE(b) */ count(*) FROM probe p LEFT JOIN build b ON p.k = b.k",
            cometConfigs)
        }

        runBenchmark("SortMergeJoin - left semi") {
          runExpressionBenchmark(
            "left semi",
            probeRows,
            "SELECT /*+ MERGE(b) */ count(*) FROM probe p LEFT SEMI JOIN build b ON p.k = b.k",
            cometConfigs)
        }

        runBenchmark("SortMergeJoin - right outer") {
          runExpressionBenchmark(
            "right outer",
            probeRows,
            "SELECT /*+ MERGE(p) */ count(*) FROM probe p RIGHT JOIN build b ON p.k = b.k",
            cometConfigs)
        }
      }
    }
  }
}
