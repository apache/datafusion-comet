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
 * Benchmark to measure performance of Comet BroadcastNestedLoopJoin. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometBroadcastNestedLoopJoinBenchmark
 * }}}
 * Results will be written to
 * "spark/benchmarks/CometBroadcastNestedLoopJoinBenchmark-**results.txt".
 */
object CometBroadcastNestedLoopJoinBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometBroadcastNestedLoopJoinBenchmark")
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

    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.ANSI_ENABLED.key, "false")
    sparkSession.conf.set("spark.sql.shuffle.partitions", "2")

    sparkSession
  }

  private val cometConfigs: Map[String, String] = Map.empty[String, String]

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val probeRows = 1024 * 1024
    val buildRows = 100

    withTempPath { dir =>
      withTempTable("probe", "build") {
        spark
          .range(probeRows)
          .selectExpr("id AS k", "id % 100 AS v")
          .write
          .parquet(s"${dir.getAbsolutePath}/probe")
        spark
          .range(buildRows)
          .selectExpr("id * 1000 AS lo", "id * 1000 + 500 AS hi")
          .write
          .parquet(s"${dir.getAbsolutePath}/build")

        spark.read.parquet(s"${dir.getAbsolutePath}/probe").createOrReplaceTempView("probe")
        spark.read.parquet(s"${dir.getAbsolutePath}/build").createOrReplaceTempView("build")

        runBenchmark("BroadcastNestedLoopJoin - range") {
          runExpressionBenchmark(
            "range join (BETWEEN)",
            probeRows,
            "SELECT /*+ BROADCAST(b) */ count(*) FROM probe p " +
              "JOIN build b ON p.k BETWEEN b.lo AND b.hi",
            cometConfigs)
        }

        runBenchmark("BroadcastNestedLoopJoin - inequality") {
          runExpressionBenchmark(
            "inequality join (>)",
            probeRows,
            "SELECT /*+ BROADCAST(b) */ count(*) FROM probe p " +
              "JOIN build b ON p.k > b.lo",
            cometConfigs)
        }

        runBenchmark("BroadcastNestedLoopJoin - left outer with non-equi") {
          runExpressionBenchmark(
            "left outer non-equi",
            probeRows,
            "SELECT /*+ BROADCAST(b) */ count(*) FROM probe p " +
              "LEFT OUTER JOIN build b ON p.k BETWEEN b.lo AND b.hi",
            cometConfigs)
        }

        runBenchmark("BroadcastNestedLoopJoin - range, materialized rows") {
          runExpressionBenchmark(
            "range join (BETWEEN, projected)",
            probeRows,
            "SELECT /*+ BROADCAST(b) */ p.k, p.v, b.lo, b.hi FROM probe p " +
              "JOIN build b ON p.k BETWEEN b.lo AND b.hi",
            cometConfigs)
        }
      }
    }
  }
}
