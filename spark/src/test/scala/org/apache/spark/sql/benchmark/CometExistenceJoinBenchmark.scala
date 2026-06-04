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
 * Benchmark to measure performance of Comet's ExistenceJoin support across the three join
 * physical operators (BHJ, SHJ, SMJ). To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometExistenceJoinBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometExistenceJoinBenchmark-**results.txt".
 */
object CometExistenceJoinBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometExistenceJoinBenchmark")
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

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val probeRows = 1024 * 1024
    val buildRows = 10000

    withTempPath { dir =>
      withTempTable("probe", "build") {
        spark
          .range(probeRows)
          .selectExpr("id AS k", "CASE WHEN id % 3 = 0 THEN 'US' ELSE 'EU' END AS region")
          .write
          .parquet(s"${dir.getAbsolutePath}/probe")
        spark
          .range(buildRows)
          .selectExpr("id * 7 AS k")
          .write
          .parquet(s"${dir.getAbsolutePath}/build")

        spark.read.parquet(s"${dir.getAbsolutePath}/probe").createOrReplaceTempView("probe")
        spark.read.parquet(s"${dir.getAbsolutePath}/build").createOrReplaceTempView("build")

        val query =
          "SELECT count(*) FROM probe p " +
            "WHERE p.region = 'US' OR EXISTS (SELECT 1 FROM build b WHERE b.k = p.k)"

        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true") {
          spark.sql(query).explain()
        }

        runBenchmark("ExistenceJoin - BroadcastHashJoin") {
          runExpressionBenchmark(
            "exists OR predicate (BHJ)",
            probeRows,
            query,
            Map(
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB",
              SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB"))
        }

        runBenchmark("ExistenceJoin - ShuffledHashJoin") {
          runExpressionBenchmark(
            "exists OR predicate (SHJ)",
            probeRows,
            query,
            Map(
              SQLConf.PREFER_SORTMERGEJOIN.key -> "false",
              "spark.sql.join.forceApplyShuffledHashJoin" -> "true",
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
              SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1"))
        }

        runBenchmark("ExistenceJoin - SortMergeJoin") {
          runExpressionBenchmark(
            "exists OR predicate (SMJ)",
            probeRows,
            query,
            Map(
              SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
              SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1"))
        }
      }
    }
  }
}
