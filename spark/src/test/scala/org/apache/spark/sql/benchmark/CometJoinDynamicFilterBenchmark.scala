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
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Benchmark to measure the effect of runtime dynamic filter pushdown
 * (spark.comet.exec.join.dynamicFilter.enabled) on broadcast hash joins with three build-side
 * shapes: sparse selective keys (membership predicate does the pruning), clustered selective keys
 * (min/max bounds do the pruning), and a non-selective build side covering the full probe domain
 * (the selectivity guard should disable the filter, bounding overhead). To run:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometJoinDynamicFilterBenchmark
 * }}}
 */
object CometJoinDynamicFilterBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometJoinDynamicFilterBenchmark")
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
    sparkSession.conf.set("spark.sql.shuffle.partitions", "2")
    sparkSession
  }

  // Force BroadcastHashJoin: builds below the 10MB threshold.
  private val cometConfigs: Map[String, String] = Map(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB",
    SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB")

  private def benchmarkQuery(name: String, cardinality: Long, query: String): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)

    benchmark.addCase("Spark") { _ =>
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.sql(query).noop()
      }
    }

    benchmark.addCase("Comet (dynamic filter off)") { _ =>
      val configs = cometConfigs + (CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ENABLED.key ->
        "false")
      withSQLConf(configs.toSeq: _*) {
        spark.sql(query).noop()
      }
    }

    benchmark.addCase("Comet (dynamic filter on)") { _ =>
      val configs = cometConfigs + (CometConf.COMET_EXEC_JOIN_DYNAMIC_FILTER_ENABLED.key ->
        "true")
      withSQLConf(configs.toSeq: _*) {
        spark.sql(query).noop()
      }
    }

    benchmark.run()
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val probeRows = 16 * 1024 * 1024
    val sparseBuildRows = 1024
    val clusteredBuildRows = 64 * 1024
    val fullDomain = 64 * 1024

    withTempPath { dir =>
      withTempTable("probe", "probe_mod", "build_sparse", "build_clustered", "build_full") {
        // Probe with unique keys across [0, probeRows).
        spark
          .range(probeRows)
          .selectExpr("id AS k", "id % 1000 AS v")
          .write
          .parquet(s"${dir.getAbsolutePath}/probe")
        // Probe whose keys all fall in [0, fullDomain) so build_full matches every row.
        spark
          .range(probeRows)
          .selectExpr(s"id % $fullDomain AS k", "id % 1000 AS v")
          .write
          .parquet(s"${dir.getAbsolutePath}/probe_mod")
        // Sparse selective build: keys spread across the whole probe domain, so min/max
        // bounds prune nothing and the membership predicate does the work (~0.006%
        // of probe rows match).
        spark
          .range(sparseBuildRows)
          .selectExpr(s"id * ${probeRows / sparseBuildRows} AS k", "id AS w")
          .write
          .parquet(s"${dir.getAbsolutePath}/build_sparse")
        // Clustered selective build: contiguous keys in the middle of the probe domain,
        // so the min/max bounds prune ~99.6% of probe rows cheaply.
        spark
          .range(clusteredBuildRows)
          .selectExpr(s"id + ${probeRows / 2} AS k", "id AS w")
          .write
          .parquet(s"${dir.getAbsolutePath}/build_clustered")
        // Non-selective build: covers every probe_mod key, so the filter keeps 100% of
        // rows and the selectivity guard should disable evaluation.
        spark
          .range(fullDomain)
          .selectExpr("id AS k", "id AS w")
          .write
          .parquet(s"${dir.getAbsolutePath}/build_full")

        Seq("probe", "probe_mod", "build_sparse", "build_clustered", "build_full").foreach { t =>
          spark.read.parquet(s"${dir.getAbsolutePath}/$t").createOrReplaceTempView(t)
        }

        runBenchmark("BroadcastHashJoin dynamic filter - sparse selective build") {
          benchmarkQuery(
            "sparse selective build (membership pruning)",
            probeRows,
            "SELECT /*+ BROADCAST(b) */ count(*), sum(p.v) FROM probe p " +
              "JOIN build_sparse b ON p.k = b.k")
        }

        runBenchmark("BroadcastHashJoin dynamic filter - clustered selective build") {
          benchmarkQuery(
            "clustered selective build (bounds pruning)",
            probeRows,
            "SELECT /*+ BROADCAST(b) */ count(*), sum(p.v) FROM probe p " +
              "JOIN build_clustered b ON p.k = b.k")
        }

        runBenchmark("BroadcastHashJoin dynamic filter - non-selective build") {
          benchmarkQuery(
            "non-selective build (guard disables filter)",
            probeRows,
            "SELECT /*+ BROADCAST(b) */ count(*), sum(p.v) FROM probe_mod p " +
              "JOIN build_full b ON p.k = b.k")
        }
      }
    }
  }
}
