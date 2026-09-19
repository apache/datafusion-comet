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
 * Measures end-to-end broadcast join time with native direct broadcast reads disabled and
 * enabled. The build side is deliberately split into many small input partitions so each run
 * includes producer serialization and driver-side coalescing, rather than measuring only the
 * consumer. Run with GC logging or a profiler to compare allocation and collection time:
 * {{{
 *   make benchmark-org.apache.spark.sql.benchmark.CometBroadcastDirectReadBenchmark
 * }}}
 */
object CometBroadcastDirectReadBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometBroadcastDirectReadBenchmark")
      .set("spark.master", "local[5]")
      .setIfMissing("spark.driver.memory", "6g")
      .setIfMissing("spark.executor.memory", "6g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")

    SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val probeRows = 4L * 1024 * 1024
    val buildRows = 512 * 1024
    val buildPartitions = 400
    val query =
      "SELECT /*+ BROADCAST(b) */ count(*) FROM direct_probe p JOIN direct_build b ON p.k = b.k"

    withTempTable("direct_probe", "direct_build") {
      spark
        .range(0, probeRows, 1, 16)
        .selectExpr("id AS k", "id % 100 AS v")
        .createOrReplaceTempView("direct_probe")
      spark
        .range(0, buildRows, 1, buildPartitions)
        .selectExpr("id AS k", "id * 10 AS w")
        .createOrReplaceTempView("direct_build")

      val benchmark = new Benchmark("broadcast direct read", probeRows, output = output)
      val common = Seq(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_EXEC_BROADCAST_FORCE_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "64MB",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "64MB")

      benchmark.addCase("direct read disabled") { _ =>
        withSQLConf(
          (common :+ (CometConf.COMET_BROADCAST_DIRECT_READ_ENABLED.key -> "false")): _*) {
          spark.sql(query).noop()
        }
      }
      benchmark.addCase("direct read enabled") { _ =>
        withSQLConf(
          (common :+ (CometConf.COMET_BROADCAST_DIRECT_READ_ENABLED.key -> "true")): _*) {
          spark.sql(query).noop()
        }
      }
      benchmark.run()
    }
  }
}
