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
 * Benchmark to measure Comet broadcast hash join performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometBroadcastHashJoinBenchmark` Results will be
 * written to "spark/benchmarks/CometBroadcastHashJoinBenchmark-**results.txt".
 */
object CometBroadcastHashJoinBenchmark extends CometBenchmarkBase {
  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometBroadcastHashJoinBenchmark")
      .set("spark.master", "local[5]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set("spark.executor.memoryOverhead", "10g")

    val sparkSession = SparkSession.builder
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    sparkSession.conf.set("parquet.enable.dictionary", "false")

    sparkSession
  }

  def broadcastHashJoinBenchmark(
      streamedRows: Int,
      broadcastRows: Int,
      joinType: String): Unit = {
    val benchmark = new Benchmark(
      s"Broadcast Hash Join ($joinType, stream=$streamedRows, broadcast=$broadcastRows)",
      streamedRows,
      output = output)

    withTempPath { dir =>
      import spark.implicits._

      // Create streamed (large) table
      val streamedDir = dir.getCanonicalPath + "/streamed"
      spark
        .range(streamedRows)
        .select(($"id" % broadcastRows).as("key"), $"id".as("value"))
        .write
        .mode("overwrite")
        .parquet(streamedDir)

      // Create broadcast (small) table
      val broadcastDir = dir.getCanonicalPath + "/broadcast"
      spark
        .range(broadcastRows)
        .select($"id".as("key"), ($"id" * 10).as("payload"))
        .write
        .mode("overwrite")
        .parquet(broadcastDir)

      spark.read.parquet(streamedDir).createOrReplaceTempView("streamed")
      spark.read.parquet(broadcastDir).createOrReplaceTempView("broadcast")

      val query =
        s"SELECT /*+ BROADCAST(broadcast) */ s.value, b.payload " +
          s"FROM streamed s $joinType JOIN broadcast b ON s.key = b.key"

      withTempTable("streamed", "broadcast") {
        benchmark.addCase("Spark") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "false",
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> s"${256 * 1024 * 1024}") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet (Scan + Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> s"${256 * 1024 * 1024}") {
            spark.sql(query).noop()
          }
        }

        benchmark.run()
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val streamedRows = 2 * 1024 * 1024
    val broadcastRows = 1000

    for (joinType <- Seq("INNER", "LEFT", "RIGHT")) {
      broadcastHashJoinBenchmark(streamedRows, broadcastRows, joinType)
    }

    // Test with larger broadcast table
    broadcastHashJoinBenchmark(streamedRows, 10000, "INNER")
  }
}
