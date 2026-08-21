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

object CometInMemoryCacheBenchmark extends CometBenchmarkBase {
  private val numRows = 5 * 1000 * 1000
  private val cacheTable = "comet_cache_bench"
  private val sourceTable = "comet_cache_bench_src"

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometInMemoryCacheBenchmark")
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set("spark.plugins", "org.apache.spark.CometPlugin")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .set(
        "spark.sql.cache.serializer",
        "org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.ANSI_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    sparkSession
  }

  override def runCometBenchmark(args: Array[String]): Unit = {
    withTempTable(sourceTable, cacheTable) {
      spark
        .range(0, numRows, 1, 16)
        .selectExpr("id", "id % 1000 AS k", "id + 1 AS v")
        .createOrReplaceTempView(sourceTable)

      runCacheBenchmark(
        "in-memory cache repeated scan",
        s"SELECT sum(id), sum(k), sum(v) FROM $cacheTable")

      runCacheBenchmark(
        "in-memory cache selective filter",
        s"""
           |SELECT sum(id), sum(k), sum(v)
           |FROM $cacheTable
           |WHERE id >= 4500000 AND id < 4750000
         """.stripMargin)
    }
  }

  private def runCacheBenchmark(name: String, query: String): Unit = {
    withCachedTable {
      withSQLConf(cacheConf(nativeCacheEnabled = false): _*) {
        verifyPlan(query, nativeCacheEnabled = false)
      }
      withSQLConf(cacheConf(nativeCacheEnabled = true): _*) {
        verifyPlan(query, nativeCacheEnabled = true)
      }

      val benchmark = new Benchmark(name, numRows, output = output)

      benchmark.addCase("Comet cache disabled") { _ =>
        withSQLConf(cacheConf(nativeCacheEnabled = false): _*) {
          spark.sql(query).noop()
        }
      }

      benchmark.addCase("Comet cache enabled") { _ =>
        withSQLConf(cacheConf(nativeCacheEnabled = true): _*) {
          spark.sql(query).noop()
        }
      }

      benchmark.run()
    }
  }

  private def withCachedTable(f: => Unit): Unit = {
    spark.catalog.clearCache()

    // Materialize the cache once using Comet's cache serializer.
    // The benchmark measures repeated cache reads by comparing the
    // fallback read path against CometInMemoryTableScan.
    withSQLConf(cacheConf(nativeCacheEnabled = true): _*) {
      spark.sql(s"SELECT id, k, v FROM $sourceTable").createOrReplaceTempView(cacheTable)
      spark.catalog.cacheTable(cacheTable)
      spark.table(cacheTable).count()
    }

    try f
    finally {
      spark.catalog.uncacheTable(cacheTable)
      spark.catalog.clearCache()
    }
  }

  private def verifyPlan(query: String, nativeCacheEnabled: Boolean): Unit = {
    val plan = spark.sql(query).queryExecution.executedPlan.toString()

    if (nativeCacheEnabled) {
      assert(plan.contains("CometInMemoryTableScan"), s"Expected native cache scan:\n$plan")
      assert(!plan.contains("CometSparkColumnarToColumnar"), s"Unexpected conversion:\n$plan")
    } else {
      assert(
        !plan.contains("CometInMemoryTableScan"),
        s"Native cache scan should be disabled:\n$plan")
    }
  }

  private def cacheConf(nativeCacheEnabled: Boolean): Seq[(String, String)] = {
    Seq(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> nativeCacheEnabled.toString,
      "spark.comet.sparkToColumnar.enabled" -> "true",
      "spark.comet.exec.onHeap.enabled" -> "true",
      "spark.sql.inMemoryColumnarStorage.batchSize" -> "10000")
  }
}
