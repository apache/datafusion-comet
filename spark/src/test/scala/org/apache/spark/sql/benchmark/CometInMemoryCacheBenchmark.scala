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

import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, LongType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

object CometInMemoryCacheBenchmark extends CometBenchmarkBase {
  private val numRows = 5 * 1000 * 1000
  private val cacheTable = "comet_cache_bench"
  private val sourceTable = "comet_cache_bench_src"
  // A sink for the benchmarked call's result: written but never read, so that neither the
  // compiler nor the JIT can treat `gatherColumnStats` as dead code. Not `private`, because
  // a private field that is only ever written is what `-Ywarn-unused:privates` reports.
  @volatile var statsResult: (Array[Any], Array[Any], Array[Int]) = _

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
    runStatsBenchmark()
    // Run just the JVM statistics loop without constructing or scanning a cached relation.
    if (args.contains("--stats-only")) return

    withTempTable(sourceTable, cacheTable) {
      spark
        .range(0, numRows.toLong, 1, 16)
        .selectExpr(
          "id",
          "id % 1000 AS k",
          "id + 1 AS v",
          "concat('str_a_', cast(id % 100000 as string)) AS s1",
          "concat('str_b_', cast(id % 7919 as string)) AS s2",
          "concat('str_c_', cast(id as string)) AS s3")
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

      // A CometCachedBatch stores each column as its own stream, so a scan decodes only what it
      // projected and cost tracks the width of the projection. These three cases span that range
      // over one cached relation: no columns, one column, and all six.
      runCacheBenchmark(
        "in-memory cache row count only (0 of 6 columns)",
        s"SELECT count(*) FROM $cacheTable")

      runCacheBenchmark(
        "in-memory cache narrow projection (1 of 6 columns)",
        s"SELECT count(k) FROM $cacheTable")

      runCacheBenchmark(
        "in-memory cache full projection (6 of 6 columns)",
        s"SELECT count(id), count(k), count(v), count(s1), count(s2), count(s3) FROM $cacheTable")
    }
  }

  private def runStatsBenchmark(): Unit = {
    val batchSize = 10000
    val types: Seq[DataType] = Seq.fill(3)(LongType) ++ Seq.fill(3)(StringType)
    val attrs = types.zipWithIndex.map { case (dt, i) => AttributeReference(s"c$i", dt)() }
    val columns = types.map(dt => new OnHeapColumnVector(batchSize, dt))
    val batch = new ColumnarBatch(columns.map(c => c: ColumnVector).toArray, batchSize)
    try {
      var r = 0
      while (r < batchSize) {
        columns(0).putLong(r, r.toLong)
        columns(1).putLong(r, (r % 1000).toLong)
        columns(2).putLong(r, (r + 1).toLong)
        columns(3).putByteArray(r, s"str_a_${r % 100000}".getBytes(StandardCharsets.UTF_8))
        columns(4).putByteArray(r, s"str_b_${r % 7919}".getBytes(StandardCharsets.UTF_8))
        columns(5).putByteArray(r, s"str_c_$r".getBytes(StandardCharsets.UTF_8))
        r += 1
      }
      val serializer = new ArrowCachedBatchSerializer
      val benchmark = new Benchmark("in-memory cache statistics", numRows.toLong, output = output)
      // One case measures this collector across commits; Spark's default cache has its own collector.
      benchmark.addCase("Comet statistics collector") { _ =>
        var i = 0
        while (i < numRows / batchSize) {
          statsResult = serializer.gatherColumnStats(batch, attrs)
          i += 1
        }
      }
      benchmark.run()
    } finally batch.close()
  }

  private def runCacheBenchmark(name: String, query: String): Unit = {
    withCachedTable {
      withSQLConf(cacheConf(nativeCacheEnabled = false): _*) {
        verifyPlan(query, nativeCacheEnabled = false)
      }
      withSQLConf(cacheConf(nativeCacheEnabled = true): _*) {
        verifyPlan(query, nativeCacheEnabled = true)
      }

      val benchmark = new Benchmark(name, numRows.toLong, output = output)

      benchmark.addCase("Spark cache scan + CometSparkColumnarToColumnar") { _ =>
        withSQLConf(cacheConf(nativeCacheEnabled = false): _*) {
          spark.sql(query).noop()
        }
      }

      benchmark.addCase("CometInMemoryTableScan") { _ =>
        withSQLConf(cacheConf(nativeCacheEnabled = true): _*) {
          spark.sql(query).noop()
        }
      }

      benchmark.run()
    }
  }

  private def withCachedTable(f: => Unit): Unit = {
    spark.catalog.clearCache()

    // Materialize the cache once using Comet's cache serializer, then read it both ways.
    //
    // What the two cases isolate is the cache-scan boundary, not the execution engine above it.
    // cacheConf turns Comet execution on for both, so the aggregation runs on Comet either way;
    // the only flag that moves is COMET_EXEC_IN_MEMORY_CACHE_ENABLED. Disabled, Spark's
    // InMemoryTableScanExec feeds those same Comet operators through a
    // CometSparkColumnarToColumnar bridge; enabled, CometInMemoryTableScan feeds them directly.
    // So the numbers measure "keep the cached scan native" against "fall back to a Spark cache
    // scan and convert" -- which is the overhead this feature exists to remove.
    //
    // Neither case is a baseline for Spark's own cache format. spark.sql.cache.serializer is a
    // static conf, so a single session cannot also materialize a DefaultCachedBatch to compare
    // against; both cases read the same Comet-written CometCachedBatch.
    withSQLConf(cacheConf(nativeCacheEnabled = true): _*) {
      spark
        .sql(s"SELECT id, k, v, s1, s2, s3 FROM $sourceTable")
        .createOrReplaceTempView(cacheTable)
      spark.catalog.cacheTable(cacheTable)
      spark.table(cacheTable).count()
    }

    try f
    finally {
      spark.catalog.uncacheTable(cacheTable)
      spark.catalog.clearCache()
    }
  }

  // Pins the shape the case labels claim: enabled reads the cache natively with no conversion,
  // disabled reads it through Spark's cache scan and a CometSparkColumnarToColumnar bridge. The
  // bridge is what makes the disabled case a scan-boundary comparison rather than a Spark-vs-Comet
  // execution one, since a Spark-columnar-to-Arrow transition only exists to feed Comet operators.
  private def verifyPlan(query: String, nativeCacheEnabled: Boolean): Unit = {
    val plan = spark.sql(query).queryExecution.executedPlan.toString()

    if (nativeCacheEnabled) {
      assert(plan.contains("CometInMemoryTableScan"), s"Expected native cache scan:\n$plan")
      assert(!plan.contains("CometSparkColumnarToColumnar"), s"Unexpected conversion:\n$plan")
    } else {
      assert(
        !plan.contains("CometInMemoryTableScan"),
        s"Native cache scan should be disabled:\n$plan")
      assert(
        plan.contains("CometSparkColumnarToColumnar"),
        s"Expected the fallback read to bridge into Comet operators:\n$plan")
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
