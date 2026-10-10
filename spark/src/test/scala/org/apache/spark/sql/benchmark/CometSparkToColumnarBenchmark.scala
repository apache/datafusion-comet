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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.comet.{CometNativeScanExec, CometSparkToColumnarExec}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Benchmark for the Spark-to-Arrow conversion in `CometSparkToColumnarExec`, which turns the
 * output of a JVM operator into Arrow batches for native operators, for nested types.
 *
 * Each case reads a Parquet table whose non-key columns are of one nested shape and filters on
 * the key, so that every nested column crosses the conversion and the native filter, and then
 * returns to Spark through columnar-to-row. Three arms are compared:
 *   - Spark: its own scan, filter and columnar-to-row.
 *   - Comet, native Parquet scan: no conversion, the control for the import and filter work.
 *   - Comet, Spark Parquet scan converted to Arrow: the same plan with the conversion in place of
 *     the native scan.
 *
 * `CometSparkToColumnarExec` only accepts arrays and maps of strings, so those are the collection
 * shapes here, alongside structs of every depth.
 *
 * To run this benchmark:
 * {{{
 * SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometSparkToColumnarBenchmark
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometSparkToColumnarBenchmark-**results.txt".
 */
object CometSparkToColumnarBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometSparkToColumnarBenchmark")
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "2g")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    sparkSession.conf.set(SQLConf.ANSI_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK.key, "false")
    sparkSession.conf.set("parquet.enable.dictionary", "false")

    sparkSession
  }

  private val cometConf =
    Seq(CometConf.COMET_ENABLED.key -> "true", CometConf.COMET_EXEC_ENABLED.key -> "true")

  /** Adds a case after checking that the plan the noop write executes has the expected scan. */
  private def addVerifiedCase(
      benchmark: Benchmark,
      name: String,
      query: String,
      conf: Seq[(String, String)],
      required: Class[_ <: SparkPlan],
      excluded: Class[_ <: SparkPlan]): Unit = {
    withSQLConf(conf: _*) {
      val plans = ArrayBuffer.empty[SparkPlan]
      val listener = new QueryExecutionListener {
        override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
          plans += qe.executedPlan
        }

        override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
          ()
      }
      spark.sparkContext.listenerBus.waitUntilEmpty()
      spark.listenerManager.register(listener)
      try {
        spark.sql(query).noop()
        spark.sparkContext.listenerBus.waitUntilEmpty()
      } finally {
        spark.listenerManager.unregister(listener)
      }
      val nodes = plans.flatMap(plan => collect(plan) { case node => node.getClass })
      require(
        nodes.contains(required) && !nodes.contains(excluded),
        s"$name must execute ${required.getSimpleName} and not ${excluded.getSimpleName}.\n" +
          plans.mkString("\n"))
      benchmark.out.println(s"Verified $name")
    }

    benchmark.addCase(name) { _ =>
      withSQLConf(conf: _*) {
        spark.sql(query).noop()
      }
    }
  }

  /** Nested shapes that `CometSparkToColumnarExec` accepts. */
  private val shapes: Seq[(String, Seq[String])] = Seq(
    "flat struct (4 fields)" ->
      Seq(
        "named_struct('i', cast(id as int), 'l', id, 'd', cast(id as double), 's', " +
          "concat('s_', cast(id as string))) AS c"),
    "nested struct (depth 3)" ->
      Seq(
        "named_struct('a', named_struct('b', named_struct('i', cast(id as int), " +
          "'s', concat('s_', cast(id as string))), 'l', id), 'd', cast(id as double)) AS c"),
    "nullable struct" ->
      Seq(
        "if(id % 5 = 0, null, named_struct('i', if(id % 3 = 0, null, cast(id as int)), " +
          "'s', if(id % 4 = 0, null, concat('s_', cast(id as string))))) AS c"),
    "wide struct (20 int fields)" ->
      Seq(
        s"named_struct(${(0 until 20).map(i => s"'f$i', cast(id + $i as int)").mkString(", ")}) AS c"),
    "array<string>" ->
      Seq(
        "array(concat('a_', cast(id as string)), concat('b_', cast(id as string)), " +
          "concat('c_', cast(id as string))) AS c"),
    "nullable array<string>" ->
      Seq(
        "if(id % 5 = 0, null, array(concat('a_', cast(id as string)), " +
          "if(id % 3 = 0, null, concat('b_', cast(id as string))))) AS c"),
    "map<string,string>" ->
      Seq("map('k1', concat('v_', cast(id as string)), 'k2', 'constant') AS c"),
    "struct holding array<string>" ->
      Seq(
        "named_struct('id', id, 'tags', array(concat('t_', cast(id % 100 as string)), " +
          "'x')) AS c"))

  def nestedBenchmark(name: String, columns: Seq[String], values: Int): Unit = {
    val benchmark =
      new Benchmark(s"Spark to Arrow - $name", values.toLong, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.range(values.toLong).selectExpr("id AS key" +: columns: _*))
        val query = "SELECT * FROM parquetV1Table WHERE key % 2 = 0"

        benchmark.addCase("Spark") { _ =>
          spark.sql(query).noop()
        }

        addVerifiedCase(
          benchmark,
          "Comet, native Parquet scan (no conversion)",
          query,
          cometConf :+ (CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true"),
          required = classOf[CometNativeScanExec],
          excluded = classOf[CometSparkToColumnarExec])

        addVerifiedCase(
          benchmark,
          "Comet, Spark Parquet scan converted to Arrow",
          query,
          cometConf ++ Seq(
            CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
            CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true"),
          required = classOf[CometSparkToColumnarExec],
          excluded = classOf[CometNativeScanExec])

        benchmark.run()
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024 // 1M rows

    shapes.foreach { case (name, columns) =>
      runBenchmark(s"Spark to Arrow - $name") {
        nestedBenchmark(name, columns, values)
      }
    }
  }
}
