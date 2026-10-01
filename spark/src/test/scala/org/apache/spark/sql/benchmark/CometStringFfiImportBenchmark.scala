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
import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometNativeColumnarToRowExec, CometNativeScanExec, CometSparkToColumnarExec}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Benchmark to measure the end-to-end cost of validating string data that native code imports
 * from the JVM over the Arrow C Data Interface.
 *
 * arrow-rs builds FFI-imported string arrays without checking UTF-8, so Comet validates every
 * imported `Utf8` column and decodes it if it is invalid. The data here is always valid, so the
 * cases measure the validation pass that every query crossing an import site pays. Two import
 * sites carry most string data:
 *   - `ScanExec`, which receives batches from JVM operators. Here it is fed by Spark's Parquet
 *     reader converted to Arrow.
 *   - native columnar-to-row conversion, which imports native batches back from the JVM.
 *
 * Each case that crosses an import site is paired with a control that does the same work without
 * one: the native Parquet scan for `ScanExec`, and the JVM conversion for columnar-to-row. When
 * comparing a build with and without validation, the controls should not move.
 *
 * To run this benchmark:
 * {{{
 * SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometStringFfiImportBenchmark
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometStringFfiImportBenchmark-**results.txt".
 */
object CometStringFfiImportBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometStringFfiImportBenchmark")
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
    // Every value is distinct, so dictionary pages would fall back anyway. Disabling them keeps
    // both import sites on plain `Utf8` arrays rather than depending on where the fallback lands.
    sparkSession.conf.set("parquet.enable.dictionary", "false")

    sparkSession
  }

  private val cometConf =
    Seq(CometConf.COMET_ENABLED.key -> "true", CometConf.COMET_EXEC_ENABLED.key -> "true")

  /**
   * Adds a case after checking that the query really takes the path the case name claims. The
   * noop write is what the case times, and its plan can differ from the SELECT's, so the check
   * inspects the plans the write executed.
   */
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

  /** A filter over a wide string column, with the column reaching native code from a scan. */
  def scanImportBenchmark(values: Int, kind: String): Unit = {
    val benchmark =
      new Benchmark(s"Filter over wide $kind strings", values.toLong, output = output)
    val query = "SELECT count(*) FROM parquetV1Table WHERE s LIKE '%999%'"

    benchmark.addCase("Spark") { _ =>
      spark.sql(query).noop()
    }

    addVerifiedCase(
      benchmark,
      "Comet, native Parquet scan (no import)",
      query,
      cometConf :+ (CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true"),
      required = classOf[CometNativeScanExec],
      excluded = classOf[CometSparkToColumnarExec])

    addVerifiedCase(
      benchmark,
      "Comet, Spark Parquet scan converted to Arrow (imported)",
      query,
      cometConf ++ Seq(
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
        CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true"),
      required = classOf[CometSparkToColumnarExec],
      excluded = classOf[CometNativeScanExec])

    benchmark.run()
  }

  /** Wide string columns returned to Spark as rows. */
  def columnarToRowImportBenchmark(values: Int, kind: String): Unit = {
    val benchmark =
      new Benchmark(s"Columnar to row of wide $kind strings", values.toLong, output = output)
    val query = "SELECT s FROM parquetV1Table"
    val nativeScan = cometConf :+ (CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true")

    benchmark.addCase("Spark") { _ =>
      spark.sql(query).noop()
    }

    addVerifiedCase(
      benchmark,
      "Comet, JVM columnar to row (no import)",
      query,
      nativeScan :+ (CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false"),
      required = classOf[CometColumnarToRowExec],
      excluded = classOf[CometNativeColumnarToRowExec])

    addVerifiedCase(
      benchmark,
      "Comet, native columnar to row (imported)",
      query,
      nativeScan :+ (CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "true"),
      required = classOf[CometNativeColumnarToRowExec],
      excluded = classOf[CometColumnarToRowExec])

    benchmark.run()
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024 // 1M rows

    // Roughly 200 bytes per value. The multibyte strings mix two- and three-byte UTF-8
    // sequences, which take the slower per-character path through validation. They are built
    // with `chr` and `unhex` so that this source file stays ASCII.
    val strings = Seq(
      "ASCII" -> "repeat(concat('value_', cast(id as string), '_'), 12)",
      "multibyte" ->
        ("repeat(concat('caf', chr(233), '_', decode(unhex('E697A5E69CACE8AA9E'), 'UTF-8'), " +
          "'_', cast(id as string), '_'), 8)"))

    strings.foreach { case (kind, expr) =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(dir, spark.range(values.toLong).selectExpr(s"$expr AS s"))

          runBenchmark(s"FFI String Import - Scan ($kind)") {
            scanImportBenchmark(values, kind)
          }

          runBenchmark(s"FFI String Import - Columnar to Row ($kind)") {
            columnarToRowImportBenchmark(values, kind)
          }
        }
      }
    }
  }
}
