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

import scala.concurrent.duration._

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.comet.{CometBaseAggregateExec, CometNativeScanExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Compare MIN/MAX with reader filtering disabled and enabled. Use a native release build:
 *
 * {{{
 * make benchmark-org.apache.spark.sql.benchmark.CometMinMaxBenchmark -- \
 *   1048576 1,4 ascending,descending,random,constant 5 off,on
 * }}}
 *
 * Arguments are rows, scan partitions, layouts, minimum measured iterations, and mode order.
 * Defaults match the example. Repeat with on,off to reverse the case order. A smoke run can use
 * 16384 rows and three iterations. Timings include planning and collection; fixture creation,
 * Spark result comparisons, plan checks, and metric collection run outside timings. Metrics come
 * from an untimed execution. Page indexes and decoder row filters are disabled to isolate
 * row-group pruning. Each file has multiple row groups and runs in a separate scan task.
 */
object CometMinMaxBenchmark extends CometBenchmarkBase {

  private val modes = Map("off" -> false, "on" -> true)
  private val filterConfig = CometConf.COMET_EXEC_AGGREGATE_DYNAMIC_FILTER_ENABLED.key

  private def writeInput(
      directory: String,
      rows: Int,
      partitions: Int,
      layout: String): (Long, Seq[Int]) = {
    val key = layout match {
      case "ascending" => s"id - ${rows / 2}"
      case "descending" => s"${rows / 2} - id - 1"
      case "random" => s"pmod(xxhash64(id), $rows) - ${rows / 2}"
      case "constant" => "0"
    }
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      spark
        .range(0, rows.toLong, 1, partitions)
        .selectExpr(s"CAST($key AS INT) AS k")
        .write
        // Fixed-width, uncompressed pages keep constant values from collapsing into one group.
        .option("compression", "uncompressed")
        .option("parquet.writer.version", "v1")
        .option("parquet.enable.dictionary", "false")
        // Target eight row groups per file, including for constant data.
        .option("parquet.block.size", (rows.toLong / partitions * 4 / 8).toString)
        .parquet(directory)
    }
    inspectParquetFixture(directory, partitions)
  }

  private def verifyAndReport(
      benchmark: Benchmark,
      query: String,
      expected: Seq[Row],
      mode: String,
      partitions: Int,
      rows: Int): Unit = {
    val enabled = modes(mode)
    val df = spark.sql(query)
    val plan = stripAQEPlan(df.queryExecution.executedPlan)
    val partials = collect(plan) {
      case node: CometBaseAggregateExec if node.modes == Seq(Partial) => node
    }
    val scans = collect(plan) { case node: CometNativeScanExec => node }
    require(partials.size == 1 && scans.size == 1, s"Expected MIN/MAX and native scan:\n$plan")
    val partial = partials.head
    require(partial.child.isInstanceOf[CometNativeScanExec], s"Expected a direct scan:\n$plan")
    require(partial.dynamicFilterEnabled == enabled)
    require(scans.head.executeColumnar().getNumPartitions == partitions)
    require(df.collect().toSeq == expected, s"Incorrect results in mode $mode")
    val metrics = scans.head.metrics
    if (!enabled) {
      require(metrics("output_rows").value == rows.toLong)
      require(metrics("row_groups_pruned_dynamic_filter").value == 0L)
    }
    require(metrics("row_groups_pruned_statistics").value == 0L)
    require(metrics("page_index_rows_pruned").value == 0L)
    require(metrics("pushdown_rows_pruned").value == 0L)
    val readerWork = formatParquetReaderMetrics(metrics)
    val attachment = Seq("attached", "skipped").map { suffix =>
      val name = s"dynamic_filter_minmax_filters_$suffix"
      val value = if (enabled) partial.metrics(name).value else 0L
      require(value == (if (enabled && suffix == "attached") partitions.toLong else 0L))
      s"$name=$value"
    }
    benchmark.out.println(s"mode=$mode, ${(readerWork ++ attachment).mkString(", ")}")
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    require(
      mainArgs.length <= 5,
      "Usage: CometMinMaxBenchmark [rows] [partitions] [layouts] [iterations] [off,on or on,off]")
    val rows = mainArgs.headOption.map(_.toInt).getOrElse(1 << 20)
    val partitions = mainArgs.lift(1).getOrElse("1,4").split(",").map(_.toInt).toSeq
    val layouts =
      mainArgs.lift(2).getOrElse("ascending,descending,random,constant").split(",").toSeq
    val iterations = mainArgs.lift(3).map(_.toInt).getOrElse(5)
    val modeOrder = mainArgs.lift(4).getOrElse("off,on").split(",").toSeq
    require(rows >= 16384, "rows must be at least 16384")
    require(
      partitions.forall(p => p > 0 && rows % p == 0 && rows / p >= 4096),
      "partitions must divide rows and leave at least 4096 rows per file")
    require(
      layouts.forall(Set("ascending", "descending", "random", "constant")),
      "Unknown layout")
    require(iterations > 0, "iterations must be positive")
    require(
      modeOrder.size == modes.size && modeOrder.toSet == modes.keySet,
      "mode order must contain off and on exactly once")

    withTempPath { root =>
      withTempTable("minmax_input") {
        for (p <- partitions; layout <- layouts) {
          val directory = s"${root.getCanonicalPath}/$layout-p$p"
          val (maxFileBytes, rowGroups) = writeInput(directory, rows, p, layout)
          // Keep one file per scan task so its bound can improve across row groups.
          val scanConfigs = rowGroupPruningScanConfigs :+
            (SQLConf.FILES_MAX_PARTITION_BYTES.key -> (maxFileBytes + 4194304L).toString)
          spark.read.parquet(directory).createOrReplaceTempView("minmax_input")
          for (function <- Seq("MIN", "MAX")) {
            val query = s"SELECT $function(k) FROM minmax_input"
            var expected = Seq.empty[Row]
            withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
              expected = spark.sql(query).collect().toSeq
            }
            val name = s"$function $layout, partitions=$p"
            runBenchmark(name) {
              val benchmark = new Benchmark(
                name,
                rows.toLong,
                minNumIters = iterations,
                warmupTime = 500.millis,
                minTime = 500.millis,
                output = output)
              withSQLConf(scanConfigs: _*) {
                benchmark.out.println(s"row groups per file: ${rowGroups.mkString(",")}")
                modeOrder.foreach { mode =>
                  withSQLConf(filterConfig -> modes(mode).toString) {
                    verifyAndReport(benchmark, query, expected, mode, p, rows)
                  }
                  benchmark.addCase(mode) { _ =>
                    withSQLConf(filterConfig -> modes(mode).toString) {
                      spark.sql(query).collect()
                    }
                  }
                }
                benchmark.run()
              }
            }
          }
        }
      }
    }
  }
}
