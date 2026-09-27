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
import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.Row
import org.apache.spark.sql.comet.{CometLocalTopKExec, CometNativeScanExec, CometTakeOrderedAndProjectExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Compare native TopK without fusion, with fusion, and with fusion plus reader pruning. Use a
 * native release build. The make target builds and packages it before running:
 *
 * {{{
 * make benchmark-org.apache.spark.sql.benchmark.CometTopKBenchmark -- \
 *   1048576 1,4 1,16 16,100000 ascending,descending,random 5 unfused,fused,pruning
 * }}}
 *
 * Arguments are rows, scan partitions, BIGINT payload columns, K values, layouts, minimum
 * measured iterations, and mode order. Defaults match the example. Repeat with
 * pruning,fused,unfused to reverse the case order. Selectors between rows and iterations accept
 * comma-separated lists. For a smoke run, use 16384 rows, K=16, and three iterations. Each case
 * warms up and measures for at least 500 ms. Timings include planning and collection; fixture
 * creation, Spark result comparisons, and plan checks run outside timings. Reader metrics come
 * from the untimed validation execution; metadata cache warmth can depend on mode order.
 *
 * Compare pruning against fused to isolate reader savings. The unfused baseline separately
 * exposes changes in scan/TopK overlap. Page indexes and decoder row filters are disabled so scan
 * metrics measure row-group pruning. Every fixture has multiple row groups per file.
 */
object CometTopKBenchmark extends CometBenchmarkBase {

  private case class Mode(name: String, fusion: Boolean, filtering: Boolean) {
    val configs: Seq[(String, String)] = Seq(
      CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> fusion.toString,
      CometConf.COMET_EXEC_TOPK_DYNAMIC_FILTER_ENABLED.key -> filtering.toString)
  }

  private val modes = Seq(
    Mode("unfused", fusion = false, filtering = false),
    Mode("fused", fusion = true, filtering = false),
    Mode("pruning", fusion = true, filtering = true)).map(mode => mode.name -> mode).toMap

  private val nativeConfigs = Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
    CometConf.COMET_BATCH_SIZE.key -> "4096",
    CometConf.COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED.key -> "false",
    CometConf.COMET_RESPECT_DATAFUSION_CONFIGS.key -> "true",
    "spark.comet.datafusion.execution.parquet.enable_page_index" -> "false",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
    SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
    SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "4194304")

  private def keyExpression(layout: String, rows: Int): String = {
    val unsigned = layout match {
      case "ascending" => "id"
      case "descending" => s"$rows - id - 1"
      // An odd multiplier permutes all values modulo a power of two, without duplicate keys.
      case "random" => s"pmod(id * 104729, $rows)"
    }
    s"CAST(($unsigned) - ${rows / 2} AS INT)"
  }

  private def writeInput(
      directory: String,
      rows: Int,
      partitions: Int,
      payloadColumns: Int,
      layout: String): (Long, Seq[Int]) = {
    // Target eight row groups per file, scaling with row width and fixture size.
    val blockBytes = rows.toLong / partitions * (4L + 8L * payloadColumns) / 8
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val payload = (0 until payloadColumns).map(i => s"id * ${17 + i * 2} AS payload_$i")
      spark
        .range(0, rows, 1, partitions)
        .selectExpr((Seq(s"${keyExpression(layout, rows)} AS k") ++ payload): _*)
        .write
        .option("compression", "snappy")
        .option("parquet.enable.dictionary", "false")
        .option("parquet.block.size", blockBytes.toString)
        .parquet(directory)
    }
    val path = new Path(directory)
    val files = path
      .getFileSystem(spark.sessionState.newHadoopConf())
      .listStatus(path)
      .filter(_.getPath.getName.endsWith(".parquet"))
    require(files.length == partitions, s"Expected $partitions files, found ${files.length}")
    val rowGroups = files.toSeq.map { file =>
      val reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(file.getPath, spark.sessionState.newHadoopConf()))
      try reader.getFooter.getBlocks.asScala.size
      finally reader.close()
    }
    require(rowGroups.forall(_ > 1), s"Expected multiple row groups per file: $rowGroups")
    (files.map(_.getLen).max, rowGroups)
  }

  private def verifyPlanAndResult(
      benchmark: Benchmark,
      query: String,
      expected: Seq[Row],
      mode: Mode,
      k: Int,
      partitions: Int,
      expectedInputRows: Int): Unit = {
    val fusion = mode.fusion
    val df = spark.sql(query)
    val plan = stripAQEPlan(df.queryExecution.executedPlan)
    val local = collect(plan) { case node: CometLocalTopKExec => node }
    val scans = collect(plan) { case node: CometNativeScanExec => node }
    val outer = collect(plan) { case node: CometTakeOrderedAndProjectExec => node }
    require(scans.size == 1 && outer.size == 1, s"Expected native TopK and scan:\n$plan")
    require(local.size == (if (fusion) 1 else 0), s"Unexpected fusion=$fusion:\n$plan")
    require(outer.head.child.executeColumnar().getNumPartitions == partitions)
    if (fusion) {
      require(local.head.limit == k)
      require(local.head.dynamicFilterEnabled == mode.filtering)
      val block = Operator.parseFrom(local.head.serializedPlanOpt.plan.get)
      require(block.hasSort && block.getSort.getFetch == k && block.getSort.getSkip == 0)
      require(block.getChildrenCount == 1 && block.getChildren(0).hasNativeScan)
      require(block.getSort.getDynamicFilterEnabled == mode.filtering)
    }
    val finalPlan = outer.head.finalNativePlan(partitions).get
    require(finalPlan.hasProjection && finalPlan.getChildrenCount == 1)
    val selection = finalPlan.getChildren(0)
    if (fusion && partitions == 1) {
      require(selection.hasLimit, "Single-partition fusion must avoid a second TopK heap")
      require(selection.getLimit.getLimit == k && selection.getLimit.getOffset == 0)
    } else {
      require(selection.hasSort && selection.getSort.getFetch == k)
    }
    require(df.collect().toSeq == expected, s"Incorrect results in mode ${mode.name}")
    val scanMetrics = scans.head.metrics
    if (!mode.filtering) {
      require(scanMetrics("output_rows").value == expectedInputRows.toLong)
      require(scanMetrics("row_groups_pruned_dynamic_filter").value == 0L)
    }
    require(scanMetrics("row_groups_pruned_statistics").value == 0L)
    require(scanMetrics("page_index_rows_pruned").value == 0L)
    require(scanMetrics("pushdown_rows_pruned").value == 0L)
    val readerWork = Seq(
      "output_rows",
      "bytes_scanned",
      "scan_io_data_bytes",
      "scan_io_metadata_bytes",
      "row_groups_pruned_dynamic_filter",
      "row_groups_pruned_statistics",
      "page_index_rows_pruned",
      "pushdown_rows_pruned")
      .map(name => s"$name=${scanMetrics(name).value}")
    val attachment = local.flatMap { topK =>
      val attached = topK.metrics("dynamic_filter_topk_filters_attached").value
      val skipped = topK.metrics("dynamic_filter_topk_filters_skipped").value
      require(attached == (if (mode.filtering) partitions.toLong else 0L))
      require(skipped == 0L)
      Seq(
        s"dynamic_filter_topk_filters_attached=$attached",
        s"dynamic_filter_topk_filters_skipped=$skipped")
    }
    benchmark.out.println(s"mode=${mode.name}, ${(readerWork ++ attachment).mkString(", ")}")
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    require(
      mainArgs.length <= 7,
      "Usage: CometTopKBenchmark [rows] [partitions] [payload-columns] [ks] [layouts] " +
        "[iterations] [unfused,fused,pruning in any order]")
    val rows = mainArgs.headOption.map(_.toInt).getOrElse(1 << 20)
    val partitions = mainArgs.lift(1).getOrElse("1,4").split(",").map(_.toInt).toSeq
    val payloadColumns = mainArgs.lift(2).getOrElse("1,16").split(",").map(_.toInt).toSeq
    val ks = mainArgs.lift(3).getOrElse("16,100000").split(",").map(_.toInt).toSeq
    val layouts = mainArgs.lift(4).getOrElse("ascending,descending,random").split(",").toSeq
    val iterations = mainArgs.lift(5).map(_.toInt).getOrElse(5)
    val modeOrder = mainArgs.lift(6).getOrElse("unfused,fused,pruning").split(",").toSeq
    require(
      modeOrder.size == modes.size && modeOrder.toSet == modes.keySet,
      "mode order must contain unfused, fused, and pruning exactly once")
    require(rows >= 16384 && (rows & (rows - 1)) == 0, "rows must be a power of two >= 16384")
    require(partitions.forall(p => p > 0 && rows % p == 0), "partitions must divide rows")
    require(payloadColumns.forall(_ > 0), "payload-columns must be positive")
    require(ks.forall(k => k > 0 && k <= rows), "ks must be positive and at most rows")
    require(layouts.forall(Set("ascending", "descending", "random")), "Unknown layout")
    require(iterations > 0, "iterations must be positive")

    withTempPath { root =>
      withTempTable("topk_input") {
        for (p <- partitions; columns <- payloadColumns; layout <- layouts) {
          val directory = s"${root.getCanonicalPath}/$layout-p$p-c$columns"
          val (maxFileBytes, rowGroups) = writeInput(directory, rows, p, columns, layout)
          // Keep each file in its own scan partition without inserting a repartition operator,
          // which would interrupt fusion. Verify the actual partition count for every case.
          val scanConfigs = nativeConfigs :+
            (SQLConf.FILES_MAX_PARTITION_BYTES.key -> (maxFileBytes + 4194304L).toString)
          spark.read.parquet(directory).createOrReplaceTempView("topk_input")
          for (k <- ks) {
            val query = s"SELECT * FROM topk_input ORDER BY k LIMIT $k"
            var expected = Seq.empty[Row]
            withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
              expected = spark.sql(query).collect().toSeq
            }
            require(expected.map(_.getInt(0)) == (-rows / 2 until -rows / 2 + k))
            val name = s"TopK $layout, partitions=$p, payloadColumns=$columns, K=$k"
            runBenchmark(name) {
              val benchmark = new Benchmark(
                name,
                rows,
                minNumIters = iterations,
                warmupTime = 500.millis,
                minTime = 500.millis,
                output = output)
              withSQLConf(scanConfigs: _*) {
                benchmark.out.println(s"row groups per file: ${rowGroups.mkString(",")}")
                modeOrder.map(modes).foreach { mode =>
                  withSQLConf(mode.configs: _*) {
                    verifyPlanAndResult(benchmark, query, expected, mode, k, p, rows)
                  }
                  benchmark.addCase(mode.name) { _ =>
                    withSQLConf(mode.configs: _*) {
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
