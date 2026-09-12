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
 * Compare the unfused TopK path, fused TopK, and fused TopK with reader filtering.
 *
 * {{{
 * make benchmark-org.apache.spark.sql.benchmark.CometTopKBenchmark -- \
 *   1048576 1,16 1,16 16,256,4096,100000 favorable,random,unfavorable
 * }}}
 *
 * Arguments are rows, scan partitions, BIGINT payload columns, K values, and layouts. Each
 * selector after rows accepts a comma-separated list. Defaults are 1048576 rows, one partition,
 * one payload column, K=16/100000, and favorable/random layouts. The full example intentionally
 * opts into the larger matrix. Use a native release build for performance results.
 */
object CometTopKBenchmark extends CometBenchmarkBase {

  private case class ExecutionMode(name: String, fusion: Boolean, filtering: Boolean) {
    def configs: Seq[(String, String)] = Seq(
      CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> fusion.toString,
      CometConf.COMET_EXEC_TOPK_DYNAMIC_FILTER_ENABLED.key -> filtering.toString)
  }

  private val modes = Seq(
    ExecutionMode("unfused", fusion = false, filtering = false),
    ExecutionMode("fused", fusion = true, filtering = false),
    ExecutionMode("fused + filtering", fusion = true, filtering = true))

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

  private case class InputMetadata(rowGroups: Int, maxFileBytes: Long)

  private def inputMetadata(directory: String, rows: Int, partitions: Int): InputMetadata = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val path = new Path(directory)
    val files = path
      .getFileSystem(hadoopConf)
      .listStatus(path)
      .filter(_.getPath.getName.endsWith(".parquet"))
    require(files.length == partitions, s"Expected $partitions files, found ${files.length}")
    var actualRows = 0L
    var groups = 0
    files.foreach { file =>
      val reader = ParquetFileReader.open(HadoopInputFile.fromPath(file.getPath, hadoopConf))
      try {
        val blocks = reader.getFooter.getBlocks
        groups += blocks.size()
        val iterator = blocks.iterator()
        while (iterator.hasNext) actualRows += iterator.next().getRowCount
      } finally {
        reader.close()
      }
    }
    require(actualRows == rows, s"Expected $rows rows, found $actualRows")
    InputMetadata(groups, files.map(_.getLen).max)
  }

  private def layoutKey(layout: String, rows: Int): String = layout match {
    case "favorable" => "CAST(id AS INT)"
    case "random" => s"CAST(pmod(id * 104729, $rows) AS INT)"
    case "unfavorable" => s"CAST($rows - id - 1 AS INT)"
  }

  private def payloadExpressions(columns: Int): Seq[String] =
    (0 until columns).map(i => s"id * ${17 + i * 2} AS payload_$i")

  private def rowGroupBytes(payloadColumns: Int): Long =
    1048576L * (4 + 8L * payloadColumns) / 12

  private def writeInput(
      directory: String,
      rows: Int,
      partitions: Int,
      payloadColumns: Int,
      layout: String): Unit = {
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      spark
        .range(0, rows, 1, partitions)
        .selectExpr((Seq(s"${layoutKey(layout, rows)} AS k") ++
          payloadExpressions(payloadColumns)): _*)
        .write
        .option("compression", "snappy")
        .option("parquet.block.size", rowGroupBytes(payloadColumns).toString)
        .option("parquet.enable.dictionary", "false")
        .parquet(directory)
    }
  }

  private def verifyAndReport(
      benchmark: Benchmark,
      query: String,
      expected: Seq[Row],
      mode: ExecutionMode,
      k: Int,
      partitions: Int,
      groups: Int): Unit = {
    withSQLConf(mode.configs: _*) {
      val df = spark.sql(query)
      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      val local = collect(plan) { case node: CometLocalTopKExec => node }
      val scans = collect(plan) { case node: CometNativeScanExec => node }
      val outer = collect(plan) { case node: CometTakeOrderedAndProjectExec => node }
      require(scans.size == 1 && outer.size == 1, s"Expected native TopK and scan:\n$plan")
      require(
        local.size == (if (mode.fusion) 1 else 0),
        s"Unexpected fusion for ${mode.name}:\n$plan")
      require(outer.head.child.executeColumnar().getNumPartitions == partitions)
      if (mode.fusion) {
        val topK = local.head
        require(topK.limit == k && topK.dynamicFilterEnabled == mode.filtering)
        val block = Operator.parseFrom(topK.serializedPlanOpt.plan.get)
        require(block.hasSort && block.getSort.getFetch == k && block.getSort.getSkip == 0)
        require(block.getChildrenCount == 1 && block.getChildren(0).hasNativeScan)
        require(block.getSort.getDynamicFilterEnabled == mode.filtering)
      }
      require(df.collect().toSeq == expected, s"Incorrect results for ${mode.name}")
      // Snapshot after collection and before any additional execution can reset the metrics.
      val scanMetrics = scans.head.metrics.map { case (name, metric) => name -> metric.value }
      val attached = local.map(_.metrics("dynamic_filter_reader_filters_attached").value).sum
      require(attached == (if (mode.filtering) partitions.toLong else 0L))
      val metricNames = Seq(
        "bytes_scanned",
        "output_rows",
        "row_groups_pruned_dynamic_filter",
        "row_groups_pruned_statistics",
        "page_index_rows_pruned",
        "page_index_pages_pruned",
        "pushdown_rows_pruned")
      require(scanMetrics("page_index_rows_pruned") == 0)
      require(scanMetrics("page_index_pages_pruned") == 0)
      require(scanMetrics("pushdown_rows_pruned") == 0)
      val metrics = metricNames.map(name => s"$name=${scanMetrics(name)}").mkString(", ")
      benchmark.out.println(
        s"mode=${mode.name}, scan_partitions=$partitions, total_row_groups=$groups, " +
          s"reader_filters_attached=$attached, $metrics")
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    require(
      mainArgs.length <= 5,
      "Usage: CometTopKBenchmark [rows] [partitions] [payload-columns] [ks] [layouts]")
    val rows = mainArgs.headOption.map(_.toInt).getOrElse(1 << 20)
    val partitions = mainArgs.lift(1).getOrElse("1").split(",").map(_.toInt).toSeq
    val payloadColumns = mainArgs.lift(2).getOrElse("1").split(",").map(_.toInt).toSeq
    val ks = mainArgs
      .lift(3)
      .getOrElse(Seq(16, 100000).filter(_ <= rows).mkString(","))
      .split(",")
      .map(_.toInt)
      .toSeq
    val layouts = mainArgs.lift(4).getOrElse("favorable,random").split(",").toSeq
    require(rows >= 16384 && (rows & (rows - 1)) == 0, "rows must be a power of two >= 16384")
    require(partitions.forall(p => p > 0 && rows % p == 0), "partitions must divide rows")
    require(payloadColumns.forall(_ > 0), "payload-columns must be positive")
    require(ks.forall(k => k > 0 && k <= rows), "ks must be positive and at most rows")
    require(layouts.forall(Set("favorable", "random", "unfavorable")), "Unknown layout")

    withTempPath { root =>
      withTempTable("topk_input") {
        for (p <- partitions; columns <- payloadColumns; layout <- layouts) {
          val directory = s"${root.getCanonicalPath}/$layout-p$p-c$columns"
          writeInput(directory, rows, p, columns, layout)
          val metadata = inputMetadata(directory, rows, p)
          // One balanced file per scan partition. Assert the resulting partition count above;
          // minPartitionNum alone may split or combine files depending on their compressed sizes.
          val openCost = 4194304L
          val scanConfigs = nativeConfigs :+
            (SQLConf.FILES_MAX_PARTITION_BYTES.key -> (metadata.maxFileBytes + openCost).toString)
          spark.read.parquet(directory).createOrReplaceTempView("topk_input")
          for (k <- ks) {
            val query = s"SELECT * FROM topk_input ORDER BY k LIMIT $k"
            var expected = Seq.empty[Row]
            withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
              expected = spark.sql(query).collect().toSeq
            }
            val name = s"TopK $layout, partitions=$p, payloadColumns=$columns, K=$k"
            runBenchmark(name) {
              val benchmark = new Benchmark(name, rows, minNumIters = 3, output = output)
              benchmark.out.println(s"target_row_group_bytes=${rowGroupBytes(columns)}")
              withSQLConf(scanConfigs: _*) {
                modes.foreach { mode =>
                  verifyAndReport(benchmark, query, expected, mode, k, p, metadata.rowGroups)
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
