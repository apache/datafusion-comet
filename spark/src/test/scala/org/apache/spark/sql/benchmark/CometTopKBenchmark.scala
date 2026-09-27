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

import org.apache.hadoop.fs.Path
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.Row
import org.apache.spark.sql.comet.{CometLocalTopKExec, CometNativeScanExec, CometTakeOrderedAndProjectExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Compare local TopK fusion enabled and disabled, using a native release build. The make target
 * builds and packages the release library before running the benchmark:
 *
 * {{{
 * make benchmark-org.apache.spark.sql.benchmark.CometTopKBenchmark -- \
 *   1048576 1,4 1,16 16,4096 ascending,descending,random 5 off,on
 * }}}
 *
 * Arguments are rows, scan partitions, BIGINT payload columns, K values, layouts, minimum
 * measured iterations, and mode order. Defaults match the example. Use on,off for a second run
 * with the opposite case order. Selectors between rows and iterations accept comma-separated
 * lists. For a quick smoke run, use 16384 rows and three iterations. Each case warms up for at
 * least 500 ms and measures for at least 500 ms. Results include query planning and collection;
 * fixtures, Spark result checks, and native plan checks run outside the timings. The wide payload
 * and descending layout expose the possible cost of losing scan/TopK overlap.
 */
object CometTopKBenchmark extends CometBenchmarkBase {

  private val nativeConfigs = Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
    CometConf.COMET_BATCH_SIZE.key -> "4096",
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
      layout: String): Long = {
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val payload = (0 until payloadColumns).map(i => s"id * ${17 + i * 2} AS payload_$i")
      spark
        .range(0, rows, 1, partitions)
        .selectExpr((Seq(s"${keyExpression(layout, rows)} AS k") ++ payload): _*)
        .write
        .option("compression", "snappy")
        .option("parquet.enable.dictionary", "false")
        .parquet(directory)
    }
    val path = new Path(directory)
    val files = path
      .getFileSystem(spark.sessionState.newHadoopConf())
      .listStatus(path)
      .filter(_.getPath.getName.endsWith(".parquet"))
    require(files.length == partitions, s"Expected $partitions files, found ${files.length}")
    files.map(_.getLen).max
  }

  private def verifyPlanAndResult(
      benchmark: Benchmark,
      query: String,
      expected: Seq[Row],
      fusion: Boolean,
      k: Int,
      partitions: Int,
      expectedInputRows: Int): Unit = {
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
      val block = Operator.parseFrom(local.head.serializedPlanOpt.plan.get)
      require(block.hasSort && block.getSort.getFetch == k && block.getSort.getSkip == 0)
      require(block.getChildrenCount == 1 && block.getChildren(0).hasNativeScan)
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
    require(df.collect().toSeq == expected, s"Incorrect results with fusion=$fusion")
    val scanMetrics = scans.head.metrics
    require(
      scanMetrics.get("output_rows").exists(_.value == expectedInputRows.toLong),
      "TopK fusion must still decode all input rows")
    val readerWork = Seq("output_rows", "bytes_scanned", "row_groups_pruned_dynamic_filter")
      .flatMap(name => scanMetrics.get(name).map(metric => s"$name=${metric.value}"))
      .mkString(", ")
    benchmark.out.println(s"fusion=$fusion, $readerWork")
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    require(
      mainArgs.length <= 7,
      "Usage: CometTopKBenchmark [rows] [partitions] [payload-columns] [ks] [layouts] " +
        "[iterations] [off,on|on,off]")
    val rows = mainArgs.headOption.map(_.toInt).getOrElse(1 << 20)
    val partitions = mainArgs.lift(1).getOrElse("1,4").split(",").map(_.toInt).toSeq
    val payloadColumns = mainArgs.lift(2).getOrElse("1,16").split(",").map(_.toInt).toSeq
    val ks = mainArgs.lift(3).getOrElse("16,4096").split(",").map(_.toInt).toSeq
    val layouts = mainArgs.lift(4).getOrElse("ascending,descending,random").split(",").toSeq
    val iterations = mainArgs.lift(5).map(_.toInt).getOrElse(5)
    val modeOrder = mainArgs.lift(6).getOrElse("off,on")
    require(Set("off,on", "on,off").contains(modeOrder), "mode order must be off,on or on,off")
    val fusionModes = modeOrder.split(",").map(_ == "on").toSeq
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
          val maxFileBytes = writeInput(directory, rows, p, columns, layout)
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
                fusionModes.foreach { fusion =>
                  val fusionConfig =
                    CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> fusion.toString
                  withSQLConf(fusionConfig) {
                    verifyPlanAndResult(benchmark, query, expected, fusion, k, p, rows)
                  }
                  benchmark.addCase(if (fusion) "fusion enabled" else "fusion disabled") { _ =>
                    withSQLConf(fusionConfig) {
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
