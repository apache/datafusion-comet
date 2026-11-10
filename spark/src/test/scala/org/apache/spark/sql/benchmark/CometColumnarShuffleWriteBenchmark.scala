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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Measures Comet's JVM columnar shuffle at partition counts above Spark's
 * `spark.shuffle.sort.bypassMergeThreshold` (200), where the sort-based writer is used. That
 * writer sorts its buffered rows by partition and writes every partition holding a row, once per
 * spill and once more at the end, so its cost per partition is paid `partitions x (spills + 1)`
 * times. Each case hash-partitions the same rows and discards the result.
 *
 * The partition count is set through `spark.sql.shuffle.partitions` rather than `repartition(n,
 * ...)`, so that AQE coalesces the read side into a few tasks and the measurement is not
 * dominated by scheduling one reduce task per partition.
 *
 * `spark.comet.shuffle.jvm.spillThreshold` makes Comet spill a fixed number of rows at a time. It
 * does not apply to Spark's shuffle, which does not spill in these cases.
 *
 * This is separate from `CometShuffleBenchmark`, which also covers the sort-based writer, because
 * it needs a session of its own. The master is fixed when the session starts, and this benchmark
 * uses `local[1]` so that the map tasks run one at a time and the time follows their cost, where
 * `CometShuffleBenchmark` uses `local[5]`. That benchmark also sets a default spill threshold, so
 * its no-spill cases would spill, and this one leaves the default so that its no-spill case does
 * not.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometColumnarShuffleWriteBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometColumnarShuffleWriteBenchmark-**results.txt".
 */
object CometColumnarShuffleWriteBenchmark extends CometBenchmarkBase {

  private val numRows = 1024 * 1024
  private val spillThreshold = 100000

  private def cometJvmShuffle(extraConfs: (String, String)*): Seq[(String, String)] =
    Seq(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") ++ extraConfs

  private def query(): DataFrame = spark.table("parquetV1Table").repartition(col("c1"))

  private def shuffleWrite(partitions: Int): Unit = {
    val benchmark =
      new Benchmark(s"Shuffle write, $partitions partitions", numRows.toLong, output = output)

    val shufflePartitions = SQLConf.SHUFFLE_PARTITIONS.key -> partitions.toString
    val cases = Seq(
      "Spark" -> Seq(CometConf.COMET_ENABLED.key -> "false"),
      "Comet JVM shuffle" -> cometJvmShuffle(),
      s"Comet JVM shuffle, spill every $spillThreshold rows" -> cometJvmShuffle(
        CometConf.COMET_SHUFFLE_JVM_SPILL_THRESHOLD.key -> spillThreshold.toString))

    cases.foreach { case (name, caseConfs) =>
      val confs = caseConfs :+ shufflePartitions
      if (name.startsWith("Comet")) {
        // Check the case measures the JVM columnar shuffle before timing it.
        val plans = capturePlans {
          withSQLConf(confs: _*) {
            query().noop()
          }
        }
        val shuffles = collectAcross(plans) {
          case s: CometShuffleExchangeExec if s.shuffleType == CometColumnarShuffle => s
        }
        assert(shuffles.nonEmpty, s"$name did not use the JVM columnar shuffle: $plans")
      }

      benchmark.addCase(name) { _ =>
        withSQLConf(confs: _*) {
          query().noop()
        }
      }
    }

    benchmark.run()
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("Sort-based JVM columnar shuffle write", numRows) { _ =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"""SELECT
                 |  value AS c1,
                 |  CAST(value % 1000 AS INT) AS c2,
                 |  CAST(value AS DOUBLE) / 3 AS c3,
                 |  CONCAT('status-', CAST(ABS(value % 100) AS STRING)) AS c4,
                 |  DATE_ADD(DATE '2020-01-01', CAST(ABS(value % 1000) AS INT)) AS c5
                 |FROM $tbl""".stripMargin))

          Seq(1000, 10000).foreach(shuffleWrite)
        }
      }
    }
  }
}
