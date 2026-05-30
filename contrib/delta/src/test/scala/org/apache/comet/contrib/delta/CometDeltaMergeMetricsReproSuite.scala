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

package org.apache.comet.contrib.delta

import org.apache.spark.sql.functions.floor

// Investigates the Spark-3.5 regression failure:
//   DescribeDeltaHistorySuite "merge-metrics: delete-only with duplicates -
//   Partitioned = false, CDF = false" -- numTargetFilesAdded expected=1, actual=2.
//
// Mirrors MergeIntoMetricsBase's scenario. Goal: determine whether Comet's extra
// output file is a benign file-layout difference (delete result still correct) or a
// real bug. Asserts the delete RESULT is correct (data), and logs numTargetFilesAdded
// for native vs vanilla so we can see the file-count divergence directly.
class CometDeltaMergeMetricsReproSuite extends CometDeltaTestBase {

  test("delete-only MERGE with duplicate matches: native result correct; file-count observed") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")

    def numFilesAdded(tablePath: String): String =
      spark.sql(s"DESCRIBE HISTORY delta.`$tablePath`")
        .orderBy(org.apache.spark.sql.functions.col("version").desc)
        .select("operationMetrics")
        .head()
        .getMap[String, String](0)
        .getOrElse("numTargetFilesAdded", "<absent>")

    // Run the identical scenario with native scan enabled (default) and disabled, so we
    // can compare both the result rows and the file count.
    // Use a var to capture the result: withSQLConf returns Unit on Spark 3.5 (Scala 2.12),
    // so it can't return the block value.
    def run(nativeEnabled: Boolean): (Array[Long], String) = {
      var result: (Array[Long], String) = (Array.empty[Long], "<none>")
      // nativeEnabled=false fully disables Comet (not just the native Delta scan) so the
      // "vanilla" leg is genuine Spark 3.5 -- isolating Comet's effect from version drift.
      withSQLConf(
        "spark.comet.enabled" -> nativeEnabled.toString,
        "spark.comet.exec.enabled" -> nativeEnabled.toString,
        "spark.comet.scan.deltaNative.enabled" -> nativeEnabled.toString) {
        withDeltaTable("merge_metrics") { tablePath =>
          val ss = spark
          import ss.implicits._
          // Target: 0..99 across 5 files, non-partitioned (matches the Delta test).
          spark.range(start = 0, end = 100, step = 1, numPartitions = 5)
            .toDF("id")
            .write.format("delta").save(tablePath)
          // Source: floor(id/2) for 50..149 -> ids 25..74, with duplicate matches.
          spark.range(start = 50, end = 150, step = 1, numPartitions = 2)
            .select(floor($"id" / 2).as("id"))
            .createOrReplaceTempView("merge_metrics_src")

          spark.sql(
            s"""MERGE INTO delta.`$tablePath` t
               |USING merge_metrics_src s
               |ON s.id = t.id
               |WHEN MATCHED THEN DELETE""".stripMargin)

          val rows = spark.read.format("delta").load(tablePath)
            .as[Long].collect().sorted
          result = (rows, numFilesAdded(tablePath))
        }
      }
      result
    }

    val (nativeRows, nativeFiles) = run(nativeEnabled = true)
    val (vanillaRows, vanillaFiles) = run(nativeEnabled = false)

    // The deleted rows are exactly the matched ids 25..74; survivors are 0..24 and 75..99.
    val expected = ((0L until 25L) ++ (75L until 100L)).toArray
    info(s"DIAG numTargetFilesAdded native=$nativeFiles vanilla=$vanillaFiles")
    info(s"DIAG result counts native=${nativeRows.length} vanilla=${vanillaRows.length} expected=${expected.length}")

    // Data correctness: native must delete exactly the right rows (== vanilla == expected).
    assert(
      nativeRows.sameElements(expected),
      s"native delete result wrong: ${nativeRows.length} rows, " +
        s"missing=${expected.toSet.diff(nativeRows.toSet).toSeq.sorted.take(10)} " +
        s"extra=${nativeRows.toSet.diff(expected.toSet).toSeq.sorted.take(10)}")
    assert(nativeRows.sameElements(vanillaRows), "native delete result differs from vanilla")
  }
}
