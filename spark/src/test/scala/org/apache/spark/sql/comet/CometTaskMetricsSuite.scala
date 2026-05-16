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

package org.apache.spark.sql.comet

import java.io.File

import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.executor.ShuffleReadMetrics
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.execution.shuffle.CometNativeShuffle
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark41Plus

class CometTaskMetricsSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.ui.enabled", "true")
  }

  import testImplicits._

  test("per-task native shuffle metrics") {
    withParquetTable((0 until 10000).map(i => (i, (i + 1).toLong)), "tbl") {
      val df = sql("SELECT * FROM tbl").sortWithinPartitions($"_1".desc)
      val shuffled = df.repartition(1, $"_1")

      val cometShuffle = find(shuffled.queryExecution.executedPlan) {
        case _: CometShuffleExchangeExec => true
        case _ => false
      }
      assert(cometShuffle.isDefined, "CometShuffleExchangeExec not found in the plan")
      assert(
        cometShuffle.get.asInstanceOf[CometShuffleExchangeExec].shuffleType == CometNativeShuffle)

      val shuffleWriteMetricsList = mutable.ArrayBuffer.empty[ShuffleWriteMetrics]
      val shuffleReadMetricsList = mutable.ArrayBuffer.empty[ShuffleReadMetrics]

      spark.sparkContext.addSparkListener(new SparkListener {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          val taskMetrics = taskEnd.taskMetrics

          if (taskEnd.taskType.contains("ShuffleMapTask")) {
            val shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics
            shuffleWriteMetricsList.synchronized {
              shuffleWriteMetricsList += shuffleWriteMetrics
            }
          } else {
            val shuffleReadMetrics = taskMetrics.shuffleReadMetrics
            shuffleReadMetricsList.synchronized {
              shuffleReadMetricsList += shuffleReadMetrics
            }
          }
        }
      })

      // Avoid receiving earlier taskEnd events
      spark.sparkContext.listenerBus.waitUntilEmpty()

      // Run the action to trigger the shuffle
      shuffled.collect()

      spark.sparkContext.listenerBus.waitUntilEmpty()

      // Check the shuffle write and read metrics
      assert(shuffleWriteMetricsList.nonEmpty, "No shuffle write metrics found")
      shuffleWriteMetricsList.foreach { metrics =>
        assert(metrics.writeTime > 0)
        assert(metrics.bytesWritten > 0)
        assert(metrics.recordsWritten > 0)
      }

      assert(shuffleReadMetricsList.nonEmpty, "No shuffle read metrics found")
      shuffleReadMetricsList.foreach { metrics =>
        assert(metrics.recordsRead > 0)
        assert(metrics.totalBytesRead > 0)
      }
    }
  }

  test("native parquet write reports task-level output metrics") {
    withParquetTable((0 until 5000).map(i => (i, (i + 1).toLong)), "tbl") {
      withTempPath { dir =>
        val outPath = new File(dir, "written").getAbsolutePath
        val expectedRows = 5000L
        val outputBytes = mutable.ArrayBuffer.empty[Long]
        val outputRecords = mutable.ArrayBuffer.empty[Long]
        val targetStageIds = mutable.HashSet.empty[Int]
        val jobGroupId = s"native-write-metrics-${java.util.UUID.randomUUID().toString}"

        val listener = new SparkListener {
          override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
            val isTargetJob = Option(jobStart.properties)
              .flatMap(props => Option(props.getProperty(SparkContext.SPARK_JOB_GROUP_ID)))
              .contains(jobGroupId)
            if (isTargetJob) {
              targetStageIds.synchronized {
                targetStageIds ++= jobStart.stageInfos.map(_.stageId)
              }
            }
          }

          override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
            val isTargetStage = targetStageIds.synchronized {
              targetStageIds.contains(taskEnd.stageId)
            }
            if (isTargetStage) {
              val om = taskEnd.taskMetrics.outputMetrics
              if (om.bytesWritten > 0) {
                outputBytes.synchronized {
                  outputBytes += om.bytesWritten
                  outputRecords += om.recordsWritten
                }
              }
            }
          }
        }
        spark.sparkContext.addSparkListener(listener)

        try {
          spark.sparkContext.listenerBus.waitUntilEmpty()

          withSQLConf(
            CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.getOperatorAllowIncompatConfigKey(
              classOf[DataWritingCommandExec]) -> "true",
            SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Halifax") {
            spark.sparkContext.setJobGroup(jobGroupId, "native parquet write output metrics")
            try {
              sql("SELECT * FROM tbl").write.parquet(outPath)
            } finally {
              spark.sparkContext.clearJobGroup()
            }
          }

          spark.sparkContext.listenerBus.waitUntilEmpty()

          assert(outputBytes.nonEmpty, "No task reported outputMetrics.bytesWritten")
          val totalOutputBytes = outputBytes.sum
          val totalOutputRecords = outputRecords.sum

          assert(
            totalOutputRecords == expectedRows,
            s"recordsWritten mismatch: metrics=$totalOutputRecords, expected=$expectedRows")

          val outputDir = new File(outPath)
          val fileBytes = Option(outputDir.listFiles())
            .getOrElse(Array.empty)
            .filter(f => f.isFile && f.getName.startsWith("part-"))
            .map(_.length())
            .sum

          assert(fileBytes > 0L, s"Expected written parquet bytes should be > 0, got $fileBytes")
          val ratio = totalOutputBytes.toDouble / fileBytes.toDouble
          assert(
            ratio >= 0.7 && ratio <= 1.3,
            s"bytesWritten ratio out of range: metrics=$totalOutputBytes, files=$fileBytes, ratio=$ratio")
        } finally {
          spark.sparkContext.removeSparkListener(listener)
        }
      }
    }
  }

  test("native_datafusion scan reports task-level input metrics matching Spark") {
    val totalRows = 10000
    withTempPath { dir =>
      spark
        .createDataFrame((0 until totalRows).map(i => (i, s"elem_$i")))
        .repartition(5)
        .write
        .parquet(dir.getAbsolutePath)
      spark.read.parquet(dir.getAbsolutePath).createOrReplaceTempView("tbl")
      // Collect baseline input metrics from vanilla Spark (Comet disabled)
      val (sparkBytes, sparkRecords, _) =
        collectInputMetrics(
          "SELECT * FROM tbl where _1 > 2000",
          CometConf.COMET_ENABLED.key -> "false")

      // Collect input metrics from Comet native_datafusion scan.
      val (cometBytes, cometRecords, cometPlan) = collectInputMetrics(
        "SELECT * FROM tbl where _1 > 2000",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION)

      // Verify the plan actually used CometNativeScanExec
      assert(
        find(cometPlan)(_.isInstanceOf[CometNativeScanExec]).isDefined,
        s"Expected CometNativeScanExec in plan:\n${cometPlan.treeString}")

      assert(sparkRecords > 0, s"Spark outputRecords should be > 0, got $sparkRecords")
      assert(cometRecords > 0, s"Comet outputRecords should be > 0, got $cometRecords")

      assert(
        cometRecords == sparkRecords,
        s"recordsRead mismatch: comet=$cometRecords, sparkRecords=$sparkRecords")

      // Bytes should be in the same ballpark -- both read the same Parquet file(s),
      // but the exact byte count can differ due to reader implementation details
      // (e.g. footer reads, page headers, buffering granularity).
      assert(sparkBytes > 0, s"Spark bytesRead should be > 0, got $sparkBytes")
      assert(cometBytes > 0, s"Comet bytesRead should be > 0, got $cometBytes")
      assertCometBytesReadInRange(cometBytes, sparkBytes)
    }
  }

  test("input metrics aggregate across multiple native scans in a join") {
    withTempPath { dir1 =>
      withTempPath { dir2 =>
        // Create two separate parquet tables
        spark
          .createDataFrame((0 until 5000).map(i => (i, s"left_$i")))
          .repartition(3)
          .write
          .parquet(dir1.getAbsolutePath)
        spark
          .createDataFrame((0 until 5000).map(i => (i, s"right_$i")))
          .repartition(3)
          .write
          .parquet(dir2.getAbsolutePath)

        spark.read.parquet(dir1.getAbsolutePath).createOrReplaceTempView("left_tbl")
        spark.read.parquet(dir2.getAbsolutePath).createOrReplaceTempView("right_tbl")

        val joinQuery = "SELECT * FROM left_tbl JOIN right_tbl ON left_tbl._1 = right_tbl._1"

        // Collect baseline from vanilla Spark
        val (sparkBytes, sparkRecords, _) =
          collectInputMetrics(joinQuery, CometConf.COMET_ENABLED.key -> "false")

        // Collect from Comet native scan
        val (cometBytes, cometRecords, cometPlan) = collectInputMetrics(
          joinQuery,
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION)

        // Verify the plan has multiple CometNativeScanExec nodes
        val scanCount = collect(cometPlan) { case s: CometNativeScanExec =>
          s
        }.size
        assert(
          scanCount >= 2,
          s"Expected at least 2 CometNativeScanExec in plan, found $scanCount:\n" +
            cometPlan.treeString)

        assert(sparkBytes > 0, s"Spark bytesRead should be > 0, got $sparkBytes")
        assert(cometBytes > 0, s"Comet bytesRead should be > 0, got $cometBytes")
        assert(sparkRecords > 0, s"Spark recordsRead should be > 0, got $sparkRecords")
        assert(cometRecords > 0, s"Comet recordsRead should be > 0, got $cometRecords")

        // Both sides should contribute to the total bytes
        assertCometBytesReadInRange(cometBytes, sparkBytes)
      }
    }
  }

  /**
   * Compare Comet's `bytesRead` against Spark's baseline. On Spark <= 4.0 the two readers report
   * the same Hadoop-FS thread-local byte count, so we keep a tight 0.7-1.3 band. Spark 4.1
   * pre-opens the parquet `SeekableInputStream` outside the FileScanRDD `compute()` thread, so
   * its `getFSBytesReadOnThreadCallback` no longer captures most of the parquet IO and
   * `inputMetrics.bytesRead` is now a small fraction of the actual file IO. Comet (via
   * DataFusion's `bytes_scanned`) still reports actual bytes, so the only safe cross-version
   * invariant on 4.1+ is that Comet >= Spark and both are positive.
   */
  private def assertCometBytesReadInRange(cometBytes: Long, sparkBytes: Long): Unit = {
    if (isSpark41Plus) {
      assert(
        cometBytes >= sparkBytes,
        s"Comet bytesRead should be >= Spark bytesRead on 4.1+: comet=$cometBytes, spark=$sparkBytes")
    } else {
      val ratio = cometBytes.toDouble / sparkBytes.toDouble
      assert(
        ratio >= 0.7 && ratio <= 1.3,
        s"bytesRead ratio out of range: comet=$cometBytes, spark=$sparkBytes, ratio=$ratio")
    }
  }

  test("input metrics aggregate across multiple native scans in a union") {
    withTempPath { dir1 =>
      withTempPath { dir2 =>
        spark
          .createDataFrame((0 until 5000).map(i => (i, s"left_$i")))
          .repartition(3)
          .write
          .parquet(dir1.getAbsolutePath)
        spark
          .createDataFrame((5000 until 10000).map(i => (i, s"right_$i")))
          .repartition(3)
          .write
          .parquet(dir2.getAbsolutePath)

        spark.read.parquet(dir1.getAbsolutePath).createOrReplaceTempView("union_left")
        spark.read.parquet(dir2.getAbsolutePath).createOrReplaceTempView("union_right")

        val unionQuery = "SELECT * FROM union_left UNION ALL SELECT * FROM union_right"

        // Collect baseline from vanilla Spark
        val (sparkBytes, sparkRecords, _) =
          collectInputMetrics(unionQuery, CometConf.COMET_ENABLED.key -> "false")

        // Collect from Comet native scan
        val (cometBytes, cometRecords, cometPlan) = collectInputMetrics(
          unionQuery,
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION)

        // Verify the plan has multiple CometNativeScanExec nodes
        val scanCount = collect(cometPlan) { case s: CometNativeScanExec =>
          s
        }.size
        assert(
          scanCount >= 2,
          s"Expected at least 2 CometNativeScanExec in plan, found $scanCount:\n" +
            cometPlan.treeString)

        assert(sparkBytes > 0, s"Spark bytesRead should be > 0, got $sparkBytes")
        assert(cometBytes > 0, s"Comet bytesRead should be > 0, got $cometBytes")
        assert(sparkRecords > 0, s"Spark recordsRead should be > 0, got $sparkRecords")
        assert(cometRecords > 0, s"Comet recordsRead should be > 0, got $cometRecords")

        assertCometBytesReadInRange(cometBytes, sparkBytes)
      }
    }
  }

  /**
   * Runs the given query with the given SQL config overrides and returns the aggregated
   * (bytesRead, recordsRead) across all tasks, along with the executed plan.
   *
   * Uses AppStatusStore (same source as Spark UI) to read task-level input metrics.
   * AppStatusStore stores immutable snapshots of metric values, unlike SparkListener's
   * InputMetrics which are backed by mutable accumulators that can be reset.
   */
  private def collectInputMetrics(
      query: String,
      confs: (String, String)*): (Long, Long, SparkPlan) = {
    val store = spark.sparkContext.statusStore

    // Record existing stage IDs so we only look at stages from our query
    val stagesBefore = store.stageList(null).map(_.stageId).toSet

    var plan: SparkPlan = null
    withSQLConf(confs: _*) {
      val df = sql(query)
      df.collect()
      plan = stripAQEPlan(df.queryExecution.executedPlan)
    }

    // Wait for listener bus to flush all events into the status store
    spark.sparkContext.listenerBus.waitUntilEmpty()

    // Sum input metrics from stages created by our query
    val newStages = store.stageList(null).filterNot(s => stagesBefore.contains(s.stageId))
    assert(newStages.nonEmpty, s"No new stages found for confs=$confs")

    val totalBytes = newStages.map(_.inputBytes).sum
    val totalRecords = newStages.map(_.inputRecords).sum

    (totalBytes, totalRecords, plan)
  }
}
