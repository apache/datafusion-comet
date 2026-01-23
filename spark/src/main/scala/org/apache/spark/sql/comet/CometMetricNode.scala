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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import org.apache.comet.serde.Metric

/**
 * A node carrying SQL metrics from SparkPlan, and metrics of its children. Native code will call
 * [[getChildNode]] and [[set]] to update the metrics.
 *
 * @param metrics
 *   the mapping between metric name of native operator to `SQLMetric` of Spark operator. For
 *   example, `numOutputRows` -> `SQLMetrics("numOutputRows")` means the native operator will
 *   update `numOutputRows` metric with the value of `SQLMetrics("numOutputRows")` in Spark
 *   operator.
 */
case class CometMetricNode(metrics: Map[String, SQLMetric], children: Seq[CometMetricNode])
    extends Logging {

  /**
   * Gets a child node. Called from native.
   */
  def getChildNode(i: Int): CometMetricNode = {
    if (i < 0 || i >= children.length) {
      // TODO: throw an exception, e.g. IllegalArgumentException, instead?
      return null
    }
    children(i)
  }

  /**
   * Update the value of a metric. This method will typically be called multiple times for the
   * same metric during multiple calls to executePlan.
   *
   * @param metricName
   *   the name of the metric at native operator.
   * @param v
   *   the value to set.
   */
  def set(metricName: String, v: Long): Unit = {
    metrics.get(metricName) match {
      case Some(metric) => metric.set(v)
      case None =>
        // no-op
        logDebug(s"Non-existing metric: $metricName. Ignored")
    }
  }

  private def set_all(metricNode: Metric.NativeMetricNode): Unit = {
    metricNode.getMetricsMap.forEach((name, value) => {
      set(name, value)
    })
    metricNode.getChildrenList.asScala.zip(children).foreach { case (child, childNode) =>
      childNode.set_all(child)
    }
  }

  def set_all_from_bytes(bytes: Array[Byte]): Unit = {
    val metricNode = Metric.NativeMetricNode.parseFrom(bytes)
    set_all(metricNode)
  }
}

object CometMetricNode {

  /**
   * The baseline SQL metrics for DataFusion `BaselineMetrics`.
   */
  def baselineMetrics(sc: SparkContext): Map[String, SQLMetric] = {
    Map(
      "output_rows" -> SQLMetrics.createMetric(sc, "number of output rows"),
      "elapsed_compute" -> SQLMetrics.createNanoTimingMetric(
        sc,
        "total time (in ms) spent in this operator"))
  }

  /**
   * Base SQL Metrics for ScanExec and BatchScanExec These are needed for various Spark systems to
   * function properly, and are calculated on the general iterator created by the scan operator,
   * regardless of which scan implementation is used.
   */
  def baseScanMetrics(sc: SparkContext): Map[String, SQLMetric] = {
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sc, "number of output rows"),
      "scanTime" -> SQLMetrics.createNanoTimingMetric(sc, "scan time"))
  }

  /**
   * Metrics specific to Parquet format in scan operators. This provides some statistics about the
   * read files, as well as some meta filtering occuring. These metrics are independent of the
   * native reader metrics.
   */
  def parquetScanMetrics(sc: SparkContext): Map[String, SQLMetric] = {
    Map(
      "ParquetRowGroups" -> SQLMetrics.createMetric(sc, "num of Parquet row groups read"),
      "ParquetNativeDecodeTime" -> SQLMetrics.createNanoTimingMetric(
        sc,
        "time spent in Parquet native decoding"),
      "ParquetNativeLoadTime" -> SQLMetrics.createNanoTimingMetric(
        sc,
        "time spent in loading Parquet native vectors"),
      "ParquetLoadRowGroupTime" -> SQLMetrics.createNanoTimingMetric(
        sc,
        "time spent in loading Parquet row groups"),
      "ParquetInputFileReadTime" -> SQLMetrics.createNanoTimingMetric(
        sc,
        "time spent in reading Parquet file from storage"),
      "ParquetInputFileReadSize" -> SQLMetrics.createSizeMetric(
        sc,
        "read size when reading Parquet file from storage (MB)"),
      "ParquetInputFileReadThroughput" -> SQLMetrics.createAverageMetric(
        sc,
        "read throughput when reading Parquet file from storage (MB/sec)"))
  }

  /**
   * SQL Metrics from the native Datafusion reader.
   */
  def nativeScanMetrics(sc: SparkContext): Map[String, SQLMetric] = {
    Map(
      "output_rows" -> SQLMetrics.createMetric(sc, "number of output rows"),
      "time_elapsed_opening" ->
        SQLMetrics.createNanoTimingMetric(sc, "Wall clock time elapsed for file opening"),
      "time_elapsed_scanning_until_data" ->
        SQLMetrics.createNanoTimingMetric(
          sc,
          "Wall clock time elapsed for file scanning + " +
            "first record batch of decompression + decoding"),
      "time_elapsed_scanning_total" ->
        SQLMetrics.createNanoTimingMetric(
          sc,
          "Elapsed wall clock time for for scanning " +
            "+ record batch decompression / decoding"),
      "time_elapsed_processing" ->
        SQLMetrics.createNanoTimingMetric(
          sc,
          "Wall clock time elapsed for data decompression + decoding"),
      "file_open_errors" ->
        SQLMetrics.createMetric(sc, "Count of errors opening file"),
      "file_scan_errors" ->
        SQLMetrics.createMetric(sc, "Count of errors scanning file"),
      "predicate_evaluation_errors" ->
        SQLMetrics.createMetric(sc, "Number of times the predicate could not be evaluated"),
      "row_groups_matched_bloom_filter" ->
        SQLMetrics.createMetric(
          sc,
          "Number of row groups whose bloom filters were checked and matched (not pruned)"),
      "row_groups_pruned_bloom_filter" ->
        SQLMetrics.createMetric(sc, "Number of row groups pruned by bloom filters"),
      "row_groups_matched_statistics" ->
        SQLMetrics.createMetric(
          sc,
          "Number of row groups whose statistics were checked and matched (not pruned)"),
      "row_groups_pruned_statistics" ->
        SQLMetrics.createMetric(sc, "Number of row groups pruned by statistics"),
      "bytes_scanned" ->
        SQLMetrics.createSizeMetric(sc, "Number of bytes scanned"),
      "pushdown_rows_pruned" ->
        SQLMetrics.createMetric(sc, "Rows filtered out by predicates pushed into parquet scan"),
      "pushdown_rows_matched" ->
        SQLMetrics.createMetric(sc, "Rows passed predicates pushed into parquet scan"),
      "row_pushdown_eval_time" ->
        SQLMetrics.createNanoTimingMetric(sc, "Time spent evaluating row-level pushdown filters"),
      "statistics_eval_time" ->
        SQLMetrics.createNanoTimingMetric(
          sc,
          "Time spent evaluating row group-level statistics filters"),
      "bloom_filter_eval_time" ->
        SQLMetrics.createNanoTimingMetric(sc, "Time spent evaluating row group Bloom Filters"),
      "page_index_rows_pruned" ->
        SQLMetrics.createMetric(sc, "Rows filtered out by parquet page index"),
      "page_index_rows_matched" ->
        SQLMetrics.createMetric(sc, "Rows passed through the parquet page index"),
      "page_index_eval_time" ->
        SQLMetrics.createNanoTimingMetric(sc, "Time spent evaluating parquet page index filters"),
      "metadata_load_time" ->
        SQLMetrics.createNanoTimingMetric(
          sc,
          "Time spent reading and parsing metadata from the footer"))
  }

  /**
   * SQL Metrics for DataFusion HashJoin
   */
  def hashJoinMetrics(sc: SparkContext): Map[String, SQLMetric] = {
    Map(
      "build_time" ->
        SQLMetrics.createNanoTimingMetric(sc, "Total time for collecting build-side of join"),
      "build_input_batches" ->
        SQLMetrics.createMetric(sc, "Number of batches consumed by build-side"),
      "build_input_rows" ->
        SQLMetrics.createMetric(sc, "Number of rows consumed by build-side"),
      "build_mem_used" ->
        SQLMetrics.createSizeMetric(sc, "Memory used by build-side"),
      "input_batches" ->
        SQLMetrics.createMetric(sc, "Number of batches consumed by probe-side"),
      "input_rows" ->
        SQLMetrics.createMetric(sc, "Number of rows consumed by probe-side"),
      "output_batches" -> SQLMetrics.createMetric(sc, "Number of batches produced"),
      "output_rows" -> SQLMetrics.createMetric(sc, "Number of rows produced"),
      "join_time" -> SQLMetrics.createNanoTimingMetric(sc, "Total time for joining"))
  }

  /**
   * SQL Metrics for DataFusion SortMergeJoin
   */
  def sortMergeJoinMetrics(sc: SparkContext): Map[String, SQLMetric] = {
    Map(
      "peak_mem_used" ->
        SQLMetrics.createSizeMetric(sc, "Memory used by build-side"),
      "input_batches" ->
        SQLMetrics.createMetric(sc, "Number of batches consumed by probe-side"),
      "input_rows" ->
        SQLMetrics.createMetric(sc, "Number of rows consumed by probe-side"),
      "output_batches" -> SQLMetrics.createMetric(sc, "Number of batches produced"),
      "output_rows" -> SQLMetrics.createMetric(sc, "Number of rows produced"),
      "join_time" -> SQLMetrics.createNanoTimingMetric(sc, "Total time for joining"),
      "spill_count" -> SQLMetrics.createMetric(sc, "Count of spills"),
      "spilled_bytes" -> SQLMetrics.createSizeMetric(sc, "Total spilled bytes"),
      "spilled_rows" -> SQLMetrics.createMetric(sc, "Total spilled rows"))
  }

  def shuffleMetrics(sc: SparkContext): Map[String, SQLMetric] = {
    Map(
      "elapsed_compute" -> SQLMetrics.createNanoTimingMetric(sc, "native shuffle writer time"),
      "repart_time" -> SQLMetrics.createNanoTimingMetric(sc, "repartition time"),
      "encode_time" -> SQLMetrics.createNanoTimingMetric(sc, "encoding and compression time"),
      "decode_time" -> SQLMetrics.createNanoTimingMetric(sc, "decoding and decompression time"),
      "spill_count" -> SQLMetrics.createMetric(sc, "number of spills"),
      "spilled_bytes" -> SQLMetrics.createSizeMetric(sc, "spilled bytes"),
      "input_batches" -> SQLMetrics.createMetric(sc, "number of input batches"))
  }

  /**
   * Creates a [[CometMetricNode]] from a [[CometPlan]].
   */
  def fromCometPlan(cometPlan: SparkPlan): CometMetricNode = {
    val children = cometPlan.children.map(fromCometPlan)
    CometMetricNode(cometPlan.metrics, children)
  }

  /**
   * Creates a [[CometMetricNode]] from a map of [[SQLMetric]].
   */
  def apply(metrics: Map[String, SQLMetric]): CometMetricNode = {
    CometMetricNode(metrics, Nil)
  }
}
