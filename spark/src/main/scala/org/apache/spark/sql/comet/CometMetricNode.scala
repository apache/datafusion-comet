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

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * A node carrying SQL metrics from SparkPlan, and metrics of its children. Native code will call
 * [[getChildNode]] and [[add]] to update the metrics.
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
   * Adds a value to a metric. Called from native.
   *
   * @param metricName
   *   the name of the metric at native operator.
   * @param v
   *   the value to add.
   */
  def add(metricName: String, v: Long): Unit = {
    metrics.get(metricName) match {
      case Some(metric) => metric.set(v)
      case None =>
        // no-op
        logDebug(s"Non-existing metric: $metricName. Ignored")
    }
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
