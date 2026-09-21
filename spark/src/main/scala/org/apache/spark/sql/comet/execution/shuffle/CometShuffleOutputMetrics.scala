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

package org.apache.spark.sql.comet.execution.shuffle

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleWriteMetricsReporter}

/**
 * Output statistics for one shuffle destination. Remote and replacement tasks update distinct
 * accumulators; only the successful destination publishes output statistics to the exchange.
 * Timing and operator metrics still account for the work done by both destinations.
 */
private[shuffle] final class CometShuffleOutputMetrics private (
    val metrics: Map[String, SQLMetric],
    @transient private val publishedMetrics: Map[String, SQLMetric],
    private val executionId: String)
    extends Serializable {

  def newDestination(sc: SparkContext): CometShuffleOutputMetrics =
    CometShuffleOutputMetrics.create(sc, publishedMetrics, executionId)

  def publish(sc: SparkContext): Unit = {
    metrics.foreach { case (name, metric) => publishedMetrics(name).set(metric.value) }
    if (executionId != null) {
      SQLMetrics.postDriverMetricUpdates(sc, executionId, publishedMetrics.values.toSeq)
    }
  }
}

private[shuffle] object CometShuffleOutputMetrics {
  import SQLShuffleWriteMetricsReporter.{SHUFFLE_BYTES_WRITTEN, SHUFFLE_RECORDS_WRITTEN}

  def apply(sc: SparkContext, metrics: Map[String, SQLMetric]): CometShuffleOutputMetrics = {
    val outputNames = Set("dataSize", SHUFFLE_BYTES_WRITTEN, SHUFFLE_RECORDS_WRITTEN)
    create(
      sc,
      metrics.filter { case (name, _) => outputNames.contains(name) },
      sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
  }

  private def create(
      sc: SparkContext,
      publishedMetrics: Map[String, SQLMetric],
      executionId: String): CometShuffleOutputMetrics = {
    val metrics = Map(
      "dataSize" -> SQLMetrics.createSizeMetric(sc, "data size"),
      SHUFFLE_BYTES_WRITTEN -> SQLMetrics.createSizeMetric(sc, "shuffle bytes written"),
      SHUFFLE_RECORDS_WRITTEN -> SQLMetrics.createMetric(sc, "shuffle records written"))
    new CometShuffleOutputMetrics(metrics, publishedMetrics, executionId)
  }
}
