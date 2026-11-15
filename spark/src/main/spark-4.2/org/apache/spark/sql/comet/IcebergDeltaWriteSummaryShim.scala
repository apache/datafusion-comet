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

import org.apache.spark.sql.connector.write.{BatchWrite, DeleteSummaryImpl, UpdateSummaryImpl, WriterCommitMessage}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.metric.SQLMetric

import org.apache.comet.iceberg.{DeltaCommand, DeltaDelete, DeltaUpdate, IcebergSemanticMetricsShim}

/** Spark 4.2 DELETE/UPDATE summary handling for the JVM PositionDelta writer. */
private[comet] object IcebergDeltaWriteSummaryShim extends AdaptiveSparkPlanHelper {
  def commit(
      batchWrite: BatchWrite,
      messages: Array[WriterCommitMessage],
      query: SparkPlan,
      command: Option[DeltaCommand]): Boolean = command match {
    case Some(DeltaUpdate) =>
      deltaWriterMetrics(query).exists { metrics =>
        batchWrite.commit(
          messages,
          UpdateSummaryImpl(value(metrics, "numUpdatedRows"), value(metrics, "numCopiedRows")))
        true
      }
    case Some(DeltaDelete) =>
      deltaWriterMetrics(query).exists { metrics =>
        batchWrite.commit(
          messages,
          DeleteSummaryImpl(value(metrics, "numDeletedRows"), value(metrics, "numCopiedRows")))
        true
      }
    case _ => false
  }

  private def deltaWriterMetrics(query: SparkPlan): Option[Map[String, SQLMetric]] =
    collectFirst(query) { case writer: IcebergWriteExec => writer.metrics }

  private def value(metrics: Map[String, SQLMetric], name: String): Long =
    metrics.get(name).map(IcebergSemanticMetricsShim.value).getOrElse(-1L)
}
