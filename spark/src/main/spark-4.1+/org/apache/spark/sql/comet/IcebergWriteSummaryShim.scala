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

import org.apache.spark.sql.connector.write.{BatchWrite, MergeSummaryImpl, WriterCommitMessage}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.MergeRowsExec

/**
 * Spark 4.1 collects MERGE metrics from the executed plan and hands them to
 * `BatchWrite.commit(messages, summary)`; Iceberg 1.11+ records them in the snapshot summary.
 */
private[comet] object IcebergWriteSummaryShim extends AdaptiveSparkPlanHelper {
  def commit(
      batchWrite: BatchWrite,
      messages: Array[WriterCommitMessage],
      query: SparkPlan): Unit = {
    collectFirst(query) { case m: MergeRowsExec => m } match {
      case Some(mergeRows) =>
        val metrics = mergeRows.metrics
        def metricValue(name: String): Long = metrics.get(name).map(_.value).getOrElse(-1L)
        batchWrite.commit(
          messages,
          MergeSummaryImpl(
            metricValue("numTargetRowsCopied"),
            metricValue("numTargetRowsDeleted"),
            metricValue("numTargetRowsUpdated"),
            metricValue("numTargetRowsInserted"),
            metricValue("numTargetRowsMatchedUpdated"),
            metricValue("numTargetRowsMatchedDeleted"),
            metricValue("numTargetRowsNotMatchedBySourceUpdated"),
            metricValue("numTargetRowsNotMatchedBySourceDeleted")))
      case None =>
        batchWrite.commit(messages)
    }
  }
}
