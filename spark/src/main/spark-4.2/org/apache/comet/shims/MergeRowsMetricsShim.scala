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

package org.apache.comet.shims

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.metric.{SQLLastAttemptMetric, SQLLastAttemptMetrics, SQLMetric}

/** Spark 4.2 uses last-attempt accumulators for semantic MERGE summaries. */
object MergeRowsMetricsShim {
  private val metricNames = Seq(
    "numTargetRowsCopied" -> "number of target rows copied",
    "numTargetRowsDeleted" -> "number of target rows deleted",
    "numTargetRowsUpdated" -> "number of target rows updated",
    "numTargetRowsInserted" -> "number of target rows inserted",
    "numTargetRowsMatchedUpdated" -> "number of matched target rows updated",
    "numTargetRowsMatchedDeleted" -> "number of matched target rows deleted",
    "numTargetRowsNotMatchedBySourceUpdated" -> "number of not matched by source target rows updated",
    "numTargetRowsNotMatchedBySourceDeleted" -> "number of not matched by source target rows deleted")

  def metrics(sc: SparkContext): Map[String, SQLMetric] =
    metricNames.map { case (key, name) =>
      key -> SQLLastAttemptMetrics.createMetric(sc, name)
    }.toMap

  def value(metric: SQLMetric): Long = metric match {
    case lastAttempt: SQLLastAttemptMetric =>
      lastAttempt.lastAttemptValueForHighestRDDId().getOrElse(lastAttempt.value)
    case other => other.value
  }
}
