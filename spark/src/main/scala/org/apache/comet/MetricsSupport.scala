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

package org.apache.comet

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * A trait for Comet operators that support SQL metrics
 */
trait MetricsSupport {
  protected var metrics: Map[String, SQLMetric] = Map.empty

  def initMetrics(sparkContext: SparkContext): Map[String, SQLMetric] = {
    metrics = Map(
      "ParquetRowGroups" -> SQLMetrics.createMetric(
        sparkContext,
        "num of Parquet row groups read"),
      "ParquetNativeDecodeTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time spent in Parquet native decoding"),
      "ParquetNativeLoadTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time spent in loading Parquet native vectors"),
      "ParquetLoadRowGroupTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time spent in loading Parquet row groups"),
      "ParquetInputFileReadTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time spent in reading Parquet file from storage"),
      "ParquetInputFileReadSize" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "read size when reading Parquet file from storage (MB)"),
      "ParquetInputFileReadThroughput" -> SQLMetrics.createAverageMetric(
        sparkContext,
        "read throughput when reading Parquet file from storage (MB/sec)"))
    metrics
  }
}
