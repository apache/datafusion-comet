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

package org.apache.spark

import org.apache.spark.metrics.source.Source

import com.codahale.metrics.{Counter, Gauge, MetricRegistry}

import org.apache.comet.CometCoverageStats

/**
 * Exposes following metrics (hooked from CometCoverageStats)
 *   - operators.native: Total operators executed natively
 *   - operators.spark: Total operators that fell back to Spark
 *   - queries.planned: Total queries processed
 *   - transitions: Total Spark-to-Comet transitions
 *   - acceleration.ratio: native / (native + spark)
 */
object CometSource extends Source {
  override val sourceName = "comet"
  override val metricRegistry = new MetricRegistry()

  val NATIVE_OPERATORS: Counter =
    metricRegistry.counter(MetricRegistry.name("operators", "native"))
  val SPARK_OPERATORS: Counter = metricRegistry.counter(MetricRegistry.name("operators", "spark"))
  val QUERIES_PLANNED: Counter = metricRegistry.counter(MetricRegistry.name("queries", "planned"))
  val TRANSITIONS: Counter = metricRegistry.counter(MetricRegistry.name("transitions"))

  metricRegistry.register(
    MetricRegistry.name("acceleration", "ratio"),
    new Gauge[Double] {
      override def getValue: Double = {
        val native = NATIVE_OPERATORS.getCount
        val total = native + SPARK_OPERATORS.getCount
        if (total > 0) native.toDouble / total else 0.0
      }
    })

  def recordStats(stats: CometCoverageStats): Unit = {
    NATIVE_OPERATORS.inc(stats.cometOperators)
    SPARK_OPERATORS.inc(stats.sparkOperators)
    TRANSITIONS.inc(stats.transitions)
    QUERIES_PLANNED.inc()
  }
}
