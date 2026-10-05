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

package org.apache.comet.exec

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometInMemoryTableScanExec
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.columnar.CometInMemoryRelationHelper
import org.apache.spark.sql.execution.metric.SQLLastAttemptMetrics

import org.apache.comet.CometConf

/** Spark 4.2's last-attempt metrics over a relation cached in Comet's format. */
class CometInMemoryCacheLastAttemptMetricSuite extends CometTestBase {

  import testImplicits._

  override protected def beforeAll(): Unit = {
    CometInMemoryRelationHelper.clearSerializer()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      CometInMemoryRelationHelper.clearSerializer()
    }
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.plugins", "org.apache.spark.CometPlugin")
    .set(
      "spark.sql.cache.serializer",
      "org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer")

  test("a last-attempt metric outside the cache keeps its value") {
    // Spark finds the metric's stages by walking the plan and its subqueries, and gives up on a
    // shuffle it does not know. A Comet shuffle inside the cached plan must stay out of that walk.
    // https://github.com/apache/datafusion-comet/pull/6577#discussion_r4174502176
    withSQLConf(CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {
      val metric = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "rows")
      val cached = spark.range(0, 100, 1, 2).repartition(2).cache()
      cached.count()
      val df = cached.map { id => metric.add(1); id }
      df.collect()
      val plan = df.queryExecution.executedPlan
      val scans = collect(plan) { case s: CometInMemoryTableScanExec => s }
      assert(scans.size == 1, plan)
      val cachedPlan = scans.head.originalPlan.relation.cachedPlan
      assert(collect(cachedPlan) { case s: CometShuffleExchangeExec => s }.nonEmpty, cachedPlan)
      assert(metric.lastAttemptValueForDataset(df) == Some(100L))
    }
  }
}
