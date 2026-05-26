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

package org.apache.comet.iceberg

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.SparkSession

/**
 * Spark 4.x -- `org.apache.spark.sql.SparkSession` is the api-module interface (no `sharedState`
 * for use with `CacheManager`), so we resolve the active classic session, whose `sharedState`
 * exposes the concrete `CacheManager` whose `recacheByPlan` expects classic.
 */
private[iceberg] object IcebergRefreshCacheShim {
  def recacheByPlan(plan: LogicalPlan): Unit = {
    val session = SparkSession.active
    session.sharedState.cacheManager.recacheByPlan(session, plan)
  }
}
