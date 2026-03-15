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

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.CometSource
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

object CometMetricsListener extends QueryExecutionListener {

  private val registered = new AtomicBoolean(false)

  def ensureRegistered(session: SparkSession): Unit = {
    if (registered.compareAndSet(false, true)) {
      session.listenerManager.register(this)
    }
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    val stats = CometCoverageStats.forPlan(qe.executedPlan)
    CometSource.recordStats(stats)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
}
