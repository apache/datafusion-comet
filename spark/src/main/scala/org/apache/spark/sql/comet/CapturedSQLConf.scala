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

import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.internal.SQLConf

/**
 * The SQL configs of the session that planned a Comet plan, captured on the driver so that the
 * plan's tasks read the same values however its job was launched.
 *
 * On an executor, `SQLConf.get` reads the task's local properties, which Spark fills with the
 * session's configs only for a job that runs inside a SQL execution. A job launched from a
 * query's RDD (`Dataset.rdd`, `QueryExecution.toRdd`) can run outside one, and Spark's
 * `SQLExecutionRDD` re-applies the session's configs only in the stage that computes it, the last
 * one. Without this, every earlier stage resolves each `spark.comet.*` setting to its default,
 * including the memory pool and the configs passed to the native plan.
 *
 * Works like `SQLExecutionRDD`: outside a SQL execution, [[withConf]] makes the captured configs
 * what `SQLConf.get` returns on the task thread while it runs. Inside one, the task's local
 * properties already carry the session's configs, and nothing changes.
 */
private[spark] class CapturedSQLConf private (configs: Map[String, String]) extends Serializable {

  @transient private lazy val executorSideConf: SQLConf = {
    val conf = new SQLConf()
    configs.foreach { case (key, value) => conf.setConfString(key, value) }
    conf
  }

  /**
   * Runs `f` with the captured configs as `SQLConf.get` on this thread, if it is a task thread
   * outside a SQL execution. Only reads made while `f` runs see them, so `f` has to read the
   * settings it needs, as the `CometExecIterator` constructor does.
   */
  def withConf[T](f: => T): T = {
    val context = TaskContext.get()
    if (context == null || context.getLocalProperty(SQLExecution.EXECUTION_ID_KEY) != null) {
      f
    } else {
      SQLConf.withExistingConf(executorSideConf)(f)
    }
  }
}

private[spark] object CapturedSQLConf {

  /** Captures the configs of the active session. Call it on the driver, while planning. */
  def capture(): CapturedSQLConf = new CapturedSQLConf(SQLConf.get.getAllConfs)
}
