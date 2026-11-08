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
import org.apache.spark.sql.internal.{ReadOnlySQLConf, SQLConf}

import org.apache.comet.CometConf

/**
 * The session's `spark.comet.*` configs, captured on the driver. Spark passes them to tasks as
 * local properties only inside a SQL execution, so jobs launched from `Dataset.rdd` or
 * `QueryExecution.toRdd` read Comet defaults in every stage before the last, which
 * `SQLExecutionRDD` covers. [[withConf]] fills that gap the same way.
 */
class CapturedSQLConf private (configs: Map[String, String]) extends Serializable {

  @transient private lazy val executorSideConf: SQLConf = {
    val conf = new SQLConf()
    configs.foreach { case (key, value) => conf.setConfString(key, value) }
    conf
  }

  /**
   * Runs `f` with the captured configs as `SQLConf.get`, unless this is not a task thread, a conf
   * is already set on it, or Spark passed the task the session's configs (it passes all or none).
   */
  def withConf[T](f: => T): T = {
    val context = TaskContext.get()
    if (configs.isEmpty || context == null || !SQLConf.get.isInstanceOf[ReadOnlySQLConf] ||
      configs.keys.exists(context.getLocalProperty(_) != null)) {
      f
    } else {
      SQLConf.withExistingConf(executorSideConf)(f)
    }
  }
}

object CapturedSQLConf {

  /** Captures the active session's Comet configs. Call it on the driver. */
  def capture(): CapturedSQLConf =
    new CapturedSQLConf(SQLConf.get.getAllConfs.filter(_._1.startsWith(CometConf.COMET_PREFIX)))
}
