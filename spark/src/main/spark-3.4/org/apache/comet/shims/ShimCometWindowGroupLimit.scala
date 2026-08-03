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

import org.apache.spark.sql.comet.CometWindowGroupLimit.Fields
import org.apache.spark.sql.execution.SparkPlan

/**
 * Spark 3.4 does not have `WindowGroupLimitExec` (introduced by SPARK-37099 in Spark 3.5). This
 * no-op shim keeps the shared Comet code compiling against Spark 3.4 while ensuring the operator
 * is never registered for conversion on this profile.
 */
object ShimCometWindowGroupLimit {

  /**
   * The `WindowGroupLimitExec` class object on Spark 3.5+, or `None` on Spark 3.4.
   * `CometExecRule` uses this to conditionally register the serde in `nativeExecs`.
   */
  def windowGroupLimitClass: Option[Class[_ <: SparkPlan]] = None

  /** Extract WGL fields when `op` is a `WindowGroupLimitExec` (Spark 3.5+). */
  def extract(op: SparkPlan): Option[Fields] = None
}
