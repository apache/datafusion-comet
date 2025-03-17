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

import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}

trait ShimCometSparkSessionExtensions {
  /**
   * TODO: delete after dropping Spark 3.x support and directly call
   *       SQLConf.EXTENDED_EXPLAIN_PROVIDERS.key
   */
  protected val EXTENDED_EXPLAIN_PROVIDERS_KEY = "spark.sql.extendedExplainProviders"

  // Extended info is available only since Spark 4.0.0
  // (https://issues.apache.org/jira/browse/SPARK-47289)
  def supportsExtendedExplainInfo(qe: QueryExecution): Boolean = {
    try {
      // Look for QueryExecution.extendedExplainInfo(scala.Function1[String, Unit], SparkPlan)
      qe.getClass.getDeclaredMethod(
        "extendedExplainInfo",
        classOf[String => Unit],
        classOf[SparkPlan])
    } catch {
      case _: NoSuchMethodException | _: SecurityException => return false
    }
    true
  }
}
