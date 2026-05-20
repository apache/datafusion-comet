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

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf

trait ShimCometSparkSessionExtensions {
  protected def getPushedAggregate(scan: ParquetScan): Option[Aggregation] = scan.pushedAggregate

  protected def supportsExtendedExplainInfo(qe: QueryExecution): Boolean = true

  protected val EXTENDED_EXPLAIN_PROVIDERS_KEY = SQLConf.EXTENDED_EXPLAIN_PROVIDERS.key

  def injectQueryStageOptimizerRuleShim(
      extensions: SparkSessionExtensions,
      rule: Rule[SparkPlan]): Unit = {
    extensions.injectQueryStageOptimizerRule(_ => rule)
  }

  // No-op on Spark >= 3.5. See the Spark 3.4 shim and
  // CometSpark34AqeDppFallbackRule's class docstring for why this shim exists.
  def injectPreSpark35QueryStagePrepRuleShim(
      extensions: SparkSessionExtensions,
      rule: Rule[SparkPlan]): Unit = {}
}
