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

package org.apache.comet.contrib.delta

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

/**
 * Base for Delta contrib suites: CometTestBase plus the Delta Lake session extension and catalog.
 */
abstract class CometDeltaTestBase extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set(DeltaScanConf.COMET_DELTA_NATIVE_ENABLED.key, "true")
    conf
  }

  /** Collect nodes of the given simple class name anywhere in the (AQE-stripped) plan. */
  protected def collectByName(plan: SparkPlan, simpleName: String): Seq[SparkPlan] =
    collectWithSubqueries(stripAQEPlan(plan)) {
      case op if op.getClass.getSimpleName == simpleName => op
    }

  protected def deltaNativeScans(df: DataFrame): Seq[SparkPlan] =
    collectByName(df.queryExecution.executedPlan, "CometDeltaNativeScanExec")

  /** Assert the query ran through the native Delta scan AND matches the comet-off answer. */
  protected def checkDeltaNativeScanAnswer(df: DataFrame): Unit = {
    checkSparkAnswer(df)
    // Re-materialize the plan after execution so AQE has finalized stages.
    assert(
      deltaNativeScans(df).nonEmpty,
      s"Expected CometDeltaNativeScanExec in plan:\n${df.queryExecution.executedPlan}")
  }
}
