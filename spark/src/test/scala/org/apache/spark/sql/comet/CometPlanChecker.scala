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

import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.{ColumnarToRowExec, InputAdapter, SparkPlan, WholeStageCodegenExec}

/**
 * Trait providing utilities to check if a Spark plan is fully running on Comet native operators.
 * Used by both CometTestBase and CometBenchmarkBase.
 */
trait CometPlanChecker {

  /**
   * Finds the first non-Comet operator in the plan, if any.
   *
   * @param plan
   *   The SparkPlan to check
   * @param excludedClasses
   *   Classes to exclude from the check (these are allowed to be non-Comet)
   * @return
   *   Some(operator) if a non-Comet operator is found, None otherwise
   */
  protected def findFirstNonCometOperator(
      plan: SparkPlan,
      excludedClasses: Class[_]*): Option[SparkPlan] = {
    val wrapped = wrapCometSparkToColumnar(plan)
    wrapped.foreach {
      case _: CometNativeScanExec | _: CometScanExec | _: CometBatchScanExec |
          _: CometIcebergNativeScanExec =>
      case _: CometSinkPlaceHolder | _: CometScanWrapper =>
      case _: CometColumnarToRowExec =>
      case _: CometSparkToColumnarExec =>
      case _: CometExec | _: CometShuffleExchangeExec =>
      case _: CometBroadcastExchangeExec =>
      case _: WholeStageCodegenExec | _: ColumnarToRowExec | _: InputAdapter =>
      case op if !excludedClasses.exists(c => c.isAssignableFrom(op.getClass)) =>
        return Some(op)
      case _ =>
    }
    None
  }

  /** Wraps the CometSparkToColumnar as ScanWrapper, so the child operators will not be checked */
  private def wrapCometSparkToColumnar(plan: SparkPlan): SparkPlan = {
    plan.transformDown {
      // don't care the native operators
      case p: CometSparkToColumnarExec => CometScanWrapper(null, p)
    }
  }
}
