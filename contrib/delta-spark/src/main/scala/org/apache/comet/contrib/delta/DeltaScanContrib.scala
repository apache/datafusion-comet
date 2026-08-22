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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.comet.CometDeltaNativeScanExec
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

import org.apache.comet.CometConf.COMET_EXEC_ENABLED
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.rules.CometScanContrib

/**
 * Claims DSv1 Delta Lake scans for native execution, discovered by core's ServiceLoader (see
 * `META-INF/services/org.apache.comet.rules.CometScanContrib`). Scans this contrib owns but
 * cannot handle are claimed with a tagged fallback reason (per the `CometScanContrib` ownership
 * contract); Spark's Delta reader then handles them.
 *
 * The produced `CometDeltaNativeScanExec` is fully converted at claim time, so this contrib does
 * NOT use `CometContribScanMarker` (which exists for planning-time nodes that `CometExecRule`
 * converts later; mixing it in here would convert the node a second time).
 */
class DeltaScanContrib extends CometScanContrib with Logging {

  override def tryTransformV1(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      relation: HadoopFsRelation): Option[SparkPlan] = {
    // Not a Delta scan: not ours; core handles it exactly as before.
    if (!DeltaScanSupport.isDeltaScan(scanExec)) {
      return None
    }

    // Contrib scans are native-exec nodes, so like core's own nativeScan they require
    // COMET_EXEC_ENABLED. Our old core-side hook gated all extensions centrally; the
    // CometScanContrib call site does not, so the gate lives here. Silent None (no tag)
    // preserves the old "never consulted" behavior and avoids double-tagging next to
    // core's own exec-disabled fallback reason.
    if (!COMET_EXEC_ENABLED.get()) {
      return None
    }

    if (!DeltaScanConf.scanEnabled) {
      // Deliberate deviation from the "own but cannot handle => claim" contract: a
      // user-disabled contrib must be fully inert (the jar alone changes nothing) and must
      // not shadow another registered Delta contrib. Tag the opt-in hint for EXPLAIN, pass.
      withFallbackReason(
        scanExec,
        "Native Delta scan not enabled: set " +
          s"${DeltaScanConf.COMET_DELTA_NATIVE_ENABLED.key}=true to opt in")
      return None
    }

    // Built before declineReason (rather than only on claim) so the multi-object-store gate
    // can inspect the scan's selected files without listing them twice; convert reuses this
    // same helper on a claim.
    val scanHelper =
      CometDeltaNativeScanExec.planningHelper(scanExec, scanExec.partitionFilters)
    DeltaScanSupport.declineReason(plan, scanExec, scanHelper) match {
      case Some(reason) =>
        Some(withFallbackReason(scanExec, reason))
      case None =>
        CometDeltaNativeScan.convert(scanExec, scanHelper) match {
          case Some(nativeOp) =>
            logDebug(
              s"COMET-DELTA-CLAIM required=${scanExec.requiredSchema.map(_.name).mkString(",")} " +
                s"output=${scanExec.output.map(_.name).mkString(",")} " +
                s"dvShape=${CometDeltaNativeScan.isDvShape(scanExec)} " +
                s"planRoot=${plan.getClass.getSimpleName}")
            val subqueryDataFilters =
              CometDeltaNativeScan.subqueryFiltersFromParent(plan, scanExec)
            Some(CometDeltaNativeScanExec(nativeOp, scanExec, subqueryDataFilters))
          case None =>
            Some(
              withFallbackReason(
                scanExec,
                "Native Delta scan does not support the scan's output data types"))
        }
    }
  }
}
