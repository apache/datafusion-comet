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

import org.apache.comet.contrib.delta.DeltaContribScan
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * `PlanDataInjector` for a Delta scan carried in core's generic `OpStruct::ContribScan` envelope.
 *
 * The contrib serialises the Delta scan in two parts to keep the closure sent to every
 * task small:
 *   - At planning time `CometDeltaNativeScan.convert` emits a `DeltaScan` proto with
 *     the `common` block (schemas, table root, filters, ...) and NO tasks; this lands
 *     in the `Operator` tree packed into `OpStruct.contrib_scan`.
 *   - Per partition, `CometDeltaNativeScanExec` puts the partition's `DeltaScan`
 *     (tasks-only) bytes into `perPartitionByKey` under a `sourceKey` derived from
 *     the common block.
 *
 * Core's `PlanDataInjector` registry discovers this provider via the JDK `ServiceLoader`, from the
 * `META-INF/services/org.apache.spark.sql.comet.PlanDataInjector` resource packaged only under
 * `-Pcontrib-delta`. Default builds carry no service file, so the registry is exactly the
 * built-ins. Deliberately a `class`, not a Scala `object`: `ServiceLoader` instantiates providers
 * through a public no-arg constructor.
 *
 * Note that [[opStructCase]] is `CONTRIB_SCAN`, which EVERY contrib scan shares -- so the
 * `type_url` check in [[canInject]] (via `DeltaContribScan.isDeltaScan`) is what keeps this
 * injector off another contrib's operators. Core keys its registry by kind to a *list* of
 * injectors and picks the first whose `canInject` accepts, precisely so contribs can coexist here.
 *
 * Without this injection the native side decodes a tasks-empty `DeltaScan` -> `EmptyExec`
 * (0 rows) for every Delta scan.
 */
class DeltaPlanDataInjector extends PlanDataInjector {

  override val opStructCase: Operator.OpStructCase = Operator.OpStructCase.CONTRIB_SCAN

  override def canInject(op: Operator): Boolean = {
    // Shared envelope: reject anything that isn't OUR contrib scan before touching its payload.
    if (!DeltaContribScan.isDeltaScan(op)) return false
    // The common-only proto produced at planning time has zero tasks. After injection
    // the operator carries the partition's tasks -- skip those (idempotent canInject).
    //
    // Note: a CDF read always has zero tasks (it carries a version sub-range, not files), so this
    // stays true even after the CDF branch in `inject` runs. That's intentionally NOT idempotent-
    // guarded the way the task branch is, and it's safe because `PlanDataInjector.injectPlanData`
    // walks each operator exactly once per partition (CometExecRDD.compute -> one inject per op).
    DeltaContribScan.get(op).getTasksCount == 0
  }

  override def getKey(op: Operator): Option[String] =
    Some(CometDeltaNativeScanExec.computeSourceKey(op))

  override def inject(
      op: Operator,
      commonBytes: Array[Byte],
      partitionBytes: Array[Byte]): Operator = {
    // `partitionBytes` is the serialised `DeltaScan` that packs only this partition's
    // tasks (no common block) to avoid duplicating schemas across partitions. Splice
    // the partition's tasks into the original common-only envelope.
    val partitionScan = OperatorOuterClass.DeltaScan.parseFrom(partitionBytes)
    val originalScan = DeltaContribScan.get(op)
    val mergedScanBuilder = OperatorOuterClass.DeltaScan
      .newBuilder(originalScan)
      .addAllTasks(partitionScan.getTasksList)
    // CDF version-range split: a Change Data Feed read carries no tasks; instead the per-partition
    // DeltaScan packs this partition's inclusive cdf sub-range in a minimal common (cdf_read marks
    // it). Splice that [start, end] over the shared common's full range so each partition's native
    // TableChanges read covers only its slice. Regular (non-CDF) per-partition bytes set no common,
    // so this is skipped and only the task list is merged.
    if (partitionScan.hasCommon && partitionScan.getCommon.getCdfRead) {
      val pc = partitionScan.getCommon
      val mergedCommon = originalScan.getCommon.toBuilder
      mergedCommon.setCdfStartVersion(pc.getCdfStartVersion)
      if (pc.hasCdfEndVersion) mergedCommon.setCdfEndVersion(pc.getCdfEndVersion)
      else mergedCommon.clearCdfEndVersion()
      mergedScanBuilder.setCommon(mergedCommon.build())
    }
    DeltaContribScan.set(op.toBuilder, mergedScanBuilder.build()).build()
  }
}
