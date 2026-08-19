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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{FileSourceScanExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

import org.apache.comet.rules.CometScanContrib

/**
 * `CometScanContrib` service provider for Delta Lake.
 *
 * Registered with the JDK `ServiceLoader` via
 * `META-INF/services/org.apache.comet.rules.CometScanContrib`, which is packaged into
 * `comet-spark.jar` only when Maven is invoked with `-Pcontrib-delta`. On a default build the
 * service file is absent, so core's registry is empty and none of this class is reachable --
 * core holds no compile-time reference to it and names no Delta type.
 *
 * Deliberately a `class` (not a Scala `object`): `ServiceLoader` instantiates providers through a
 * public no-arg constructor, which a Scala `object` does not expose. Same shape as the Delta
 * contrib's `PlanDataInjector` provider.
 *
 * Two of the trait's three hooks are overridden: [[tryTransformV1]] for ordinary Delta reads
 * (which reach Comet as V1 `FileSourceScanExec` nodes) and [[tryTransformRowScan]] for Change
 * Data Feed reads (`readChangeFeed`, a `RowDataSourceScanExec` over `DeltaCDFRelation`). The
 * trait's `tryTransformV2` default (`None`) leaves V2 scans to Comet's generic handling and to
 * any other registered contrib (e.g. Lance).
 *
 * All the real claim/decline logic lives in [[DeltaScanRule]] and [[CometDeltaNativeScan]]; this
 * is a thin adapter so that logic stays independently testable and free of any SPI plumbing.
 */
class DeltaScanRuleContrib extends CometScanContrib {

  override def tryTransformV1(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      relation: HadoopFsRelation): Option[SparkPlan] =
    DeltaScanRule.transformV1IfDelta(plan, session, scanExec, relation)

  /**
   * Claim a Delta Change Data Feed read. The relation is identified by class name (`DeltaCDFRelation`)
   * so this stays a cheap check on the far more common non-Delta row scans -- core hands us every
   * `RowDataSourceScanExec` in the plan, whatever its source.
   */
  override def tryTransformRowScan(scanExec: RowDataSourceScanExec): Option[SparkPlan] =
    if (isCdfRelation(scanExec.relation)) CometDeltaNativeScan.convertCdf(scanExec) else None

  /** True for Delta's Change Data Feed relation, produced by a `readChangeFeed` read. */
  private def isCdfRelation(relation: Any): Boolean =
    relation != null && relation.getClass.getName.contains("DeltaCDFRelation")
}
