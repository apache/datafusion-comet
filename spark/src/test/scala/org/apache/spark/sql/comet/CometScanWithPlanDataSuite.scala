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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Unit coverage for the core SPI that lets out-of-tree contrib leaf scans (e.g. the Delta
 * contrib's `CometDeltaNativeScanExec`) participate in Comet native planning without core holding
 * a compile-time reference to them: the [[CometScanWithPlanData]] trait contract and the
 * reflective `DeltaPlanDataInjector` slot in [[PlanDataInjector]]'s registry.
 *
 * Deliberately does not exercise the per-op injector registry mechanics; that surface is owned by
 * `PlanDataInjectorSuite`.
 */
class CometScanWithPlanDataSuite extends AnyFunSuite {

  /** Minimal trait implementation that opts into none of the optional DPP behaviour. */
  private class StubScan extends CometScanWithPlanData {
    override def sourceKey: String = "stub-key"
    override def commonData: Array[Byte] = Array.emptyByteArray
    override def perPartitionData: Array[Array[Byte]] = Array.empty
  }

  test(
    "CometScanWithPlanData defaults: no DPP filters and withDynamicPruningFilters is unsupported") {
    val scan = new StubScan
    assert(scan.dynamicPruningFilters == Nil, "a scan must expose no DPP filters by default")
    // The default exists only so the DPP rule never calls it for scans that leave
    // dynamicPruningFilters empty; if a scan does expose filters it must override this.
    val ex = intercept[UnsupportedOperationException] {
      scan.withDynamicPruningFilters(Seq.empty[Expression])
    }
    assert(
      ex.getMessage.contains("withDynamicPruningFilters"),
      s"default failure must name the un-overridden method, got: ${ex.getMessage}")
  }

  test("contrib DeltaPlanDataInjector slot is absent in default builds") {
    // PlanDataInjector appends a reflective `DeltaPlanDataInjector$` slot to its registry only
    // when the contrib-delta build bundled it. A default build must not contain the class, so the
    // reflective lookup degrades to a plain ClassNotFoundException (-> None) and the registry keeps
    // only its built-in injectors. (That the registry still passes non-scan operator trees through
    // unchanged is covered by `PlanDataInjectorSuite`, which exercises the post-registry behaviour
    // directly; this suite asserts only the contract A.1 adds -- the slot's optional presence.)
    val deltaSlotAbsent =
      try {
        // scalastyle:off classforname
        Class.forName("org.apache.spark.sql.comet.DeltaPlanDataInjector$")
        // scalastyle:on classforname
        false
      } catch {
        case _: ClassNotFoundException => true
      }
    assert(
      deltaSlotAbsent,
      "default (non -Pcontrib-delta) build must not carry the contrib DeltaPlanDataInjector")
  }
}
