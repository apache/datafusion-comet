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

import org.apache.comet.serde.OperatorOuterClass.Operator

class PlanDataInjectorSuite extends AnyFunSuite {

  test("injectPlanData leaves a non-scan operator tree unchanged") {
    // An operator with no injectable scan (here, an empty op_struct, but the same holds for
    // Filter/Projection/etc.) must pass through untouched. This exercises the O(1)
    // injectorsByKind miss path (`case _ =>`) that replaced the per-injector canInject walk.
    val child = Operator.newBuilder().setPlanId(2).build()
    val root = Operator.newBuilder().setPlanId(1).addChildren(child).build()

    val result = PlanDataInjector.injectPlanData(root, Map.empty, Map.empty)

    assert(result == root, "non-scan operator tree should be returned unchanged")
  }

  test("each registered injector is reachable by its opStructCase") {
    // The O(1) lookup keys injectors by opStructCase, so two injectors sharing a kind would
    // silently shadow one another in the map. Guard that every registered injector resolves back
    // to itself via its declared opStructCase (i.e. the kinds are distinct and the map is complete).
    val injectors = Seq(IcebergPlanDataInjector, NativeScanPlanDataInjector)
    val byKind = injectors.map(i => i.opStructCase -> i).toMap
    assert(byKind.size == injectors.size, "injectors must have distinct opStructCase keys")
    injectors.foreach { i =>
      assert(byKind(i.opStructCase) eq i)
    }
    assert(IcebergPlanDataInjector.opStructCase == Operator.OpStructCase.ICEBERG_SCAN)
    assert(NativeScanPlanDataInjector.opStructCase == Operator.OpStructCase.NATIVE_SCAN)
  }
}
