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

package org.apache.comet

import org.scalatest.funsuite.AnyFunSuite

import org.apache.comet.ExpressionReference._

class ExpressionReferenceSuite extends AnyFunSuite {

  test("status symbols") {
    assert(Supported.symbol == "✅")
    assert(Planned.symbol == "🔜")
    assert(NotPlanned.symbol == "💤")
    assert(Unclassified.symbol == "🔜")
  }

  test("real statuses have distinct symbols") {
    assert(Set(Supported.symbol, Planned.symbol, NotPlanned.symbol).size == 3)
  }

  test("PlannedExpr rejects non-planned status") {
    assertThrows[IllegalArgumentException](PlannedExpr(Supported))
  }
}
