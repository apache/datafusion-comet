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

  test("PlannedExpr rejects Unclassified status") {
    assertThrows[IllegalArgumentException](PlannedExpr(Unclassified))
  }

  test("PlannedExpr allows Supported for non-serde paths") {
    PlannedExpr(Supported, note = Some("via rewrite"))
  }

  private val arrayAppend = FunctionEntry("array_append", "array_funcs", "x.ArrayAppend")
  private val arrayExcept = FunctionEntry("array_except", "array_funcs", "x.ArrayExcept")
  private val kurtosis = FunctionEntry("kurtosis", "agg_funcs", "x.Kurtosis")
  private val newThing = FunctionEntry("new_thing", "math_funcs", "x.NewThing")

  test("serde-backed with no compat content -> Supported, summary only") {
    val info = SerdeDocInfo(
      Some("Interval types fall back"),
      hasCompatContent = false,
      None,
      "arrayappend")
    val (row, warn) = resolveRow(arrayAppend, Some(info), None)
    assert(row.status == Supported)
    assert(row.notes == "Interval types fall back")
    assert(warn.isEmpty)
  }

  test("serde-backed with compat content -> Supported, summary + link") {
    val info = SerdeDocInfo(
      Some("Falls back by default"),
      hasCompatContent = true,
      Some("array"),
      "arrayexcept")
    val (row, _) = resolveRow(arrayExcept, Some(info), None)
    assert(
      row.notes ==
        "Falls back by default [details](compatibility/expressions/array.md#arrayexcept)")
  }

  test("serde-backed compat content but category has no compat page -> no link") {
    val info = SerdeDocInfo(None, hasCompatContent = true, None, "x")
    val (row, _) = resolveRow(arrayAppend, Some(info), None)
    assert(row.notes == "")
  }

  test("planned with issue -> Planned, issue link") {
    val (row, warn) = resolveRow(kurtosis, None, Some(PlannedExpr(Planned, issue = Some(4098))))
    assert(row.status == Planned)
    assert(row.notes == "[#4098](https://github.com/apache/datafusion-comet/issues/4098)")
    assert(warn.isEmpty)
  }

  test("not planned with note -> NotPlanned") {
    val (row, _) = resolveRow(kurtosis, None, Some(PlannedExpr(NotPlanned, note = Some("Niche"))))
    assert(row.status == NotPlanned)
    assert(row.notes == "Niche")
  }

  test("serde-backed compat content, category present, no summary -> bare link") {
    val info = SerdeDocInfo(None, hasCompatContent = true, Some("array"), "arrayexcept")
    val (row, _) = resolveRow(arrayExcept, Some(info), None)
    assert(row.status == Supported)
    assert(row.notes == "[details](compatibility/expressions/array.md#arrayexcept)")
  }

  test("unclassified -> placeholder row and warning") {
    val (row, warn) = resolveRow(newThing, None, None)
    assert(row.status == Unclassified)
    assert(row.notes == "unclassified; not yet reviewed")
    assert(warn.exists(_.contains("new_thing")))
    assert(warn.exists(_.contains("math_funcs")))
  }

  test("renderRow uses backticked name, status symbol, single-space padding") {
    assert(renderRow(ReferenceRow("any", Supported, "")) == "| `any` | ✅ |  |")
    assert(
      renderRow(ReferenceRow("kurtosis", Planned, "[#4098](u)")) ==
        "| `kurtosis` | 🔜 | [#4098](u) |")
  }

  test("renderTable sorts rows by name and emits header") {
    val rows = Seq(ReferenceRow("b", Supported, ""), ReferenceRow("a", Planned, "x"))
    val expected =
      """|| Function | Status | Notes |
        || --- | --- | --- |
        || `a` | 🔜 | x |
        || `b` | ✅ |  |""".stripMargin
    assert(renderTable(rows) == expected)
  }
}
