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

/**
 * Pure helpers for generating the Spark expression reference table (`expressions.md`). No file IO
 * or SparkSession dependency, so it is unit-testable in isolation. The impure parts (enumerating
 * Spark's FunctionRegistry, reading the serde maps, writing files) live in [[GenerateDocs]].
 */
object ExpressionReference {

  /** Status shown in the reference table. */
  sealed trait ExprStatus { def symbol: String }
  case object Supported extends ExprStatus { override val symbol = "✅" }
  case object Planned extends ExprStatus { override val symbol = "🔜" }
  case object NotPlanned extends ExprStatus { override val symbol = "💤" }

  /**
   * A built-in that is neither serde-backed nor listed in `plannedExpressions`. Rendered with a
   * warning and the same "🔜" glyph as [[Planned]] on purpose: an unclassified function is
   * treated as provisionally planned in the table, and its Notes cell ("unclassified; not yet
   * reviewed") is what distinguishes it from a deliberately planned entry.
   */
  case object Unclassified extends ExprStatus { override val symbol = "🔜" }

  /**
   * Curated metadata for a function Comet does not serde-support. Lives in the
   * `plannedExpressions` map in [[GenerateDocs]] (a CI-exempt file). `status` must be `Planned`
   * or `NotPlanned`.
   */
  case class PlannedExpr(
      status: ExprStatus,
      issue: Option[Int] = None,
      note: Option[String] = None) {
    require(
      status == Planned || status == NotPlanned,
      s"PlannedExpr.status must be Planned or NotPlanned, got $status")
  }

  /**
   * Serde-derived doc facts for one expression class. `hasCompatContent` is true when the serde
   * produces any compat-guide content (a non-empty getCompatibleNotes / getIncompatibleReasons /
   * getUnsupportedReasons), which is what gates whether the reference table emits a compat-guide
   * link.
   */
  case class SerdeDocInfo(
      summary: Option[String],
      hasCompatContent: Boolean,
      category: Option[String],
      anchor: String)

  /** One Spark built-in as seen in FunctionRegistry. */
  case class FunctionEntry(name: String, group: String, className: String)

  /** A fully resolved row ready to render. */
  case class ReferenceRow(name: String, status: ExprStatus, notes: String)

  private def issueLink(n: Int): String =
    s"[#$n](https://github.com/apache/datafusion-comet/issues/$n)"

  /**
   * Resolve one function to a row, plus an optional warning string when the function is
   * unclassified (no serde and not in the planned list).
   */
  def resolveRow(
      entry: FunctionEntry,
      serde: Option[SerdeDocInfo],
      planned: Option[PlannedExpr]): (ReferenceRow, Option[String]) = {
    serde match {
      case Some(info) =>
        val link =
          if (info.hasCompatContent && info.category.isDefined) {
            Some(s"[details](compatibility/expressions/${info.category.get}.md#${info.anchor})")
          } else {
            None
          }
        val notes = (info.summary ++ link).mkString(" ")
        (ReferenceRow(entry.name, Supported, notes), None)
      case None =>
        planned match {
          case Some(p) =>
            val notes = (p.note ++ p.issue.map(issueLink)).mkString(" ")
            (ReferenceRow(entry.name, p.status, notes), None)
          case None =>
            (
              ReferenceRow(entry.name, Unclassified, "unclassified; not yet reviewed"),
              Some(entry.name))
        }
    }
  }
}
