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

package org.apache.comet.shims

import org.apache.spark.sql.catalyst.expressions.{Attribute, DayName, Expression, MonthName}

import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithFallbackReason, scalarFunctionExprToProtoWithReturnType}

/**
 * Expression conversions shared across all Spark 4.x minor versions, compiled from the
 * `spark-4.x` source root. Spark 4.0+ expression classes (e.g. `DayName` / `MonthName`) do not
 * exist in 3.x, so they cannot live in the version-agnostic serde, but they are identical across
 * 4.0 / 4.1 / 4.2 and belong here rather than being duplicated in each per-minor-version
 * `CometExprShim`.
 */
trait CometExprShim4x {

  /**
   * `dayname` / `monthname` (Spark 4.0+) map a `DateType` value to a fixed US-English abbreviated
   * name. Spark's `DateTimeUtils.getDayName` / `getMonthName` use `DayOfWeek` / `Month`
   * `getDisplayName(TextStyle.SHORT, Locale.US)` (`DateFormatter.defaultLocale` is the constant
   * `Locale.US`), so there is no session-locale or timezone dependence and they map directly to
   * the native `dayname` / `monthname` scalar functions.
   */
  protected def convertDayMonthName(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = expr match {
    case d: DayName =>
      val childExpr = exprToProtoInternal(d.child, inputs, binding)
      val nameExpr =
        scalarFunctionExprToProtoWithReturnType("dayname", d.dataType, false, childExpr)
      optExprWithFallbackReason(nameExpr, d, d.child)
    case m: MonthName =>
      val childExpr = exprToProtoInternal(m.child, inputs, binding)
      val nameExpr =
        scalarFunctionExprToProtoWithReturnType("monthname", m.dataType, false, childExpr)
      optExprWithFallbackReason(nameExpr, m, m.child)
    case _ => None
  }
}
