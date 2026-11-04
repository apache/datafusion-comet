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

package org.apache.comet.codegen

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression, Unevaluable}
import org.apache.spark.sql.types.DataType

/**
 * Ships `child` to the dispatcher with an instruction to compile it without subexpression
 * elimination. The kernel otherwise eliminates common subexpressions when
 * `spark.sql.subexpressionElimination.enabled` is on, as Spark's `ProjectExec` does. Spark's
 * whole-stage `FilterExec` does not eliminate them in its predicate before Spark 4.2
 * (`FilterExec` without whole-stage codegen does), and doing so can move a repeated subexpression
 * out of an `AND`'s short circuit: in `x <> 0 AND abs(10 div x) + abs(10 div x) > 1` the division
 * would run, and raise under ANSI, for `x = 0`. The JVM detour wraps a filter condition or
 * projection item in this when Spark plans to evaluate it without elimination (see
 * `CometJvmDetour.filterSubexpressionElimination` and `projectSubexpressionElimination`). The
 * dispatcher unwraps it before it compiles `child`, so it never reaches generated code, and it is
 * part of the serialized bytes, so the kernel gets its own cache entry.
 */
private[comet] case class WithoutSubexpressionElimination(child: Expression)
    extends UnaryExpression
    with Unevaluable {
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override protected def withNewChildInternal(
      newChild: Expression): WithoutSubexpressionElimination =
    copy(child = newChild)
}

private[comet] object WithoutSubexpressionElimination {

  /** The expression to compile, and whether to eliminate common subexpressions in it. */
  def unwrap(expr: Expression): (Expression, Boolean) = expr match {
    case WithoutSubexpressionElimination(child) => (child, false)
    case other => (other, true)
  }
}
