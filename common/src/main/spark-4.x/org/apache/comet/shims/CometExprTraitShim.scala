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

import org.apache.spark.sql.catalyst.expressions.{Expression, ResolvedCollation}

/**
 * Spark 4.x replaced the `NullIntolerant` marker trait with a boolean method on `Expression`, and
 * introduced a `stateful` boolean method covering scalar expressions that carry per-row state
 * (e.g. `Rand`, `Uuid`). Neither concept exists as a trait in 4.x, so pattern matches against
 * them would fail to compile. This shim routes the checks through the method form.
 */
trait CometExprTraitShim {
  def isNullIntolerant(expr: Expression): Boolean = expr.nullIntolerant
  def isStateful(expr: Expression): Boolean = expr.stateful

  // `ResolvedCollation` is an `Unevaluable` leaf that only lives in `Collate.collation` as a
  // type-level marker. `Collate.genCode` passes through to its child and never touches the
  // collation slot, so the leaf is never invoked in generated code. Spark 4.1 analyzes it away,
  // but 4.0 leaves it in the tree, so the dispatcher's `Unevaluable` guard trips on 4.0 without
  // this exemption.
  def isCodegenInertUnevaluable(expr: Expression): Boolean = expr match {
    case _: ResolvedCollation => true
    case _ => false
  }
}
