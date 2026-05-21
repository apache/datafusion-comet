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
 * Spark 4.x replaced the `NullIntolerant` marker trait with a boolean method on `Expression` and
 * added a `stateful` boolean. Neither exists as a trait in 4.x. This shim routes the checks
 * through the method form.
 */
trait CometExprTraitShim {
  def isNullIntolerant(expr: Expression): Boolean = expr.nullIntolerant
  def isStateful(expr: Expression): Boolean = expr.stateful

  // `ResolvedCollation` is an `Unevaluable` leaf living only in `Collate.collation` as a
  // type-level marker. `Collate.genCode` passes through to its child and never invokes it. Spark
  // 4.1 analyzes it away; 4.0 leaves it in the tree, so the dispatcher's `Unevaluable` guard
  // would trip without this exemption.
  def isCodegenInertUnevaluable(expr: Expression): Boolean = expr match {
    case _: ResolvedCollation => true
    case _ => false
  }
}
