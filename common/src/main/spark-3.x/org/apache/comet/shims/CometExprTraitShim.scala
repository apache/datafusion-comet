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

import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}

/**
 * Per-profile view of expression traits that shifted shape across Spark versions. Spark 3.x has a
 * `NullIntolerant` marker trait and no scalar-expression `Stateful` concept at all (the notion
 * was added in 4.x as a boolean method on `Expression`). Routing checks through one shim lets the
 * dispatcher ask "is this expression null-intolerant / stateful" without sprinkling version
 * pattern matches through the codebase.
 */
trait CometExprTraitShim {
  def isNullIntolerant(expr: Expression): Boolean = expr.isInstanceOf[NullIntolerant]

  // No scalar `Stateful` trait in 3.x. Aggregate/window/generator stateful cases are rejected
  // elsewhere in `canHandle`, so treating all scalar expressions as non-stateful here is
  // conservative-correct on this profile.
  def isStateful(expr: Expression): Boolean = false
}
