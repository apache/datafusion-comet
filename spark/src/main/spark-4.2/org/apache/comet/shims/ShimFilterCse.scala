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

import org.apache.spark.sql.catalyst.expressions.{EquivalentExpressions, IsNotNull, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.CollapseProject
import org.apache.spark.sql.execution.FilterExec

/**
 * Shim for whether whole-stage `FilterExec` eliminates common subexpressions in its condition.
 * SPARK-56032 and its follow-ups added this to `FilterExec.doConsume`, first shipped in Spark
 * 4.2.0, and every later release inherits it, so a shim for Spark 4.3 or later should copy this
 * one. It evaluates each common subexpression just before the first conjunct that reads it,
 * interleaved with the `IsNotNull` checks, a form the JVM detour's kernel does not reproduce.
 *
 * This mirrors the gate in Spark 4.2.0's `FilterExec.doConsume` (`basicPhysicalOperators.scala`):
 * `spark.sql.subexpressionElimination.enabled` and
 * `spark.sql.subexpressionElimination.filterExec.enabled` are on, and `EquivalentExpressions`
 * over the conjuncts other than the split-out `IsNotNull`s finds a common subexpression that
 * `CollapseProject.isCheap` does not call cheap. Spark binds those conjuncts to its output first,
 * which does not change which subexpressions are common.
 */
object ShimFilterCse extends PredicateHelper {
  def wholeStageEliminatesCommonSubexpressions(filter: FilterExec): Boolean = {
    val sqlConf = filter.conf
    sqlConf.subexpressionEliminationEnabled &&
    sqlConf.subexpressionEliminationFilterExecEnabled && {
      // The same split as `FilterExec.notNullPreds` / `otherPreds`.
      val otherPreds = splitConjunctivePredicates(filter.condition).filterNot {
        case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(filter.child.outputSet)
        case _ => false
      }
      otherPreds.nonEmpty && {
        val equivalentExpressions = new EquivalentExpressions
        otherPreds.foreach(equivalentExpressions.addExprTree(_))
        equivalentExpressions.getCommonSubexpressions.exists(!CollapseProject.isCheap(_))
      }
    }
  }
}
