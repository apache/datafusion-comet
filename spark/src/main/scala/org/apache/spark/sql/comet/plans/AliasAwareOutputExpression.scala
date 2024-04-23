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

package org.apache.spark.sql.comet.plans

import scala.collection.mutable

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin

/**
 * A trait that provides functionality to handle aliases in the `outputExpressions`.
 */
trait AliasAwareOutputExpression extends SQLConfHelper {
  // `SQLConf.EXPRESSION_PROJECTION_CANDIDATE_LIMIT` is Spark 3.4+ only.
  // Use a default value for now.
  protected val aliasCandidateLimit = 100
  protected def outputExpressions: Seq[NamedExpression]

  /**
   * This method can be used to strip expression which does not affect the result, for example:
   * strip the expression which is ordering agnostic for output ordering.
   */
  protected def strip(expr: Expression): Expression = expr

  // Build an `Expression` -> `Attribute` alias map.
  // There can be multiple alias defined for the same expressions but it doesn't make sense to store
  // more than `aliasCandidateLimit` attributes for an expression. In those cases the old logic
  // handled only the last alias so we need to make sure that we give precedence to that.
  // If the `outputExpressions` contain simple attributes we need to add those too to the map.
  @transient
  private lazy val aliasMap = {
    val aliases = mutable.Map[Expression, mutable.ArrayBuffer[Attribute]]()
    outputExpressions.reverse.foreach {
      case a @ Alias(child, _) =>
        val buffer =
          aliases.getOrElseUpdate(strip(child).canonicalized, mutable.ArrayBuffer.empty)
        if (buffer.size < aliasCandidateLimit) {
          buffer += a.toAttribute
        }
      case _ =>
    }
    outputExpressions.foreach {
      case a: Attribute if aliases.contains(a.canonicalized) =>
        val buffer = aliases(a.canonicalized)
        if (buffer.size < aliasCandidateLimit) {
          buffer += a
        }
      case _ =>
    }
    aliases
  }

  protected def hasAlias: Boolean = aliasMap.nonEmpty

  /**
   * Return a stream of expressions in which the original expression is projected with `aliasMap`.
   */
  protected def projectExpression(expr: Expression): Stream[Expression] = {
    val outputSet = AttributeSet(outputExpressions.map(_.toAttribute))
    multiTransformDown(expr) {
      // Mapping with aliases
      case e: Expression if aliasMap.contains(e.canonicalized) =>
        aliasMap(e.canonicalized).toSeq ++ (if (e.containsChild.nonEmpty) Seq(e) else Seq.empty)

      // Prune if we encounter an attribute that we can't map and it is not in output set.
      // This prune will go up to the closest `multiTransformDown()` call and returns `Stream.empty`
      // there.
      case a: Attribute if !outputSet.contains(a) => Seq.empty
    }
  }

  // Copied from Spark 3.4+ to make it available in Spark 3.2+.
  def multiTransformDown(expr: Expression)(
      rule: PartialFunction[Expression, Seq[Expression]]): Stream[Expression] = {

    // We could return `Seq(this)` if the `rule` doesn't apply and handle both
    // - the doesn't apply
    // - and the rule returns a one element `Seq(originalNode)`
    // cases together. The returned `Seq` can be a `Stream` and unfortunately it doesn't seem like
    // there is a way to match on a one element stream without eagerly computing the tail's head.
    // This contradicts with the purpose of only taking the necessary elements from the
    // alternatives. I.e. the "multiTransformDown is lazy" test case in `TreeNodeSuite` would fail.
    // Please note that this behaviour has a downside as well that we can only mark the rule on the
    // original node ineffective if the rule didn't match.
    var ruleApplied = true
    val afterRules = CurrentOrigin.withOrigin(expr.origin) {
      rule.applyOrElse(
        expr,
        (_: Expression) => {
          ruleApplied = false
          Seq.empty
        })
    }

    val afterRulesStream = if (afterRules.isEmpty) {
      if (ruleApplied) {
        // If the rule returned with empty alternatives then prune
        Stream.empty
      } else {
        // If the rule was not applied then keep the original node
        Stream(expr)
      }
    } else {
      // If the rule was applied then use the returned alternatives
      afterRules.toStream.map { afterRule =>
        if (expr fastEquals afterRule) {
          expr
        } else {
          afterRule.copyTagsFrom(expr)
          afterRule
        }
      }
    }

    afterRulesStream.flatMap { afterRule =>
      if (afterRule.containsChild.nonEmpty) {
        generateCartesianProduct(afterRule.children.map(c => () => multiTransformDown(c)(rule)))
          .map(afterRule.withNewChildren)
      } else {
        Stream(afterRule)
      }
    }
  }

  def generateCartesianProduct[T](elementSeqs: Seq[() => Seq[T]]): Stream[Seq[T]] = {
    elementSeqs.foldRight(Stream(Seq.empty[T]))((elements, elementTails) =>
      for {
        elementTail <- elementTails
        element <- elements()
      } yield element +: elementTail)
  }
}
