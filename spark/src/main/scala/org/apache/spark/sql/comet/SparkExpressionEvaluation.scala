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

package org.apache.spark.sql.comet

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Abs, Add, And, Attribute, BinaryComparison, Cast, CodegenObjectFactoryMode, Divide, DivModLike, EqualNullSafe, EvalMode, Expression, ExpressionEquals, IntegralDivide, IsNotNull, IsNull, LeafExpression, Literal, Multiply, Not, Or, PredicateHelper, Remainder, Subtract, UnaryMinus}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegralType, LongType}

import org.apache.comet.CometSparkSessionExtensions.isSpark42Plus
import org.apache.comet.rules.CometExecRule
import org.apache.comet.serde.QueryPlanSerde

private[comet] object SparkExpressionEvaluation extends PredicateHelper {
  private def containsAnsiDivision(expr: Expression): Boolean = expr.exists {
    case d: Divide => d.evalMode == EvalMode.ANSI
    case d: IntegralDivide => d.evalMode == EvalMode.ANSI
    case r: Remainder => r.evalMode == EvalMode.ANSI
    case _ => false
  }

  private def widening(c: Cast): Boolean = c.child.dataType match {
    case _: IntegralType => c.dataType == LongType || c.dataType == DoubleType
    case FloatType => c.dataType == DoubleType
    case _ => false
  }

  private def simpleOperand(expr: Expression): Boolean = expr match {
    case _: Literal | _: Attribute => true
    case c: Cast => widening(c) && simpleOperand(c.child)
    case _ => false
  }

  // Moving an expression must preserve which of its operands can fail. Spark skips some
  // operands on NULL (#7246, #7468) and can precompute shared terms inside a Boolean guard.
  private def hasMatchingOperandGuards(expr: Expression): Boolean = expr match {
    case _: Literal | _: Attribute => true
    case c: Cast => widening(c) && hasMatchingOperandGuards(c.child)
    case _: Abs | _: UnaryMinus | _: IsNull | _: IsNotNull | _: Not | _: EqualNullSafe =>
      expr.children.forall(hasMatchingOperandGuards)
    case _: And | _: Or =>
      QueryPlanSerde.canSkipEvaluation(expr.children.tail) &&
      expr.children.forall(hasMatchingOperandGuards)
    case d: DivModLike => simpleOperand(d.left) && hasMatchingOperandGuards(d.right)
    case _: Add | _: Subtract | _: Multiply | _: BinaryComparison =>
      val left = expr.children.head
      val right = expr.children.last
      hasMatchingOperandGuards(left) &&
      (if (left.nullable) simpleOperand(right) else hasMatchingOperandGuards(right))
    case _ => false
  }

  private def codegenFactoryEnabled(op: SparkPlan): Boolean =
    !op.conf
      .getConfString(SQLConf.CODEGEN_FACTORY_MODE.key)
      .equalsIgnoreCase(CodegenObjectFactoryMode.NO_CODEGEN.toString)

  // CollapseCodegenStages applies these checks to operators that support codegen.
  def wholeStageCodegenEnabled(op: SparkPlan): Boolean =
    op.conf.wholeStageEnabled && codegenFactoryEnabled(op) &&
      !WholeStageCodegenExec.isTooManyFields(op.conf, op.schema) &&
      !op.children.exists(child =>
        WholeStageCodegenExec.isTooManyFields(op.conf, child.schema)) &&
      !op.expressions.exists(_.exists(e =>
        e.isInstanceOf[CodegenFallback] && !e.isInstanceOf[LeafExpression]))

  def filterCondition(op: FilterExec): Expression = {
    if (op.getTagValue(CometExecRule.PARTIAL_READER).isDefined ||
      !op.condition.isInstanceOf[And] ||
      !containsAnsiDivision(op.condition) ||
      !wholeStageCodegenEnabled(op)) {
      return op.condition
    }

    val conjuncts = splitConjunctivePredicates(op.condition)
    val (nullChecks, predicates) = conjuncts.partition {
      case IsNotNull(e) => isNullIntolerant(e) && e.references.subsetOf(op.child.outputSet)
      case _ => false
    }
    // Spark 4.2 can precompute a shared term before the first predicate that refers to it.
    val filterCse = isSpark42Plus &&
      op.conf.getConfString("spark.sql.subexpressionElimination.filterExec.enabled").toBoolean
    val canReorder = QueryPlanSerde.withSharedSubexpressions(if (filterCse) predicates else Nil) {
      conjuncts.forall(hasMatchingOperandGuards)
    }
    if (!canReorder) return op.condition

    // GeneratePredicateHelper checks a required column at first use, even when the original
    // IS NOT NULL tests a null-intolerant expression over that column. Remaining checks run last.
    val required = nullChecks.flatMap(_.references).map(_.exprId).toSet
    val ordered = (predicates.flatMap { p =>
      p.references.toSeq.filter(a => required.contains(a.exprId)).map(IsNotNull(_)) :+ p
    } ++ nullChecks).distinct

    // A generated filter rejects a NULL conjunct immediately. SQL AND continues past NULL
    // because a later false can still decide the result.
    val guards = ordered.init.map { p =>
      if (p.nullable) EqualNullSafe(p, Literal.TrueLiteral) else p
    }
    buildBalancedPredicate(guards :+ ordered.last, And.apply)
  }

  /**
   * Spark's generated projections evaluate shared expressions before testing the left side of an
   * AND or OR. Its interpreted projection only caches them on first use.
   */
  def sharedGuardedDivisions(op: ProjectExec): Seq[Expression] = {
    if (op.getTagValue(CometExecRule.PARTIAL_READER).isDefined ||
      !op.conf.subexpressionEliminationEnabled ||
      !op.projectList.exists(containsAnsiDivision) ||
      !codegenFactoryEnabled(op)) {
      return Nil
    }

    val guarded = mutable.Set.empty[ExpressionEquals]
    def visit(expr: Expression): Unit = expr match {
      case _: And | _: Or =>
        visit(expr.children.head)
        val right = expr.children.last
        if (containsAnsiDivision(right)) {
          right.foreach(e => guarded += ExpressionEquals(e))
        }
      case _ => expr.children.foreach(visit)
    }
    op.projectList.foreach(visit)
    if (guarded.isEmpty) return Nil

    QueryPlanSerde.sharedExpressionsBeforePromotion.filter { e =>
      guarded.contains(ExpressionEquals(e)) && containsAnsiDivision(e) &&
      hasMatchingOperandGuards(e)
    }
  }
}
