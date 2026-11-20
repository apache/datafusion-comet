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

import org.apache.spark.sql.catalyst.expressions.{Abs, Add, And, Attribute, BinaryComparison, Cast, CodegenObjectFactoryMode, Divide, DivModLike, EqualNullSafe, EvalMode, Expression, ExpressionEquals, ExprId, IntegralDivide, IsNotNull, IsNull, LeafExpression, Literal, Multiply, Not, Or, PredicateHelper, Remainder, Subtract, UnaryMinus}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DoubleType, IntegralType, LongType}

import org.apache.comet.serde.QueryPlanSerde

private[comet] object SparkExpressionEvaluation extends PredicateHelper {
  private def containsAnsiDivision(expr: Expression): Boolean = expr.exists {
    case d: Divide => d.evalMode == EvalMode.ANSI
    case d: IntegralDivide => d.evalMode == EvalMode.ANSI
    case r: Remainder => r.evalMode == EvalMode.ANSI
    case _ => false
  }

  // Spark can skip an operand of arithmetic or a comparison on NULL (#7246, #7468).
  // Moving the parent earlier must not expose a failure from such an operand. Include
  // the implicit widening Spark inserts for integral columns in DIV and /.
  private def simpleOperand(expr: Expression): Boolean = expr match {
    case _: Literal | _: Attribute => true
    case c: Cast
        if c.child.dataType.isInstanceOf[IntegralType] &&
          (c.dataType == LongType || c.dataType == DoubleType) =>
      simpleOperand(c.child)
    case _ => false
  }

  private def hasMatchingOperandGuards(expr: Expression): Boolean = expr match {
    case _: Literal | _: Attribute => true
    case _: Abs | _: Cast | _: UnaryMinus | _: IsNull | _: IsNotNull | _: Not | _: And | _: Or |
        _: EqualNullSafe =>
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
    if (!op.condition.isInstanceOf[And] ||
      !containsAnsiDivision(op.condition) ||
      !wholeStageCodegenEnabled(op) ||
      !hasMatchingOperandGuards(op.condition)) {
      return op.condition
    }

    // Spark's GeneratePredicateHelper checks an attribute for NULL at its first use, even
    // when the original IS NOT NULL tests a null-intolerant expression over that attribute.
    // It runs the remaining checks last, so they cannot suppress an earlier division error.
    val (nullChecks, predicates) = splitConjunctivePredicates(op.condition).partition {
      case IsNotNull(e) => isNullIntolerant(e) && e.references.subsetOf(op.child.outputSet)
      case _ => false
    }
    val pending = mutable.Set.empty[ExprId]
    pending ++= nullChecks.flatMap(_.references).map(_.exprId)
    val ordered = Seq.newBuilder[Expression]
    predicates.foreach { predicate =>
      predicate.references.foreach { attr =>
        if (pending.remove(attr.exprId)) ordered += IsNotNull(attr)
      }
      ordered += predicate
    }
    ordered ++= nullChecks.filter {
      case IsNotNull(a: Attribute) => pending.contains(a.exprId)
      case _ => true
    }

    // A generated filter rejects a NULL conjunct immediately. SQL AND continues past NULL
    // because a later false can still decide the result.
    val conjuncts = ordered.result()
    val guards = conjuncts.init.map { p =>
      if (p.nullable) EqualNullSafe(p, Literal.TrueLiteral) else p
    }
    buildBalancedPredicate(guards :+ conjuncts.last, And.apply)
  }

  /**
   * Spark's generated projections evaluate shared expressions before testing the left side of an
   * AND or OR. Its interpreted projection only caches them on first use.
   */
  def sharedGuardedDivisions(op: ProjectExec): Seq[Expression] = {
    if (!op.conf.subexpressionEliminationEnabled ||
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
