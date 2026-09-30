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

package org.apache.comet.serde

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{Attribute, CaseWhen, Cast, Coalesce, Expression, If, IsNotNull}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import org.apache.comet.DataTypeSupport.deepNullable
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, liftFallbackReasons}

/**
 * Serializes a CASE WHEN or coalesce result branch. A native CASE coerces its branches to one
 * type, folding from the ELSE branch, so a merged struct takes the ELSE branch's field names,
 * where Spark names it after the first branch; with case-insensitive analysis the two can differ
 * (`s` beside `named_struct('A', id)`). A consumer that compares types exactly (`array(...)`, the
 * set ops) then meets a struct named differently from its sibling. A branch of a struct-bearing
 * type is cast to the expression's own type, made deeply nullable, so every branch has Spark's
 * names and the coercion has nothing left to change; struct casts are positional, so values are
 * kept.
 */
private[serde] object CaseBranch {
  def serialize(
      expr: Expression,
      branch: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!hasStruct(expr.dataType)) {
      return exprToProtoInternal(branch, inputs, binding)
    }
    // The cast is not in the plan, so a reason recorded under it is lifted onto `expr`.
    val aligned = Cast(branch, deepNullable(expr.dataType))
    exprToProtoInternal(aligned, inputs, binding).orElse {
      liftFallbackReasons(aligned, expr)
      None
    }
  }

  private def hasStruct(dt: DataType): Boolean = dt match {
    case _: StructType => true
    case ArrayType(element, _) => hasStruct(element)
    case MapType(key, value, _) => hasStruct(key) || hasStruct(value)
    case _ => false
  }
}

object CometIf extends CometExpressionSerde[If] {
  override def convert(
      expr: If,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val predicateExpr = exprToProtoInternal(expr.predicate, inputs, binding)
    val trueExpr = exprToProtoInternal(expr.trueValue, inputs, binding)
    val falseExpr = exprToProtoInternal(expr.falseValue, inputs, binding)
    if (predicateExpr.isDefined && trueExpr.isDefined && falseExpr.isDefined) {
      val builder = ExprOuterClass.IfExpr.newBuilder()
      builder.setIfExpr(predicateExpr.get)
      builder.setTrueExpr(trueExpr.get)
      builder.setFalseExpr(falseExpr.get)
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setIf(builder)
          .build())
    } else {
      None
    }
  }
}

object CometCaseWhen extends CometExpressionSerde[CaseWhen] {
  override def convert(
      expr: CaseWhen,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    var allBranches: Seq[Expression] = Seq()
    val whenSeq = expr.branches.map(elements => {
      allBranches = allBranches :+ elements._1
      exprToProtoInternal(elements._1, inputs, binding)
    })
    val thenSeq = expr.branches.map(elements => {
      allBranches = allBranches :+ elements._2
      CaseBranch.serialize(expr, elements._2, inputs, binding)
    })
    assert(whenSeq.length == thenSeq.length)
    if (whenSeq.forall(_.isDefined) && thenSeq.forall(_.isDefined)) {
      val builder = ExprOuterClass.CaseWhen.newBuilder()
      builder.addAllWhen(whenSeq.map(_.get).asJava)
      builder.addAllThen(thenSeq.map(_.get).asJava)
      if (expr.elseValue.isDefined) {
        val elseValueExpr = CaseBranch.serialize(expr, expr.elseValue.get, inputs, binding)
        if (elseValueExpr.isDefined) {
          builder.setElseExpr(elseValueExpr.get)
        } else {
          return None
        }
      }
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setCaseWhen(builder)
          .build())
    } else {
      None
    }
  }
}

object CometCoalesce extends CometExpressionSerde[Coalesce] with CodegenDispatchFallback {

  override def getUnsupportedReasons(): Seq[String] = Seq(NullGuard.reason)

  // Every child but the last is a guard; the last one is the ELSE, evaluated on the rows the
  // guards left over. Only the guarded children are serialized twice (predicate and THEN), so
  // only they need the single-evaluation check; the ELSE is serialized once, as Spark evaluates
  // it once.
  override def getSupportLevel(expr: Coalesce): SupportLevel =
    NullGuard.supportLevel(expr.children.dropRight(1): _*)

  override def convert(
      expr: Coalesce,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // The optimizer normally reduces a one-argument coalesce to its argument, but not when
    // `NullPropagation` and `SimplifyConditionals` are excluded, and a CASE over no guarded
    // children would have no WHEN clause, which native CASE rejects.
    if (expr.children.size == 1) {
      return exprToProtoInternal(expr.children.head, inputs, binding)
    }
    val branches = expr.children.dropRight(1).map { child =>
      (IsNotNull(child), child)
    }
    val elseValue = expr.children.last
    val whenSeq = branches.map(elements => {
      exprToProtoInternal(elements._1, inputs, binding)
    })
    val thenSeq = branches.map(elements => {
      CaseBranch.serialize(expr, elements._2, inputs, binding)
    })
    assert(whenSeq.length == thenSeq.length)
    if (whenSeq.forall(_.isDefined) && thenSeq.forall(_.isDefined)) {
      val builder = ExprOuterClass.CaseWhen.newBuilder()
      builder.addAllWhen(whenSeq.map(_.get).asJava)
      builder.addAllThen(thenSeq.map(_.get).asJava)
      val elseValueExpr = CaseBranch.serialize(expr, elseValue, inputs, binding)
      if (elseValueExpr.isDefined) {
        builder.setElseExpr(elseValueExpr.get)
      } else {
        return None
      }
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setCaseWhen(builder)
          .build())
    } else {
      None
    }
  }
}
