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

import org.apache.spark.sql.catalyst.expressions.{Attribute, CaseWhen, Coalesce, Expression, If, IsNotNull}
import org.apache.spark.sql.types.NullType

import org.apache.comet.serde.QueryPlanSerde.exprToProtoInternal

/**
 * Native CASE merges the rows each branch produced with Arrow's `merge_n`, which builds the
 * result through a `MutableArrayData` that carries a validity bitmap; a `NullArray` cannot hold
 * one ("Arrays of type Null cannot contain a null bitmask"), so a CASE whose result type is
 * `NullType` fails whenever more than one branch contributes rows. Spark normally folds such an
 * expression away (`IF(c, NULL, NULL)`), but a `NullType`-typed non-foldable branch keeps it.
 */
private[serde] object NullTypeBranches {
  def supportLevel(expr: Expression): SupportLevel =
    if (expr.dataType == NullType) {
      Unsupported(Some("native CASE cannot merge NullType branches"))
    } else {
      Compatible()
    }
}

object CometIf extends CometExpressionSerde[If] {

  override def getSupportLevel(expr: If): SupportLevel = NullTypeBranches.supportLevel(expr)

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

  override def getSupportLevel(expr: CaseWhen): SupportLevel =
    NullTypeBranches.supportLevel(expr)

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
      exprToProtoInternal(elements._2, inputs, binding)
    })
    assert(whenSeq.length == thenSeq.length)
    if (whenSeq.forall(_.isDefined) && thenSeq.forall(_.isDefined)) {
      val builder = ExprOuterClass.CaseWhen.newBuilder()
      builder.addAllWhen(whenSeq.map(_.get).asJava)
      builder.addAllThen(thenSeq.map(_.get).asJava)
      if (expr.elseValue.isDefined) {
        val elseValueExpr =
          exprToProtoInternal(expr.elseValue.get, inputs, binding)
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

object CometCoalesce extends CometExpressionSerde[Coalesce] {

  // Every child but the last is a guard; the last one is the ELSE, evaluated on the rows the
  // guards left over. The result is a native CASE, so it shares that serde's NullType rule.
  override def getSupportLevel(expr: Coalesce): SupportLevel =
    NullTypeBranches.supportLevel(expr) match {
      case _: Compatible => NullGuard.supportLevel(expr.children: _*)
      case unsupported => unsupported
    }

  override def convert(
      expr: Coalesce,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val branches = expr.children.dropRight(1).map { child =>
      (IsNotNull(child), child)
    }
    val elseValue = expr.children.last
    val whenSeq = branches.map(elements => {
      exprToProtoInternal(elements._1, inputs, binding)
    })
    val thenSeq = branches.map(elements => {
      exprToProtoInternal(elements._2, inputs, binding)
    })
    assert(whenSeq.length == thenSeq.length)
    if (whenSeq.forall(_.isDefined) && thenSeq.forall(_.isDefined)) {
      val builder = ExprOuterClass.CaseWhen.newBuilder()
      builder.addAllWhen(whenSeq.map(_.get).asJava)
      builder.addAllThen(thenSeq.map(_.get).asJava)
      val elseValueExpr = exprToProtoInternal(elseValue, inputs, binding)
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
