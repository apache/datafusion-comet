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

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.expressions.{Attribute, CaseWhen, Coalesce, Expression, If, IsNotNull}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.exprToProtoInternal

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
      withInfo(expr, expr.predicate, expr.trueValue, expr.falseValue)
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
          withInfo(expr, expr.elseValue.get)
          return None
        }
      }
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setCaseWhen(builder)
          .build())
    } else {
      withInfo(expr, allBranches: _*)
      None
    }
  }
}

object CometCoalesce extends CometExpressionSerde[Coalesce] {
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
          withInfo(expr, elseValue)
          return None
        }
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setCaseWhen(builder)
          .build())
    } else {
      withInfo(expr, branches: _*)
      None
    }
  }
}
