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

import org.apache.spark.sql.catalyst.expressions.{Attribute, CaseWhen, Coalesce, Expression, Greatest, If, IsNotNull, Least}

import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, scalarFunctionExprToProto}

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

/**
 * `Greatest` / `Least` serialize as plain DataFusion scalar functions, and their arguments only
 * have to share a datatype up to nullability (Spark's `TypeCoercion.haveSameType` ignores
 * `nullable` / `containsNull` / `valueContainsNull`). DataFusion's `greatest`/`least` coerce
 * every argument to one common type; Comet's planner then inserts a `CastExpr` for any argument
 * whose Arrow type differs from that common type -- differing nested nullability, but also a
 * struct/list field *name* that Spark's `DataType` cannot express (e.g. `greatest(array_repeat(n,
 * 1).e, array_repeat(n.e, 1))`, whose elements are `list<e: struct<>>` vs `list<item:
 * struct<>>`). Casting a zero-field struct errors, so decline any multi-argument call whose
 * argument types contain an empty struct; a single argument is never coerced against anything.
 * See `SupportLevel.containsEmptyStruct` and
 * https://github.com/apache/datafusion-comet/pull/5414.
 */
abstract class CometLeastGreatest[T <: Expression](name: String) extends CometExpressionSerde[T] {
  override def getSupportLevel(expr: T): SupportLevel = {
    val types = expr.children.map(_.dataType)
    if (expr.children.size > 1 && types.exists(SupportLevel.containsEmptyStruct)) {
      Unsupported(
        Some(
          s"$name over more than one operand whose type contains an empty struct is not " +
            "supported (DataFusion coerces the arguments to a common type, which casts a " +
            "zero-field struct and errors)"))
    } else {
      Compatible()
    }
  }

  override def convert(
      expr: T,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = expr.children.map(exprToProtoInternal(_, inputs, binding))
    scalarFunctionExprToProto(name, childExpr: _*)
  }
}

object CometGreatest extends CometLeastGreatest[Greatest]("greatest")

object CometLeast extends CometLeastGreatest[Least]("least")

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
