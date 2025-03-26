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

import org.apache.spark.sql.catalyst.expressions.{ArrayJoin, ArrayRemove, Attribute, Expression, Literal}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde._
import org.apache.comet.shims.CometExprShim

object CometArrayRemove extends CometExpressionSerde with CometExprShim {

  /** Exposed for unit testing */
  def isTypeSupported(dt: DataType): Boolean = {
    import DataTypes._
    dt match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
          _: DecimalType | DateType | TimestampType | TimestampNTZType | StringType |
          BinaryType =>
        true
      case ArrayType(elementType, _) => isTypeSupported(elementType)
      case _: StructType =>
        // https://github.com/apache/datafusion-comet/issues/1307
        false
      case _ => false
    }
  }

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val ar = expr.asInstanceOf[ArrayRemove]
    val inputTypes: Set[DataType] = ar.children.map(_.dataType).toSet
    for (dt <- inputTypes) {
      if (!isTypeSupported(dt)) {
        withInfo(expr, s"data type not supported: $dt")
        return None
      }
    }
    val arrayExprProto = exprToProto(ar.left, inputs, binding)
    val keyExprProto = exprToProto(ar.right, inputs, binding)

    val arrayRemoveScalarExpr =
      scalarExprToProto("array_remove_all", arrayExprProto, keyExprProto)

    val isNotNullExpr = createUnaryExpr(
      expr,
      ar.right,
      inputs,
      binding,
      (builder, unaryExpr) => builder.setIsNotNull(unaryExpr))

    val nullLiteralProto = exprToProto(Literal(null, ar.right.dataType), Seq.empty)

    if (arrayRemoveScalarExpr.isDefined && isNotNullExpr.isDefined && nullLiteralProto.isDefined) {
      val caseWhenExpr = ExprOuterClass.CaseWhen
        .newBuilder()
        .addWhen(isNotNullExpr.get)
        .addThen(arrayRemoveScalarExpr.get)
        .setElseExpr(nullLiteralProto.get)
        .build()
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setCaseWhen(caseWhenExpr)
          .build())
    } else {
      withInfo(expr, expr.children: _*)
      None
    }
  }
}

object CometArrayAppend extends CometExpressionSerde with IncompatExpr {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val child = expr.children.head
    val elementType = child.dataType.asInstanceOf[ArrayType].elementType

    val arrayExprProto = exprToProto(expr.children(0), inputs, binding)
    val keyExprProto = exprToProto(expr.children(1), inputs, binding)

    val arrayAppendScalarExpr = scalarExprToProto("array_append", arrayExprProto, keyExprProto)

    val isNotNullExpr = createUnaryExpr(
      expr,
      expr.children(0),
      inputs,
      binding,
      (builder, unaryExpr) => builder.setIsNotNull(unaryExpr))

    val nullLiteralProto = exprToProto(Literal(null, elementType), Seq.empty)

    if (arrayAppendScalarExpr.isDefined && isNotNullExpr.isDefined && nullLiteralProto.isDefined) {
      val caseWhenExpr = ExprOuterClass.CaseWhen
        .newBuilder()
        .addWhen(isNotNullExpr.get)
        .addThen(arrayAppendScalarExpr.get)
        .setElseExpr(nullLiteralProto.get)
        .build()
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setCaseWhen(caseWhenExpr)
          .build())
    } else {
      withInfo(expr, expr.children: _*)
      None
    }
  }
}

object CometArrayContains extends CometExpressionSerde with IncompatExpr {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val arrayExprProto = exprToProto(expr.children(0), inputs, binding)
    val keyExprProto = exprToProto(expr.children(1), inputs, binding)

    val arrayContainsScalarExpr =
      scalarExprToProto("array_has", arrayExprProto, keyExprProto)
    optExprWithInfo(arrayContainsScalarExpr, expr, expr.children: _*)
  }
}

object CometArrayIntersect extends CometExpressionSerde with IncompatExpr {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val leftArrayExprProto = exprToProto(expr.children(0), inputs, binding)
    val rightArrayExprProto = exprToProto(expr.children(1), inputs, binding)

    val arraysIntersectScalarExpr =
      scalarExprToProto("array_intersect", leftArrayExprProto, rightArrayExprProto)
    optExprWithInfo(arraysIntersectScalarExpr, expr, expr.children: _*)
  }
}

object CometArraysOverlap extends CometExpressionSerde with IncompatExpr {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val leftArrayExprProto = exprToProto(expr.children(0), inputs, binding)
    val rightArrayExprProto = exprToProto(expr.children(1), inputs, binding)

    val arraysOverlapScalarExpr = scalarExprToProtoWithReturnType(
      "array_has_any",
      BooleanType,
      leftArrayExprProto,
      rightArrayExprProto)
    optExprWithInfo(arraysOverlapScalarExpr, expr, expr.children: _*)
  }
}

object CometArrayCompact extends CometExpressionSerde with IncompatExpr {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val child = expr.children.head
    val elementType = child.dataType.asInstanceOf[ArrayType].elementType

    val arrayExprProto = exprToProto(child, inputs, binding)
    val nullLiteralProto = exprToProto(Literal(null, elementType), Seq.empty)

    val arrayCompactScalarExpr = scalarExprToProtoWithReturnType(
      "array_remove_all",
      ArrayType(elementType = elementType),
      arrayExprProto,
      nullLiteralProto)
    optExprWithInfo(arrayCompactScalarExpr, expr, expr.children: _*)
  }
}

object CometArrayExcept extends CometExpressionSerde with CometExprShim with IncompatExpr {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val leftArrayExprProto = exprToProto(expr.children(0), inputs, binding)
    val rightArrayExprProto = exprToProto(expr.children(1), inputs, binding)

    val arrayExceptScalarExpr =
      scalarExprToProto("array_except", leftArrayExprProto, rightArrayExprProto)
    optExprWithInfo(arrayExceptScalarExpr, expr, expr.children: _*)
  }
}

object CometArrayJoin extends CometExpressionSerde with IncompatExpr {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val arrayExpr = expr.asInstanceOf[ArrayJoin]
    val arrayExprProto = exprToProto(arrayExpr.array, inputs, binding)
    val delimiterExprProto = exprToProto(arrayExpr.delimiter, inputs, binding)

    arrayExpr.nullReplacement match {
      case Some(nullReplacementExpr) =>
        val nullReplacementExprProto = exprToProto(nullReplacementExpr, inputs, binding)

        val arrayJoinScalarExpr = scalarExprToProto(
          "array_to_string",
          arrayExprProto,
          delimiterExprProto,
          nullReplacementExprProto)

        optExprWithInfo(
          arrayJoinScalarExpr,
          expr,
          arrayExpr,
          arrayExpr.delimiter,
          nullReplacementExpr)
      case None =>
        val arrayJoinScalarExpr =
          scalarExprToProto("array_to_string", arrayExprProto, delimiterExprProto)

        optExprWithInfo(arrayJoinScalarExpr, expr, arrayExpr, arrayExpr.delimiter)
    }
  }
}

object CometArrayInsert extends CometExpressionSerde with IncompatExpr {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val srcExprProto = exprToProtoInternal(expr.children(0), inputs, binding)
    val posExprProto = exprToProtoInternal(expr.children(1), inputs, binding)
    val itemExprProto = exprToProtoInternal(expr.children(2), inputs, binding)
    val legacyNegativeIndex =
      SQLConf.get.getConfString("spark.sql.legacy.negativeIndexInArrayInsert").toBoolean
    if (srcExprProto.isDefined && posExprProto.isDefined && itemExprProto.isDefined) {
      val arrayInsertBuilder = ExprOuterClass.ArrayInsert
        .newBuilder()
        .setSrcArrayExpr(srcExprProto.get)
        .setPosExpr(posExprProto.get)
        .setItemExpr(itemExprProto.get)
        .setLegacyNegativeIndex(legacyNegativeIndex)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setArrayInsert(arrayInsertBuilder)
          .build())
    } else {
      withInfo(
        expr,
        "unsupported arguments for ArrayInsert",
        expr.children(0),
        expr.children(1),
        expr.children(2))
      None
    }
  }
}
