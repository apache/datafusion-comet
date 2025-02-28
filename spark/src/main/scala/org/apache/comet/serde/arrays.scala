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
import org.apache.spark.sql.types._

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.{createBinaryExpr, exprToProto, optExprWithInfo, scalarExprToProtoWithReturnType}
import org.apache.comet.shims.CometExprShim

object CometArrayRemove extends CometExpressionSerde with CometExprShim {

  /** Exposed for unit testing */
  def isTypeSupported(dt: DataType): Boolean = {
    import DataTypes._
    dt match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
          _: DecimalType | DateType | TimestampType | StringType | BinaryType =>
        true
      case t if isTimestampNTZType(t) => true
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
    createBinaryExpr(
      expr,
      expr.children(0),
      expr.children(1),
      inputs,
      binding,
      (builder, binaryExpr) => builder.setArrayRemove(binaryExpr))
  }
}

object CometArrayAppend extends CometExpressionSerde with IncompatExpr {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.children(0),
      expr.children(1),
      inputs,
      binding,
      (builder, binaryExpr) => builder.setArrayAppend(binaryExpr))
  }
}

object CometArrayContains extends CometExpressionSerde with IncompatExpr {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.children(0),
      expr.children(1),
      inputs,
      binding,
      (builder, binaryExpr) => builder.setArrayContains(binaryExpr))
  }
}

object CometArrayIntersect extends CometExpressionSerde with IncompatExpr {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.children(0),
      expr.children(1),
      inputs,
      binding,
      (builder, binaryExpr) => builder.setArrayIntersect(binaryExpr))
  }
}

object CometArraysOverlap extends CometExpressionSerde with IncompatExpr {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.children(0),
      expr.children(1),
      inputs,
      binding,
      (builder, binaryExpr) => builder.setArraysOverlap(binaryExpr))
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
    arrayCompactScalarExpr match {
      case None =>
        withInfo(expr, "unsupported arguments for ArrayCompact", expr.children: _*)
        None
      case expr => expr
    }
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

    if (arrayExprProto.isDefined && delimiterExprProto.isDefined) {
      val arrayJoinBuilder = arrayExpr.nullReplacement match {
        case Some(nrExpr) =>
          val nullReplacementExprProto = exprToProto(nrExpr, inputs, binding)
          ExprOuterClass.ArrayJoin
            .newBuilder()
            .setArrayExpr(arrayExprProto.get)
            .setDelimiterExpr(delimiterExprProto.get)
            .setNullReplacementExpr(nullReplacementExprProto.get)
        case None =>
          ExprOuterClass.ArrayJoin
            .newBuilder()
            .setArrayExpr(arrayExprProto.get)
            .setDelimiterExpr(delimiterExprProto.get)
      }
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setArrayJoin(arrayJoinBuilder)
          .build())
    } else {
      val exprs: List[Expression] = arrayExpr.nullReplacement match {
        case Some(nrExpr) => List(arrayExpr, arrayExpr.delimiter, nrExpr)
        case None => List(arrayExpr, arrayExpr.delimiter)
      }
      withInfo(expr, "unsupported arguments for ArrayJoin", exprs: _*)
      None
    }
  }
}
