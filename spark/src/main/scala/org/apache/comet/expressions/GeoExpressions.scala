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

package org.apache.comet.expressions

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, StringType}
import org.apache.spark.unsafe.types.UTF8String

// ---- Binary geo predicates -----------------------------------------------

case class StContains(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant {
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.contains(g1.toString, g2.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      (g1, g2) =>
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
          s".contains($g1.toString(), $g2.toString())")
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StIntersects(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant {
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.intersects(g1.toString, g2.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      (g1, g2) =>
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
          s".intersects($g1.toString(), $g2.toString())")
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StWithin(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant {
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.within(g1.toString, g2.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      (g1, g2) =>
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
          s".within($g1.toString(), $g2.toString())")
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StDistance(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant {
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.distance(g1.toString, g2.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      (g1, g2) =>
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
          s".distance($g1.toString(), $g2.toString())")
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

// ---- Unary geo functions --------------------------------------------------

case class StArea(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g: Any): Any = CometGeoFallback.area(g.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      g => s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$.area($g.toString())")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StCentroid(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g: Any): Any =
    UTF8String.fromString(CometGeoFallback.centroid(g.toString))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      g =>
        s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
          s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$.centroid($g.toString()))")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

// ---- Registration helpers for SparkSessionExtensions.injectFunction ------

object GeoExpressions {

  type FunctionDescription =
    (FunctionIdentifier, ExpressionInfo, Seq[Expression] => Expression)

  private def desc(
      name: String,
      cls: Class[_],
      builder: Seq[Expression] => Expression): FunctionDescription =
    (
      new FunctionIdentifier(name),
      new ExpressionInfo(cls.getName, name),
      builder)

  val stContainsInfo: FunctionDescription =
    desc("st_contains", classOf[StContains], args => StContains(args(0), args(1)))

  val stIntersectsInfo: FunctionDescription =
    desc("st_intersects", classOf[StIntersects], args => StIntersects(args(0), args(1)))

  val stWithinInfo: FunctionDescription =
    desc("st_within", classOf[StWithin], args => StWithin(args(0), args(1)))

  val stDistanceInfo: FunctionDescription =
    desc("st_distance", classOf[StDistance], args => StDistance(args(0), args(1)))

  val stAreaInfo: FunctionDescription =
    desc("st_area", classOf[StArea], args => StArea(args(0)))

  val stCentroidInfo: FunctionDescription =
    desc("st_centroid", classOf[StCentroid], args => StCentroid(args(0)))
}
