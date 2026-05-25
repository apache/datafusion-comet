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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, LongType, StringType}
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

case class StUnion(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    UTF8String.fromString(CometGeoFallback.union(g1.toString, g2.toString))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      (g1, g2) =>
        s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
          s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
          s".union($g1.toString(), $g2.toString()))")
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StIntersection(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    UTF8String.fromString(CometGeoFallback.intersection(g1.toString, g2.toString))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      (g1, g2) =>
        s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
          s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
          s".intersection($g1.toString(), $g2.toString()))")
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

case class StLength(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g: Any): Any = CometGeoFallback.length(g.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      g => s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$.length($g.toString())")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StIsEmpty(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g: Any): Any = CometGeoFallback.isEmpty(g.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      g => s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$.isEmpty($g.toString())")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StGeometryType(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g: Any): Any =
    UTF8String.fromString(CometGeoFallback.geometryType(g.toString))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      g =>
        s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
          s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
          s".geometryType($g.toString()))")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StNumPoints(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = LongType
  override def nullSafeEval(g: Any): Any = CometGeoFallback.numPoints(g.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      g => s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$.numPoints($g.toString())")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StX(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g: Any): Any = CometGeoFallback.stX(g.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      g => s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$.stX($g.toString())")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StY(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g: Any): Any = CometGeoFallback.stY(g.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      g => s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$.stY($g.toString())")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StEnvelope(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g: Any): Any =
    UTF8String.fromString(CometGeoFallback.envelope(g.toString))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      g =>
        s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
          s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$.envelope($g.toString()))")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StConvexHull(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g: Any): Any =
    UTF8String.fromString(CometGeoFallback.convexHull(g.toString))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, g =>
      s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".convexHull($g.toString()))")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

// st_simplify and st_buffer take two args (geom + numeric param)

case class StSimplify(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g: Any, t: Any): Any =
    UTF8String.fromString(CometGeoFallback.simplify(g.toString, t.toString.toDouble))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g, t) =>
      s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".simplify($g.toString(), Double.parseDouble($t.toString())))")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

case class StBuffer(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g: Any, d: Any): Any =
    UTF8String.fromString(CometGeoFallback.buffer(g.toString, d.toString.toDouble))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g, d) =>
      s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".buffer($g.toString(), Double.parseDouble($d.toString())))")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

// ---- Constructors --------------------------------------------------------

case class StGeomFromWkt(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g: Any): Any =
    UTF8String.fromString(CometGeoFallback.geomFromWkt(g.toString))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, g =>
      s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$.geomFromWkt($g.toString()))")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StGeomFromGeoJson(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g: Any): Any =
    UTF8String.fromString(CometGeoFallback.geomFromGeoJson(g.toString))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, g =>
      s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$.geomFromGeoJson($g.toString()))")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StPoint(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    UTF8String.fromString("POINT(" + g1.toString + " " + g2.toString + ")")
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g1, g2) =>
      s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
        s"\"POINT(\" + $g1.toString() + \" \" + $g2.toString() + \")\")")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

case class StMakeEnvelope(
    xmin: Expression,
    ymin: Expression,
    xmax: Expression,
    ymax: Expression)
    extends Expression
    with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullable: Boolean = true
  override def children: Seq[Expression] = Seq(xmin, ymin, xmax, ymax)
  override def supportCodegen: Boolean = false
  override def eval(input: InternalRow): Any = {
    val xv = xmin.eval(input); val yv = ymin.eval(input)
    val xv2 = xmax.eval(input); val yv2 = ymax.eval(input)
    if (xv == null || yv == null || xv2 == null || yv2 == null) null
    else UTF8String.fromString(CometGeoFallback.makeEnvelope(
      xv.toString.toDouble, yv.toString.toDouble, xv2.toString.toDouble, yv2.toString.toDouble))
  }
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ev
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(
      xmin = newChildren(0),
      ymin = newChildren(1),
      xmax = newChildren(2),
      ymax = newChildren(3))
}

case class StMakeLine(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    UTF8String.fromString(CometGeoFallback.makeLine(g1.toString, g2.toString))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g1, g2) =>
      s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".makeLine($g1.toString(), $g2.toString()))")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

// ---- Serializers ---------------------------------------------------------

case class StAsText(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g: Any): Any =
    UTF8String.fromString(CometGeoFallback.asText(g.toString))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, g =>
      s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$.asText($g.toString()))")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StAsGeoJson(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g: Any): Any =
    UTF8String.fromString(CometGeoFallback.asGeoJson(g.toString))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, g =>
      s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$.asGeoJson($g.toString()))")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

// ---- Additional predicates -----------------------------------------------

case class StCovers(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant {
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.covers(g1.toString, g2.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g1, g2) =>
      s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".covers($g1.toString(), $g2.toString())")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

case class StCoveredBy(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant {
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.coveredBy(g1.toString, g2.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g1, g2) =>
      s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".coveredBy($g1.toString(), $g2.toString())")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

case class StEquals(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant {
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.equals(g1.toString, g2.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g1, g2) =>
      s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".equals($g1.toString(), $g2.toString())")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

case class StTouches(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant {
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.touches(g1.toString, g2.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g1, g2) =>
      s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".touches($g1.toString(), $g2.toString())")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

case class StCrosses(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant {
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.crosses(g1.toString, g2.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g1, g2) =>
      s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".crosses($g1.toString(), $g2.toString())")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

case class StDisjoint(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant {
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.disjoint(g1.toString, g2.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g1, g2) =>
      s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".disjoint($g1.toString(), $g2.toString())")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

case class StOverlaps(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant {
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.overlaps(g1.toString, g2.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g1, g2) =>
      s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".overlaps($g1.toString(), $g2.toString())")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

// ---- Additional measurements ---------------------------------------------

case class StDistanceSphere(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant {
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.distanceSphere(g1.toString, g2.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g1, g2) =>
      s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".distanceSphere($g1.toString(), $g2.toString())")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

case class StPerimeter(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g: Any): Any = CometGeoFallback.perimeter(g.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, g =>
      s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$.perimeter($g.toString())")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StHausdorffDistance(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant {
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.hausdorffDistance(g1.toString, g2.toString)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g1, g2) =>
      s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".hausdorffDistance($g1.toString(), $g2.toString())")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

// ---- Additional transformations ------------------------------------------

case class StSimplifyPreserveTopology(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    UTF8String.fromString(CometGeoFallback.simplifyPreserveTopology(
      g1.toString, g2.toString.toDouble))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g1, g2) =>
      s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".simplifyPreserveTopology($g1.toString(), Double.parseDouble($g2.toString())))")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

case class StFlipCoordinates(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g: Any): Any =
    UTF8String.fromString(CometGeoFallback.flipCoordinates(g.toString))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, g =>
      s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".flipCoordinates($g.toString()))")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StBoundary(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g: Any): Any =
    UTF8String.fromString(CometGeoFallback.boundary(g.toString))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, g =>
      s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$.boundary($g.toString()))")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

// ---- Additional set operations -------------------------------------------

case class StDifference(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    UTF8String.fromString(CometGeoFallback.difference(g1.toString, g2.toString))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g1, g2) =>
      s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".difference($g1.toString(), $g2.toString()))")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

case class StSymDifference(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    UTF8String.fromString(CometGeoFallback.symDifference(g1.toString, g2.toString))
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (g1, g2) =>
      s"org.apache.spark.unsafe.types.UTF8String.fromString(" +
        s"org.apache.comet.expressions.CometGeoFallback$$.MODULE$$" +
        s".symDifference($g1.toString(), $g2.toString()))")
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

// ---- Registration helpers for SparkSessionExtensions.injectFunction ------

object GeoExpressions {

  type FunctionDescription =
    (FunctionIdentifier, ExpressionInfo, Seq[Expression] => Expression)

  private def desc(
      name: String,
      cls: Class[_],
      builder: Seq[Expression] => Expression): FunctionDescription =
    (new FunctionIdentifier(name), new ExpressionInfo(cls.getName, name), builder)

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
  val stLengthInfo: FunctionDescription =
    desc("st_length", classOf[StLength], args => StLength(args(0)))
  val stIsEmptyInfo: FunctionDescription =
    desc("st_isempty", classOf[StIsEmpty], args => StIsEmpty(args(0)))
  val stGeometryTypeInfo: FunctionDescription =
    desc("st_geometrytype", classOf[StGeometryType], args => StGeometryType(args(0)))
  val stNumPointsInfo: FunctionDescription =
    desc("st_numpoints", classOf[StNumPoints], args => StNumPoints(args(0)))
  val stXInfo: FunctionDescription =
    desc("st_x", classOf[StX], args => StX(args(0)))
  val stYInfo: FunctionDescription =
    desc("st_y", classOf[StY], args => StY(args(0)))
  val stEnvelopeInfo: FunctionDescription =
    desc("st_envelope", classOf[StEnvelope], args => StEnvelope(args(0)))
  val stConvexHullInfo: FunctionDescription =
    desc("st_convexhull", classOf[StConvexHull], args => StConvexHull(args(0)))
  val stSimplifyInfo: FunctionDescription =
    desc("st_simplify", classOf[StSimplify], args => StSimplify(args(0), args(1)))
  val stBufferInfo: FunctionDescription =
    desc("st_buffer", classOf[StBuffer], args => StBuffer(args(0), args(1)))
  val stUnionInfo: FunctionDescription =
    desc("st_union", classOf[StUnion], args => StUnion(args(0), args(1)))
  val stIntersectionInfo: FunctionDescription =
    desc("st_intersection", classOf[StIntersection], args => StIntersection(args(0), args(1)))
  val stGeomFromWktInfo: FunctionDescription =
    desc("st_geomfromwkt", classOf[StGeomFromWkt], args => StGeomFromWkt(args(0)))
  val stGeomFromGeoJsonInfo: FunctionDescription =
    desc("st_geomfromgeojson", classOf[StGeomFromGeoJson], args => StGeomFromGeoJson(args(0)))
  val stPointInfo: FunctionDescription =
    desc("st_point", classOf[StPoint], args => StPoint(args(0), args(1)))
  val stMakeEnvelopeInfo: FunctionDescription =
    desc("st_makeenvelope", classOf[StMakeEnvelope],
      args => StMakeEnvelope(args(0), args(1), args(2), args(3)))
  val stMakeLineInfo: FunctionDescription =
    desc("st_makeline", classOf[StMakeLine], args => StMakeLine(args(0), args(1)))
  val stAsTextInfo: FunctionDescription =
    desc("st_astext", classOf[StAsText], args => StAsText(args(0)))
  val stAsGeoJsonInfo: FunctionDescription =
    desc("st_asgeojson", classOf[StAsGeoJson], args => StAsGeoJson(args(0)))
  val stCoversInfo: FunctionDescription =
    desc("st_covers", classOf[StCovers], args => StCovers(args(0), args(1)))
  val stCoveredByInfo: FunctionDescription =
    desc("st_coveredby", classOf[StCoveredBy], args => StCoveredBy(args(0), args(1)))
  val stEqualsInfo: FunctionDescription =
    desc("st_equals", classOf[StEquals], args => StEquals(args(0), args(1)))
  val stTouchesInfo: FunctionDescription =
    desc("st_touches", classOf[StTouches], args => StTouches(args(0), args(1)))
  val stCrossesInfo: FunctionDescription =
    desc("st_crosses", classOf[StCrosses], args => StCrosses(args(0), args(1)))
  val stDisjointInfo: FunctionDescription =
    desc("st_disjoint", classOf[StDisjoint], args => StDisjoint(args(0), args(1)))
  val stOverlapsInfo: FunctionDescription =
    desc("st_overlaps", classOf[StOverlaps], args => StOverlaps(args(0), args(1)))
  val stDistanceSphereInfo: FunctionDescription =
    desc("st_distancesphere", classOf[StDistanceSphere], args => StDistanceSphere(args(0), args(1)))
  val stPerimeterInfo: FunctionDescription =
    desc("st_perimeter", classOf[StPerimeter], args => StPerimeter(args(0)))
  val stHausdorffDistanceInfo: FunctionDescription =
    desc("st_hausdorffdistance", classOf[StHausdorffDistance],
      args => StHausdorffDistance(args(0), args(1)))
  val stSimplifyPreserveTopologyInfo: FunctionDescription =
    desc("st_simplifypreservetopology", classOf[StSimplifyPreserveTopology],
      args => StSimplifyPreserveTopology(args(0), args(1)))
  val stFlipCoordinatesInfo: FunctionDescription =
    desc("st_flipcoordinates", classOf[StFlipCoordinates], args => StFlipCoordinates(args(0)))
  val stBoundaryInfo: FunctionDescription =
    desc("st_boundary", classOf[StBoundary], args => StBoundary(args(0)))
  val stDifferenceInfo: FunctionDescription =
    desc("st_difference", classOf[StDifference], args => StDifference(args(0), args(1)))
  val stSymDifferenceInfo: FunctionDescription =
    desc("st_symdifference", classOf[StSymDifference], args => StSymDifference(args(0), args(1)))
}
