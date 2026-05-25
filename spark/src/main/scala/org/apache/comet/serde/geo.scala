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

import scala.util.Try

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

import org.apache.comet.expressions.{StArea, StBuffer, StCentroid, StContains, StConvexHull, StDistance, StEnvelope, StGeometryType, StIntersection, StIntersects, StIsEmpty, StLength, StNumPoints, StSimplify, StUnion, StWithin, StX, StY}
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto}

/**
 * Serde for native Comet geo expressions and optional Sedona ST_ expressions. Maps each to the
 * corresponding named ScalarFunc so the DataFusion planner resolves it to the Rust geo UDF.
 *
 * Sedona entries are added only when Sedona is present on the classpath at runtime.
 */
private[serde] object CometGeoExpr {

  def buildSerdeMap(): Map[Class[_ <: Expression], CometExpressionSerde[_]] = {
    val nativeEntries: Map[Class[_ <: Expression], CometExpressionSerde[_]] = Map(
      classOf[StContains] -> new CometGeoScalarFunc("st_contains"),
      classOf[StIntersects] -> new CometGeoScalarFunc("st_intersects"),
      classOf[StWithin] -> new CometGeoScalarFunc("st_within"),
      classOf[StDistance] -> new CometGeoScalarFunc("st_distance"),
      classOf[StArea] -> new CometGeoScalarFunc("st_area"),
      classOf[StCentroid] -> new CometGeoScalarFunc("st_centroid"),
      classOf[StLength] -> new CometGeoScalarFunc("st_length"),
      classOf[StIsEmpty] -> new CometGeoScalarFunc("st_isempty"),
      classOf[StGeometryType] -> new CometGeoScalarFunc("st_geometrytype"),
      classOf[StNumPoints] -> new CometGeoScalarFunc("st_numpoints"),
      classOf[StX] -> new CometGeoScalarFunc("st_x"),
      classOf[StY] -> new CometGeoScalarFunc("st_y"),
      classOf[StEnvelope] -> new CometGeoScalarFunc("st_envelope"),
      classOf[StConvexHull] -> new CometGeoScalarFunc("st_convexhull"),
      classOf[StSimplify] -> new CometGeoScalarFunc("st_simplify"),
      classOf[StBuffer] -> new CometGeoScalarFunc("st_buffer"),
      classOf[StUnion] -> new CometGeoScalarFunc("st_union"),
      classOf[StIntersection] -> new CometGeoScalarFunc("st_intersection"))

    val sedonaEntries: Map[Class[_ <: Expression], CometExpressionSerde[_]] = Seq(
      "org.apache.sedona.sql.utils.expressions.ST_Contains" -> "st_contains",
      "org.apache.sedona.sql.utils.expressions.ST_Intersects" -> "st_intersects",
      "org.apache.sedona.sql.utils.expressions.ST_Distance" -> "st_distance",
      "org.apache.sedona.sql.utils.expressions.ST_Within" -> "st_within",
      "org.apache.sedona.sql.utils.expressions.ST_Area" -> "st_area",
      "org.apache.sedona.sql.utils.expressions.ST_Centroid" -> "st_centroid",
      "org.apache.sedona.sql.utils.expressions.ST_Length" -> "st_length",
      "org.apache.sedona.sql.utils.expressions.ST_IsEmpty" -> "st_isempty",
      "org.apache.sedona.sql.utils.expressions.ST_GeometryType" -> "st_geometrytype",
      "org.apache.sedona.sql.utils.expressions.ST_NumPoints" -> "st_numpoints",
      "org.apache.sedona.sql.utils.expressions.ST_X" -> "st_x",
      "org.apache.sedona.sql.utils.expressions.ST_Y" -> "st_y",
      "org.apache.sedona.sql.utils.expressions.ST_Envelope" -> "st_envelope",
      "org.apache.sedona.sql.utils.expressions.ST_ConvexHull" -> "st_convexhull",
      "org.apache.sedona.sql.utils.expressions.ST_Simplify" -> "st_simplify",
      "org.apache.sedona.sql.utils.expressions.ST_Buffer" -> "st_buffer",
      "org.apache.sedona.sql.utils.expressions.ST_Union" -> "st_union",
      "org.apache.sedona.sql.utils.expressions.ST_Intersection" -> "st_intersection").flatMap {
      case (className, funcName) =>
        // scalastyle:off classforname
        Try(Class.forName(className).asInstanceOf[Class[Expression]])
          // scalastyle:on classforname
          .toOption
          .map(cls => cls -> new CometGeoScalarFunc(funcName))
    }.toMap

    nativeEntries ++ sedonaEntries
  }
}

/**
 * Generic serde for a geo expression: emits ScalarFunc { func = funcName } so the DataFusion
 * planner resolves it to the named Rust UDF registered in the SessionContext.
 */
private[serde] class CometGeoScalarFunc(funcName: String)
    extends CometExpressionSerde[Expression] {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val childExprs = expr.children.map(exprToProtoInternal(_, inputs, binding))
    val optExpr = scalarFunctionExprToProto(funcName, childExprs: _*)
    optExprWithInfo(optExpr, expr, expr.children: _*)
  }
}
