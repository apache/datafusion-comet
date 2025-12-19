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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{ArrayType, MapType}

import org.apache.comet.serde.QueryPlanSerde.{exprToProto, exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto, scalarFunctionExprToProtoWithReturnType}

object CometMapKeys extends CometExpressionSerde[MapKeys] {

  override def convert(
      expr: MapKeys,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val mapKeysScalarExpr = scalarFunctionExprToProto("map_keys", childExpr)
    optExprWithInfo(mapKeysScalarExpr, expr, expr.children: _*)
  }
}

object CometMapEntries extends CometExpressionSerde[MapEntries] {

  override def convert(
      expr: MapEntries,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val mapEntriesScalarExpr = scalarFunctionExprToProto("map_entries", childExpr)
    optExprWithInfo(mapEntriesScalarExpr, expr, expr.children: _*)
  }
}

object CometMapValues extends CometExpressionSerde[MapValues] {

  override def convert(
      expr: MapValues,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val mapValuesScalarExpr = scalarFunctionExprToProto("map_values", childExpr)
    optExprWithInfo(mapValuesScalarExpr, expr, expr.children: _*)
  }
}

object CometMapExtract extends CometExpressionSerde[GetMapValue] {

  override def convert(
      expr: GetMapValue,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val mapExpr = exprToProtoInternal(expr.child, inputs, binding)
    val keyExpr = exprToProtoInternal(expr.key, inputs, binding)
    val mapExtractExpr = scalarFunctionExprToProto("map_extract", mapExpr, keyExpr)
    optExprWithInfo(mapExtractExpr, expr, expr.children: _*)
  }
}

object CometMapFromArrays extends CometExpressionSerde[MapFromArrays] {

  override def convert(
      expr: MapFromArrays,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val keysExpr = exprToProtoInternal(expr.left, inputs, binding)
    val valuesExpr = exprToProtoInternal(expr.right, inputs, binding)
    val keyType = expr.left.dataType.asInstanceOf[ArrayType].elementType
    val valueType = expr.right.dataType.asInstanceOf[ArrayType].elementType
    val returnType = MapType(keyType = keyType, valueType = valueType)
    val mapFromArraysExpr =
      scalarFunctionExprToProtoWithReturnType("map", returnType, false, keysExpr, valuesExpr)
    optExprWithInfo(mapFromArraysExpr, expr, expr.children: _*)
  }
}

object CometCreateMap extends CometExpressionSerde[CreateMap] {
  override def convert(
      expr: CreateMap,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val keys = CreateArray(expr.keys)
    val values = CreateArray(expr.values)
    val keysProtoExpr = exprToProtoInternal(keys, inputs, binding)
    val valuesProtoExpr = exprToProtoInternal(values, inputs, binding)
    // scalastyle:off println
    println(keysProtoExpr)
    // scalastyle:on println line=102 column=4
    // scalastyle:off println
    // println(valuesProtoExpr)
    // scalastyle:on println line=103 column=4
    val createMapScalarExpr =
      scalarFunctionExprToProtoWithReturnType(
        "map",
        expr.dataType,
        false,
        keysProtoExpr,
        valuesProtoExpr)
    optExprWithInfo(createMapScalarExpr, expr, expr.children: _*)
  }
}
