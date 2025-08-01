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

import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto, scalarFunctionExprToProtoWithReturnType}

object CometMapKeys extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val mk = expr.asInstanceOf[MapKeys]
    val childExpr = exprToProtoInternal(mk.child, inputs, binding)
    val mapKeysScalarExpr = scalarFunctionExprToProto("map_keys", childExpr)
    optExprWithInfo(mapKeysScalarExpr, expr, expr.children: _*)
  }
}

object CometMapEntries extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val mk = expr.asInstanceOf[MapEntries]
    val childExpr = exprToProtoInternal(mk.child, inputs, binding)
    val mapEntriesScalarExpr = scalarFunctionExprToProto("map_entries", childExpr)
    optExprWithInfo(mapEntriesScalarExpr, expr, expr.children: _*)
  }
}

object CometMapValues extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val mv = expr.asInstanceOf[MapValues]
    val childExpr = exprToProtoInternal(mv.child, inputs, binding)
    val mapValuesScalarExpr = scalarFunctionExprToProto("map_values", childExpr)
    optExprWithInfo(mapValuesScalarExpr, expr, expr.children: _*)
  }
}

object CometMapExtract extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val gmv = expr.asInstanceOf[GetMapValue]
    val mapExpr = exprToProtoInternal(gmv.child, inputs, binding)
    val keyExpr = exprToProtoInternal(gmv.key, inputs, binding)
    val mapExtractExpr = scalarFunctionExprToProto("map_extract", mapExpr, keyExpr)
    optExprWithInfo(mapExtractExpr, expr, expr.children: _*)
  }
}

object CometMapFromArrays extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val mfa = expr.asInstanceOf[MapFromArrays]
    val keysExpr = exprToProtoInternal(mfa.left, inputs, binding)
    val valuesExpr = exprToProtoInternal(mfa.right, inputs, binding)
    val keyType = mfa.left.dataType.asInstanceOf[ArrayType].elementType
    val valueType = mfa.right.dataType.asInstanceOf[ArrayType].elementType
    val returnType = MapType(keyType = keyType, valueType = valueType)
    val mapFromArraysExpr =
      scalarFunctionExprToProtoWithReturnType("map", returnType, keysExpr, valuesExpr)
    optExprWithInfo(mapFromArraysExpr, expr, expr.children: _*)
  }
}
