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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, DecimalType, MapType, StructType}
import org.apache.spark.sql.types.DataTypes.{BinaryType, BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampNTZType, TimestampType}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto, scalarFunctionExprToProtoWithReturnType, serializeDataType}

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

  override def getSupportLevel(expr: CreateMap): SupportLevel = {
    Compatible(None)
  }

  override def convert(
      expr: CreateMap,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val keysArray = CreateArray(expr.keys)
    val valuesArray = CreateArray(expr.values)
    val keysExprProto = exprToProtoInternal(keysArray, inputs, binding)
    val valuesExprProto = exprToProtoInternal(valuesArray, inputs, binding)
    val createMapExprProto =
      scalarFunctionExprToProtoWithReturnType(
        "map_from_arrays",
        expr.dataType,
        false,
        keysExprProto,
        valuesExprProto)
    optExprWithInfo(createMapExprProto, expr, expr.children: _*)
  }
}

sealed trait MapBase {

  def containsBinary(dataType: DataType): Boolean = {
    dataType match {
      case BinaryType => true
      case StructType(fields) => fields.exists(field => containsBinary(field.dataType))
      case ArrayType(elementType, _) => containsBinary(elementType)
      case _ => false
    }
  }
}
