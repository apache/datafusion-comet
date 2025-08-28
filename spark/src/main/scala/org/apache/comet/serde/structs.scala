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

import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateNamedStruct, GetArrayStructFields, GetStructField, StructsToJson}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, MapType, StructType}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.exprToProtoInternal

object CometCreateNamedStruct extends CometExpressionSerde[CreateNamedStruct] {
  override def convert(
      expr: CreateNamedStruct,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (expr.names.length != expr.names.distinct.length) {
      withInfo(expr, "CreateNamedStruct with duplicate field names are not supported")
      return None
    }

    val valExprs = expr.valExprs.map(exprToProtoInternal(_, inputs, binding))

    if (valExprs.forall(_.isDefined)) {
      val structBuilder = ExprOuterClass.CreateNamedStruct.newBuilder()
      structBuilder.addAllValues(valExprs.map(_.get).asJava)
      structBuilder.addAllNames(expr.names.map(_.toString).asJava)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setCreateNamedStruct(structBuilder)
          .build())
    } else {
      withInfo(expr, "unsupported arguments for CreateNamedStruct", expr.valExprs: _*)
      None
    }

  }
}

object CometGetStructField extends CometExpressionSerde[GetStructField] {
  override def convert(
      expr: GetStructField,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    exprToProtoInternal(expr.child, inputs, binding).map { childExpr =>
      val getStructFieldBuilder = ExprOuterClass.GetStructField
        .newBuilder()
        .setChild(childExpr)
        .setOrdinal(expr.ordinal)

      ExprOuterClass.Expr
        .newBuilder()
        .setGetStructField(getStructFieldBuilder)
        .build()
    }
  }
}

object CometGetArrayStructFields extends CometExpressionSerde[GetArrayStructFields] {
  override def convert(
      expr: GetArrayStructFields,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)

    if (childExpr.isDefined) {
      val arrayStructFieldsBuilder = ExprOuterClass.GetArrayStructFields
        .newBuilder()
        .setChild(childExpr.get)
        .setOrdinal(expr.ordinal)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setGetArrayStructFields(arrayStructFieldsBuilder)
          .build())
    } else {
      withInfo(expr, "unsupported arguments for GetArrayStructFields", expr.child)
      None
    }
  }
}

object CometStructsToJson extends CometExpressionSerde[StructsToJson] {

  override def convert(
      expr: StructsToJson,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (expr.options.nonEmpty) {
      withInfo(expr, "StructsToJson with options is not supported")
      None
    } else {

      def isSupportedType(dt: DataType): Boolean = {
        dt match {
          case StructType(fields) =>
            fields.forall(f => isSupportedType(f.dataType))
          case DataTypes.BooleanType | DataTypes.ByteType | DataTypes.ShortType |
              DataTypes.IntegerType | DataTypes.LongType | DataTypes.FloatType |
              DataTypes.DoubleType | DataTypes.StringType =>
            true
          case DataTypes.DateType | DataTypes.TimestampType =>
            // TODO implement these types with tests for formatting options and timezone
            false
          case _: MapType | _: ArrayType =>
            // Spark supports map and array in StructsToJson but this is not yet
            // implemented in Comet
            false
          case _ => false
        }
      }

      val isSupported = expr.child.dataType match {
        case s: StructType =>
          s.fields.forall(f => isSupportedType(f.dataType))
        case _: MapType | _: ArrayType =>
          // Spark supports map and array in StructsToJson but this is not yet
          // implemented in Comet
          false
        case _ =>
          false
      }

      if (isSupported) {
        exprToProtoInternal(expr.child, inputs, binding) match {
          case Some(p) =>
            val toJson = ExprOuterClass.ToJson
              .newBuilder()
              .setChild(p)
              .setTimezone(expr.timeZoneId.getOrElse("UTC"))
              .setIgnoreNullFields(true)
              .build()
            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setToJson(toJson)
                .build())
          case _ =>
            withInfo(expr, expr.child)
            None
        }
      } else {
        withInfo(expr, "Unsupported data type", expr.child)
        None
      }
    }
  }
}
