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

package org.apache.comet.parquet

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.serde.ExprOuterClass
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

object SourceFilterSerde extends Logging {

  def createNameExpr(name: String, schema: StructType): Option[ExprOuterClass.Expr] = {
    val filedWithIndex = schema.fields.zipWithIndex.find { case (field, _) =>
      field.name == name
    }
    if (filedWithIndex.isDefined) {
      val (field, index) = filedWithIndex.get
      val dataType = serializeDataType(field.dataType)
      if (dataType.isDefined) {
        val boundExpr = ExprOuterClass.BoundReference
          .newBuilder()
          .setIndex(index)
          .setDatatype(dataType.get)
          .build()
        Some(
          ExprOuterClass.Expr
            .newBuilder()
            .setBound(boundExpr)
            .build())
      } else {
        None
      }
    } else {
      None
    }

  }

  def createValueExpr(value: Any): Option[ExprOuterClass.Expr] = {
    val exprBuilder = ExprOuterClass.Literal.newBuilder()
    if (value == null) {
      exprBuilder.setIsNull(true)
      Some(ExprOuterClass.Expr.newBuilder().setLiteral(exprBuilder).build())
    } else {
      exprBuilder.setIsNull(false)
      val dataType: Option[DataType] = value match {
        case v: Boolean =>
          exprBuilder.setBoolVal(v)
          Some(BooleanType)
        case v: Byte =>
          exprBuilder.setByteVal(v)
          Some(ByteType)
        case v: Short =>
          exprBuilder.setShortVal(v)
          Some(ShortType)
        case v: Int =>
          exprBuilder.setIntVal(v)
          Some(IntegerType)
        case v: Long =>
          exprBuilder.setLongVal(v)
          Some(LongType)
        case v: Float =>
          exprBuilder.setFloatVal(v)
          Some(FloatType)
        case v: Double =>
          exprBuilder.setDoubleVal(v)
          Some(DoubleType)
        case v: UTF8String =>
          exprBuilder.setStringVal(v.toString)
          Some(StringType)
        case v: Decimal =>
          val unscaled = v.toBigDecimal.underlying.unscaledValue
          exprBuilder.setDecimalVal(com.google.protobuf.ByteString.copyFrom(unscaled.toByteArray))
          Some(DecimalType(v.precision, v.scale))
        case v: Array[Byte] =>
          val byteStr =
            com.google.protobuf.ByteString.copyFrom(v)
          exprBuilder.setBytesVal(byteStr)
          Some(BinaryType)
        case v: java.sql.Date =>
          exprBuilder.setIntVal(v.getTime.toInt)
          Some(DateType)
        case v: java.sql.Timestamp =>
          exprBuilder.setLongVal(v.getTime)
          Some(TimestampType)
        case v: java.time.Instant =>
          exprBuilder.setLongVal(v.toEpochMilli)
          Some(TimestampType)
        case _ =>
          logWarning(s"Unsupported literal type: ${value.getClass}")
          None
      }
      if (dataType.isDefined) {
        val dt = serializeDataType(dataType.get)
        exprBuilder.setDatatype(dt.get)
        Some(
          ExprOuterClass.Expr
            .newBuilder()
            .setLiteral(exprBuilder)
            .build())
      } else {
        None
      }
    }
  }

  def createUnaryExpr(
      childExpr: Expr,
      f: (ExprOuterClass.Expr.Builder, ExprOuterClass.UnaryExpr) => ExprOuterClass.Expr.Builder)
      : ExprOuterClass.Expr = {
    // create the generic UnaryExpr message
    val inner = ExprOuterClass.UnaryExpr
      .newBuilder()
      .setChild(childExpr)
      .build()
    f(
      ExprOuterClass.Expr
        .newBuilder(),
      inner).build()
  }

  def createBinaryExpr(
      leftExpr: Expr,
      rightExpr: Expr,
      f: (ExprOuterClass.Expr.Builder, ExprOuterClass.BinaryExpr) => ExprOuterClass.Expr.Builder)
      : ExprOuterClass.Expr = {
    // create the generic BinaryExpr message
    val inner = ExprOuterClass.BinaryExpr
      .newBuilder()
      .setLeft(leftExpr)
      .setRight(rightExpr)
      .build()
    f(
      ExprOuterClass.Expr
        .newBuilder(),
      inner).build()
  }

}
