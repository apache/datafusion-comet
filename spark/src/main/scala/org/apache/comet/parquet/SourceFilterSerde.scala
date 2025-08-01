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

import java.math.{BigDecimal => JavaBigDecimal}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

import org.apache.comet.serde.ExprOuterClass
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.Datatype.DataType
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

object SourceFilterSerde extends Logging {

  def createNameExpr(
      name: String,
      schema: StructType): Option[(DataType, ExprOuterClass.Expr)] = {
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
          field.dataType,
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

  /**
   * create a literal value native expression for source filter value, the value is a scala value
   */
  def createValueExpr(value: Any, dataType: DataType): Option[ExprOuterClass.Expr] = {
    val exprBuilder = ExprOuterClass.Literal.newBuilder()
    var valueIsSet = true
    if (value == null) {
      exprBuilder.setIsNull(true)
    } else {
      exprBuilder.setIsNull(false)
      // value is a scala value, not a catalyst value
      // refer to org.apache.spark.sql.catalyst.CatalystTypeConverters.CatalystTypeConverter#toScala
      dataType match {
        case _: BooleanType => exprBuilder.setBoolVal(value.asInstanceOf[Boolean])
        case _: ByteType => exprBuilder.setByteVal(value.asInstanceOf[Byte])
        case _: ShortType => exprBuilder.setShortVal(value.asInstanceOf[Short])
        case _: IntegerType => exprBuilder.setIntVal(value.asInstanceOf[Int])
        case _: LongType => exprBuilder.setLongVal(value.asInstanceOf[Long])
        case _: FloatType => exprBuilder.setFloatVal(value.asInstanceOf[Float])
        case _: DoubleType => exprBuilder.setDoubleVal(value.asInstanceOf[Double])
        case _: StringType => exprBuilder.setStringVal(value.asInstanceOf[String])
        case _: TimestampType =>
          value match {
            case v: Timestamp => exprBuilder.setLongVal(DateTimeUtils.fromJavaTimestamp(v))
            case v: Instant => exprBuilder.setLongVal(DateTimeUtils.instantToMicros(v))
            case v: Long => exprBuilder.setLongVal(v)
            case _ =>
              valueIsSet = false
              logWarning(s"Unexpected timestamp type '${value.getClass}' for value '$value'")
          }
        case _: TimestampNTZType =>
          value match {
            case v: LocalDateTime =>
              exprBuilder.setLongVal(DateTimeUtils.localDateTimeToMicros(v))
            case v: Long => exprBuilder.setLongVal(v)
            case _ =>
              valueIsSet = false
              logWarning(s"Unexpected timestamp type '${value.getClass}' for value' $value'")
          }
        case _: DecimalType =>
          // Pass decimal literal as bytes.
          val unscaled = value.asInstanceOf[JavaBigDecimal].unscaledValue
          exprBuilder.setDecimalVal(com.google.protobuf.ByteString.copyFrom(unscaled.toByteArray))
        case _: BinaryType =>
          val byteStr =
            com.google.protobuf.ByteString.copyFrom(value.asInstanceOf[Array[Byte]])
          exprBuilder.setBytesVal(byteStr)
        case _: DateType =>
          value match {
            case v: LocalDate => exprBuilder.setIntVal(DateTimeUtils.localDateToDays(v))
            case v: Date => exprBuilder.setIntVal(DateTimeUtils.fromJavaDate(v))
            case v: Int => exprBuilder.setIntVal(v)
            case _ =>
              valueIsSet = false
              logWarning(s"Unexpected date type '${value.getClass}' for value '$value'")
          }
        case dt =>
          valueIsSet = false
          logWarning(s"Unexpected data type '$dt' for literal value '$value'")
      }
    }

    val dt = serializeDataType(dataType)

    if (valueIsSet && dt.isDefined) {
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
