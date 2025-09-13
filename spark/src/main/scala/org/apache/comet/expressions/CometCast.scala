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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, DecimalType, NullType, StructType}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.{CometExpressionSerde, Compatible, ExprOuterClass, Incompatible, SupportLevel, Unsupported}
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{evalModeToProto, exprToProtoInternal, serializeDataType}
import org.apache.comet.shims.CometExprShim

object CometCast extends CometExpressionSerde[Cast] with CometExprShim {

  def supportedTypes: Seq[DataType] =
    Seq(
      DataTypes.BooleanType,
      DataTypes.ByteType,
      DataTypes.ShortType,
      DataTypes.IntegerType,
      DataTypes.LongType,
      DataTypes.FloatType,
      DataTypes.DoubleType,
      DataTypes.createDecimalType(10, 2),
      DataTypes.StringType,
      DataTypes.BinaryType,
      DataTypes.DateType,
      DataTypes.TimestampType)
  // TODO add DataTypes.TimestampNTZType for Spark 3.4 and later
  // https://github.com/apache/datafusion-comet/issues/378

  override def getSupportLevel(cast: Cast): SupportLevel = {
    isSupported(cast.child.dataType, cast.dataType, cast.timeZoneId, evalMode(cast))
  }

  override def convert(
      cast: Cast,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(cast.child, inputs, binding)
    if (childExpr.isDefined) {
      castToProto(cast, cast.timeZoneId, cast.dataType, childExpr.get, evalMode(cast))
    } else {
      withInfo(cast, cast.child)
      None
    }
  }

  /**
   * Wrap an already serialized expression in a cast.
   */
  def castToProto(
      expr: Expression,
      timeZoneId: Option[String],
      dt: DataType,
      childExpr: Expr,
      evalMode: CometEvalMode.Value): Option[Expr] = {
    serializeDataType(dt) match {
      case Some(dataType) =>
        val castBuilder = ExprOuterClass.Cast.newBuilder()
        castBuilder.setChild(childExpr)
        castBuilder.setDatatype(dataType)
        castBuilder.setEvalMode(evalModeToProto(evalMode))
        castBuilder.setAllowIncompat(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.get())
        castBuilder.setTimezone(timeZoneId.getOrElse("UTC"))
        Some(
          ExprOuterClass.Expr
            .newBuilder()
            .setCast(castBuilder)
            .build())
      case _ =>
        withInfo(expr, s"Unsupported datatype in castToProto: $dt")
        None
    }
  }

  def isSupported(
      fromType: DataType,
      toType: DataType,
      timeZoneId: Option[String],
      evalMode: CometEvalMode.Value): SupportLevel = {

    if (fromType == toType) {
      return Compatible()
    }

    (fromType, toType) match {
      case (dt: ArrayType, _: ArrayType) if dt.elementType == NullType => Compatible()
      case (dt: ArrayType, dt1: ArrayType) =>
        isSupported(dt.elementType, dt1.elementType, timeZoneId, evalMode)
      case (dt: DataType, _) if dt.typeName == "timestamp_ntz" =>
        // https://github.com/apache/datafusion-comet/issues/378
        toType match {
          case DataTypes.TimestampType | DataTypes.DateType | DataTypes.StringType =>
            Incompatible()
          case _ =>
            unsupported(fromType, toType)
        }
      case (_: DecimalType, _: DecimalType) =>
        Compatible()
      case (DataTypes.StringType, _) =>
        canCastFromString(toType, timeZoneId, evalMode)
      case (_, DataTypes.StringType) =>
        canCastToString(fromType, timeZoneId, evalMode)
      case (DataTypes.TimestampType, _) =>
        canCastFromTimestamp(toType)
      case (_: DecimalType, _) =>
        canCastFromDecimal(toType)
      case (DataTypes.BooleanType, _) =>
        canCastFromBoolean(toType)
      case (DataTypes.ByteType, _) =>
        canCastFromByte(toType)
      case (DataTypes.ShortType, _) =>
        canCastFromShort(toType)
      case (DataTypes.IntegerType, _) =>
        canCastFromInt(toType)
      case (DataTypes.LongType, _) =>
        canCastFromLong(toType)
      case (DataTypes.FloatType, _) =>
        canCastFromFloat(toType)
      case (DataTypes.DoubleType, _) =>
        canCastFromDouble(toType)
      case (from_struct: StructType, to_struct: StructType) =>
        from_struct.fields.zip(to_struct.fields).foreach { case (a, b) =>
          isSupported(a.dataType, b.dataType, timeZoneId, evalMode) match {
            case Compatible(_) =>
            // all good
            case other =>
              return other
          }
        }
        Compatible()
      case _ => unsupported(fromType, toType)
    }
  }

  private def canCastFromString(
      toType: DataType,
      timeZoneId: Option[String],
      evalMode: CometEvalMode.Value): SupportLevel = {
    toType match {
      case DataTypes.BooleanType =>
        Compatible()
      case DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType |
          DataTypes.LongType =>
        Compatible()
      case DataTypes.BinaryType =>
        Compatible()
      case DataTypes.FloatType | DataTypes.DoubleType =>
        // https://github.com/apache/datafusion-comet/issues/326
        Incompatible(
          Some(
            "Does not support inputs ending with 'd' or 'f'. Does not support 'inf'. " +
              "Does not support ANSI mode."))
      case _: DecimalType =>
        // https://github.com/apache/datafusion-comet/issues/325
        Incompatible(
          Some("Does not support inputs ending with 'd' or 'f'. Does not support 'inf'. " +
            "Does not support ANSI mode. Returns 0.0 instead of null if input contains no digits"))
      case DataTypes.DateType =>
        // https://github.com/apache/datafusion-comet/issues/327
        Compatible(Some("Only supports years between 262143 BC and 262142 AD"))
      case DataTypes.TimestampType if timeZoneId.exists(tz => tz != "UTC") =>
        Incompatible(Some(s"Cast will use UTC instead of $timeZoneId"))
      case DataTypes.TimestampType if evalMode == CometEvalMode.ANSI =>
        Incompatible(Some("ANSI mode not supported"))
      case DataTypes.TimestampType =>
        // https://github.com/apache/datafusion-comet/issues/328
        Incompatible(Some("Not all valid formats are supported"))
      case _ =>
        unsupported(DataTypes.StringType, toType)
    }
  }

  private def canCastToString(
      fromType: DataType,
      timeZoneId: Option[String],
      evalMode: CometEvalMode.Value): SupportLevel = {
    fromType match {
      case DataTypes.BooleanType => Compatible()
      case DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType |
          DataTypes.LongType =>
        Compatible()
      case DataTypes.DateType => Compatible()
      case DataTypes.TimestampType => Compatible()
      case DataTypes.FloatType | DataTypes.DoubleType =>
        Compatible(
          Some(
            "There can be differences in precision. " +
              "For example, the input \"1.4E-45\" will produce 1.0E-45 " +
              "instead of 1.4E-45"))
      case _: DecimalType =>
        // https://github.com/apache/datafusion-comet/issues/1068
        Compatible(
          Some(
            "There can be formatting differences in some case due to Spark using " +
              "scientific notation where Comet does not"))
      case DataTypes.BinaryType =>
        // Before Spark 4.0, the default format is discrete HEX string
        // Since Spark 4.0, the format is controlled by Spark's SQLConf.BINARY_OUTPUT_STYLE
        Compatible()
      case StructType(fields) =>
        for (field <- fields) {
          isSupported(field.dataType, DataTypes.StringType, timeZoneId, evalMode) match {
            case s: Incompatible =>
              return s
            case u: Unsupported =>
              return u
            case _ =>
          }
        }
        Compatible()
      case _ => unsupported(fromType, DataTypes.StringType)
    }
  }

  private def canCastFromTimestamp(toType: DataType): SupportLevel = {
    toType match {
      case DataTypes.BooleanType | DataTypes.ByteType | DataTypes.ShortType |
          DataTypes.IntegerType =>
        // https://github.com/apache/datafusion-comet/issues/352
        // this seems like an edge case that isn't important for us to support
        unsupported(DataTypes.TimestampType, toType)
      case DataTypes.LongType =>
        // https://github.com/apache/datafusion-comet/issues/352
        Compatible()
      case DataTypes.StringType => Compatible()
      case DataTypes.DateType => Compatible()
      case _ => unsupported(DataTypes.TimestampType, toType)
    }
  }

  private def canCastFromBoolean(toType: DataType): SupportLevel = toType match {
    case DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType | DataTypes.LongType |
        DataTypes.FloatType | DataTypes.DoubleType =>
      Compatible()
    case _ => unsupported(DataTypes.BooleanType, toType)
  }

  private def canCastFromByte(toType: DataType): SupportLevel = toType match {
    case DataTypes.BooleanType =>
      Compatible()
    case DataTypes.ShortType | DataTypes.IntegerType | DataTypes.LongType =>
      Compatible()
    case DataTypes.FloatType | DataTypes.DoubleType | _: DecimalType =>
      Compatible()
    case _ =>
      unsupported(DataTypes.ByteType, toType)
  }

  private def canCastFromShort(toType: DataType): SupportLevel = toType match {
    case DataTypes.BooleanType =>
      Compatible()
    case DataTypes.ByteType | DataTypes.IntegerType | DataTypes.LongType =>
      Compatible()
    case DataTypes.FloatType | DataTypes.DoubleType | _: DecimalType =>
      Compatible()
    case _ =>
      unsupported(DataTypes.ShortType, toType)
  }

  private def canCastFromInt(toType: DataType): SupportLevel = toType match {
    case DataTypes.BooleanType =>
      Compatible()
    case DataTypes.ByteType | DataTypes.ShortType | DataTypes.LongType =>
      Compatible()
    case DataTypes.FloatType | DataTypes.DoubleType =>
      Compatible()
    case _: DecimalType =>
      Incompatible(Some("No overflow check"))
    case _ =>
      unsupported(DataTypes.IntegerType, toType)
  }

  private def canCastFromLong(toType: DataType): SupportLevel = toType match {
    case DataTypes.BooleanType =>
      Compatible()
    case DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType =>
      Compatible()
    case DataTypes.FloatType | DataTypes.DoubleType =>
      Compatible()
    case _: DecimalType =>
      Incompatible(Some("No overflow check"))
    case _ =>
      unsupported(DataTypes.LongType, toType)
  }

  private def canCastFromFloat(toType: DataType): SupportLevel = toType match {
    case DataTypes.BooleanType | DataTypes.DoubleType | DataTypes.ByteType | DataTypes.ShortType |
        DataTypes.IntegerType | DataTypes.LongType =>
      Compatible()
    case _: DecimalType =>
      // https://github.com/apache/datafusion-comet/issues/1371
      Incompatible(Some("There can be rounding differences"))
    case _ =>
      unsupported(DataTypes.FloatType, toType)
  }

  private def canCastFromDouble(toType: DataType): SupportLevel = toType match {
    case DataTypes.BooleanType | DataTypes.FloatType | DataTypes.ByteType | DataTypes.ShortType |
        DataTypes.IntegerType | DataTypes.LongType =>
      Compatible()
    case _: DecimalType =>
      // https://github.com/apache/datafusion-comet/issues/1371
      Incompatible(Some("There can be rounding differences"))
    case _ => unsupported(DataTypes.DoubleType, toType)
  }

  private def canCastFromDecimal(toType: DataType): SupportLevel = toType match {
    case DataTypes.FloatType | DataTypes.DoubleType | DataTypes.ByteType | DataTypes.ShortType |
        DataTypes.IntegerType | DataTypes.LongType =>
      Compatible()
    case _ => Unsupported(Some(s"Cast from DecimalType to $toType is not supported"))
  }

  private def unsupported(fromType: DataType, toType: DataType): Unsupported = {
    Unsupported(Some(s"Cast from $fromType to $toType is not supported"))
  }
}
