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

import org.apache.spark.sql.types.{DataType, DataTypes, DecimalType, StructType}

sealed trait SupportLevel

/** We support this feature with full compatibility with Spark */
case class Compatible(notes: Option[String] = None) extends SupportLevel

/** We support this feature but results can be different from Spark */
case class Incompatible(notes: Option[String] = None) extends SupportLevel

/** We do not support this feature */
object Unsupported extends SupportLevel

object CometCast {

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

  def isSupported(
      fromType: DataType,
      toType: DataType,
      timeZoneId: Option[String],
      evalMode: CometEvalMode.Value): SupportLevel = {

    if (fromType == toType) {
      return Compatible()
    }

    (fromType, toType) match {
      case (dt: DataType, _) if dt.typeName == "timestamp_ntz" =>
        // https://github.com/apache/datafusion-comet/issues/378
        toType match {
          case DataTypes.TimestampType | DataTypes.DateType | DataTypes.StringType =>
            Incompatible()
          case _ =>
            Unsupported
        }
      case (from: DecimalType, to: DecimalType) =>
        if (to.precision < from.precision) {
          // https://github.com/apache/datafusion/issues/13492
          Incompatible(Some("Casting to smaller precision is not supported"))
        } else {
          Compatible()
        }
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
      case _ => Unsupported
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
      case DataTypes.TimestampType if evalMode == "ANSI" =>
        Incompatible(Some("ANSI mode not supported"))
      case DataTypes.TimestampType =>
        // https://github.com/apache/datafusion-comet/issues/328
        Incompatible(Some("Not all valid formats are supported"))
      case _ =>
        Unsupported
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
        // https://github.com/apache/datafusion-comet/issues/377
        Incompatible(Some("Only works for binary data representing valid UTF-8 strings"))
      case StructType(fields) =>
        for (field <- fields) {
          isSupported(field.dataType, DataTypes.StringType, timeZoneId, evalMode) match {
            case s: Incompatible =>
              return s
            case Unsupported =>
              return Unsupported
            case _ =>
          }
        }
        Compatible()
      case _ => Unsupported
    }
  }

  private def canCastFromTimestamp(toType: DataType): SupportLevel = {
    toType match {
      case DataTypes.BooleanType | DataTypes.ByteType | DataTypes.ShortType |
          DataTypes.IntegerType =>
        // https://github.com/apache/datafusion-comet/issues/352
        // this seems like an edge case that isn't important for us to support
        Unsupported
      case DataTypes.LongType =>
        // https://github.com/apache/datafusion-comet/issues/352
        Compatible()
      case DataTypes.StringType => Compatible()
      case DataTypes.DateType => Compatible()
      case _ => Unsupported
    }
  }

  private def canCastFromBoolean(toType: DataType): SupportLevel = toType match {
    case DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType | DataTypes.LongType |
        DataTypes.FloatType | DataTypes.DoubleType =>
      Compatible()
    case _ => Unsupported
  }

  private def canCastFromByte(toType: DataType): SupportLevel = toType match {
    case DataTypes.BooleanType =>
      Compatible()
    case DataTypes.ShortType | DataTypes.IntegerType | DataTypes.LongType =>
      Compatible()
    case DataTypes.FloatType | DataTypes.DoubleType | _: DecimalType =>
      Compatible()
    case _ =>
      Unsupported
  }

  private def canCastFromShort(toType: DataType): SupportLevel = toType match {
    case DataTypes.BooleanType =>
      Compatible()
    case DataTypes.ByteType | DataTypes.IntegerType | DataTypes.LongType =>
      Compatible()
    case DataTypes.FloatType | DataTypes.DoubleType | _: DecimalType =>
      Compatible()
    case _ =>
      Unsupported
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
      Unsupported
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
      Unsupported
  }

  private def canCastFromFloat(toType: DataType): SupportLevel = toType match {
    case DataTypes.BooleanType | DataTypes.DoubleType | DataTypes.ByteType | DataTypes.ShortType |
        DataTypes.IntegerType | DataTypes.LongType =>
      Compatible()
    case _: DecimalType => Compatible()
    case _ => Unsupported
  }

  private def canCastFromDouble(toType: DataType): SupportLevel = toType match {
    case DataTypes.BooleanType | DataTypes.FloatType | DataTypes.ByteType | DataTypes.ShortType |
        DataTypes.IntegerType | DataTypes.LongType =>
      Compatible()
    case _: DecimalType => Compatible()
    case _ => Unsupported
  }

  private def canCastFromDecimal(toType: DataType): SupportLevel = toType match {
    case DataTypes.FloatType | DataTypes.DoubleType | DataTypes.ByteType | DataTypes.ShortType |
        DataTypes.IntegerType | DataTypes.LongType =>
      Compatible()
    case _ => Unsupported
  }

}
