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

import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types.{DataType, DataTypes, DecimalType}

sealed trait SupportLevel

/** We support this feature with full compatibility with Spark */
object Compatible extends SupportLevel

/** We support this feature but results can be different from Spark */
object Incompatible extends SupportLevel

/** We do not support this feature */
object Unsupported extends SupportLevel

object CometCast {

  def isSupported(
      cast: Cast,
      fromType: DataType,
      toType: DataType,
      timeZoneId: Option[String],
      evalMode: String): SupportLevel = {

    if (fromType == toType) {
      return Compatible
    }

    (fromType, toType) match {
      case (dt: DataType, _) if dt.typeName == "timestamp_ntz" =>
        toType match {
          case DataTypes.TimestampType | DataTypes.DateType | DataTypes.StringType =>
            Incompatible
          case _ =>
            Unsupported
        }
      case (DataTypes.DoubleType, _: DecimalType) =>
        Incompatible
      case (DataTypes.TimestampType, DataTypes.LongType) =>
        Incompatible
      case (DataTypes.BinaryType | DataTypes.FloatType, DataTypes.StringType) =>
        Incompatible
      case (DataTypes.StringType, DataTypes.BinaryType) =>
        Incompatible
      case (_: DecimalType, _: DecimalType) =>
        // TODO we need to file an issue for adding specific tests for casting
        // between decimal types with different precision and scale
        Compatible
      case (DataTypes.StringType, _) =>
        canCastFromString(toType)
      case (_, DataTypes.StringType) =>
        canCastToString(fromType)
      case (DataTypes.TimestampType, _) =>
        canCastFromTimestamp(toType)
      case (_: DecimalType, _) =>
        canCastFromDecimal(toType)
      case (DataTypes.BooleanType, _) =>
        canCastFromBoolean(toType)
      case (
            DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType | DataTypes.LongType,
            _) =>
        canCastFromInt(toType)
      case (DataTypes.FloatType, _) =>
        canCastFromFloat(toType)
      case (DataTypes.DoubleType, _) =>
        canCastFromDouble(toType)
      case _ => Unsupported
    }
  }

  private def canCastFromString(toType: DataType): SupportLevel = {
    toType match {
      case DataTypes.BooleanType =>
        Compatible
      case DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType |
          DataTypes.LongType =>
        Compatible
      case DataTypes.BinaryType =>
        Compatible
      case DataTypes.FloatType | DataTypes.DoubleType =>
        // https://github.com/apache/datafusion-comet/issues/326
        Unsupported
      case _: DecimalType =>
        // https://github.com/apache/datafusion-comet/issues/325
        Unsupported
      case DataTypes.DateType =>
        // https://github.com/apache/datafusion-comet/issues/327
        Unsupported
      case DataTypes.TimestampType =>
        // https://github.com/apache/datafusion-comet/issues/328
        Incompatible
      case _ =>
        Unsupported
    }
  }

  private def canCastToString(fromType: DataType): SupportLevel = {
    fromType match {
      case DataTypes.BooleanType => Compatible
      case DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType |
          DataTypes.LongType =>
        Compatible
      case DataTypes.DateType => Compatible
      case DataTypes.TimestampType => Compatible
      case DataTypes.FloatType | DataTypes.DoubleType =>
        // https://github.com/apache/datafusion-comet/issues/326
        Unsupported
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
        Unsupported
      case DataTypes.StringType => Compatible
      case DataTypes.DateType => Compatible
      case _ => Unsupported
    }
  }

  private def canCastFromBoolean(toType: DataType): SupportLevel = toType match {
    case DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType | DataTypes.LongType |
        DataTypes.FloatType | DataTypes.DoubleType =>
      Compatible
    case _ => Unsupported
  }

  private def canCastFromInt(toType: DataType): SupportLevel = toType match {
    case DataTypes.BooleanType | DataTypes.ByteType | DataTypes.ShortType |
        DataTypes.IntegerType | DataTypes.LongType | DataTypes.FloatType | DataTypes.DoubleType |
        _: DecimalType =>
      Compatible
    case _ => Unsupported
  }

  private def canCastFromFloat(toType: DataType): SupportLevel = toType match {
    case DataTypes.BooleanType | DataTypes.DoubleType => Compatible
    case _ => Unsupported
  }

  private def canCastFromDouble(toType: DataType): SupportLevel = toType match {
    case DataTypes.BooleanType | DataTypes.FloatType => Compatible
    case _ => Unsupported
  }

  private def canCastFromDecimal(toType: DataType): SupportLevel = toType match {
    case DataTypes.FloatType | DataTypes.DoubleType => Compatible
    case _ => Unsupported
  }

}
