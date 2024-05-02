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

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo

object CometCast {

  def isSupported(
      cast: Cast,
      fromType: DataType,
      toType: DataType,
      timeZoneId: Option[String],
      evalMode: String): Boolean = {

    if (fromType == toType) {
      return true
    }

    (fromType, toType) match {

      // TODO this is a temporary hack to allow casts that we either know are
      // incompatible with Spark, or are just not well tested yet, just to avoid
      // regressions in existing tests with this PR

      // BEGIN HACK
      case (dt: DataType, _) if dt.typeName == "timestamp_ntz" =>
        toType match {
          case DataTypes.TimestampType | DataTypes.DateType | DataTypes.StringType =>
            true
          case _ => false
        }
      case (DataTypes.DoubleType, _: DecimalType) =>
        true
      case (DataTypes.TimestampType, DataTypes.LongType) =>
        true
      case (DataTypes.BinaryType | DataTypes.FloatType, DataTypes.StringType) =>
        true
      case (_, DataTypes.BinaryType) =>
        true
      // END HACK

      case (_: DecimalType, _: DecimalType) =>
        // TODO we need to file an issue for adding specific tests for casting
        // between decimal types with different precision and scale
        true
      case (DataTypes.StringType, _) =>
        canCastFromString(cast, toType)
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
      case _ => false
    }
  }

  private def canCastFromString(cast: Cast, toType: DataType): Boolean = {
    toType match {
      case DataTypes.BooleanType =>
        true
      case DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType |
          DataTypes.LongType =>
        true
      case DataTypes.BinaryType =>
        true
      case DataTypes.FloatType | DataTypes.DoubleType =>
        // https://github.com/apache/datafusion-comet/issues/326
        false
      case _: DecimalType =>
        // https://github.com/apache/datafusion-comet/issues/325
        false
      case DataTypes.DateType =>
        // https://github.com/apache/datafusion-comet/issues/327
        false
      case DataTypes.TimestampType =>
        val enabled = CometConf.COMET_CAST_STRING_TO_TIMESTAMP.get()
        if (!enabled) {
          // https://github.com/apache/datafusion-comet/issues/328
          withInfo(cast, s"${CometConf.COMET_CAST_STRING_TO_TIMESTAMP.key} is disabled")
        }
        enabled
      case _ =>
        false
    }
  }

  private def canCastToString(fromType: DataType): Boolean = {
    fromType match {
      case DataTypes.BooleanType => true
      case DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType |
          DataTypes.LongType =>
        true
      case DataTypes.DateType => true
      case DataTypes.TimestampType => true
      case DataTypes.FloatType | DataTypes.DoubleType =>
        // https://github.com/apache/datafusion-comet/issues/326
        false
      case _ => false
    }
  }

  private def canCastFromTimestamp(toType: DataType): Boolean = {
    toType match {
      case DataTypes.BooleanType | DataTypes.ByteType | DataTypes.ShortType |
          DataTypes.IntegerType =>
        // https://github.com/apache/datafusion-comet/issues/352
        // this seems like an edge case that isn't important for us to support
        false
      case DataTypes.LongType =>
        // https://github.com/apache/datafusion-comet/issues/352
        false
      case DataTypes.StringType => true
      case DataTypes.DateType => true
      case _ => false
    }
  }

  private def canCastFromBoolean(toType: DataType) = toType match {
    case DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType | DataTypes.LongType |
        DataTypes.FloatType | DataTypes.DoubleType =>
      true
    case _ => false
  }

  private def canCastFromInt(toType: DataType) = toType match {
    case DataTypes.BooleanType | DataTypes.ByteType | DataTypes.ShortType |
        DataTypes.IntegerType | DataTypes.LongType | DataTypes.FloatType | DataTypes.DoubleType |
        _: DecimalType =>
      true
    case _ => false
  }

  private def canCastFromFloat(toType: DataType) = toType match {
    case DataTypes.BooleanType | DataTypes.DoubleType => true
    case _ => false
  }

  private def canCastFromDouble(toType: DataType) = toType match {
    case DataTypes.BooleanType | DataTypes.FloatType => true
    case _ => false
  }

  private def canCastFromDecimal(toType: DataType) = toType match {
    case DataTypes.FloatType | DataTypes.DoubleType => true
    case _ => false
  }

}
