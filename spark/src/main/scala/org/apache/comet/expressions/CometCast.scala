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
    (fromType, toType) match {
      case (DataTypes.StringType, _) =>
        castFromStringSupported(cast, toType, timeZoneId, evalMode)
      case (_, DataTypes.StringType) =>
        castToStringSupported(cast, fromType, timeZoneId, evalMode)
      case (DataTypes.TimestampType, _) =>
        castFromTimestampSupported(cast, toType, timeZoneId, evalMode)
      case (_: DecimalType, DataTypes.FloatType | DataTypes.DoubleType) => true
      case (
            DataTypes.BooleanType,
            DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType |
            DataTypes.LongType | DataTypes.FloatType | DataTypes.DoubleType) =>
        true
      case (
            DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType | DataTypes.LongType,
            DataTypes.BooleanType | DataTypes.ByteType | DataTypes.ShortType |
            DataTypes.IntegerType | DataTypes.LongType | DataTypes.FloatType |
            DataTypes.DoubleType) =>
        true
      case (
            DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType | DataTypes.LongType,
            _: DecimalType) =>
        true
      case (DataTypes.FloatType, DataTypes.BooleanType | DataTypes.DoubleType) => true
      case (DataTypes.DoubleType, DataTypes.BooleanType | DataTypes.FloatType) => true
      case _ => false
    }
  }

  private def castFromStringSupported(
      cast: Cast,
      toType: DataType,
      timeZoneId: Option[String],
      evalMode: String): Boolean = {
    toType match {
      case DataTypes.BooleanType =>
        true
      case DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType |
          DataTypes.LongType =>
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

  private def castToStringSupported(
      cast: Cast,
      fromType: DataType,
      timeZoneId: Option[String],
      evalMode: String): Boolean = {
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

  private def castFromTimestampSupported(
      cast: Cast,
      toType: DataType,
      timeZoneId: Option[String],
      evalMode: String): Boolean = {
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

}
