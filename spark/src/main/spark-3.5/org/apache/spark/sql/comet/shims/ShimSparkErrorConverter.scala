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

package org.apache.spark.sql.comet.shims

import org.apache.spark.{QueryContext, SparkException}
import org.apache.spark.sql.catalyst.trees.SQLQueryContext
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Spark 3.5 implementation for converting error types to proper Spark exceptions.
 *
 * Handles all error cases using the Spark 3.5 QueryExecutionErrors API. The 4 cases with API
 * differences from Spark 4.0 are handled with Spark 3.x-specific calls.
 */
trait ShimSparkErrorConverter {

  private def sqlCtx(context: Array[QueryContext]): SQLQueryContext =
    context.headOption.map(_.asInstanceOf[SQLQueryContext]).getOrElse(null)

  def convertErrorType(
      errorType: String,
      errorClass: String,
      params: Map[String, Any],
      context: Array[QueryContext],
      summary: String): Option[Throwable] = {
    val _ = (errorClass, summary)

    errorType match {

      case "DivideByZero" =>
        Some(QueryExecutionErrors.divideByZeroError(sqlCtx(context)))

      case "RemainderByZero" =>
        Some(
          new SparkException(
            errorClass = "REMAINDER_BY_ZERO",
            messageParameters = params.map { case (k, v) => (k, v.toString) },
            cause = null))

      case "IntervalDividedByZero" =>
        Some(QueryExecutionErrors.intervalDividedByZeroError(sqlCtx(context)))

      case "BinaryArithmeticOverflow" =>
        // Spark 3.x does not take functionName parameter
        Some(
          QueryExecutionErrors.binaryArithmeticCauseOverflowError(
            params("value1").toString.toShort,
            params("symbol").toString,
            params("value2").toString.toShort))

      case "ArithmeticOverflow" =>
        val fromType = params("fromType").toString
        Some(QueryExecutionErrors.arithmeticOverflowError(fromType + " overflow", ""))

      case "IntegralDivideOverflow" =>
        Some(QueryExecutionErrors.overflowInIntegralDivideError(sqlCtx(context)))

      case "DecimalSumOverflow" =>
        // Spark 3.x takes SQLQueryContext, not QueryContext
        Some(QueryExecutionErrors.overflowInSumOfDecimalError(sqlCtx(context)))

      case "NumericValueOutOfRange" =>
        val decimal = Decimal(params("value").toString)
        Some(
          QueryExecutionErrors.cannotChangeDecimalPrecisionError(
            decimal,
            params("precision").toString.toInt,
            params("scale").toString.toInt,
            sqlCtx(context)))

      case "DatetimeOverflow" =>
        Some(
          new SparkException(
            errorClass = "DATETIME_OVERFLOW",
            messageParameters = params.map { case (k, v) => (k, v.toString) },
            cause = null))

      case "InvalidArrayIndex" =>
        Some(
          QueryExecutionErrors.invalidArrayIndexError(
            params("indexValue").toString.toInt,
            params("arraySize").toString.toInt,
            sqlCtx(context)))

      case "InvalidElementAtIndex" =>
        Some(
          QueryExecutionErrors.invalidElementAtIndexError(
            params("indexValue").toString.toInt,
            params("arraySize").toString.toInt,
            sqlCtx(context)))

      case "InvalidIndexOfZero" =>
        Some(QueryExecutionErrors.invalidIndexOfZeroError(sqlCtx(context)))

      case "InvalidBitmapPosition" =>
        Some(
          QueryExecutionErrors.invalidBitmapPositionError(
            params("bitPosition").toString.toLong,
            params("bitmapNumBytes").toString.toLong))

      case "DuplicatedMapKey" =>
        Some(QueryExecutionErrors.duplicateMapKeyFoundError(params("key")))

      case "NullMapKey" =>
        Some(QueryExecutionErrors.nullAsMapKeyNotAllowedError())

      case "MapKeyValueDiffSizes" =>
        Some(QueryExecutionErrors.mapDataKeyArrayLengthDiffersFromValueArrayLengthError())

      case "ExceedMapSizeLimit" =>
        Some(QueryExecutionErrors.exceedMapSizeLimitError(params("size").toString.toInt))

      case "CollectionSizeLimitExceeded" =>
        Some(
          QueryExecutionErrors.createArrayWithElementsExceedLimitError(
            "array",
            params("numElements").toString.toLong))

      case "NotNullAssertViolation" =>
        Some(
          QueryExecutionErrors.foundNullValueForNotNullableFieldError(
            params("fieldName").toString))

      case "ValueIsNull" =>
        Some(
          QueryExecutionErrors.fieldCannotBeNullError(
            params.getOrElse("rowIndex", 0).toString.toInt,
            params("fieldName").toString))

      case "CannotParseTimestamp" =>
        Some(
          QueryExecutionErrors.ansiDateTimeParseError(new Exception(params("message").toString)))

      case "InvalidFractionOfSecond" =>
        Some(QueryExecutionErrors.invalidFractionOfSecondError())

      case "CastInvalidValue" =>
        val str = UTF8String.fromString(params("value").toString)
        val targetType = getDataType(params("toType").toString)
        Some(
          QueryExecutionErrors
            .invalidInputInCastToNumberError(targetType, str, sqlCtx(context)))

      case "CastOverFlow" =>
        val fromType = getDataType(params("fromType").toString)
        val toType = getDataType(params("toType").toString)
        val valueStr = params("value").toString

        val typedValue: Any = fromType match {
          case _: DecimalType =>
            val cleanStr = if (valueStr.endsWith("BD")) valueStr.dropRight(2) else valueStr
            Decimal(cleanStr)
          case ByteType =>
            val cleanStr = if (valueStr.endsWith("T")) valueStr.dropRight(1) else valueStr
            cleanStr.toByte
          case ShortType =>
            val cleanStr = if (valueStr.endsWith("S")) valueStr.dropRight(1) else valueStr
            cleanStr.toShort
          case IntegerType => valueStr.toInt
          case LongType =>
            val cleanStr = if (valueStr.endsWith("L")) valueStr.dropRight(1) else valueStr
            cleanStr.toLong
          case FloatType => valueStr.toFloat
          case DoubleType => valueStr.toDouble
          case StringType => UTF8String.fromString(valueStr)
          case _ => valueStr
        }

        Some(QueryExecutionErrors.castingCauseOverflowError(typedValue, fromType, toType))

      case "CannotParseDecimal" =>
        Some(QueryExecutionErrors.cannotParseDecimalError())

      case "InvalidUtf8String" =>
        // invalidUTF8StringError does not exist in Spark 3.x; use generic fallback
        Some(
          new SparkException(
            errorClass = "INVALID_UTF8_STRING",
            messageParameters = params.map { case (k, v) => (k, v.toString) },
            cause = null))

      case "UnexpectedPositiveValue" =>
        Some(
          QueryExecutionErrors.unexpectedValueForStartInFunctionError(
            params("parameterName").toString))

      case "UnexpectedNegativeValue" =>
        Some(
          QueryExecutionErrors.unexpectedValueForLengthInFunctionError(
            params("parameterName").toString))

      case "InvalidRegexGroupIndex" =>
        Some(
          QueryExecutionErrors.invalidRegexGroupIndexError(
            params("functionName").toString,
            params("groupCount").toString.toInt,
            params("groupIndex").toString.toInt))

      case "DatatypeCannotOrder" =>
        Some(
          QueryExecutionErrors.orderedOperationUnsupportedByDataTypeError(
            params("dataType").toString))

      case "ScalarSubqueryTooManyRows" =>
        // multipleRowScalarSubqueryError was renamed to multipleRowSubqueryError in Spark 3.x
        Some(QueryExecutionErrors.multipleRowSubqueryError(sqlCtx(context)))

      case "IntervalArithmeticOverflowWithSuggestion" =>
        // Spark 3.x uses a single intervalArithmeticOverflowError method
        Some(
          QueryExecutionErrors.intervalArithmeticOverflowError(
            "Interval arithmetic overflow",
            params.get("functionName").map(_.toString).getOrElse(""),
            sqlCtx(context)))

      case "IntervalArithmeticOverflowWithoutSuggestion" =>
        Some(
          QueryExecutionErrors
            .intervalArithmeticOverflowError("Interval arithmetic overflow", "", sqlCtx(context)))

      case _ =>
        None
    }
  }

  private def getDataType(typeName: String): DataType = {
    typeName.toUpperCase match {
      case "BYTE" | "TINYINT" => ByteType
      case "SHORT" | "SMALLINT" => ShortType
      case "INT" | "INTEGER" => IntegerType
      case "LONG" | "BIGINT" => LongType
      case "FLOAT" | "REAL" => FloatType
      case "DOUBLE" => DoubleType
      case "DECIMAL" => DecimalType.SYSTEM_DEFAULT
      case "STRING" | "VARCHAR" => StringType
      case "BINARY" => BinaryType
      case "BOOLEAN" => BooleanType
      case "DATE" => DateType
      case "TIMESTAMP" => TimestampType
      case _ =>
        try {
          DataType.fromDDL(typeName)
        } catch {
          case _: Exception => StringType
        }
    }
  }
}
