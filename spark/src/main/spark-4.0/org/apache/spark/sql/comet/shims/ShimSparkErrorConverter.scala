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

import org.apache.spark.QueryContext
import org.apache.spark.SparkException
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Spark 4.0-specific implementation for converting error types to proper Spark exceptions.
 */
trait ShimSparkErrorConverter {

  /**
   * Convert error type string and parameters to appropriate Spark exception. Version-specific
   * implementations call the correct QueryExecutionErrors.* methods.
   *
   * @param errorType
   *   The error type from JSON (e.g., "DivideByZero")
   * @param errorClass
   *   The Spark error class (e.g., "DIVIDE_BY_ZERO")
   * @param params
   *   Error parameters from JSON
   * @param context
   *   QueryContext array with SQL text and position information
   * @param summary
   *   Formatted summary string showing error location
   * @return
   *   Throwable (specific exception type from QueryExecutionErrors), or None if unknown
   */
  def convertErrorType(
      errorType: String,
      _errorClass: String,
      params: Map[String, Any],
      context: Array[QueryContext],
      _summary: String): Option[Throwable] = {

    errorType match {

      case "DivideByZero" =>
        Some(QueryExecutionErrors.divideByZeroError(context.headOption.orNull))

      case "RemainderByZero" =>
        // SPARK 4.0 REMOVED remainderByZeroError  so we use generic arithmetic exception
        Some(
          new SparkException(
            errorClass = "REMAINDER_BY_ZERO",
            messageParameters = params.map { case (k, v) => (k, v.toString) },
            cause = null))

      case "IntervalDividedByZero" =>
        Some(QueryExecutionErrors.intervalDividedByZeroError(context.headOption.orNull))

      case "BinaryArithmeticOverflow" =>
        Some(
          QueryExecutionErrors.binaryArithmeticCauseOverflowError(
            params("value1").toString.toShort,
            params("symbol").toString,
            params("value2").toString.toShort,
            params("functionName").toString))

      case "ArithmeticOverflow" =>
        val fromType = params("fromType").toString
        Some(QueryExecutionErrors.arithmeticOverflowError(fromType + " overflow", ""))

      case "IntegralDivideOverflow" =>
        Some(QueryExecutionErrors.overflowInIntegralDivideError(context.headOption.orNull))

      case "DecimalSumOverflow" =>
        Some(QueryExecutionErrors.overflowInSumOfDecimalError(context.headOption.orNull, ""))

      case "NumericValueOutOfRange" =>
        val decimal = Decimal(params("value").toString)
        Some(
          QueryExecutionErrors.cannotChangeDecimalPrecisionError(
            decimal,
            params("precision").toString.toInt,
            params("scale").toString.toInt,
            context.headOption.orNull))

      case "DatetimeOverflow" =>
        // Spark 4.0 doesn't have datetimeOverflowError
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
            context.headOption.orNull))

      case "InvalidElementAtIndex" =>
        Some(
          QueryExecutionErrors.invalidElementAtIndexError(
            params("indexValue").toString.toInt,
            params("arraySize").toString.toInt,
            context.headOption.orNull))

      case "InvalidIndexOfZero" =>
        Some(QueryExecutionErrors.invalidIndexOfZeroError(context.headOption.orNull))

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
          QueryExecutionErrors.ansiDateTimeParseError(
            new Exception(params("message").toString),
            params("suggestedFunc").toString))

      case "InvalidFractionOfSecond" =>
        Some(QueryExecutionErrors.invalidFractionOfSecondError(params("value").toString.toDouble))

      case "CastInvalidValue" =>
        val str = UTF8String.fromString(params("value").toString)
        val targetType = getDataType(params("toType").toString)
        Some(
          QueryExecutionErrors
            .invalidInputInCastToNumberError(targetType, str, context.headOption.orNull))

      case "CastOverFlow" =>
        val fromType = getDataType(params("fromType").toString)
        val toType = getDataType(params("toType").toString)
        val valueStr = params("value").toString

        // Convert string value to appropriate type for toSQLValue
        val typedValue: Any = fromType match {
          case _: DecimalType =>
            // Parse decimal string (may have "BD" suffix from BigDecimal.toString)
            val cleanStr = if (valueStr.endsWith("BD")) valueStr.dropRight(2) else valueStr
            Decimal(cleanStr)
          case ByteType =>
            // Strip "T" suffix for TINYINT literals
            val cleanStr = if (valueStr.endsWith("T")) valueStr.dropRight(1) else valueStr
            cleanStr.toByte
          case ShortType =>
            // Strip "S" suffix for SMALLINT literals
            val cleanStr = if (valueStr.endsWith("S")) valueStr.dropRight(1) else valueStr
            cleanStr.toShort
          case IntegerType => valueStr.toInt
          case LongType =>
            // Strip "L" suffix for BIGINT literals
            val cleanStr = if (valueStr.endsWith("L")) valueStr.dropRight(1) else valueStr
            cleanStr.toLong
          case FloatType => valueStr.toFloat
          case DoubleType => valueStr.toDouble
          case StringType => UTF8String.fromString(valueStr)
          case _ => valueStr // Fallback to string
        }

        Some(QueryExecutionErrors.castingCauseOverflowError(typedValue, fromType, toType))

      case "CannotParseDecimal" =>
        Some(QueryExecutionErrors.cannotParseDecimalError())

      case "InvalidUtf8String" =>
        val hexStr = UTF8String.fromString(params("hexString").toString)
        Some(QueryExecutionErrors.invalidUTF8StringError(hexStr))

      case "UnexpectedPositiveValue" =>
        Some(
          QueryExecutionErrors.unexpectedValueForStartInFunctionError(
            params("parameterName").toString))

      case "UnexpectedNegativeValue" =>
        Some(
          QueryExecutionErrors.unexpectedValueForLengthInFunctionError(
            params("parameterName").toString,
            params("actualValue").toString.toInt))

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
        Some(QueryExecutionErrors.multipleRowScalarSubqueryError(context.headOption.orNull))

      case "IntervalArithmeticOverflowWithSuggestion" =>
        Some(
          QueryExecutionErrors.withSuggestionIntervalArithmeticOverflowError(
            params.get("functionName").map(_.toString).getOrElse(""),
            context.headOption.orNull))

      case "IntervalArithmeticOverflowWithoutSuggestion" =>
        Some(
          QueryExecutionErrors.withoutSuggestionIntervalArithmeticOverflowError(
            context.headOption.orNull))

      case _ =>
        // Unknown error type - return None to trigger fallback
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
