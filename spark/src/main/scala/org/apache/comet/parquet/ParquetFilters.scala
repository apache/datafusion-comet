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

import java.io.ByteArrayOutputStream
import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Timestamp}
import java.time.{Duration, Instant, LocalDate, Period}
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.parquet.column.statistics.{Statistics => ParquetStatistics}
import org.apache.parquet.filter2.predicate._
import org.apache.parquet.filter2.predicate.SparkFilterApi._
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation, MessageType, PrimitiveComparator, PrimitiveType, Type}
import org.apache.parquet.schema.LogicalTypeAnnotation.{DecimalLogicalTypeAnnotation, TimeUnit}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, CaseInsensitiveMap, DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.{rebaseGregorianToJulianDays, rebaseGregorianToJulianMicros, RebaseSpec}
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.parquet.SourceFilterSerde.{createBinaryExpr, createNameExpr, createUnaryExpr, createValueExpr}
import org.apache.comet.serde.ExprOuterClass
import org.apache.comet.serde.QueryPlanSerde.scalarFunctionExprToProto
import org.apache.comet.shims.ShimSQLConf

/**
 * Copied from Spark 3.4, in order to fix Parquet shading issue. TODO: find a way to remove this
 * duplication
 *
 * Some utility function to convert Spark data source filters to Parquet filters.
 */
class ParquetFilters(
    schema: MessageType,
    dataSchema: StructType,
    pushDownDate: Boolean,
    pushDownTimestamp: Boolean,
    pushDownDecimal: Boolean,
    pushDownStringPredicate: Boolean,
    pushDownInFilterThreshold: Int,
    caseSensitive: Boolean,
    datetimeRebaseSpec: RebaseSpec)
    extends ShimSQLConf {
  // A map which contains parquet field name and data type, if predicate push down applies.
  //
  // Each key in `nameToParquetField` represents a column; `dots` are used as separators for
  // nested columns. If any part of the names contains `dots`, it is quoted to avoid confusion.
  // See `org.apache.spark.sql.connector.catalog.quote` for implementation details.
  private val nameToParquetField: Map[String, ParquetPrimitiveField] = {
    // Recursively traverse the parquet schema to get primitive fields that can be pushed-down.
    // `parentFieldNames` is used to keep track of the current nested level when traversing.
    def getPrimitiveFields(
        fields: Seq[Type],
        parentFieldNames: Array[String] = Array.empty): Seq[ParquetPrimitiveField] = {
      fields.flatMap {
        // Parquet only supports predicate push-down for non-repeated primitive types.
        // TODO(SPARK-39393): Remove extra condition when parquet added filter predicate support for
        //                    repeated columns (https://issues.apache.org/jira/browse/PARQUET-34)
        case p: PrimitiveType if p.getRepetition != Repetition.REPEATED =>
          Some(
            ParquetPrimitiveField(
              fieldNames = parentFieldNames :+ p.getName,
              fieldType = ParquetSchemaType(
                p.getLogicalTypeAnnotation,
                p.getPrimitiveTypeName,
                p.getTypeLength)))
        // Note that when g is a `Struct`, `g.getOriginalType` is `null`.
        // When g is a `Map`, `g.getOriginalType` is `MAP`.
        // When g is a `List`, `g.getOriginalType` is `LIST`.
        case g: GroupType if g.getOriginalType == null =>
          getPrimitiveFields(g.getFields.asScala.toSeq, parentFieldNames :+ g.getName)
        // Parquet only supports push-down for primitive types; as a result, Map and List types
        // are removed.
        case _ => None
      }
    }

    val primitiveFields = getPrimitiveFields(schema.getFields.asScala.toSeq).map { field =>
      (field.fieldNames.toSeq.map(quoteIfNeeded).mkString("."), field)
    }
    if (caseSensitive) {
      primitiveFields.toMap
    } else {
      // Don't consider ambiguity here, i.e. more than one field is matched in case insensitive
      // mode, just skip pushdown for these fields, they will trigger Exception when reading,
      // See: SPARK-25132.
      val dedupPrimitiveFields =
        primitiveFields
          .groupBy(_._1.toLowerCase(Locale.ROOT))
          .filter(_._2.size == 1)
          .mapValues(_.head._2)
      CaseInsensitiveMap(dedupPrimitiveFields.toMap)
    }
  }

  /**
   * Holds a single primitive field information stored in the underlying parquet file.
   *
   * @param fieldNames
   *   a field name as an array of string multi-identifier in parquet file
   * @param fieldType
   *   field type related info in parquet file
   */
  private case class ParquetPrimitiveField(
      fieldNames: Array[String],
      fieldType: ParquetSchemaType)

  private case class ParquetSchemaType(
      logicalTypeAnnotation: LogicalTypeAnnotation,
      primitiveTypeName: PrimitiveTypeName,
      length: Int)

  private val ParquetBooleanType = ParquetSchemaType(null, BOOLEAN, 0)
  private val ParquetByteType =
    ParquetSchemaType(LogicalTypeAnnotation.intType(8, true), INT32, 0)
  private val ParquetShortType =
    ParquetSchemaType(LogicalTypeAnnotation.intType(16, true), INT32, 0)
  private val ParquetIntegerType = ParquetSchemaType(null, INT32, 0)
  private val ParquetLongType = ParquetSchemaType(null, INT64, 0)
  private val ParquetFloatType = ParquetSchemaType(null, FLOAT, 0)
  private val ParquetDoubleType = ParquetSchemaType(null, DOUBLE, 0)
  private val ParquetStringType =
    ParquetSchemaType(LogicalTypeAnnotation.stringType(), BINARY, 0)
  private val ParquetBinaryType = ParquetSchemaType(null, BINARY, 0)
  private val ParquetDateType =
    ParquetSchemaType(LogicalTypeAnnotation.dateType(), INT32, 0)
  private val ParquetTimestampMicrosType =
    ParquetSchemaType(LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS), INT64, 0)
  private val ParquetTimestampMillisType =
    ParquetSchemaType(LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS), INT64, 0)

  private def dateToDays(date: Any): Int = {
    val gregorianDays = date match {
      case d: Date => DateTimeUtils.fromJavaDate(d)
      case ld: LocalDate => DateTimeUtils.localDateToDays(ld)
    }
    datetimeRebaseSpec.mode match {
      case LEGACY => rebaseGregorianToJulianDays(gregorianDays)
      case _ => gregorianDays
    }
  }

  private def timestampToMicros(v: Any): JLong = {
    val gregorianMicros = v match {
      case i: Instant => DateTimeUtils.instantToMicros(i)
      case t: Timestamp => DateTimeUtils.fromJavaTimestamp(t)
    }
    datetimeRebaseSpec.mode match {
      case LEGACY =>
        rebaseGregorianToJulianMicros(datetimeRebaseSpec.timeZone, gregorianMicros)
      case _ => gregorianMicros
    }
  }

  private def decimalToInt32(decimal: JBigDecimal): Integer = decimal.unscaledValue().intValue()

  private def decimalToInt64(decimal: JBigDecimal): JLong = decimal.unscaledValue().longValue()

  private def decimalToByteArray(decimal: JBigDecimal, numBytes: Int): Binary = {
    val decimalBuffer = new Array[Byte](numBytes)
    val bytes = decimal.unscaledValue().toByteArray

    val fixedLengthBytes = if (bytes.length == numBytes) {
      bytes
    } else {
      val signByte = if (bytes.head < 0) -1: Byte else 0: Byte
      java.util.Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte)
      System.arraycopy(bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length)
      decimalBuffer
    }
    Binary.fromConstantByteArray(fixedLengthBytes, 0, numBytes)
  }

  private def timestampToMillis(v: Any): JLong = {
    val micros = timestampToMicros(v)
    val millis = DateTimeUtils.microsToMillis(micros)
    millis.asInstanceOf[JLong]
  }

  private def toIntValue(v: Any): Integer = {
    Option(v)
      .map {
        case p: Period => IntervalUtils.periodToMonths(p)
        case n => n.asInstanceOf[Number].intValue
      }
      .map(_.asInstanceOf[Integer])
      .orNull
  }

  private def toLongValue(v: Any): JLong = v match {
    case d: Duration => IntervalUtils.durationToMicros(d)
    case l => l.asInstanceOf[JLong]
  }

  private val makeEq
      : PartialFunction[ParquetSchemaType, (Array[String], Any) => FilterPredicate] = {
    case ParquetBooleanType =>
      (n: Array[String], v: Any) => FilterApi.eq(booleanColumn(n), v.asInstanceOf[JBoolean])
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: Array[String], v: Any) =>
        FilterApi.eq(
          intColumn(n),
          Option(v).map(_.asInstanceOf[Number].intValue.asInstanceOf[Integer]).orNull)
    case ParquetLongType =>
      (n: Array[String], v: Any) => FilterApi.eq(longColumn(n), v.asInstanceOf[JLong])
    case ParquetFloatType =>
      (n: Array[String], v: Any) => FilterApi.eq(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: Array[String], v: Any) => FilterApi.eq(doubleColumn(n), v.asInstanceOf[JDouble])

    // Binary.fromString and Binary.fromByteArray don't accept null values
    case ParquetStringType =>
      (n: Array[String], v: Any) =>
        FilterApi.eq(
          binaryColumn(n),
          Option(v).map(s => Binary.fromString(s.asInstanceOf[String])).orNull)
    case ParquetBinaryType =>
      (n: Array[String], v: Any) =>
        FilterApi.eq(
          binaryColumn(n),
          Option(v).map(_ => Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]])).orNull)
    case ParquetDateType if pushDownDate =>
      (n: Array[String], v: Any) =>
        FilterApi.eq(
          intColumn(n),
          Option(v).map(date => dateToDays(date).asInstanceOf[Integer]).orNull)
    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: Array[String], v: Any) =>
        FilterApi.eq(longColumn(n), Option(v).map(timestampToMicros).orNull)
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: Array[String], v: Any) =>
        FilterApi.eq(longColumn(n), Option(v).map(timestampToMillis).orNull)

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT32, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.eq(
          intColumn(n),
          Option(v).map(d => decimalToInt32(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT64, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.eq(
          longColumn(n),
          Option(v).map(d => decimalToInt64(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, FIXED_LEN_BYTE_ARRAY, length)
        if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.eq(
          binaryColumn(n),
          Option(v).map(d => decimalToByteArray(d.asInstanceOf[JBigDecimal], length)).orNull)
  }

  private val makeNotEq
      : PartialFunction[ParquetSchemaType, (Array[String], Any) => FilterPredicate] = {
    case ParquetBooleanType =>
      (n: Array[String], v: Any) => FilterApi.notEq(booleanColumn(n), v.asInstanceOf[JBoolean])
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: Array[String], v: Any) =>
        FilterApi.notEq(
          intColumn(n),
          Option(v).map(_.asInstanceOf[Number].intValue.asInstanceOf[Integer]).orNull)
    case ParquetLongType =>
      (n: Array[String], v: Any) => FilterApi.notEq(longColumn(n), v.asInstanceOf[JLong])
    case ParquetFloatType =>
      (n: Array[String], v: Any) => FilterApi.notEq(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: Array[String], v: Any) => FilterApi.notEq(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
      (n: Array[String], v: Any) =>
        FilterApi.notEq(
          binaryColumn(n),
          Option(v).map(s => Binary.fromString(s.asInstanceOf[String])).orNull)
    case ParquetBinaryType =>
      (n: Array[String], v: Any) =>
        FilterApi.notEq(
          binaryColumn(n),
          Option(v).map(_ => Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]])).orNull)
    case ParquetDateType if pushDownDate =>
      (n: Array[String], v: Any) =>
        FilterApi.notEq(
          intColumn(n),
          Option(v).map(date => dateToDays(date).asInstanceOf[Integer]).orNull)
    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: Array[String], v: Any) =>
        FilterApi.notEq(longColumn(n), Option(v).map(timestampToMicros).orNull)
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: Array[String], v: Any) =>
        FilterApi.notEq(longColumn(n), Option(v).map(timestampToMillis).orNull)

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT32, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.notEq(
          intColumn(n),
          Option(v).map(d => decimalToInt32(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT64, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.notEq(
          longColumn(n),
          Option(v).map(d => decimalToInt64(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, FIXED_LEN_BYTE_ARRAY, length)
        if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.notEq(
          binaryColumn(n),
          Option(v).map(d => decimalToByteArray(d.asInstanceOf[JBigDecimal], length)).orNull)
  }

  private val makeLt
      : PartialFunction[ParquetSchemaType, (Array[String], Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: Array[String], v: Any) =>
        FilterApi.lt(intColumn(n), v.asInstanceOf[Number].intValue.asInstanceOf[Integer])
    case ParquetLongType =>
      (n: Array[String], v: Any) => FilterApi.lt(longColumn(n), v.asInstanceOf[JLong])
    case ParquetFloatType =>
      (n: Array[String], v: Any) => FilterApi.lt(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: Array[String], v: Any) => FilterApi.lt(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
      (n: Array[String], v: Any) =>
        FilterApi.lt(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: Array[String], v: Any) =>
        FilterApi.lt(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
    case ParquetDateType if pushDownDate =>
      (n: Array[String], v: Any) =>
        FilterApi.lt(intColumn(n), dateToDays(v).asInstanceOf[Integer])
    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.lt(longColumn(n), timestampToMicros(v))
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.lt(longColumn(n), timestampToMillis(v))

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT32, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.lt(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT64, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.lt(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, FIXED_LEN_BYTE_ARRAY, length)
        if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.lt(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
  }

  private val makeLtEq
      : PartialFunction[ParquetSchemaType, (Array[String], Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: Array[String], v: Any) =>
        FilterApi.ltEq(intColumn(n), v.asInstanceOf[Number].intValue.asInstanceOf[Integer])
    case ParquetLongType =>
      (n: Array[String], v: Any) => FilterApi.ltEq(longColumn(n), v.asInstanceOf[JLong])
    case ParquetFloatType =>
      (n: Array[String], v: Any) => FilterApi.ltEq(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: Array[String], v: Any) => FilterApi.ltEq(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
      (n: Array[String], v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: Array[String], v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
    case ParquetDateType if pushDownDate =>
      (n: Array[String], v: Any) =>
        FilterApi.ltEq(intColumn(n), dateToDays(v).asInstanceOf[Integer])
    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.ltEq(longColumn(n), timestampToMicros(v))
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.ltEq(longColumn(n), timestampToMillis(v))

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT32, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.ltEq(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT64, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.ltEq(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, FIXED_LEN_BYTE_ARRAY, length)
        if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.ltEq(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
  }

  private val makeGt
      : PartialFunction[ParquetSchemaType, (Array[String], Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: Array[String], v: Any) =>
        FilterApi.gt(intColumn(n), v.asInstanceOf[Number].intValue.asInstanceOf[Integer])
    case ParquetLongType =>
      (n: Array[String], v: Any) => FilterApi.gt(longColumn(n), v.asInstanceOf[JLong])
    case ParquetFloatType =>
      (n: Array[String], v: Any) => FilterApi.gt(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: Array[String], v: Any) => FilterApi.gt(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
      (n: Array[String], v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: Array[String], v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
    case ParquetDateType if pushDownDate =>
      (n: Array[String], v: Any) =>
        FilterApi.gt(intColumn(n), dateToDays(v).asInstanceOf[Integer])
    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.gt(longColumn(n), timestampToMicros(v))
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.gt(longColumn(n), timestampToMillis(v))

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT32, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.gt(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT64, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.gt(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, FIXED_LEN_BYTE_ARRAY, length)
        if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.gt(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
  }

  private val makeGtEq
      : PartialFunction[ParquetSchemaType, (Array[String], Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: Array[String], v: Any) =>
        FilterApi.gtEq(intColumn(n), v.asInstanceOf[Number].intValue.asInstanceOf[Integer])
    case ParquetLongType =>
      (n: Array[String], v: Any) => FilterApi.gtEq(longColumn(n), v.asInstanceOf[JLong])
    case ParquetFloatType =>
      (n: Array[String], v: Any) => FilterApi.gtEq(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: Array[String], v: Any) => FilterApi.gtEq(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
      (n: Array[String], v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: Array[String], v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
    case ParquetDateType if pushDownDate =>
      (n: Array[String], v: Any) =>
        FilterApi.gtEq(intColumn(n), dateToDays(v).asInstanceOf[Integer])
    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.gtEq(longColumn(n), timestampToMicros(v))
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.gtEq(longColumn(n), timestampToMillis(v))

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT32, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.gtEq(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT64, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.gtEq(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, FIXED_LEN_BYTE_ARRAY, length)
        if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.gtEq(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
  }

  private val makeInPredicate: PartialFunction[
    ParquetSchemaType,
    (Array[String], Array[Any], ParquetStatistics[_]) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: Array[String], v: Array[Any], statistics: ParquetStatistics[_]) =>
        v.map(toIntValue(_).toInt).foreach(statistics.updateStats)
        FilterApi.and(
          FilterApi.gtEq(intColumn(n), statistics.genericGetMin().asInstanceOf[Integer]),
          FilterApi.ltEq(intColumn(n), statistics.genericGetMax().asInstanceOf[Integer]))

    case ParquetLongType =>
      (n: Array[String], v: Array[Any], statistics: ParquetStatistics[_]) =>
        v.map(toLongValue).foreach(statistics.updateStats(_))
        FilterApi.and(
          FilterApi.gtEq(longColumn(n), statistics.genericGetMin().asInstanceOf[JLong]),
          FilterApi.ltEq(longColumn(n), statistics.genericGetMax().asInstanceOf[JLong]))

    case ParquetFloatType =>
      (n: Array[String], v: Array[Any], statistics: ParquetStatistics[_]) =>
        v.map(_.asInstanceOf[JFloat]).foreach(statistics.updateStats(_))
        FilterApi.and(
          FilterApi.gtEq(floatColumn(n), statistics.genericGetMin().asInstanceOf[JFloat]),
          FilterApi.ltEq(floatColumn(n), statistics.genericGetMax().asInstanceOf[JFloat]))

    case ParquetDoubleType =>
      (n: Array[String], v: Array[Any], statistics: ParquetStatistics[_]) =>
        v.map(_.asInstanceOf[JDouble]).foreach(statistics.updateStats(_))
        FilterApi.and(
          FilterApi.gtEq(doubleColumn(n), statistics.genericGetMin().asInstanceOf[JDouble]),
          FilterApi.ltEq(doubleColumn(n), statistics.genericGetMax().asInstanceOf[JDouble]))

    case ParquetStringType =>
      (n: Array[String], v: Array[Any], statistics: ParquetStatistics[_]) =>
        v.map(s => Binary.fromString(s.asInstanceOf[String])).foreach(statistics.updateStats)
        FilterApi.and(
          FilterApi.gtEq(binaryColumn(n), statistics.genericGetMin().asInstanceOf[Binary]),
          FilterApi.ltEq(binaryColumn(n), statistics.genericGetMax().asInstanceOf[Binary]))

    case ParquetBinaryType =>
      (n: Array[String], v: Array[Any], statistics: ParquetStatistics[_]) =>
        v.map(b => Binary.fromReusedByteArray(b.asInstanceOf[Array[Byte]]))
          .foreach(statistics.updateStats)
        FilterApi.and(
          FilterApi.gtEq(binaryColumn(n), statistics.genericGetMin().asInstanceOf[Binary]),
          FilterApi.ltEq(binaryColumn(n), statistics.genericGetMax().asInstanceOf[Binary]))

    case ParquetDateType if pushDownDate =>
      (n: Array[String], v: Array[Any], statistics: ParquetStatistics[_]) =>
        v.map(dateToDays).map(_.asInstanceOf[Integer]).foreach(statistics.updateStats(_))
        FilterApi.and(
          FilterApi.gtEq(intColumn(n), statistics.genericGetMin().asInstanceOf[Integer]),
          FilterApi.ltEq(intColumn(n), statistics.genericGetMax().asInstanceOf[Integer]))

    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: Array[String], v: Array[Any], statistics: ParquetStatistics[_]) =>
        v.map(timestampToMicros).foreach(statistics.updateStats(_))
        FilterApi.and(
          FilterApi.gtEq(longColumn(n), statistics.genericGetMin().asInstanceOf[JLong]),
          FilterApi.ltEq(longColumn(n), statistics.genericGetMax().asInstanceOf[JLong]))

    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: Array[String], v: Array[Any], statistics: ParquetStatistics[_]) =>
        v.map(timestampToMillis).foreach(statistics.updateStats(_))
        FilterApi.and(
          FilterApi.gtEq(longColumn(n), statistics.genericGetMin().asInstanceOf[JLong]),
          FilterApi.ltEq(longColumn(n), statistics.genericGetMax().asInstanceOf[JLong]))

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT32, _) if pushDownDecimal =>
      (n: Array[String], v: Array[Any], statistics: ParquetStatistics[_]) =>
        v.map(_.asInstanceOf[JBigDecimal]).map(decimalToInt32).foreach(statistics.updateStats(_))
        FilterApi.and(
          FilterApi.gtEq(intColumn(n), statistics.genericGetMin().asInstanceOf[Integer]),
          FilterApi.ltEq(intColumn(n), statistics.genericGetMax().asInstanceOf[Integer]))

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT64, _) if pushDownDecimal =>
      (n: Array[String], v: Array[Any], statistics: ParquetStatistics[_]) =>
        v.map(_.asInstanceOf[JBigDecimal]).map(decimalToInt64).foreach(statistics.updateStats(_))
        FilterApi.and(
          FilterApi.gtEq(longColumn(n), statistics.genericGetMin().asInstanceOf[JLong]),
          FilterApi.ltEq(longColumn(n), statistics.genericGetMax().asInstanceOf[JLong]))

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, FIXED_LEN_BYTE_ARRAY, length)
        if pushDownDecimal =>
      (path: Array[String], v: Array[Any], statistics: ParquetStatistics[_]) =>
        v.map(d => decimalToByteArray(d.asInstanceOf[JBigDecimal], length))
          .foreach(statistics.updateStats)
        FilterApi.and(
          FilterApi.gtEq(binaryColumn(path), statistics.genericGetMin().asInstanceOf[Binary]),
          FilterApi.ltEq(binaryColumn(path), statistics.genericGetMax().asInstanceOf[Binary]))
  }

  // Returns filters that can be pushed down when reading Parquet files.
  def convertibleFilters(filters: Seq[sources.Filter]): Seq[sources.Filter] = {
    filters.flatMap(convertibleFiltersHelper(_, canPartialPushDown = true))
  }

  private def convertibleFiltersHelper(
      predicate: sources.Filter,
      canPartialPushDown: Boolean): Option[sources.Filter] = {
    predicate match {
      case sources.And(left, right) =>
        val leftResultOptional = convertibleFiltersHelper(left, canPartialPushDown)
        val rightResultOptional = convertibleFiltersHelper(right, canPartialPushDown)
        (leftResultOptional, rightResultOptional) match {
          case (Some(leftResult), Some(rightResult)) => Some(sources.And(leftResult, rightResult))
          case (Some(leftResult), None) if canPartialPushDown => Some(leftResult)
          case (None, Some(rightResult)) if canPartialPushDown => Some(rightResult)
          case _ => None
        }

      case sources.Or(left, right) =>
        val leftResultOptional = convertibleFiltersHelper(left, canPartialPushDown)
        val rightResultOptional = convertibleFiltersHelper(right, canPartialPushDown)
        if (leftResultOptional.isEmpty || rightResultOptional.isEmpty) {
          None
        } else {
          Some(sources.Or(leftResultOptional.get, rightResultOptional.get))
        }
      case sources.Not(pred) =>
        val resultOptional = convertibleFiltersHelper(pred, canPartialPushDown = false)
        resultOptional.map(sources.Not)

      case other =>
        if (createFilter(other).isDefined) {
          Some(other)
        } else {
          None
        }
    }
  }

  /**
   * Converts data sources filters to Parquet filter predicates.
   */
  def createFilter(predicate: sources.Filter): Option[FilterPredicate] = {
    createFilterHelper(predicate, canPartialPushDownConjuncts = true)
  }

  // Parquet's type in the given file should be matched to the value's type
  // in the pushed filter in order to push down the filter to Parquet.
  private def valueCanMakeFilterOn(name: String, value: Any): Boolean = {
    value == null || (nameToParquetField(name).fieldType match {
      case ParquetBooleanType => value.isInstanceOf[JBoolean]
      case ParquetByteType | ParquetShortType | ParquetIntegerType =>
        value match {
          // Byte/Short/Int are all stored as INT32 in Parquet so filters are built using type
          // Int. We don't create a filter if the value would overflow.
          case _: JByte | _: JShort | _: Integer => true
          case v: JLong => v.longValue() >= Int.MinValue && v.longValue() <= Int.MaxValue
          case _ => false
        }
      case ParquetLongType => value.isInstanceOf[JLong]
      case ParquetFloatType => value.isInstanceOf[JFloat]
      case ParquetDoubleType => value.isInstanceOf[JDouble]
      case ParquetStringType => value.isInstanceOf[String]
      case ParquetBinaryType => value.isInstanceOf[Array[Byte]]
      case ParquetDateType =>
        value.isInstanceOf[Date] || value.isInstanceOf[LocalDate]
      case ParquetTimestampMicrosType | ParquetTimestampMillisType =>
        value.isInstanceOf[Timestamp] || value.isInstanceOf[Instant]
      case ParquetSchemaType(decimalType: DecimalLogicalTypeAnnotation, INT32, _) =>
        isDecimalMatched(value, decimalType)
      case ParquetSchemaType(decimalType: DecimalLogicalTypeAnnotation, INT64, _) =>
        isDecimalMatched(value, decimalType)
      case ParquetSchemaType(
            decimalType: DecimalLogicalTypeAnnotation,
            FIXED_LEN_BYTE_ARRAY,
            _) =>
        isDecimalMatched(value, decimalType)
      case _ => false
    })
  }

  // Decimal type must make sure that filter value's scale matched the file.
  // If doesn't matched, which would cause data corruption.
  private def isDecimalMatched(
      value: Any,
      decimalLogicalType: DecimalLogicalTypeAnnotation): Boolean = value match {
    case decimal: JBigDecimal =>
      decimal.scale == decimalLogicalType.getScale
    case _ => false
  }

  private def canMakeFilterOn(name: String, value: Any): Boolean = {
    nameToParquetField.contains(name) && valueCanMakeFilterOn(name, value)
  }

  /**
   * @param predicate
   *   the input filter predicates. Not all the predicates can be pushed down.
   * @param canPartialPushDownConjuncts
   *   whether a subset of conjuncts of predicates can be pushed down safely. Pushing ONLY one
   *   side of AND down is safe to do at the top level or none of its ancestors is NOT and OR.
   * @return
   *   the Parquet-native filter predicates that are eligible for pushdown.
   */
  private def createFilterHelper(
      predicate: sources.Filter,
      canPartialPushDownConjuncts: Boolean): Option[FilterPredicate] = {
    // NOTE:
    //
    // For any comparison operator `cmp`, both `a cmp NULL` and `NULL cmp a` evaluate to `NULL`,
    // which can be casted to `false` implicitly. Please refer to the `eval` method of these
    // operators and the `PruneFilters` rule for details.

    // Hyukjin:
    // I added [[EqualNullSafe]] with [[org.apache.parquet.filter2.predicate.Operators.Eq]].
    // So, it performs equality comparison identically when given [[sources.Filter]] is [[EqualTo]].
    // The reason why I did this is, that the actual Parquet filter checks null-safe equality
    // comparison.
    // So I added this and maybe [[EqualTo]] should be changed. It still seems fine though, because
    // physical planning does not set `NULL` to [[EqualTo]] but changes it to [[IsNull]] and etc.
    // Probably I missed something and obviously this should be changed.

    predicate match {
      case sources.IsNull(name) if canMakeFilterOn(name, null) =>
        makeEq
          .lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, null))
      case sources.IsNotNull(name) if canMakeFilterOn(name, null) =>
        makeNotEq
          .lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, null))

      case sources.EqualTo(name, value) if canMakeFilterOn(name, value) =>
        makeEq
          .lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))
      case sources.Not(sources.EqualTo(name, value)) if canMakeFilterOn(name, value) =>
        makeNotEq
          .lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))

      case sources.EqualNullSafe(name, value) if canMakeFilterOn(name, value) =>
        makeEq
          .lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))
      case sources.Not(sources.EqualNullSafe(name, value)) if canMakeFilterOn(name, value) =>
        makeNotEq
          .lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))

      case sources.LessThan(name, value) if (value != null) && canMakeFilterOn(name, value) =>
        makeLt
          .lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))
      case sources.LessThanOrEqual(name, value)
          if (value != null) && canMakeFilterOn(name, value) =>
        makeLtEq
          .lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))

      case sources.GreaterThan(name, value) if (value != null) && canMakeFilterOn(name, value) =>
        makeGt
          .lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))
      case sources.GreaterThanOrEqual(name, value)
          if (value != null) && canMakeFilterOn(name, value) =>
        makeGtEq
          .lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))

      case sources.And(lhs, rhs) =>
        // At here, it is not safe to just convert one side and remove the other side
        // if we do not understand what the parent filters are.
        //
        // Here is an example used to explain the reason.
        // Let's say we have NOT(a = 2 AND b in ('1')) and we do not understand how to
        // convert b in ('1'). If we only convert a = 2, we will end up with a filter
        // NOT(a = 2), which will generate wrong results.
        //
        // Pushing one side of AND down is only safe to do at the top level or in the child
        // AND before hitting NOT or OR conditions, and in this case, the unsupported predicate
        // can be safely removed.
        val lhsFilterOption =
          createFilterHelper(lhs, canPartialPushDownConjuncts)
        val rhsFilterOption =
          createFilterHelper(rhs, canPartialPushDownConjuncts)

        (lhsFilterOption, rhsFilterOption) match {
          case (Some(lhsFilter), Some(rhsFilter)) => Some(FilterApi.and(lhsFilter, rhsFilter))
          case (Some(lhsFilter), None) if canPartialPushDownConjuncts => Some(lhsFilter)
          case (None, Some(rhsFilter)) if canPartialPushDownConjuncts => Some(rhsFilter)
          case _ => None
        }

      case sources.Or(lhs, rhs) =>
        // The Or predicate is convertible when both of its children can be pushed down.
        // That is to say, if one/both of the children can be partially pushed down, the Or
        // predicate can be partially pushed down as well.
        //
        // Here is an example used to explain the reason.
        // Let's say we have
        // (a1 AND a2) OR (b1 AND b2),
        // a1 and b1 is convertible, while a2 and b2 is not.
        // The predicate can be converted as
        // (a1 OR b1) AND (a1 OR b2) AND (a2 OR b1) AND (a2 OR b2)
        // As per the logical in And predicate, we can push down (a1 OR b1).
        for {
          lhsFilter <- createFilterHelper(lhs, canPartialPushDownConjuncts)
          rhsFilter <- createFilterHelper(rhs, canPartialPushDownConjuncts)
        } yield FilterApi.or(lhsFilter, rhsFilter)

      case sources.Not(pred) =>
        createFilterHelper(pred, canPartialPushDownConjuncts = false)
          .map(FilterApi.not)

      case sources.In(name, values)
          if pushDownInFilterThreshold > 0 && values.nonEmpty &&
            canMakeFilterOn(name, values.head) =>
        val fieldType = nameToParquetField(name).fieldType
        val fieldNames = nameToParquetField(name).fieldNames
        if (values.length <= pushDownInFilterThreshold) {
          values.distinct
            .flatMap { v =>
              makeEq.lift(fieldType).map(_(fieldNames, v))
            }
            .reduceLeftOption(FilterApi.or)
        } else if (canPartialPushDownConjuncts) {
          val primitiveType = schema.getColumnDescription(fieldNames).getPrimitiveType
          val statistics: ParquetStatistics[_] = ParquetStatistics.createStats(primitiveType)
          if (values.contains(null)) {
            Seq(
              makeEq.lift(fieldType).map(_(fieldNames, null)),
              makeInPredicate
                .lift(fieldType)
                .map(_(fieldNames, values.filter(_ != null), statistics))).flatten
              .reduceLeftOption(FilterApi.or)
          } else {
            makeInPredicate.lift(fieldType).map(_(fieldNames, values, statistics))
          }
        } else {
          None
        }

      case sources.StringStartsWith(name, prefix)
          if pushDownStringPredicate && canMakeFilterOn(name, prefix) =>
        Option(prefix).map { v =>
          FilterApi.userDefined(
            binaryColumn(nameToParquetField(name).fieldNames),
            new UserDefinedPredicate[Binary] with Serializable {
              private val strToBinary = Binary.fromReusedByteArray(v.getBytes)
              private val size = strToBinary.length

              override def canDrop(statistics: Statistics[Binary]): Boolean = {
                val comparator = PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR
                val max = statistics.getMax
                val min = statistics.getMin
                comparator.compare(max.slice(0, math.min(size, max.length)), strToBinary) < 0 ||
                comparator.compare(min.slice(0, math.min(size, min.length)), strToBinary) > 0
              }

              override def inverseCanDrop(statistics: Statistics[Binary]): Boolean = {
                val comparator = PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR
                val max = statistics.getMax
                val min = statistics.getMin
                comparator.compare(max.slice(0, math.min(size, max.length)), strToBinary) == 0 &&
                comparator.compare(min.slice(0, math.min(size, min.length)), strToBinary) == 0
              }

              override def keep(value: Binary): Boolean = {
                value != null && UTF8String
                  .fromBytes(value.getBytes)
                  .startsWith(UTF8String.fromBytes(strToBinary.getBytes))
              }
            })
        }

      case sources.StringEndsWith(name, suffix)
          if pushDownStringPredicate && canMakeFilterOn(name, suffix) =>
        Option(suffix).map { v =>
          FilterApi.userDefined(
            binaryColumn(nameToParquetField(name).fieldNames),
            new UserDefinedPredicate[Binary] with Serializable {
              private val suffixStr = UTF8String.fromString(v)
              override def canDrop(statistics: Statistics[Binary]): Boolean = false
              override def inverseCanDrop(statistics: Statistics[Binary]): Boolean = false
              override def keep(value: Binary): Boolean = {
                value != null && UTF8String.fromBytes(value.getBytes).endsWith(suffixStr)
              }
            })
        }

      case sources.StringContains(name, value)
          if pushDownStringPredicate && canMakeFilterOn(name, value) =>
        Option(value).map { v =>
          FilterApi.userDefined(
            binaryColumn(nameToParquetField(name).fieldNames),
            new UserDefinedPredicate[Binary] with Serializable {
              private val subStr = UTF8String.fromString(v)
              override def canDrop(statistics: Statistics[Binary]): Boolean = false
              override def inverseCanDrop(statistics: Statistics[Binary]): Boolean = false
              override def keep(value: Binary): Boolean = {
                value != null && UTF8String.fromBytes(value.getBytes).contains(subStr)
              }
            })
        }

      case _ => None
    }
  }

  def createNativeFilters(predicates: Seq[sources.Filter]): Option[Array[Byte]] = {
    predicates.reduceOption(sources.And).flatMap(createNativeFilter).map { expr =>
      val outputStream = new ByteArrayOutputStream()
      expr.writeTo(outputStream)
      outputStream.close()
      outputStream.toByteArray
    }
  }

  private def createNativeFilter(predicate: sources.Filter): Option[ExprOuterClass.Expr] = {
    def nameUnaryExpr(name: String)(
        f: (ExprOuterClass.Expr.Builder, ExprOuterClass.UnaryExpr) => ExprOuterClass.Expr.Builder)
        : Option[ExprOuterClass.Expr] = {
      createNameExpr(name, dataSchema).map { case (_, childExpr) =>
        createUnaryExpr(childExpr, f)
      }
    }

    def nameValueBinaryExpr(name: String, value: Any)(
        f: (
            ExprOuterClass.Expr.Builder,
            ExprOuterClass.BinaryExpr) => ExprOuterClass.Expr.Builder)
        : Option[ExprOuterClass.Expr] = {
      createNameExpr(name, dataSchema).flatMap { case (dataType, childExpr) =>
        createValueExpr(value, dataType).map(createBinaryExpr(childExpr, _, f))
      }
    }

    predicate match {
      case sources.IsNull(name) if canMakeFilterOn(name, null) =>
        nameUnaryExpr(name) { (builder, unaryExpr) =>
          builder.setIsNull(unaryExpr)
        }
      case sources.IsNotNull(name) if canMakeFilterOn(name, null) =>
        nameUnaryExpr(name) { (builder, unaryExpr) =>
          builder.setIsNotNull(unaryExpr)
        }

      case sources.EqualTo(name, value) if canMakeFilterOn(name, value) =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setEq(binaryExpr)
        }

      case sources.Not(sources.EqualTo(name, value)) if canMakeFilterOn(name, value) =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setNeq(binaryExpr)
        }

      case sources.EqualNullSafe(name, value) if canMakeFilterOn(name, value) =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setEqNullSafe(binaryExpr)
        }

      case sources.Not(sources.EqualNullSafe(name, value)) if canMakeFilterOn(name, value) =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setNeqNullSafe(binaryExpr)
        }

      case sources.LessThan(name, value) if (value != null) && canMakeFilterOn(name, value) =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setLt(binaryExpr)
        }

      case sources.LessThanOrEqual(name, value)
          if (value != null) && canMakeFilterOn(name, value) =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setLtEq(binaryExpr)
        }

      case sources.GreaterThan(name, value) if (value != null) && canMakeFilterOn(name, value) =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setGt(binaryExpr)
        }

      case sources.GreaterThanOrEqual(name, value)
          if (value != null) && canMakeFilterOn(name, value) =>
        nameValueBinaryExpr(name, value) { (builder, binaryExpr) =>
          builder.setGtEq(binaryExpr)
        }

      case sources.And(lhs, rhs) =>
        (createNativeFilter(lhs), createNativeFilter(rhs)) match {
          case (Some(leftExpr), Some(rightExpr)) =>
            Some(
              createBinaryExpr(
                leftExpr,
                rightExpr,
                (builder, binaryExpr) => builder.setAnd(binaryExpr)))
          case _ => None
        }

      case sources.Or(lhs, rhs) =>
        (createNativeFilter(lhs), createNativeFilter(rhs)) match {
          case (Some(leftExpr), Some(rightExpr)) =>
            Some(
              createBinaryExpr(
                leftExpr,
                rightExpr,
                (builder, binaryExpr) => builder.setOr(binaryExpr)))
          case _ => None
        }

      case sources.Not(pred) =>
        val childExpr = createNativeFilter(pred)
        childExpr.map { expr =>
          createUnaryExpr(expr, (builder, unaryExpr) => builder.setNot(unaryExpr))
        }

      case sources.In(name, values)
          if pushDownInFilterThreshold > 0 && values.nonEmpty &&
            canMakeFilterOn(name, values.head) =>
        createNameExpr(name, dataSchema).flatMap { case (dataType, nameExpr) =>
          val valueExprs = values.flatMap(createValueExpr(_, dataType))
          if (valueExprs.length != values.length) {
            None
          } else {
            val builder = ExprOuterClass.In.newBuilder()
            builder.setInValue(nameExpr)
            builder.addAllLists(valueExprs.toSeq.asJava)
            builder.setNegated(false)
            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setIn(builder)
                .build())
          }
        }

      case sources.StringStartsWith(attribute, prefix)
          if pushDownStringPredicate && canMakeFilterOn(attribute, prefix) =>
        val attributeExpr = createNameExpr(attribute, dataSchema)
        val prefixExpr = attributeExpr.flatMap { case (dataType, _) =>
          createValueExpr(prefix, dataType)
        }
        scalarFunctionExprToProto("starts_with", Some(attributeExpr.get._2), prefixExpr)

      case sources.StringEndsWith(attribute, suffix)
          if pushDownStringPredicate && canMakeFilterOn(attribute, suffix) =>
        val attributeExpr = createNameExpr(attribute, dataSchema)
        val suffixExpr = attributeExpr.flatMap { case (dataType, _) =>
          createValueExpr(suffix, dataType)
        }
        scalarFunctionExprToProto("ends_with", Some(attributeExpr.get._2), suffixExpr)

      case sources.StringContains(attribute, value)
          if pushDownStringPredicate && canMakeFilterOn(attribute, value) =>
        val attributeExpr = createNameExpr(attribute, dataSchema)
        val valueExpr = attributeExpr.flatMap { case (dataType, _) =>
          createValueExpr(value, dataType)
        }
        scalarFunctionExprToProto("contains", Some(attributeExpr.get._2), valueExpr)

      case _ => None
    }
  }
}
