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

package org.apache.comet.serde

import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateNamedStruct, GetArrayStructFields, GetStructField, JsonToStructs, StructsToCsv, StructsToJson}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.DataTypeSupport
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}

object CometCreateNamedStruct extends CometExpressionSerde[CreateNamedStruct] {

  private val duplicateNamesReason =
    "`CreateNamedStruct` with duplicate field names is not supported"

  override def getUnsupportedReasons(): Seq[String] = Seq(duplicateNamesReason)

  override def getSupportLevel(expr: CreateNamedStruct): SupportLevel = {
    if (expr.names.length != expr.names.distinct.length) {
      Unsupported(Some(duplicateNamesReason))
    } else {
      Compatible()
    }
  }

  override def convert(
      expr: CreateNamedStruct,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val valExprs = expr.valExprs.map(exprToProtoInternal(_, inputs, binding))

    if (valExprs.forall(_.isDefined)) {
      val structBuilder = ExprOuterClass.CreateNamedStruct.newBuilder()
      structBuilder.addAllValues(valExprs.map(_.get).asJava)
      structBuilder.addAllNames(expr.names.map(_.toString).asJava)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setCreateNamedStruct(structBuilder)
          .build())
    } else {
      withFallbackReason(expr, "unsupported arguments for CreateNamedStruct", expr.valExprs: _*)
      None
    }

  }
}

object CometGetStructField extends CometExpressionSerde[GetStructField] {
  override def convert(
      expr: GetStructField,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    exprToProtoInternal(expr.child, inputs, binding).map { childExpr =>
      val getStructFieldBuilder = ExprOuterClass.GetStructField
        .newBuilder()
        .setChild(childExpr)
        .setOrdinal(expr.ordinal)

      ExprOuterClass.Expr
        .newBuilder()
        .setGetStructField(getStructFieldBuilder)
        .build()
    }
  }
}

object CometGetArrayStructFields extends CometExpressionSerde[GetArrayStructFields] {
  override def convert(
      expr: GetArrayStructFields,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)

    if (childExpr.isDefined) {
      val arrayStructFieldsBuilder = ExprOuterClass.GetArrayStructFields
        .newBuilder()
        .setChild(childExpr.get)
        .setOrdinal(expr.ordinal)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setGetArrayStructFields(arrayStructFieldsBuilder)
          .build())
    } else {
      withFallbackReason(expr, "unsupported arguments for GetArrayStructFields", expr.child)
      None
    }
  }
}

/**
 * `to_json` runs Spark's own implementation through the codegen dispatcher by default, for
 * byte-exact compatibility. The native (rust) path is faster but only covers struct inputs of
 * supported types with no options, so it is opt-in via
 * `spark.comet.expression.StructsToJson.allowIncompatible`; any case it does not cover
 * (unsupported types or options) falls through to the codegen dispatcher via
 * [[CometCodegenDispatch]].
 */
object CometStructsToJson extends CometCodegenDispatch[StructsToJson] {

  private def nativeSupported(expr: StructsToJson): Boolean =
    expr.options.isEmpty && isSupportedType(expr.child.dataType)

  override def convert(
      expr: StructsToJson,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] =
    if (CometConf.isExprAllowIncompat(getExprConfigName(expr)) && nativeSupported(expr)) {
      val ignoreNullFields = SQLConf.get.jsonGeneratorIgnoreNullFields
      exprToProtoInternal(expr.child, inputs, binding) match {
        case Some(p) =>
          val toJson = ExprOuterClass.ToJson
            .newBuilder()
            .setChild(p)
            .setTimezone(expr.timeZoneId.getOrElse("UTC"))
            .setIgnoreNullFields(ignoreNullFields)
            .build()
          Some(
            ExprOuterClass.Expr
              .newBuilder()
              .setToJson(toJson)
              .build())
        case _ =>
          withFallbackReason(expr, expr.child)
          None
      }
    } else {
      super.convert(expr, inputs, binding)
    }

  def isSupportedType(dt: DataType): Boolean = {
    dt match {
      case StructType(fields) =>
        fields.forall(f => isSupportedType(f.dataType))
      case DataTypes.BooleanType | DataTypes.ByteType | DataTypes.ShortType |
          DataTypes.IntegerType | DataTypes.LongType | DataTypes.FloatType |
          DataTypes.DoubleType | DataTypes.StringType =>
        true
      case DataTypes.DateType | DataTypes.TimestampType =>
        // TODO implement these types with tests for formatting options and timezone
        false
      case _: MapType | _: ArrayType =>
        // Spark supports map and array in StructsToJson but this is not yet
        // implemented in Comet
        false
      case _ => false
    }
  }
}

/**
 * `from_json` runs Spark's own implementation through the codegen dispatcher by default. The
 * native (rust) path is partially implemented and not comprehensively tested, so it is opt-in via
 * `spark.comet.expression.JsonToStructs.allowIncompatible` and only for schemas it supports; any
 * other case falls through to the codegen dispatcher via [[CometCodegenDispatch]].
 */
object CometJsonToStructs extends CometCodegenDispatch[JsonToStructs] {

  private def nativeSupported(expr: JsonToStructs): Boolean =
    expr.schema != null && isSupportedSchema(expr.schema)

  override def convert(
      expr: JsonToStructs,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {

    if (!(CometConf.isExprAllowIncompat(getExprConfigName(expr)) && nativeSupported(expr))) {
      return super.convert(expr, inputs, binding)
    }

    val options = expr.options
    if (options.nonEmpty) {
      val mode = options.getOrElse("mode", "PERMISSIVE")
      if (mode != "PERMISSIVE") {
        withFallbackReason(expr, s"from_json: Only PERMISSIVE mode supported, got: $mode")
        return None
      }
      val knownOptions = Set("mode")
      val unknownOpts = options.keySet -- knownOptions
      if (unknownOpts.nonEmpty) {
        withFallbackReason(
          expr,
          s"from_json: Ignoring unsupported options: ${unknownOpts.mkString(", ")}")
      }
    }

    // Convert child expression and schema to protobuf
    for {
      childProto <- exprToProtoInternal(expr.child, inputs, binding)
      schemaProto <- serializeDataType(expr.schema)
    } yield {
      val fromJson = ExprOuterClass.FromJson
        .newBuilder()
        .setChild(childProto)
        .setSchema(schemaProto)
        .setTimezone(expr.timeZoneId.getOrElse("UTC"))
        .build()
      ExprOuterClass.Expr.newBuilder().setFromJson(fromJson).build()
    }
  }

  private def isSupportedSchema(dt: DataType): Boolean = dt match {
    case StructType(fields) =>
      fields.nonEmpty && fields.forall(f => isSupportedSchema(f.dataType))
    case DataTypes.IntegerType | DataTypes.LongType | DataTypes.FloatType | DataTypes.DoubleType |
        DataTypes.BooleanType | DataTypes.StringType =>
      true
    case _ => false
  }
}

object CometStructsToCsv extends CometExpressionSerde[StructsToCsv] {

  private val incompatibleDataTypes = Seq(DateType, TimestampType, TimestampNTZType, BinaryType)

  override def getIncompatibleReasons(): Seq[String] = Seq(
    "Date, Timestamp, TimestampNTZ, and Binary data types may produce different results" +
      " (https://github.com/apache/datafusion-comet/issues/3232)")

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "Complex types (arrays, maps, structs) in the schema are not supported")

  override def getSupportLevel(expr: StructsToCsv): SupportLevel = {
    val dataTypes = expr.inputSchema.fields.map(_.dataType)
    val containsComplexType = dataTypes.exists(DataTypeSupport.isComplexType)
    if (containsComplexType) {
      return Unsupported(
        Some(
          s"The schema ${expr.inputSchema} is not supported because it includes a complex type"))
    }
    val containsIncompatibleDataTypes = dataTypes.exists(incompatibleDataTypes.contains)
    if (containsIncompatibleDataTypes) {
      return Incompatible(
        Some(
          s"The schema ${expr.inputSchema} is not supported because " +
            s"it includes a incompatible data types: $incompatibleDataTypes"))
    }
    // https://github.com/apache/datafusion-comet/issues/3232
    Incompatible()
  }

  override def convert(
      expr: StructsToCsv,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    for {
      childProto <- exprToProtoInternal(expr.child, inputs, binding)
    } yield {
      val optionsProto = options2Proto(expr.options, expr.timeZoneId)
      val toCsv = ExprOuterClass.ToCsv
        .newBuilder()
        .setChild(childProto)
        .setOptions(optionsProto)
        .build()
      ExprOuterClass.Expr.newBuilder().setToCsv(toCsv).build()
    }
  }

  private def options2Proto(
      options: Map[String, String],
      timeZoneId: Option[String]): ExprOuterClass.CsvWriteOptions = {
    ExprOuterClass.CsvWriteOptions
      .newBuilder()
      .setDelimiter(options.getOrElse("delimiter", ","))
      .setQuote(options.getOrElse("quote", "\""))
      .setEscape(options.getOrElse("escape", "\\"))
      .setNullValue(options.getOrElse("nullValue", ""))
      .setTimezone(timeZoneId.getOrElse("UTC"))
      .setIgnoreLeadingWhiteSpace(options
        .get("ignoreLeadingWhiteSpace")
        .flatMap(ignoreLeadingWhiteSpace => Try(ignoreLeadingWhiteSpace.toBoolean).toOption)
        .getOrElse(true))
      .setIgnoreTrailingWhiteSpace(options
        .get("ignoreTrailingWhiteSpace")
        .flatMap(ignoreTrailingWhiteSpace => Try(ignoreTrailingWhiteSpace.toBoolean).toOption)
        .getOrElse(true))
      .setQuoteAll(options
        .get("quoteAll")
        .flatMap(quoteAll => Try(quoteAll.toBoolean).toOption)
        .getOrElse(false))
      .build()
  }
}
