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
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.DataTypeSupport
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}

/**
 * Routing decision for a JSON expression. Each JSON serde delegates to [[JsonRoute.choose]] to
 * pick between the native Rust path and the Arrow-direct codegen dispatcher (which
 * Janino-compiles Spark's own `doGenCode` for the expression).
 */
private[serde] sealed trait JsonRoute
private[serde] object JsonRoute {

  /** Run the native DataFusion JSON implementation. */
  case object Native extends JsonRoute

  /**
   * Route through the codegen dispatcher. Spark's `doGenCode` for the expression compiles into a
   * per-batch kernel, so semantics match Spark exactly.
   */
  case object JvmCodegen extends JsonRoute

  /** Decline to run natively; the operator falls back to Spark with the given reason. */
  case class Fallback(reason: String) extends JsonRoute

  /**
   * Pick a route given the user's config. `engine=rust` (default) runs the native path.
   * `engine=java` routes through the codegen dispatcher when
   * [[CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED]] is true; otherwise Spark fallback.
   */
  def choose(exprName: String): JsonRoute = {
    val engine = CometConf.COMET_JSON_ENGINE.get()
    val codegenEnabled = CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.get()
    engine match {
      case CometConf.JSON_ENGINE_RUST => Native
      case CometConf.JSON_ENGINE_JAVA =>
        if (codegenEnabled) {
          JvmCodegen
        } else {
          Fallback(
            s"$exprName requires ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=true when " +
              s"${CometConf.COMET_JSON_ENGINE.key}=${CometConf.JSON_ENGINE_JAVA}. " +
              "The codegen dispatcher is experimental and disabled by default.")
        }
      case other => Fallback(s"Unknown ${CometConf.COMET_JSON_ENGINE.key}=$other")
    }
  }
}

object CometCreateNamedStruct extends CometExpressionSerde[CreateNamedStruct] {
  override def convert(
      expr: CreateNamedStruct,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (expr.names.length != expr.names.distinct.length) {
      withInfo(expr, "CreateNamedStruct with duplicate field names are not supported")
      return None
    }

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
      withInfo(expr, "unsupported arguments for CreateNamedStruct", expr.valExprs: _*)
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
      withInfo(expr, "unsupported arguments for GetArrayStructFields", expr.child)
      None
    }
  }
}

object CometStructsToJson extends CometExpressionSerde[StructsToJson] {

  override def getSupportLevel(expr: StructsToJson): SupportLevel = {
    JsonRoute.choose("to_json") match {
      case JsonRoute.JvmCodegen =>
        expr.child.dataType match {
          case s: StructType if s.fields.forall(f => isSupportedType(f.dataType)) =>
            Compatible(None)
          case _ =>
            Unsupported(Some("to_json: only StructType with supported fields is supported"))
        }
      case JsonRoute.Native =>
        if (expr.options.nonEmpty) {
          return Unsupported(Some("StructsToJson with options is not supported"))
        }
        val dataType = expr.child.dataType
        if (!isSupportedType(dataType)) {
          return Unsupported(Some(s"Struct type: $dataType contains unsupported types"))
        }
        Compatible()
      case JsonRoute.Fallback(reason) => Unsupported(Some(reason))
    }
  }

  override def convert(
      expr: StructsToJson,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] =
    JsonRoute.choose("to_json") match {
      case JsonRoute.JvmCodegen => CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
      case JsonRoute.Fallback(reason) =>
        withInfo(expr, reason)
        None
      case JsonRoute.Native =>
        if (expr.options.nonEmpty) {
          withInfo(expr, "StructsToJson with options is not supported")
          return None
        }
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
            withInfo(expr, expr.child)
            None
        }
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

object CometJsonToStructs extends CometExpressionSerde[JsonToStructs] {

  override def getIncompatibleReasons(): Seq[String] = Seq(
    "Partially implemented and not comprehensively tested")

  override def getUnsupportedReasons(): Seq[String] = Seq("Requires an explicit schema")

  override def getSupportLevel(expr: JsonToStructs): SupportLevel = {
    JsonRoute.choose("from_json") match {
      case JsonRoute.JvmCodegen =>
        expr.schema match {
          case s: StructType if isSupportedSchema(s) => Compatible(None)
          case _ => Unsupported(Some("from_json: only StructType schemas are supported"))
        }
      case JsonRoute.Native =>
        // this feature is partially implemented and not comprehensively tested yet
        Incompatible()
      case JsonRoute.Fallback(reason) => Unsupported(Some(reason))
    }
  }

  override def convert(
      expr: JsonToStructs,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {

    if (expr.schema == null) {
      withInfo(expr, "from_json requires explicit schema")
      return None
    }

    JsonRoute.choose("from_json") match {
      case JsonRoute.JvmCodegen => CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
      case JsonRoute.Fallback(reason) =>
        withInfo(expr, reason)
        None
      case JsonRoute.Native =>
        if (!isSupportedSchema(expr.schema)) {
          withInfo(expr, "from_json: Unsupported schema type")
          return None
        }

        val options = expr.options
        if (options.nonEmpty) {
          val mode = options.getOrElse("mode", "PERMISSIVE")
          if (mode != "PERMISSIVE") {
            withInfo(expr, s"from_json: Only PERMISSIVE mode supported, got: $mode")
            return None
          }
          val knownOptions = Set("mode")
          val unknownOpts = options.keySet -- knownOptions
          if (unknownOpts.nonEmpty) {
            withInfo(
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
