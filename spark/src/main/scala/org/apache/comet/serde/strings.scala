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

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Concat, ConcatWs, Expression, GetJsonObject, If, InitCap, IsNull, Left, Length, Like, Literal, Lower, RegExpExtract, RegExpExtractAll, RegExpInStr, RegExpReplace, Right, RLike, StringLPad, StringRepeat, StringRPad, StringSplit, Substring, SubstringIndex, Upper}
import org.apache.spark.sql.types.{BinaryType, DataTypes, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.expressions.{CometCast, CometEvalMode}
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{createBinaryExpr, exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto, scalarFunctionExprToProtoWithReturnType}

/**
 * Routing decision for a regex expression. Each regex serde delegates to [[RegexpRoute.choose]]
 * to pick between the native Rust regex engine, the JVM codegen dispatcher, or Spark fallback.
 */
private sealed trait RegexpRoute
private object RegexpRoute {

  /** Run the native Rust regex implementation. */
  case object Native extends RegexpRoute

  /**
   * Route through the Arrow-direct codegen dispatcher. The dispatcher Janino-compiles Spark's own
   * `doGenCode` for the expression, so semantics match Spark exactly.
   */
  case object JvmCodegen extends RegexpRoute

  /** Decline to run natively; the operator falls back to Spark with the given reason. */
  case class Fallback(reason: String) extends RegexpRoute

  /**
   * SupportLevel returned by serdes whose route is [[Native]]. Surfaced as `Incompatible` so the
   * standard gating in `QueryPlanSerde` can recognize `spark.comet.exec.regexp.engine=rust` as
   * the opt-in (via `optedInBy`) without each serde repeating the check.
   */
  val nativeIncompatible: Incompatible =
    Incompatible(
      Some("Rust regexp engine has different semantics from Java regexp"),
      Some(s"${CometConf.COMET_REGEXP_ENGINE.key}=${CometConf.REGEXP_ENGINE_RUST}"))

  /**
   * Pick a route given the user's config and whether a native Rust implementation exists for the
   * expression. `engine=java` (default) routes through the codegen dispatcher when
   * [[CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED]] is true; otherwise Spark fallback.
   * `engine=rust` runs native if available; else Spark fallback.
   */
  def choose(exprName: String, hasNative: Boolean): RegexpRoute = {
    val engine = CometConf.COMET_REGEXP_ENGINE.get()
    val codegenEnabled = CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.get()

    engine match {
      case CometConf.REGEXP_ENGINE_RUST =>
        if (hasNative) {
          Native
        } else {
          Fallback(
            s"$exprName has no native Rust implementation. Set " +
              s"${CometConf.COMET_REGEXP_ENGINE.key}=${CometConf.REGEXP_ENGINE_JAVA} with " +
              s"${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=true to route through the " +
              "codegen dispatcher.")
        }

      case CometConf.REGEXP_ENGINE_JAVA =>
        if (codegenEnabled) {
          JvmCodegen
        } else {
          Fallback(
            s"$exprName requires ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=true when " +
              s"${CometConf.COMET_REGEXP_ENGINE.key}=${CometConf.REGEXP_ENGINE_JAVA}. " +
              "The codegen dispatcher is experimental and disabled by default.")
        }

      case other => Fallback(s"Unknown ${CometConf.COMET_REGEXP_ENGINE.key}=$other")
    }
  }
}

object CometStringRepeat extends CometExpressionSerde[StringRepeat] {

  override def getCompatibleNotes(): Seq[String] = Seq(
    "A negative argument for the number of times to repeat throws an exception" +
      " instead of returning an empty string as Spark does")

  override def convert(
      expr: StringRepeat,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val children = expr.children
    val leftCast = Cast(children(0), StringType)
    val rightCast = Cast(children(1), LongType)
    val leftExpr = exprToProtoInternal(leftCast, inputs, binding)
    val rightExpr = exprToProtoInternal(rightCast, inputs, binding)
    val optExpr = scalarFunctionExprToProto("repeat", leftExpr, rightExpr)
    optExprWithInfo(optExpr, expr, leftCast, rightCast)
  }
}

class CometCaseConversionBase[T <: Expression](function: String)
    extends CometScalarFunction[T](function) {

  override def getIncompatibleReasons(): Seq[String] = Seq(
    "Results can vary depending on locale and character set." +
      s" Requires `${CometConf.COMET_CASE_CONVERSION_ENABLED.key}=true` to enable.")

  override def convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    if (!CometConf.COMET_CASE_CONVERSION_ENABLED.get()) {
      withInfo(
        expr,
        "Comet is not compatible with Spark for case conversion in " +
          s"locale-specific cases. Set ${CometConf.COMET_CASE_CONVERSION_ENABLED.key}=true " +
          "to enable it anyway.")
      return None
    }
    super.convert(expr, inputs, binding)
  }
}

object CometUpper extends CometCaseConversionBase[Upper]("upper")

object CometLower extends CometCaseConversionBase[Lower]("lower")

object CometLength extends CometScalarFunction[Length]("length") {
  override def getUnsupportedReasons(): Seq[String] = Seq("`BinaryType` input is not supported")

  override def getSupportLevel(expr: Length): SupportLevel = expr.child.dataType match {
    case _: BinaryType => Unsupported(Some("Length on BinaryType is not supported"))
    case _ => Compatible()
  }
}

object CometInitCap extends CometScalarFunction[InitCap]("initcap") {

  override def getIncompatibleReasons(): Seq[String] = Seq(
    "Treats hyphen as a word separator (e.g. `robert rose-smith` produces `Robert Rose-Smith`" +
      " instead of Spark's `Robert Rose-smith`)" +
      " (https://github.com/apache/datafusion-comet/issues/1052)")

  override def getSupportLevel(expr: InitCap): SupportLevel = {
    // Behavior differs from Spark. One example is that for the input "robert rose-smith", Spark
    // will produce "Robert Rose-smith", but Comet will produce "Robert Rose-Smith".
    // https://github.com/apache/datafusion-comet/issues/1052
    Incompatible(None)
  }

  override def convert(expr: InitCap, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    super.convert(expr, inputs, binding)
  }
}

object CometSubstring extends CometExpressionSerde[Substring] {

  override def convert(
      expr: Substring,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    (expr.pos, expr.len) match {
      case (Literal(pos, _), Literal(len, _)) =>
        exprToProtoInternal(expr.str, inputs, binding) match {
          case Some(strExpr) =>
            val builder = ExprOuterClass.Substring.newBuilder()
            builder.setChild(strExpr)
            builder.setStart(pos.asInstanceOf[Int])
            builder.setLen(len.asInstanceOf[Int])
            Some(ExprOuterClass.Expr.newBuilder().setSubstring(builder).build())
          case None =>
            withInfo(expr, expr.str)
            None
        }
      case _ =>
        withInfo(expr, "Substring pos and len must be literals")
        None
    }
  }
}

object CometSubstringIndex extends CometExpressionSerde[SubstringIndex] {

  override def convert(
      expr: SubstringIndex,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val strExpr = exprToProtoInternal(expr.strExpr, inputs, binding)
    val delimExpr = exprToProtoInternal(expr.delimExpr, inputs, binding)
    val countCast = Cast(expr.countExpr, LongType)
    val countExpr = exprToProtoInternal(countCast, inputs, binding)
    val optExpr =
      scalarFunctionExprToProto("substring_index", strExpr, delimExpr, countExpr)
    optExprWithInfo(optExpr, expr, expr.strExpr, expr.delimExpr, expr.countExpr)
  }
}

object CometLeft extends CometExpressionSerde[Left] {

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "Only supports `BinaryType` and `StringType` input",
    "The length argument must be a literal value")

  override def convert(expr: Left, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    expr.len match {
      case Literal(lenValue, _) =>
        exprToProtoInternal(expr.str, inputs, binding) match {
          case Some(strExpr) =>
            val builder = ExprOuterClass.Substring.newBuilder()
            builder.setChild(strExpr)
            builder.setStart(1)
            builder.setLen(lenValue.asInstanceOf[Int])
            Some(ExprOuterClass.Expr.newBuilder().setSubstring(builder).build())
          case None =>
            withInfo(expr, expr.str)
            None
        }
      case _ =>
        withInfo(expr, "LEFT len must be a literal")
        None
    }
  }

  override def getSupportLevel(expr: Left): SupportLevel = {
    expr.str.dataType match {
      case _: BinaryType | _: StringType => Compatible()
      case _ => Unsupported(Some(s"LEFT does not support ${expr.str.dataType}"))
    }
  }
}

object CometRight extends CometExpressionSerde[Right] {

  override def convert(expr: Right, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    expr.len match {
      case Literal(lenValue, _) =>
        val lenInt = lenValue.asInstanceOf[Int]
        if (lenInt <= 0) {
          // Match Spark's behavior: If(IsNull(str), NULL, "")
          // This ensures NULL propagation: RIGHT(NULL, 0) -> NULL, RIGHT("hello", 0) -> ""
          val isNullExpr = IsNull(expr.str)
          val nullLiteral = Literal.create(null, StringType)
          val emptyStringLiteral = Literal(UTF8String.EMPTY_UTF8, StringType)
          val ifExpr = If(isNullExpr, nullLiteral, emptyStringLiteral)

          // Serialize the If expression using existing infrastructure
          exprToProtoInternal(ifExpr, inputs, binding)
        } else {
          exprToProtoInternal(expr.str, inputs, binding) match {
            case Some(strExpr) =>
              val builder = ExprOuterClass.Substring.newBuilder()
              builder.setChild(strExpr)
              builder.setStart(-lenInt)
              builder.setLen(lenInt)
              Some(ExprOuterClass.Expr.newBuilder().setSubstring(builder).build())
            case None =>
              withInfo(expr, expr.str)
              None
          }
        }
      case _ =>
        withInfo(expr, "RIGHT len must be a literal")
        None
    }
  }

  override def getUnsupportedReasons(): Seq[String] = Seq("Only supports `StringType` input")

  override def getSupportLevel(expr: Right): SupportLevel = {
    expr.str.dataType match {
      case _: StringType => Compatible()
      case _ => Unsupported(Some(s"RIGHT does not support ${expr.str.dataType}"))
    }
  }
}

object CometConcat extends CometScalarFunction[Concat]("concat") {
  val unsupportedReason = "CONCAT supports only string input parameters"

  override def getIncompatibleReasons(): Seq[String] = Seq(unsupportedReason)

  override def getSupportLevel(expr: Concat): SupportLevel = {
    if (expr.children.forall(_.dataType == DataTypes.StringType)) {
      Compatible()
    } else {
      Incompatible(Some(unsupportedReason))
    }
  }
}

object CometConcatWs extends CometExpressionSerde[ConcatWs] {

  override def convert(expr: ConcatWs, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    expr.children.headOption match {
      // Match Spark behavior: when the separator is NULL, the result of concat_ws is NULL.
      case Some(Literal(null, _)) =>
        val nullLiteral = Literal.create(null, expr.dataType)
        exprToProtoInternal(nullLiteral, inputs, binding)

      case _ if expr.children.forall(_.foldable) =>
        // Fall back to Spark for all-literal args so ConstantFolding can handle it
        withInfo(expr, "all arguments are foldable")
        None

      case _ =>
        // For all other cases, use the generic scalar function implementation.
        CometScalarFunction[ConcatWs]("concat_ws").convert(expr, inputs, binding)
    }
  }
}

object CometLike extends CometExpressionSerde[Like] {

  override def convert(expr: Like, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    if (expr.escapeChar == '\\') {
      createBinaryExpr(
        expr,
        expr.left,
        expr.right,
        inputs,
        binding,
        (builder, binaryExpr) => builder.setLike(binaryExpr))
    } else {
      withInfo(expr, s"custom escape character ${expr.escapeChar} not supported in LIKE")
      None
    }
  }
}

object CometRLike extends CometExpressionSerde[RLike] {

  override def getSupportLevel(expr: RLike): SupportLevel =
    RegexpRoute.choose("rlike", hasNative = true) match {
      case RegexpRoute.Native => RegexpRoute.nativeIncompatible
      case RegexpRoute.JvmCodegen => Compatible(None)
      case RegexpRoute.Fallback(reason) => Unsupported(Some(reason))
    }

  override def convert(expr: RLike, inputs: Seq[Attribute], binding: Boolean): Option[Expr] =
    RegexpRoute.choose("rlike", hasNative = true) match {
      case RegexpRoute.Native => convertViaNativeRegex(expr, inputs, binding)
      case RegexpRoute.JvmCodegen => CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
      case RegexpRoute.Fallback(reason) =>
        withInfo(expr, reason)
        None
    }

  private def convertViaNativeRegex(
      expr: RLike,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    expr.right match {
      case Literal(_, DataTypes.StringType) =>
        createBinaryExpr(
          expr,
          expr.left,
          expr.right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setRlike(binaryExpr))
      case _ =>
        withInfo(expr, "Only scalar regexp patterns are supported on the native path")
        None
    }
  }
}

/**
 * Regex expression with no native Rust path: always routes through the JVM codegen dispatcher, or
 * falls back to Spark when the engine selector or codegen flag forbids it.
 */
abstract class CometRegexpCodegenOnly[T <: Expression](exprName: String)
    extends CometExpressionSerde[T] {

  override def getSupportLevel(expr: T): SupportLevel =
    RegexpRoute.choose(exprName, hasNative = false) match {
      case RegexpRoute.JvmCodegen => Compatible(None)
      case RegexpRoute.Fallback(reason) => Unsupported(Some(reason))
      case RegexpRoute.Native => Unsupported(Some(s"$exprName has no native Rust implementation"))
    }

  override def convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[Expr] =
    RegexpRoute.choose(exprName, hasNative = false) match {
      case RegexpRoute.JvmCodegen => CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
      case RegexpRoute.Fallback(reason) =>
        withInfo(expr, reason)
        None
      case RegexpRoute.Native =>
        withInfo(expr, s"$exprName has no native Rust implementation")
        None
    }
}

object CometRegExpExtract extends CometRegexpCodegenOnly[RegExpExtract]("regexp_extract")

object CometRegExpExtractAll
    extends CometRegexpCodegenOnly[RegExpExtractAll]("regexp_extract_all")

object CometRegExpInStr extends CometRegexpCodegenOnly[RegExpInStr]("regexp_instr")

object CometStringRPad extends CometExpressionSerde[StringRPad] {

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "Scalar values are not supported for the `str` argument." +
      " Only scalar values are supported for the `pad` argument.")

  override def getSupportLevel(expr: StringRPad): SupportLevel = {
    if (expr.str.isInstanceOf[Literal]) {
      return Unsupported(Some("Scalar values are not supported for the str argument"))
    }
    if (!expr.pad.isInstanceOf[Literal]) {
      return Unsupported(Some("Only scalar values are supported for the pad argument"))
    }
    Compatible()
  }

  override def convert(
      expr: StringRPad,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {

    scalarFunctionExprToProto(
      "rpad",
      exprToProtoInternal(expr.str, inputs, binding),
      exprToProtoInternal(expr.len, inputs, binding),
      exprToProtoInternal(expr.pad, inputs, binding))
  }
}

object CometStringLPad extends CometExpressionSerde[StringLPad] {

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "Scalar values are not supported for the `str` argument." +
      " Only scalar values are supported for the `pad` argument.")

  override def getSupportLevel(expr: StringLPad): SupportLevel = {
    if (expr.str.isInstanceOf[Literal]) {
      return Unsupported(Some("Scalar values are not supported for the str argument"))
    }
    if (!expr.pad.isInstanceOf[Literal]) {
      return Unsupported(Some("Only scalar values are supported for the pad argument"))
    }
    Compatible()
  }

  override def convert(
      expr: StringLPad,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    scalarFunctionExprToProto(
      "lpad",
      exprToProtoInternal(expr.str, inputs, binding),
      exprToProtoInternal(expr.len, inputs, binding),
      exprToProtoInternal(expr.pad, inputs, binding))
  }
}

object CometRegExpReplace extends CometExpressionSerde[RegExpReplace] {

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "Only supports `regexp_replace` with an offset of 1 (no offset)")

  override def getSupportLevel(expr: RegExpReplace): SupportLevel = {
    expr.pos match {
      case Literal(value, DataTypes.IntegerType) if value == 1 =>
        RegexpRoute.choose("regexp_replace", hasNative = true) match {
          case RegexpRoute.Native => RegexpRoute.nativeIncompatible
          case RegexpRoute.JvmCodegen => Compatible(None)
          case RegexpRoute.Fallback(reason) => Unsupported(Some(reason))
        }
      case _ =>
        Unsupported(Some("Comet only supports regexp_replace with an offset of 1 (no offset)."))
    }
  }

  override def convert(
      expr: RegExpReplace,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    RegexpRoute.choose("regexp_replace", hasNative = true) match {
      case RegexpRoute.Native => convertViaNativeRegex(expr, inputs, binding)
      case RegexpRoute.JvmCodegen => CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
      case RegexpRoute.Fallback(reason) =>
        withInfo(expr, reason)
        None
    }
  }

  private def convertViaNativeRegex(
      expr: RegExpReplace,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    expr.regexp match {
      case _: Literal =>
        val subjectExpr = exprToProtoInternal(expr.subject, inputs, binding)
        val patternExpr = exprToProtoInternal(expr.regexp, inputs, binding)
        val replacementExpr = exprToProtoInternal(expr.rep, inputs, binding)
        // DataFusion's regexp_replace stops at the first match. We need to add the 'g' flag
        // to apply the regex globally to match Spark behavior.
        val flagsExpr = exprToProtoInternal(Literal("g"), inputs, binding)
        val optExpr = scalarFunctionExprToProto(
          "regexp_replace",
          subjectExpr,
          patternExpr,
          replacementExpr,
          flagsExpr)
        optExprWithInfo(optExpr, expr, expr.subject, expr.regexp, expr.rep, expr.pos)
      case _ =>
        withInfo(expr, "Only scalar regexp patterns are supported on the native path")
        None
    }
  }
}

/**
 * Serde for StringSplit expression. This is a custom Comet function (not a built-in DataFusion
 * function), so we need to include the return type in the protobuf to avoid DataFusion registry
 * lookup failures.
 */
object CometStringSplit extends CometExpressionSerde[StringSplit] {

  override def getSupportLevel(expr: StringSplit): SupportLevel =
    RegexpRoute.choose("split", hasNative = true) match {
      case RegexpRoute.Native => RegexpRoute.nativeIncompatible
      case RegexpRoute.JvmCodegen => Compatible(None)
      case RegexpRoute.Fallback(reason) => Unsupported(Some(reason))
    }

  override def convert(
      expr: StringSplit,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] =
    RegexpRoute.choose("split", hasNative = true) match {
      case RegexpRoute.Native => convertViaNativeRegex(expr, inputs, binding)
      case RegexpRoute.JvmCodegen => CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
      case RegexpRoute.Fallback(reason) =>
        withInfo(expr, reason)
        None
    }

  private def convertViaNativeRegex(
      expr: StringSplit,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    expr.regex match {
      case _: Literal =>
        val strExpr = exprToProtoInternal(expr.str, inputs, binding)
        val regexExpr = exprToProtoInternal(expr.regex, inputs, binding)
        val limitExpr = exprToProtoInternal(expr.limit, inputs, binding)
        val optExpr = scalarFunctionExprToProtoWithReturnType(
          "split",
          expr.dataType,
          false,
          strExpr,
          regexExpr,
          limitExpr)
        optExprWithInfo(optExpr, expr, expr.str, expr.regex, expr.limit)
      case _ =>
        withInfo(expr, "Only scalar regex patterns are supported on the native path")
        None
    }
  }
}

object CometGetJsonObject extends CometExpressionSerde[GetJsonObject] {

  override def getIncompatibleReasons(): Seq[String] = Seq(
    "Spark allows single-quoted JSON and unescaped control characters which Comet does not" +
      " support")

  override def getSupportLevel(expr: GetJsonObject): SupportLevel =
    Incompatible(
      Some(
        "Spark allows single-quoted JSON and unescaped control characters " +
          "which Comet does not support"))

  override def convert(
      expr: GetJsonObject,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val jsonExpr = exprToProtoInternal(expr.json, inputs, binding)
    val pathExpr = exprToProtoInternal(expr.path, inputs, binding)
    val optExpr = scalarFunctionExprToProtoWithReturnType(
      "get_json_object",
      expr.dataType,
      false,
      jsonExpr,
      pathExpr)
    optExprWithInfo(optExpr, expr, expr.json, expr.path)
  }
}

trait CommonStringExprs {

  def stringDecode(
      expr: Expression,
      charset: Expression,
      bin: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    charset match {
      case Literal(str, DataTypes.StringType)
          if str.toString.toLowerCase(Locale.ROOT) == "utf-8" =>
        // decode(col, 'utf-8') can be treated as a cast with "try" eval mode that puts nulls
        // for invalid strings.
        // Left child is the binary expression.
        val binExpr = exprToProtoInternal(bin, inputs, binding)
        if (binExpr.isDefined) {
          CometCast.castToProto(expr, None, DataTypes.StringType, binExpr.get, CometEvalMode.TRY)
        } else {
          withInfo(expr, bin)
          None
        }
      case _ =>
        withInfo(expr, "Comet only supports decoding with 'utf-8'.")
        None
    }
  }
}
