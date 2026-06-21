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

import org.apache.spark.sql.catalyst.expressions.{Attribute, BitLength, Cast, Concat, ConcatWs, Elt, Expression, FindInSet, FormatNumber, FormatString, GetJsonObject, If, InitCap, IsNull, Left, Length, Levenshtein, Like, Literal, Lower, Mask, OctetLength, Overlay, RegExpExtract, RegExpExtractAll, RegExpInStr, RegExpReplace, Right, RLike, SoundEx, StringLocate, StringLPad, StringRepeat, StringReplace, StringRPad, StringSplit, StringTranslate, Substring, SubstringIndex, ToCharacter, ToNumber, TryToNumber, UnBase64, Upper}
import org.apache.spark.sql.types.{BinaryType, DataTypes, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{createBinaryExpr, exprToProtoInternal, optExprWithFallbackReason, scalarFunctionExprToProto, scalarFunctionExprToProtoWithReturnType}
import org.apache.comet.shims.CometTypeShim

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
    optExprWithFallbackReason(optExpr, expr, leftCast, rightCast)
  }
}

class CometCaseConversionBase[T <: Expression](function: String)
    extends CometScalarFunction[T](function) {

  override def getSupportLevel(expr: T): SupportLevel = Compatible()

  override def convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    if (CometConf.COMET_CASE_CONVERSION_ENABLED.get()) {
      // Native scalar function: faster but does not match Spark for locale-specific characters
      // (e.g. Turkish dotted/dotless I). Opt-in.
      super.convert(expr, inputs, binding)
    } else {
      // Default: route through the codegen dispatcher so Spark's own doGenCode runs inside the
      // Comet pipeline. This guarantees Spark-compatible behavior across 3.4 / 3.5 / 4.0.
      // Falls through to Spark when the dispatcher is disabled.
      CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
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

object CometBitLength extends CometScalarFunction[BitLength]("bit_length") {
  override def getUnsupportedReasons(): Seq[String] = Seq("`BinaryType` input is not supported")

  override def getSupportLevel(expr: BitLength): SupportLevel = expr.child.dataType match {
    case _: BinaryType => Unsupported(Some("bit_length on BinaryType is not supported"))
    case _ => Compatible()
  }
}

object CometOctetLength extends CometScalarFunction[OctetLength]("octet_length") {
  override def getUnsupportedReasons(): Seq[String] = Seq("`BinaryType` input is not supported")

  override def getSupportLevel(expr: OctetLength): SupportLevel = expr.child.dataType match {
    case _: BinaryType => Unsupported(Some("octet_length on BinaryType is not supported"))
    case _ => Compatible()
  }
}

object CometStringTranslate extends CometScalarFunction[StringTranslate]("translate") {
  private val incompatReason =
    "DataFusion's translate iterates over Unicode graphemes (Spark uses code points) and" +
      " substitutes U+0000 instead of treating it as a deletion sentinel"

  override def getIncompatibleReasons(): Seq[String] = Seq(incompatReason)

  override def getSupportLevel(expr: StringTranslate): SupportLevel = Incompatible(
    Some(incompatReason))
}

object CometInitCap extends CometScalarFunction[InitCap]("initcap") {

  override def getSupportLevel(expr: InitCap): SupportLevel = Compatible()

  override def convert(expr: InitCap, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    if (CometConf.isExprAllowIncompat(getExprConfigName(expr))) {
      // Native path: faster but treats hyphen as a word separator (e.g.
      // `robert rose-smith` produces `Robert Rose-Smith` instead of Spark's `Robert Rose-smith`).
      // https://github.com/apache/datafusion-comet/issues/1052
      super.convert(expr, inputs, binding)
    } else {
      // Default: route through the codegen dispatcher so Spark's own doGenCode runs inside the
      // Comet pipeline. This guarantees Spark-compatible behavior across 3.4 / 3.5 / 4.0.
      // Falls through to Spark when the dispatcher is disabled.
      CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
  }
}

object CometStringReplace extends CometScalarFunction[StringReplace]("replace") {

  override def getSupportLevel(expr: StringReplace): SupportLevel = Compatible()

  override def convert(
      expr: StringReplace,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    if (CometConf.isExprAllowIncompat(getExprConfigName(expr))) {
      // The native DataFusion `replace` avoids the JVM allocations of the codegen
      // dispatcher but is not Spark-compatible for an empty search string, so it is
      // only used when incompatibility is explicitly allowed.
      super.convert(expr, inputs, binding)
    } else {
      // Run Spark's own generated code inside the Comet pipeline so the result matches Spark
      // exactly. Falls back to Spark when the codegen dispatcher is disabled.
      CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
  }
}

object CometSubstring extends CometExpressionSerde[Substring] {

  override def getSupportLevel(expr: Substring): SupportLevel = (expr.pos, expr.len) match {
    case (_: Literal, _: Literal) => Compatible()
    case _ => Unsupported(Some("Substring pos and len must be literals"))
  }

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
            withFallbackReason(expr, expr.str)
            None
        }
      case _ =>
        // Unreachable: getSupportLevel gates non-literal pos/len.
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
    optExprWithFallbackReason(optExpr, expr, expr.strExpr, expr.delimExpr, expr.countExpr)
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
            withFallbackReason(expr, expr.str)
            None
        }
      case _ =>
        // Unreachable: getSupportLevel gates a non-literal length.
        None
    }
  }

  override def getSupportLevel(expr: Left): SupportLevel = {
    expr.str.dataType match {
      case _: BinaryType | _: StringType =>
        expr.len match {
          case _: Literal => Compatible()
          case _ => Unsupported(Some("LEFT len must be a literal"))
        }
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
              withFallbackReason(expr, expr.str)
              None
          }
        }
      case _ =>
        // Unreachable: getSupportLevel gates a non-literal length.
        None
    }
  }

  override def getUnsupportedReasons(): Seq[String] = Seq("Only supports `StringType` input")

  override def getSupportLevel(expr: Right): SupportLevel = {
    expr.str.dataType match {
      case _: StringType =>
        expr.len match {
          case _: Literal => Compatible()
          case _ => Unsupported(Some("RIGHT len must be a literal"))
        }
      case _ => Unsupported(Some(s"RIGHT does not support ${expr.str.dataType}"))
    }
  }
}

object CometConcat
    extends CometScalarFunction[Concat]("concat")
    with CometTypeShim
    with CodegenDispatchFallback {
  private val unsupportedReason = "CONCAT supports only string input parameters"

  // Spark 4.0 widens Concat to accept collated strings and preserves the collation in the merged
  // result type. The native concat UDF always produces UTF8 (UTF8_BINARY semantics), so a
  // non-default collation diverges from Spark.
  private val collationReason =
    "concat does not support non-UTF8_BINARY collations " +
      "(https://github.com/apache/datafusion-comet/issues/2190)"

  override def getUnsupportedReasons(): Seq[String] = Seq(unsupportedReason)

  override def getIncompatibleReasons(): Seq[String] = Seq(collationReason)

  override def getSupportLevel(expr: Concat): SupportLevel = {
    // Use isInstanceOf rather than `== DataTypes.StringType` so that collated strings (a
    // StringType with a non-default collationId, which is not == the default StringType) are still
    // recognised as string input and routed to the collation check below rather than reported as
    // an unsupported input type.
    if (!expr.children.forall(_.dataType.isInstanceOf[StringType])) {
      Unsupported(Some(unsupportedReason))
    } else if (hasNonDefaultStringCollation(expr.dataType) ||
      expr.children.exists(c => hasNonDefaultStringCollation(c.dataType))) {
      Incompatible(Some(collationReason))
    } else {
      Compatible()
    }
  }
}

object CometConcatWs extends CometExpressionSerde[ConcatWs] {

  override def getSupportLevel(expr: ConcatWs): SupportLevel = expr.children.headOption match {
    // A NULL separator converts directly to a NULL result, so it stays supported.
    case Some(Literal(null, _)) => Compatible()
    // Fall back to Spark for all-literal args so ConstantFolding can handle it.
    case _ if expr.children.forall(_.foldable) =>
      Unsupported(Some("all arguments are foldable"))
    case _ => Compatible()
  }

  override def convert(expr: ConcatWs, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    expr.children.headOption match {
      // Match Spark behavior: when the separator is NULL, the result of concat_ws is NULL.
      case Some(Literal(null, _)) =>
        val nullLiteral = Literal.create(null, expr.dataType)
        exprToProtoInternal(nullLiteral, inputs, binding)

      case _ =>
        // For all other cases, use the generic scalar function implementation.
        CometScalarFunction[ConcatWs]("concat_ws").convert(expr, inputs, binding)
    }
  }
}

object CometLike extends CometExpressionSerde[Like] {

  override def getSupportLevel(expr: Like): SupportLevel = {
    if (expr.escapeChar == '\\') {
      Compatible()
    } else {
      Unsupported(Some(s"custom escape character ${expr.escapeChar} not supported in LIKE"))
    }
  }

  override def convert(expr: Like, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setLike(binaryExpr))
  }
}

/**
 * `rlike` runs Spark's own implementation through the codegen dispatcher by default, for
 * byte-exact results. The native (rust) regexp engine is faster but has different semantics from
 * Java regexp, so it is opt-in via `spark.comet.expression.RLike.allowIncompatible`; any case it
 * does not cover (a non-scalar pattern) falls through to the codegen dispatcher via
 * [[CometScalaUDF]].
 */
object CometRLike extends CometExpressionSerde[RLike] {

  override def getSupportLevel(expr: RLike): SupportLevel = Compatible()

  override def convert(expr: RLike, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    if (CometConf.isExprAllowIncompat(getExprConfigName(expr))) {
      expr.right match {
        case Literal(_, DataTypes.StringType) =>
          // Native path: the Rust regexp engine has different semantics from Java regexp.
          return createBinaryExpr(
            expr,
            expr.left,
            expr.right,
            inputs,
            binding,
            (builder, binaryExpr) => builder.setRlike(binaryExpr))
        case _ =>
        // Non-scalar pattern: the native path cannot handle it, fall through to the dispatcher.
      }
    }
    // Default: route through the codegen dispatcher so Spark's own doGenCode runs inside the Comet
    // pipeline. Falls back to Spark when the dispatcher is disabled.
    CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
  }
}

private object PadReasons {
  val literalStrReason = "Scalar values are not supported for the `str` argument."
  val nonLiteralPadReason = "Only scalar values are supported for the `pad` argument."
}

object CometStringRPad extends CometExpressionSerde[StringRPad] {

  override def getUnsupportedReasons(): Seq[String] =
    Seq(PadReasons.literalStrReason, PadReasons.nonLiteralPadReason)

  override def getSupportLevel(expr: StringRPad): SupportLevel = {
    if (expr.str.isInstanceOf[Literal]) {
      return Unsupported(Some(PadReasons.literalStrReason))
    }
    if (!expr.pad.isInstanceOf[Literal]) {
      return Unsupported(Some(PadReasons.nonLiteralPadReason))
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

  override def getUnsupportedReasons(): Seq[String] =
    Seq(PadReasons.literalStrReason, PadReasons.nonLiteralPadReason)

  override def getSupportLevel(expr: StringLPad): SupportLevel = {
    if (expr.str.isInstanceOf[Literal]) {
      return Unsupported(Some(PadReasons.literalStrReason))
    }
    if (!expr.pad.isInstanceOf[Literal]) {
      return Unsupported(Some(PadReasons.nonLiteralPadReason))
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

/**
 * `regexp_extract` runs Spark's own implementation through the codegen dispatcher by default, for
 * byte-exact results. The native (rust) regexp engine is faster but has different semantics from
 * Java regexp, so it is opt-in via `spark.comet.expression.RegExpExtract.allowIncompatible` and
 * only when the pattern and idx are integer literals; any other case falls through to the codegen
 * dispatcher.
 */
object CometRegExpExtract extends CometExpressionSerde[RegExpExtract] {

  override def getSupportLevel(expr: RegExpExtract): SupportLevel = Compatible()

  private def nativeSupported(expr: RegExpExtract): Boolean =
    expr.regexp.isInstanceOf[Literal] && expr.idx.isInstanceOf[Literal]

  override def convert(
      expr: RegExpExtract,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    if (CometConf.isExprAllowIncompat(getExprConfigName(expr)) && nativeSupported(expr)) {
      val subjectExpr = exprToProtoInternal(expr.subject, inputs, binding)
      val patternExpr = exprToProtoInternal(expr.regexp, inputs, binding)
      val idxExpr = exprToProtoInternal(expr.idx, inputs, binding)
      val optExpr = scalarFunctionExprToProtoWithReturnType(
        "regexp_extract",
        expr.dataType,
        failOnError = false,
        subjectExpr,
        patternExpr,
        idxExpr)
      optExprWithFallbackReason(optExpr, expr, expr.subject, expr.regexp, expr.idx)
    } else {
      // Default: route through the codegen dispatcher so Spark's own doGenCode runs inside the
      // Comet pipeline. Falls back to Spark when the dispatcher is disabled.
      CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
  }
}

/**
 * `regexp_extract_all` runs Spark's own implementation through the codegen dispatcher by default,
 * for byte-exact results. The native (rust) regexp engine is faster but has different semantics
 * from Java regexp, so it is opt-in via
 * `spark.comet.expression.RegExpExtractAll.allowIncompatible` and only when the pattern and idx
 * are integer literals; any other case falls through to the codegen dispatcher.
 */
object CometRegExpExtractAll extends CometExpressionSerde[RegExpExtractAll] {

  override def getSupportLevel(expr: RegExpExtractAll): SupportLevel = Compatible()

  private def nativeSupported(expr: RegExpExtractAll): Boolean =
    expr.regexp.isInstanceOf[Literal] && expr.idx.isInstanceOf[Literal]

  override def convert(
      expr: RegExpExtractAll,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    if (CometConf.isExprAllowIncompat(getExprConfigName(expr)) && nativeSupported(expr)) {
      val subjectExpr = exprToProtoInternal(expr.subject, inputs, binding)
      val patternExpr = exprToProtoInternal(expr.regexp, inputs, binding)
      val idxExpr = exprToProtoInternal(expr.idx, inputs, binding)
      val optExpr = scalarFunctionExprToProtoWithReturnType(
        "regexp_extract_all",
        expr.dataType,
        failOnError = false,
        subjectExpr,
        patternExpr,
        idxExpr)
      optExprWithFallbackReason(optExpr, expr, expr.subject, expr.regexp, expr.idx)
    } else {
      // Default: route through the codegen dispatcher so Spark's own doGenCode runs inside the
      // Comet pipeline. Falls back to Spark when the dispatcher is disabled.
      CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
  }
}

/**
 * `regexp_replace` runs Spark's own implementation through the codegen dispatcher by default, for
 * byte-exact results. The native (rust) regexp engine is faster but has different semantics from
 * Java regexp, so it is opt-in via `spark.comet.expression.RegExpReplace.allowIncompatible` and
 * only for an offset of 1; any other case falls through to the codegen dispatcher.
 */
object CometRegExpReplace extends CometExpressionSerde[RegExpReplace] {

  override def getSupportLevel(expr: RegExpReplace): SupportLevel = Compatible()

  private def nativeSupported(expr: RegExpReplace): Boolean = expr.pos match {
    case Literal(value, DataTypes.IntegerType) if value == 1 => true
    case _ => false
  }

  override def convert(
      expr: RegExpReplace,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    if (CometConf.isExprAllowIncompat(getExprConfigName(expr)) && nativeSupported(expr)) {
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
      optExprWithFallbackReason(optExpr, expr, expr.subject, expr.regexp, expr.rep, expr.pos)
    } else {
      // Default: route through the codegen dispatcher so Spark's own doGenCode runs inside the
      // Comet pipeline. Falls back to Spark when the dispatcher is disabled.
      CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
  }
}

/**
 * Serde for StringSplit expression. `split` runs Spark's own implementation through the codegen
 * dispatcher by default, for byte-exact results. The native (rust) regexp engine is faster but
 * has different semantics from Java regexp, so it is opt-in via
 * `spark.comet.expression.StringSplit.allowIncompatible`.
 *
 * The native path is a custom Comet function (not a built-in DataFusion function), so the return
 * type is included in the protobuf to avoid DataFusion registry lookup failures.
 */
object CometStringSplit extends CometExpressionSerde[StringSplit] {

  override def getSupportLevel(expr: StringSplit): SupportLevel = Compatible()

  override def convert(
      expr: StringSplit,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    if (CometConf.isExprAllowIncompat(getExprConfigName(expr))) {
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
      optExprWithFallbackReason(optExpr, expr, expr.str, expr.regex, expr.limit)
    } else {
      // Default: route through the codegen dispatcher so Spark's own doGenCode runs inside the
      // Comet pipeline. Falls back to Spark when the dispatcher is disabled.
      CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
  }
}

// These have no native (rust) implementation, so they always run through the codegen dispatcher.
object CometRegExpInStr extends CometCodegenDispatch[RegExpInStr]

/**
 * `get_json_object` runs Spark's own implementation through the codegen dispatcher by default,
 * for byte-exact results. The native (rust) path is faster but incompatible with Spark for
 * single-quoted JSON and unescaped control characters, so it is opt-in via
 * `spark.comet.expression.GetJsonObject.allowIncompatible`; otherwise it rides the codegen
 * dispatcher via [[CometCodegenDispatch]].
 */
object CometGetJsonObject extends CometCodegenDispatch[GetJsonObject] {

  override def convert(
      expr: GetJsonObject,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] =
    if (CometConf.isExprAllowIncompat(getExprConfigName(expr))) {
      val jsonExpr = exprToProtoInternal(expr.json, inputs, binding)
      val pathExpr = exprToProtoInternal(expr.path, inputs, binding)
      val optExpr = scalarFunctionExprToProtoWithReturnType(
        "get_json_object",
        expr.dataType,
        false,
        jsonExpr,
        pathExpr)
      optExprWithFallbackReason(optExpr, expr, expr.json, expr.path)
    } else {
      super.convert(expr, inputs, binding)
    }
}

// Expressions routed through the JVM codegen dispatcher: no native implementation, so Spark's own
// doGenCode runs inside the Comet pipeline, matching Spark exactly.
object CometLevenshtein extends CometCodegenDispatch[Levenshtein]

object CometElt extends CometCodegenDispatch[Elt]

object CometFindInSet extends CometCodegenDispatch[FindInSet]

object CometFormatNumber extends CometCodegenDispatch[FormatNumber]

object CometFormatString extends CometCodegenDispatch[FormatString]

object CometOverlay extends CometCodegenDispatch[Overlay]

object CometSoundEx extends CometCodegenDispatch[SoundEx]

object CometStringLocate extends CometCodegenDispatch[StringLocate]

object CometUnBase64 extends CometCodegenDispatch[UnBase64]

object CometToCharacter extends CometCodegenDispatch[ToCharacter]

object CometToNumber extends CometCodegenDispatch[ToNumber]

object CometTryToNumber extends CometCodegenDispatch[TryToNumber]

object CometMask extends CometCodegenDispatch[Mask]
