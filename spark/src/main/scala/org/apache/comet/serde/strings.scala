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

import java.nio.charset.StandardCharsets
import java.util.Arrays

import org.apache.spark.sql.catalyst.expressions.{Attribute, Base64, BitLength, BoundReference, Cast, Concat, ConcatWs, Contains, Elt, Empty2Null, EndsWith, Expression, FindInSet, FormatNumber, FormatString, GetJsonObject, InitCap, Left, Length, Levenshtein, Like, Literal, Lower, Mask, OctetLength, Overlay, RegExpExtract, RegExpExtractAll, RegExpInStr, RegExpReplace, Right, RLike, SoundEx, StartsWith, StringLocate, StringLPad, StringRepeat, StringReplace, StringRPad, StringSplit, StringTranslate, Substring, SubstringIndex, ToCharacter, ToNumber, TryToNumber, UnBase64, Upper}
import org.apache.spark.sql.types.{BinaryType, DataTypes, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometConf
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{createBinaryExpr, exprToProtoInternal, scalarFunctionExprToProto, scalarFunctionExprToProtoWithReturnType}
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
    optExpr
  }
}

class CometCaseConversionBase[T <: Expression](function: String)
    extends CometScalarFunction[T](function)
    with NativeOptInAvailable {

  override def getIncompatibleReasons(): Seq[String] =
    Seq("Results can vary depending on locale and character set")

  override def nativeOptInConfigKeyOverride: Option[String] =
    Some(CometConf.COMET_CASE_CONVERSION_ENABLED.key)

  override def getSupportLevel(expr: T): SupportLevel =
    if (!CometConf.COMET_CASE_CONVERSION_ENABLED.get()) {
      Compatible(nativeOptIn = Some(NativeOptIn(CometConf.COMET_CASE_CONVERSION_ENABLED.key)))
    } else {
      Compatible()
    }

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

object CometLevenshtein extends CometExpressionSerde[Levenshtein] {

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "Non-default collation (non-UTF8_BINARY) is not supported")

  override def getSupportLevel(expr: Levenshtein): SupportLevel =
    if (expr.children.exists(child => QueryPlanSerde.isStringCollationType(child.dataType))) {
      Unsupported(Some("Levenshtein with non-default collation is not supported"))
    } else {
      Compatible()
    }

  override def convert(
      expr: Levenshtein,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val childExprs = expr.children.map(exprToProtoInternal(_, inputs, binding))
    val optExpr =
      scalarFunctionExprToProtoWithReturnType("levenshtein", IntegerType, false, childExprs: _*)
    optExpr
  }
}

object CometInitCap extends CometScalarFunction[InitCap]("initcap") with NativeOptInAvailable {

  override def getIncompatibleReasons(): Seq[String] =
    Seq(
      "Treats hyphen as a word separator (e.g. `robert rose-smith` produces `Robert Rose-Smith`" +
        " instead of Spark's `Robert Rose-smith`)" +
        " (https://github.com/apache/datafusion-comet/issues/1052)")

  override def getSupportLevel(expr: InitCap): SupportLevel =
    if (!CometConf.isExprAllowIncompat(getExprConfigName(expr))) {
      Compatible(nativeOptIn =
        Some(NativeOptIn(CometConf.getExprAllowIncompatConfigKey(getExprConfigName(expr)))))
    } else {
      Compatible()
    }

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

object CometStringReplace
    extends CometScalarFunction[StringReplace]("replace")
    with NativeOptInAvailable {

  /**
   * The DataFusion `replace` kernel matches Spark only for a non-empty search string. Kernel
   * compatibility is not enough: `CometLiteral` serializes strings via `UTF8String.toString`
   * (malformed UTF-8 becomes U+FFFD), DataFusion evaluates every child before `replace` (so a
   * NULL `src` does not skip a throwing replacement), and scalar literals are broadcast into
   * Arrow `Utf8` arrays that overflow 32-bit offsets on a large batch.
   *
   * The default native path is therefore limited to a plan-time subset that avoids those
   * boundaries. `src` uses the same whitelist as `replace`: a short well-formed literal or a
   * column. Nested expressions (`substring`, `concat`, …) stay on the dispatcher so a throwing or
   * malformed child cannot hide inside the source tree. Non-default collations stay on the
   * dispatcher. https://github.com/apache/datafusion-comet/issues/4496
   */
  private def nativeSafeSubset(expr: StringReplace): Boolean = {
    val children = expr.children
    if (children.length != 3) {
      return false
    }
    val sourceIsSafe = isNativeSafeStringArg(children(0), allowEmptyLiteral = true)
    val searchIsSafe = children(1) match {
      case Literal(v: UTF8String, _) => isNativeSafeStringLiteral(v, allowEmpty = false)
      case _ => false
    }
    val replacementIsSafe = isNativeSafeStringArg(children(2), allowEmptyLiteral = true)
    val utf8BinaryCollation =
      !children.exists(c => QueryPlanSerde.isStringCollationType(c.dataType))
    utf8BinaryCollation && sourceIsSafe && searchIsSafe && replacementIsSafe
  }

  /** A column, null literal, or a short well-formed string literal. */
  private def isNativeSafeStringArg(expr: Expression, allowEmptyLiteral: Boolean): Boolean =
    expr match {
      case Literal(null, _) => true
      case Literal(v: UTF8String, _) => isNativeSafeStringLiteral(v, allowEmptyLiteral)
      case _: Attribute | _: BoundReference => true
      case _ => false
    }

  /**
   * `CometLiteral` encodes a string as `UTF8String.toString`, so only literals whose bytes
   * survive that round-trip can be sent natively. The size cap keeps a broadcast scalar under
   * Arrow `Utf8`'s 32-bit offset limit. Native operators feeding projections must emit batches no
   * larger than `spark.comet.batchSize`; Comet's explode path enforces that with
   * `BatchSplitExec`.
   */
  private def isNativeSafeStringLiteral(v: UTF8String, allowEmpty: Boolean): Boolean = {
    if (v == null) {
      return false
    }
    if (!allowEmpty && v.numBytes() == 0) {
      return false
    }
    val maxBytes = Int.MaxValue / math.max(CometConf.COMET_BATCH_SIZE.get(), 1)
    if (v.numBytes() > maxBytes) {
      return false
    }
    Arrays.equals(v.getBytes, v.toString.getBytes(StandardCharsets.UTF_8))
  }

  override def getCompatibleNotes(): Seq[String] =
    Seq(
      "When `src` and `replace` are each a short well-formed literal or a column, and " +
        "`search` is a short, well-formed, non-empty `UTF8_BINARY` literal, Comet evaluates " +
        "`replace` natively by default.")

  override def getIncompatibleReasons(): Seq[String] =
    Seq("Produces different results from Spark when the search string is empty")

  override def getSupportLevel(expr: StringReplace): SupportLevel =
    if (CometConf.isExprAllowIncompat(getExprConfigName(expr)) || nativeSafeSubset(expr)) {
      Compatible()
    } else {
      Compatible(nativeOptIn =
        Some(NativeOptIn(CometConf.getExprAllowIncompatConfigKey(getExprConfigName(expr)))))
    }

  override def convert(
      expr: StringReplace,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    if (CometConf.isExprAllowIncompat(getExprConfigName(expr)) || nativeSafeSubset(expr)) {
      // Native path for the plan-time safe subset, or when the user has opted in.
      super.convert(expr, inputs, binding)
    } else {
      // Nested / malformed / oversized / throwing children, or a non-default collation: run
      // Spark's own generated code inside the Comet pipeline. Falls back to Spark when the
      // codegen dispatcher is disabled.
      CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
  }
}

object CometSubstring extends CometScalarFunction[Substring]("substring")

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
    optExpr
  }
}

object CometLeft extends CometExpressionSerde[Left] {

  override def convert(expr: Left, inputs: Seq[Attribute], binding: Boolean): Option[Expr] =
    // Left is RuntimeReplaceable; its `replacement` is `Substring(str, Literal(1), len)`,
    // which routes through CometSubstring -> DataFusion's SparkSubstring UDF and handles
    // non-literal `len`, len <= 0, len > length(str), NULL propagation, and BinaryType input.
    exprToProtoInternal(expr.replacement, inputs, binding)

  override def getSupportLevel(expr: Left): SupportLevel = expr.str.dataType match {
    case _: BinaryType | _: StringType => Compatible()
    case dt => Unsupported(Some(s"LEFT does not support $dt"))
  }
}

object CometRight extends CometExpressionSerde[Right] {

  override def convert(expr: Right, inputs: Seq[Attribute], binding: Boolean): Option[Expr] =
    // Right is RuntimeReplaceable; its `replacement` is
    //   If(IsNull(str), NULL, If(len <= 0, "", Substring(str, -len, len)))
    // Serializing that tree preserves Spark's NULL-propagation for len <= 0 (RIGHT(NULL, 0)
    // must return NULL, not "") and routes the substring path through SparkSubstring.
    exprToProtoInternal(expr.replacement, inputs, binding)

  override def getSupportLevel(expr: Right): SupportLevel = expr.str.dataType match {
    case _: StringType => Compatible()
    case dt => Unsupported(Some(s"RIGHT does not support $dt"))
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

object CometLike extends CometExpressionSerde[Like] with CodegenDispatchFallback {

  private val customEscapeReason =
    "LIKE with a custom escape character (only `\\` is supported natively)"

  override def getUnsupportedReasons(): Seq[String] =
    Seq(customEscapeReason, ComparisonUtils.nonDefaultCollationDocReason)

  override def getSupportLevel(expr: Like): SupportLevel = {
    if (ComparisonUtils.hasCollatedOperand(expr.left, expr.right)) {
      Unsupported(Some(ComparisonUtils.nonDefaultCollationReason("Like")))
    } else if (expr.escapeChar != '\\') {
      Unsupported(Some(s"custom escape character ${expr.escapeChar} not supported in LIKE"))
    } else {
      Compatible()
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
 * Serdes for `Contains` / `StartsWith` / `EndsWith` that reject non-UTF8_BINARY collated operands
 * and otherwise delegate to the generic `contains` / `starts_with` / `ends_with` scalar-function
 * bridge. The native kernels compare raw bytes and cannot honour case- or accent-insensitive
 * collations, so a collated operand must fall back to Spark.
 */
object CometContains
    extends CometScalarFunction[Contains]("contains")
    with CollationAwareBinaryPredicate[Contains]

object CometStartsWith
    extends CometScalarFunction[StartsWith]("starts_with")
    with CollationAwareBinaryPredicate[StartsWith]

object CometEndsWith
    extends CometScalarFunction[EndsWith]("ends_with")
    with CollationAwareBinaryPredicate[EndsWith]

/**
 * `rlike` runs Spark's own implementation through the codegen dispatcher by default, for
 * byte-exact results. The native (rust) regexp engine is faster but has different semantics from
 * Java regexp, so it is opt-in via `spark.comet.expression.RLike.allowIncompatible`; any case it
 * does not cover (a non-scalar pattern) falls through to the codegen dispatcher via
 * [[CometScalaUDF]].
 */
object CometRLike extends CometExpressionSerde[RLike] with NativeOptInAvailable {

  override def getIncompatibleReasons(): Seq[String] =
    Seq("Uses Rust regexp engine, which has different behavior to Java regexp engine")

  private def nativeApplicable(expr: RLike): Boolean = expr.right match {
    case Literal(_, DataTypes.StringType) => true
    case _ => false
  }

  override def getSupportLevel(expr: RLike): SupportLevel =
    if (!CometConf.isExprAllowIncompat(getExprConfigName(expr)) && nativeApplicable(expr)) {
      Compatible(nativeOptIn =
        Some(NativeOptIn(CometConf.getExprAllowIncompatConfigKey(getExprConfigName(expr)))))
    } else {
      Compatible()
    }

  override def convert(expr: RLike, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    if (CometConf.isExprAllowIncompat(getExprConfigName(expr)) && nativeApplicable(expr)) {
      // Native path: the Rust regexp engine has different semantics from Java regexp.
      return createBinaryExpr(
        expr,
        expr.left,
        expr.right,
        inputs,
        binding,
        (builder, binaryExpr) => builder.setRlike(binaryExpr))
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
      optExpr
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
      optExpr
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
object CometRegExpReplace extends CometExpressionSerde[RegExpReplace] with NativeOptInAvailable {

  override def getIncompatibleReasons(): Seq[String] =
    Seq("Regexp pattern may not be compatible with Spark")

  private def nativeSupported(expr: RegExpReplace): Boolean = expr.pos match {
    case Literal(value, DataTypes.IntegerType) if value == 1 => true
    case _ => false
  }

  override def getSupportLevel(expr: RegExpReplace): SupportLevel =
    if (!CometConf.isExprAllowIncompat(getExprConfigName(expr)) && nativeSupported(expr)) {
      Compatible(nativeOptIn =
        Some(NativeOptIn(CometConf.getExprAllowIncompatConfigKey(getExprConfigName(expr)))))
    } else {
      Compatible()
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
      optExpr
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
object CometStringSplit extends CometExpressionSerde[StringSplit] with NativeOptInAvailable {

  override def getIncompatibleReasons(): Seq[String] =
    Seq("Regex engine differences between Java and Rust")

  override def getSupportLevel(expr: StringSplit): SupportLevel =
    if (!CometConf.isExprAllowIncompat(getExprConfigName(expr))) {
      Compatible(nativeOptIn =
        Some(NativeOptIn(CometConf.getExprAllowIncompatConfigKey(getExprConfigName(expr)))))
    } else {
      Compatible()
    }

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
      optExpr
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
object CometGetJsonObject extends CometCodegenDispatch[GetJsonObject] with NativeOptInAvailable {

  override def getIncompatibleReasons(): Seq[String] =
    Seq(
      "Spark allows single-quoted JSON and unescaped control characters" +
        " which Comet does not support",
      "For JSON objects containing duplicate keys, Spark returns the value of the first" +
        " occurrence while Comet's native implementation returns the last occurrence" +
        " ([#4947](https://github.com/apache/datafusion-comet/issues/4947))")

  override def getSupportLevel(expr: GetJsonObject): SupportLevel =
    if (!CometConf.isExprAllowIncompat(getExprConfigName(expr))) {
      Compatible(nativeOptIn =
        Some(NativeOptIn(CometConf.getExprAllowIncompatConfigKey(getExprConfigName(expr)))))
    } else {
      Compatible()
    }

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
      optExpr
    } else {
      super.convert(expr, inputs, binding)
    }
}

// Expressions routed through the JVM codegen dispatcher: no native implementation, so Spark's own
// doGenCode runs inside the Comet pipeline, matching Spark exactly.
object CometElt extends CometCodegenDispatch[Elt]

object CometFindInSet extends CometCodegenDispatch[FindInSet]

object CometFormatNumber extends CometCodegenDispatch[FormatNumber]

object CometFormatString extends CometCodegenDispatch[FormatString]

object CometOverlay extends CometCodegenDispatch[Overlay]

object CometSoundEx extends CometScalarFunction[SoundEx]("soundex")

object CometStringLocate extends CometCodegenDispatch[StringLocate]

// On Spark 3.4 `Base64` is a plain expression node and always chunks the output (it uses
// `java.util.Base64.getMimeEncoder()` with no arguments). On Spark 3.5+ it is RuntimeReplaceable
// and lowers to a `StaticInvoke`, handled by CometBase64StaticInvoke instead.
object CometBase64 extends CometExpressionSerde[Base64] {
  override def convert(expr: Base64, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val chunkExpr = exprToProtoInternal(Literal(true), inputs, binding)
    val optExpr =
      scalarFunctionExprToProtoWithReturnType(
        "base64",
        StringType,
        failOnError = false,
        childExpr,
        chunkExpr)
    optExpr
  }
}

object CometUnBase64 extends CometCodegenDispatch[UnBase64]

object CometToCharacter extends CometCodegenDispatch[ToCharacter]

object CometToNumber extends CometCodegenDispatch[ToNumber]

object CometTryToNumber extends CometCodegenDispatch[TryToNumber]

object CometMask extends CometCodegenDispatch[Mask]

// A internal function that converts the empty string to null for partition values.
// This function should be only used in V1Writes.
object CometEmpty2Null extends CometCodegenDispatch[Empty2Null]
