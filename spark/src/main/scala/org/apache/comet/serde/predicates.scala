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

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BinaryExpression, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, InSet, IsNaN, IsNotNull, IsNull, KnownFloatingPointNormalized, LessThan, LessThanOrEqual, Literal, Not, Or}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark40Plus}
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde._

object CometNot extends CometExpressionSerde[Not] {
  override def convert(
      expr: Not,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {

    // The fused fast paths below build a single native node from `Not` and its child, bypassing
    // `exprToProtoInternal`, and with it the child serde's `getSupportLevel` gate. Consult that
    // gate here so the fast path is only taken when the child has a native path, rather than
    // re-checking the child's conditions by hand.
    def hasNativePath[T <: Expression](serde: CometExpressionSerde[T], child: T): Boolean =
      serde.getSupportLevel(child).isInstanceOf[Compatible]

    expr.child match {
      case inner: EqualTo if hasNativePath(CometEqualTo, inner) =>
        createBinaryExpr(
          inner,
          inner.left,
          inner.right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setNeq(binaryExpr))
      case inner: EqualNullSafe if hasNativePath(CometEqualNullSafe, inner) =>
        createBinaryExpr(
          inner,
          inner.left,
          inner.right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setNeqNullSafe(binaryExpr))
      case inner: In if hasNativePath(CometIn, inner) =>
        ComparisonUtils.in(inner, inner.value, inner.list, inputs, binding, negate = true)
      case _ =>
        // Includes the cases the child serdes above declare as having no native path, such as
        // non-UTF8_BINARY collated operands and the legacy `null IN ()` behavior: fall through so
        // the child expression's own serde is consulted. `exprToProtoInternal` then either routes
        // the child through the JVM codegen dispatcher (Spark's own `doGenCode`), keeping this Not
        // native, or returns None, which cascades this Not to None and falls the enclosing
        // operator back to Spark.
        createUnaryExpr(
          expr,
          expr.child,
          inputs,
          binding,
          (builder, unaryExpr) => builder.setNot(unaryExpr))
    }
  }
}

object CometAnd extends CometExpressionSerde[And] {
  override def convert(
      expr: And,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // Rebalance the (associative) AND chain so deep `a AND b AND ...` predicates produce a
    // shallow proto instead of a left-deep one that overflows protobuf's recursion limit when
    // the plan is re-parsed (see createBalancedBinaryExpr).
    val operands = flattenAssociative(
      expr,
      { case _: And => true; case _ => false },
      { case a: And => (a.left, a.right) })
    createBalancedBinaryExpr(
      expr,
      operands,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setAnd(binaryExpr))
  }
}

object CometOr extends CometExpressionSerde[Or] {
  override def convert(
      expr: Or,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val operands = flattenAssociative(
      expr,
      { case _: Or => true; case _ => false },
      { case o: Or => (o.left, o.right) })
    createBalancedBinaryExpr(
      expr,
      operands,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setOr(binaryExpr))
  }
}

object CometEqualTo extends CollationAwareBinaryPredicate[EqualTo] {
  override def convert(
      expr: EqualTo,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setEq(binaryExpr))
  }
}

object CometEqualNullSafe extends CollationAwareBinaryPredicate[EqualNullSafe] {
  override def convert(
      expr: EqualNullSafe,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setEqNullSafe(binaryExpr))
  }
}

object CometGreaterThan extends CollationAwareBinaryPredicate[GreaterThan] {
  override def convert(
      expr: GreaterThan,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setGt(binaryExpr))
  }
}

object CometGreaterThanOrEqual extends CollationAwareBinaryPredicate[GreaterThanOrEqual] {
  override def convert(
      expr: GreaterThanOrEqual,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setGtEq(binaryExpr))
  }
}

object CometLessThan extends CollationAwareBinaryPredicate[LessThan] {
  override def convert(
      expr: LessThan,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setLt(binaryExpr))
  }
}

object CometLessThanOrEqual extends CollationAwareBinaryPredicate[LessThanOrEqual] {
  override def convert(
      expr: LessThanOrEqual,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setLtEq(binaryExpr))
  }
}

object CometIsNull extends CometExpressionSerde[IsNull] {
  override def convert(
      expr: IsNull,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createUnaryExpr(
      expr,
      expr.child,
      inputs,
      binding,
      (builder, unaryExpr) => builder.setIsNull(unaryExpr))
  }
}

object CometIsNotNull extends CometExpressionSerde[IsNotNull] {
  override def convert(
      expr: IsNotNull,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createUnaryExpr(
      expr,
      expr.child,
      inputs,
      binding,
      (builder, unaryExpr) => builder.setIsNotNull(unaryExpr))
  }
}

object CometIsNaN extends CometExpressionSerde[IsNaN] {
  override def convert(
      expr: IsNaN,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val optExpr = scalarFunctionExprToProtoWithReturnType("isnan", BooleanType, false, childExpr)

    optExpr
  }
}

object CometIn extends CometExpressionSerde[In] with CodegenDispatchFallback {

  override def getSupportLevel(expr: In): SupportLevel =
    ComparisonUtils.inSupportLevel("In", expr.list, (expr.value +: expr.list): _*)

  override def getUnsupportedReasons(): Seq[String] = ComparisonUtils.inUnsupportedReasons

  override def convert(
      expr: In,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    ComparisonUtils.in(expr, expr.value, expr.list, inputs, binding, negate = false)
  }
}

object CometInSet extends CometExpressionSerde[InSet] with CodegenDispatchFallback {

  override def getSupportLevel(expr: InSet): SupportLevel =
    ComparisonUtils.inSupportLevel("InSet", expr.hset, expr.child)

  override def getUnsupportedReasons(): Seq[String] = ComparisonUtils.inUnsupportedReasons

  override def convert(
      expr: InSet,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val valueDataType = expr.child.dataType
    val list = expr.hset.map { setVal =>
      Literal(setVal, valueDataType)
    }.toSeq
    // Change `InSet` to `In` expression
    // We do Spark `InSet` optimization in native (DataFusion) side.
    ComparisonUtils.in(expr, expr.child, list, inputs, binding, negate = false)
  }
}

/**
 * Mixin for serdes of binary predicates whose native kernel compares raw bytes: any operand
 * carrying a non-UTF8_BINARY collation is unsupported natively. Mixing in
 * `CodegenDispatchFallback` routes these through the JVM codegen dispatcher (Spark's own
 * `doGenCode`), so the collation-aware evaluation runs inline in the Comet pipeline instead of
 * falling the whole operator back to Spark.
 */
trait CollationAwareBinaryPredicate[T <: BinaryExpression]
    extends CometExpressionSerde[T]
    with CodegenDispatchFallback {
  override def getSupportLevel(expr: T): SupportLevel =
    ComparisonUtils.collationSupportLevel(expr.prettyName, expr.left, expr.right)

  override def getUnsupportedReasons(): Seq[String] =
    Seq(ComparisonUtils.nonDefaultCollationDocReason)
}

object ComparisonUtils {

  // Comet's native equality/ordering/hashing compare raw bytes, so any predicate operand carrying
  // a non-UTF8_BINARY collation (Spark 4+) would produce wrong answers on the native path — e.g.
  // `'a' = 'A'` under `UNICODE_CI` returns true in Spark but false byte-wise. Every binary
  // comparison and `In`/`InSet` serde routes its `getSupportLevel` through here, marking the case
  // `Unsupported`; `CodegenDispatchFallback` then routes those cases through Spark's own
  // `doGenCode` inside the Comet pipeline. `hasNonDefaultStringCollation` walks nested types too,
  // so collated strings inside array/map/struct operands are also caught.
  def nonDefaultCollationReason(exprName: String): String =
    s"$exprName does not support non-UTF8_BINARY collated operands; " +
      "native comparison is byte-wise and cannot honour collation semantics."

  // Doc-friendly variant of `nonDefaultCollationReason` for `getUnsupportedReasons`. The compat
  // guide already scopes the reason to a specific expression, so no per-expr name is needed.
  val nonDefaultCollationDocReason: String =
    "Non-UTF8_BINARY collated operands are routed through the JVM codegen dispatcher " +
      "(Spark's own `doGenCode`) because native comparison is byte-wise."

  def hasCollatedOperand(operands: Expression*): Boolean =
    operands.exists(op => hasNonDefaultStringCollation(op.dataType))

  def collationSupportLevel(exprName: String, operands: Expression*): SupportLevel =
    if (hasCollatedOperand(operands: _*)) {
      Unsupported(Some(nonDefaultCollationReason(exprName)))
    } else {
      Compatible()
    }

  // Spark's `In` / `InSet` return `false` for any operand against an empty list, except under the
  // legacy `null IN ()` behavior, where a `NULL` operand returns `NULL` (SPARK-44550). Comet's
  // native `in` kernel only implements the non-legacy behavior, so the legacy case is marked
  // `Unsupported` and, because both serdes mix in `CodegenDispatchFallback`, routed through the
  // JVM codegen dispatcher (Spark's own `doGenCode` inside the Comet pipeline).
  private val legacyNullInEmptyListConfig = "spark.sql.legacy.nullInEmptyListBehavior"

  private val legacyNullInEmptyListReason: String =
    "An empty `IN` list has no native path when Spark's legacy `null IN ()` behavior is in " +
      s"effect (`$legacyNullInEmptyListConfig`), because a `NULL` operand then evaluates to " +
      "`NULL` rather than `false`."

  // Spark 3.4 has no config and always uses the legacy behavior, Spark 3.5 defaults the config to
  // true, and Spark 4.0+ makes it optional with a default of `!spark.sql.ansi.enabled`. Read by
  // string key so this compiles against every supported Spark version.
  private def legacyNullInEmptyListBehavior: Boolean =
    !isSpark35Plus || {
      val default = !isSpark40Plus || !SQLConf.get.ansiEnabled
      CometConf.getBooleanConf(legacyNullInEmptyListConfig, default, SQLConf.get)
    }

  /**
   * Support level shared by the `In` and `InSet` serdes: `list` is the set of values being tested
   * against, and `operands` are the expressions whose collation must be checked.
   */
  def inSupportLevel(exprName: String, list: Iterable[_], operands: Expression*): SupportLevel =
    if (list.isEmpty && legacyNullInEmptyListBehavior) {
      Unsupported(Some(legacyNullInEmptyListReason))
    } else {
      collationSupportLevel(exprName, operands: _*)
    }

  val inUnsupportedReasons: Seq[String] =
    Seq(nonDefaultCollationDocReason, legacyNullInEmptyListReason)

  /**
   * Normalize one top-level floating-point membership operand to Spark's NaN and signed-zero
   * equality. Literal normalization is folded immediately so it remains a scalar literal; a
   * dynamic expression gets Spark's normalization wrapper; non-floating operands are unchanged.
   *
   * @param expr
   *   The value or candidate operand being serialized.
   * @return
   *   The operand to serialize, possibly folded or wrapped for Spark-compatible equality.
   */
  private def normalizeInOperand(expr: Expression): Expression = expr.dataType match {
    case FloatType | DoubleType =>
      expr match {
        case _: KnownFloatingPointNormalized => expr
        // DataFusion's static IN filter hashes raw floating-point bits. Fold literal
        // normalization here so the list remains scalar and can still use that filter.
        case literal: Literal =>
          Literal(NormalizeNaNAndZero(literal).eval(), literal.dataType)
        case _ => KnownFloatingPointNormalized(NormalizeNaNAndZero(expr))
      }
    case _ => expr
  }

  /**
   * Keep an all-literal floating-point membership list on DataFusion's static-filter path while
   * matching Spark's signed-zero equality. DataFusion hashes the raw floating-point bits for a
   * static list, so a list containing one zero sign also needs the other sign. A NaN literal or a
   * dynamic candidate cannot be made equivalent by enumeration and must use normalization
   * instead.
   *
   * @param list
   *   The original Spark `IN` / `InSet` candidates.
   * @return
   *   `Some` with a static candidate list, including any missing opposite-signed zeros, or `None`
   *   when the caller must normalize the membership operands.
   */
  private def staticFloatingInList(list: Seq[Expression]): Option[Seq[Expression]] = {
    val canStayStatic = list.forall {
      case Literal(null, _) => true
      case Literal(v: Float, FloatType) => !java.lang.Float.isNaN(v)
      case Literal(v: Double, DoubleType) => !java.lang.Double.isNaN(v)
      case _ => false
    }
    if (!canStayStatic) {
      None
    } else {
      val hasPositiveFloatZero = list.exists {
        case Literal(v: Float, FloatType) =>
          java.lang.Float.floatToRawIntBits(v) == java.lang.Float.floatToRawIntBits(0.0f)
        case _ => false
      }
      val hasNegativeFloatZero = list.exists {
        case Literal(v: Float, FloatType) =>
          java.lang.Float.floatToRawIntBits(v) == java.lang.Float.floatToRawIntBits(-0.0f)
        case _ => false
      }
      val hasPositiveDoubleZero = list.exists {
        case Literal(v: Double, DoubleType) =>
          java.lang.Double.doubleToRawLongBits(v) == java.lang.Double.doubleToRawLongBits(0.0d)
        case _ => false
      }
      val hasNegativeDoubleZero = list.exists {
        case Literal(v: Double, DoubleType) =>
          java.lang.Double.doubleToRawLongBits(v) == java.lang.Double.doubleToRawLongBits(-0.0d)
        case _ => false
      }
      val missingZeroSigns = Seq(
        if (hasPositiveFloatZero && !hasNegativeFloatZero) Some(Literal(-0.0f)) else None,
        if (hasNegativeFloatZero && !hasPositiveFloatZero) Some(Literal(0.0f)) else None,
        if (hasPositiveDoubleZero && !hasNegativeDoubleZero) Some(Literal(-0.0d)) else None,
        if (hasNegativeDoubleZero && !hasPositiveDoubleZero) Some(Literal(0.0d))
        else None).flatten
      Some(list ++ missingZeroSigns)
    }
  }

  /**
   * Serialize Spark membership while preserving Spark-compatible top-level floating-point
   * equality and DataFusion's static-filter pruning whenever that is safe.
   *
   * @param expr
   *   The original membership expression, used as the fallback-reason owner.
   * @param value
   *   The value being tested for membership.
   * @param list
   *   The membership candidates.
   * @param inputs
   *   The attributes available for bound-expression serialization.
   * @param binding
   *   Whether attributes should be bound to input ordinals.
   * @param negate
   *   Whether to serialize `NOT IN` rather than `IN`.
   * @return
   *   The native membership protobuf, or `None` after copying any synthesized-expression fallback
   *   reasons back to `expr`.
   */
  def in(
      expr: Expression,
      value: Expression,
      list: Seq[Expression],
      inputs: Seq[Attribute],
      binding: Boolean,
      negate: Boolean): Option[Expr] = {
    val serializedOperands: (Expression, Seq[Expression]) = value.dataType match {
      case FloatType | DoubleType =>
        staticFloatingInList(list) match {
          // All-literal non-NaN lists stay static so native Parquet scans can still prune using
          // column statistics. Enumerating both zero signs makes raw-bit membership match Spark.
          case Some(staticList) => value -> staticList
          // NaN literals and dynamic candidates need both sides normalized, including fused NOT IN.
          case None => normalizeInOperand(value) -> list.map(normalizeInOperand)
        }
      // Nested floating-point leaves are intentionally outside this scalar normalization path.
      case _ => value -> list
    }
    val (serializedValue, serializedList) = serializedOperands
    val valueExpr = exprToProtoInternal(serializedValue, inputs, binding)
    val listExprs = serializedList.map(exprToProtoInternal(_, inputs, binding))
    if (valueExpr.isDefined && listExprs.forall(_.isDefined)) {
      val builder = ExprOuterClass.In.newBuilder()
      builder.setInValue(valueExpr.get)
      builder.addAllLists(listExprs.map(_.get).asJava)
      builder.setNegated(negate)
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setIn(builder)
          .build())
    } else {
      // Normalization and static-list expansion create temporary wrappers and literals outside the
      // original tree. Keep their failure reasons on the membership expression so the operator can
      // explain fallback.
      liftFallbackReasons(serializedValue, expr)
      serializedList.foreach(liftFallbackReasons(_, expr))
      None
    }
  }
}
