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

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{And, ArrayAggregate, ArrayAppend, ArrayContains, ArrayExcept, ArrayExists, ArrayFilter, ArrayForAll, ArrayInsert, ArrayIntersect, ArrayJoin, ArrayMax, ArrayMin, ArrayPosition, ArrayRemove, ArraySort, ArraysOverlap, ArraysZip, ArrayTransform, ArrayUnion, Attribute, BoundReference, Cast, CreateArray, ElementAt, EmptyRow, Expression, Flatten, GetArrayItem, IsNotNull, IsNull, LambdaFunction, Literal, NamedLambdaVariable, Reverse, Sequence, Size, Slice, SortArray, ZipWith}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.DataTypeSupport.{deepNullable, isComplexType}
import org.apache.comet.serde.QueryPlanSerde._
import org.apache.comet.shims.{CometExprShim, CometTypeShim}

object CometArrayRemove
    extends CometExpressionSerde[ArrayRemove]
    with CometExprShim
    with ArraysBase {

  override def getSupportLevel(expr: ArrayRemove): SupportLevel = childTypesSupportLevel(expr)

  override def convert(
      expr: ArrayRemove,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val arrayExprProto = exprToProto(expr.left, inputs, binding)
    val keyExprProto = exprToProto(expr.right, inputs, binding)

    scalarFunctionExprToProto("array_remove_all", arrayExprProto, keyExprProto)
  }
}

object CometArrayAppend extends CometExpressionSerde[ArrayAppend] {

  override def convert(
      expr: ArrayAppend,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val child = expr.children.head
    val elementType = child.dataType.asInstanceOf[ArrayType].elementType

    val arrayExprProto = exprToProto(expr.children.head, inputs, binding)
    val keyExprProto = exprToProto(expr.children(1), inputs, binding)

    // DataFusion's array_append always returns a list with nullable elements,
    // so we must promise ArrayType(elementType, containsNull = true) here even if
    // Spark's expr.dataType has containsNull = false (e.g. for array(1,2,3)).
    val arrayAppendScalarExpr =
      scalarFunctionExprToProtoWithReturnType(
        "array_append",
        ArrayType(elementType, containsNull = true),
        false,
        arrayExprProto,
        keyExprProto)

    val isNotNullExpr = createUnaryExpr(
      expr,
      expr.children.head,
      inputs,
      binding,
      (builder, unaryExpr) => builder.setIsNotNull(unaryExpr))

    val nullLiteralProto = exprToProto(Literal(null, elementType), Seq.empty)

    if (arrayAppendScalarExpr.isDefined && isNotNullExpr.isDefined && nullLiteralProto.isDefined) {
      val caseWhenExpr = ExprOuterClass.CaseWhen
        .newBuilder()
        .addWhen(isNotNullExpr.get)
        .addThen(arrayAppendScalarExpr.get)
        .setElseExpr(nullLiteralProto.get)
        .build()
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setCaseWhen(caseWhenExpr)
          .build())
    } else {
      None
    }
  }
}

object CometArrayContains
    extends CometExpressionSerde[ArrayContains]
    with CodegenDispatchFallback {

  private val floatingPointReason: String =
    "Spark compares array elements with ordering.equiv, so -0.0 matches +0.0 and all NaNs match " +
      "each other; Comet's native array_contains compares the raw Arrow values bitwise"

  override def getIncompatibleReasons(): Seq[String] = Seq(floatingPointReason)

  override def getSupportLevel(expr: ArrayContains): SupportLevel = expr.left.dataType match {
    // Native array_contains compares floating-point elements bitwise, disagreeing with Spark for
    // -0.0/+0.0 and NaN. Report Incompatible (not Unsupported) for float/double element types (at
    // any nesting level) so the expression routes through the JVM codegen dispatcher (Spark's own
    // doGenCode) and stays native + Spark-exact under the default config, while non-float arrays
    // keep the fast native kernel. Under allowIncompatible=true the native kernel is used
    // as before.
    case ArrayType(elementType, _)
        if SupportLevel.containsType(elementType, classOf[FloatType], classOf[DoubleType]) =>
      Incompatible(Some(floatingPointReason))
    case _ => Compatible()
  }

  override def convert(
      expr: ArrayContains,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val arrayExprProto = exprToProto(expr.children.head, inputs, binding)
    val keyExprProto = exprToProto(expr.children(1), inputs, binding)

    scalarFunctionExprToProto("array_contains", arrayExprProto, keyExprProto)
  }
}

object CometSortArray extends CometExpressionSerde[SortArray] with CodegenDispatchFallback {

  override def getIncompatibleReasons(): Seq[String] = Seq(
    "When `" + CometConf.COMET_EXEC_STRICT_FLOATING_POINT.key + "=true`, sorting on" +
      " floating-point types is not 100% compatible with Spark")

  private def supportedSortArrayElementType(dt: DataType): Boolean = {
    dt match {
      case _: NullType =>
        true
      case ArrayType(elementType, _) =>
        supportedSortArrayElementType(elementType)
      case StructType(fields) =>
        fields.forall(f => supportedSortArrayElementType(f.dataType))
      case _ =>
        supportedScalarSortElementType(dt)
    }
  }

  override def getSupportLevel(expr: SortArray): SupportLevel = {
    val elementType = expr.base.dataType.asInstanceOf[ArrayType].elementType

    if (!supportedSortArrayElementType(elementType)) {
      Unsupported(Some(s"Sort on array element type $elementType is not supported"))
    } else {
      SupportLevel
        .strictFloatingPointReason(elementType, "Sorting on floating-point")
        .map(reason => Incompatible(Some(reason)))
        .getOrElse(expr.ascendingOrder match {
          // Spark 3.x requires a boolean Literal; Spark 4.0+ widens ascendingOrder to any
          // foldable boolean. Accept both; convert evaluates the foldable expression.
          case ao if ao.foldable && ao.dataType == BooleanType => Compatible()
          case other =>
            Unsupported(Some(s"ascendingOrder must be a foldable boolean: $other"))
        })
    }
  }

  override def convert(
      expr: SortArray,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val arrayExprProto = exprToProtoInternal(expr.base, inputs, binding)
    // ascendingOrder is a foldable boolean (gated in getSupportLevel). Evaluate it; a null result
    // unboxes to false, matching Spark's `right.eval().asInstanceOf[Boolean]`.
    val ascending = expr.ascendingOrder.eval(EmptyRow).asInstanceOf[Boolean]
    val direction = if (ascending) "ASC" else "DESC"
    val nullOrdering = if (ascending) "NULLS FIRST" else "NULLS LAST"
    val sortDirectionExprProto = exprToProtoInternal(Literal(direction), inputs, binding)
    val nullOrderingExprProto = exprToProtoInternal(Literal(nullOrdering), inputs, binding)

    val sortArrayScalarExpr =
      scalarFunctionExprToProto(
        "array_sort",
        arrayExprProto,
        sortDirectionExprProto,
        nullOrderingExprProto)
    sortArrayScalarExpr
  }
}

object CometArrayIntersect
    extends CometExpressionSerde[ArrayIntersect]
    with CometTypeShim
    with CodegenDispatchFallback {

  private val incompatReason: String =
    "Result array element order may differ from Spark when the right array is longer " +
      "than the left (DataFusion probes the longer side)."

  private val collationReason: String =
    "array_intersect does not propagate non-UTF8_BINARY collations to the output array elements " +
      "(https://github.com/apache/datafusion-comet/issues/2190)"

  override def getIncompatibleReasons(): Seq[String] = Seq(incompatReason, collationReason)

  override def getSupportLevel(expr: ArrayIntersect): SupportLevel = {
    // The native array_intersect dedups by raw bytes, which is wrong under non-default collations,
    // so report Incompatible rather than Unsupported: the JVM codegen dispatcher (Spark's own
    // doGenCode) performs collation-aware set membership and keeps execution native, matching
    // Spark. Only the output elements' collation metadata is dropped, consistent with CometReverse
    // and CometArrayJoin.
    if (hasNonDefaultStringCollation(expr.dataType)) {
      Incompatible(Some(collationReason))
    } else {
      Incompatible(Some(incompatReason))
    }
  }

  override def convert(
      expr: ArrayIntersect,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val leftArrayExprProto = exprToProto(expr.children.head, inputs, binding)
    val rightArrayExprProto = exprToProto(expr.children(1), inputs, binding)

    val arraysIntersectScalarExpr =
      scalarFunctionExprToProto("array_intersect", leftArrayExprProto, rightArrayExprProto)
    arraysIntersectScalarExpr
  }
}

object CometArrayMax extends CometExpressionSerde[ArrayMax] {
  override def convert(
      expr: ArrayMax,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val arrayExprProto = exprToProto(expr.children.head, inputs, binding)

    val arrayMaxScalarExpr =
      scalarFunctionExprToProto("array_max", arrayExprProto)
    arrayMaxScalarExpr
  }
}

object CometArrayMin extends CometExpressionSerde[ArrayMin] {
  override def convert(
      expr: ArrayMin,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val arrayExprProto = exprToProto(expr.children.head, inputs, binding)

    val arrayMinScalarExpr = scalarFunctionExprToProto("array_min", arrayExprProto)
    arrayMinScalarExpr
  }
}

object CometArraysOverlap extends CometExpressionSerde[ArraysOverlap] with ArraysBase {

  override def getIncompatibleReasons(): Seq[String] = Seq(nestedFloatIncompatibilityReason)

  override def getSupportLevel(expr: ArraysOverlap): SupportLevel = {
    if (hasNestedFloatElements(expr.left.dataType)) {
      Incompatible(Some(nestedFloatIncompatibilityReason))
    } else {
      Compatible()
    }
  }

  override def convert(
      expr: ArraysOverlap,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val leftArrayExprProto = exprToProto(expr.left, inputs, binding)
    val rightArrayExprProto = exprToProto(expr.right, inputs, binding)

    val arraysOverlapScalarExpr = scalarFunctionExprToProtoWithReturnType(
      "spark_arrays_overlap",
      BooleanType,
      false,
      leftArrayExprProto,
      rightArrayExprProto)
    arraysOverlapScalarExpr
  }
}

object CometArrayCompact extends CometExpressionSerde[Expression] {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val child = expr.children.head
    val arrayExprProto = exprToProto(child, inputs, binding)

    val arrayCompactScalarExpr = scalarFunctionExprToProto("array_compact", arrayExprProto)
    arrayCompactScalarExpr
  }
}

object CometArrayExcept
    extends CometExpressionSerde[ArrayExcept]
    with CometExprShim
    with CodegenDispatchFallback {

  private val incompatReason = "Null handling and ordering may differ from Spark"

  override def getIncompatibleReasons(): Seq[String] = Seq(incompatReason)

  override def getSupportLevel(expr: ArrayExcept): SupportLevel = {
    // Surface the native element-type restriction in EXPLAIN. We report Incompatible (not
    // Unsupported) for these types so the JVM codegen dispatcher still evaluates them natively
    // under the default config; the convert-time guard below is only reached under
    // allowIncompatible=true, where the native array_except cannot handle them.
    val reason = expr.children.map(_.dataType).find(dt => !isTypeSupported(dt)) match {
      case Some(dt) => s"native array_except does not support element type $dt: $incompatReason"
      case None => incompatReason
    }
    Incompatible(Some(reason))
  }

  @tailrec
  def isTypeSupported(dt: DataType): Boolean = {
    import DataTypes._
    dt match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
          _: DecimalType | DateType | TimestampType | TimestampNTZType | StringType =>
        true
      case BinaryType => false
      case ArrayType(elementType, _) => isTypeSupported(elementType)
      case _: StructType =>
        false
      case _ => false
    }
  }

  override def convert(
      expr: ArrayExcept,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // Defensive: only reached under allowIncompatible=true (the default-config Incompatible path
    // routes through the codegen dispatcher before convert). Native array_except cannot handle
    // these element types, so decline and let Spark evaluate.
    expr.children.map(_.dataType).find(dt => !isTypeSupported(dt)) match {
      case Some(dt) =>
        withFallbackReason(expr, s"data type not supported: $dt")
        return None
      case None =>
    }
    val leftArrayExprProto = exprToProto(expr.left, inputs, binding)
    val rightArrayExprProto = exprToProto(expr.right, inputs, binding)

    val arrayExceptScalarExpr =
      scalarFunctionExprToProto("array_except", leftArrayExprProto, rightArrayExprProto)
    arrayExceptScalarExpr
  }
}

object CometArrayJoin
    extends CometExpressionSerde[ArrayJoin]
    with CometTypeShim
    with CodegenDispatchFallback {

  private val collationReason =
    "array_join does not propagate non-UTF8_BINARY collations to the output string " +
      "(https://github.com/apache/datafusion-comet/issues/2190)"

  private val eagerEvalReason =
    "array_join evaluates its delimiter and null replacement eagerly, while Spark short-circuits " +
      "past them (https://github.com/apache/datafusion-comet/issues/3178)"

  /**
   * Whether evaluating `expr` earlier than Spark would is unobservable.
   *
   * Spark skips ArrayJoin's later arguments once an earlier one is null, and `eval` and
   * `doGenCode` disagree on that order, while DataFusion evaluates every argument up front. A
   * literal or column read cannot throw, carry state or have a side effect, so ordering cannot be
   * observed for it; anything else goes to the codegen dispatcher. `foldable` is not usable here:
   * ConstantFolding leaves a throwing foldable expression unfolded in a conditional branch.
   */
  private def orderInsensitive(expr: Expression): Boolean = expr match {
    case _: Literal | _: Attribute | _: BoundReference => true
    case _ => false
  }

  override def getIncompatibleReasons(): Seq[String] = Seq(collationReason, eagerEvalReason)

  override def getSupportLevel(expr: ArrayJoin): SupportLevel = {
    // Spark 4.0 widens ArrayJoin's input to StringTypeWithCollation. Concatenation itself is
    // collation-independent, so the joined value is always correct; only the output string's
    // collation metadata is dropped (Comet columns are UTF8_BINARY). Report Incompatible rather
    // than Unsupported so the JVM codegen dispatcher (Spark's own doGenCode) keeps collated
    // array_join native and matching Spark, consistent with CometReverse's #2190 handling.
    if (hasNonDefaultStringCollation(expr.array.dataType)) {
      Incompatible(Some(collationReason))
    } else if (!(expr.delimiter +: expr.nullReplacement.toSeq).forall(orderInsensitive)) {
      Incompatible(Some(eagerEvalReason))
    } else {
      Compatible()
    }
  }

  override def convert(
      expr: ArrayJoin,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val arrayExprProto = exprToProto(expr.array, inputs, binding)
    val delimiterExprProto = exprToProto(expr.delimiter, inputs, binding)

    val joined = expr.nullReplacement match {
      case Some(nullReplacementExpr) =>
        scalarFunctionExprToProto(
          "array_to_string",
          arrayExprProto,
          delimiterExprProto,
          exprToProto(nullReplacementExpr, inputs, binding))
      case None =>
        scalarFunctionExprToProto("array_to_string", arrayExprProto, delimiterExprProto)
    }

    // Spark returns null as soon as nullReplacement is null, whatever the array holds, while
    // array_to_string reads a null null_string as "omit nulls" (#3178).
    expr.nullReplacement.filter(_.nullable) match {
      case Some(nullReplacementExpr) =>
        for {
          innerProto <- joined
          replacementIsNull <- exprToProto(IsNull(nullReplacementExpr), inputs, binding)
          nullLiteral <- exprToProto(Literal(null, expr.dataType), inputs, binding)
        } yield ExprOuterClass.Expr
          .newBuilder()
          .setIf(
            ExprOuterClass.IfExpr
              .newBuilder()
              .setIfExpr(replacementIsNull)
              .setTrueExpr(nullLiteral)
              .setFalseExpr(innerProto))
          .build()
      case None => joined
    }
  }
}

object CometArrayInsert extends CometExpressionSerde[ArrayInsert] with ArraysBase {

  override def getSupportLevel(expr: ArrayInsert): SupportLevel = Compatible()

  override def convert(
      expr: ArrayInsert,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val srcArray = expr.children.head
    val item = expr.children(2)
    val srcElementType = srcArray.dataType.asInstanceOf[ArrayType].elementType

    // Native ArrayInsert requires the item's Arrow type to equal the source array's element type
    // exactly, including nested nullability. For complex element types the two sides can disagree:
    // a `CreateArray` source is widened to a deeply-nullable element type (see CometCreateArray),
    // while a standalone item (e.g. `map(2, coalesce(id, 0))`) keeps Spark's Catalyst nullability.
    // Cast BOTH sides in lockstep to the same deeply-nullable element type so their Arrow types
    // match; Spark's `ArrayInsert.dataType` is `first.dataType.asNullable`, so this also
    // matches the declared output element type. Casting only widens metadata and never
    // changes values. Primitive element types are byte-identical on both sides, so the gate
    // leaves them untouched.
    val (srcChild, itemChild) = if (isComplexType(srcElementType)) {
      val elementType = deepNullable(srcElementType)
      val arrayType = ArrayType(elementType, containsNull = true)
      val widenedSrc = if (srcArray.dataType == arrayType) srcArray else Cast(srcArray, arrayType)
      val widenedItem = if (item.dataType == elementType) item else Cast(item, elementType)
      (widenedSrc, widenedItem)
    } else {
      (srcArray, item)
    }

    val srcExprProto = exprToProtoInternal(srcChild, inputs, binding)
    val posExprProto = exprToProtoInternal(expr.children(1), inputs, binding)
    val itemExprProto = exprToProtoInternal(itemChild, inputs, binding)
    val legacyNegativeIndex =
      SQLConf.get.getConfString("spark.sql.legacy.negativeIndexInArrayInsert").toBoolean
    if (srcExprProto.isDefined && posExprProto.isDefined && itemExprProto.isDefined) {
      val arrayInsertBuilder = ExprOuterClass.ArrayInsert
        .newBuilder()
        .setSrcArrayExpr(srcExprProto.get)
        .setPosExpr(posExprProto.get)
        .setItemExpr(itemExprProto.get)
        .setLegacyNegativeIndex(legacyNegativeIndex)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setArrayInsert(arrayInsertBuilder)
          .build())
    } else {
      withFallbackReason(expr, "unsupported arguments for ArrayInsert")
      None
    }
  }
}

object CometSlice extends CometExpressionSerde[Slice] {
  override def convert(
      expr: Slice,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val elementType = expr.x.dataType.asInstanceOf[ArrayType].elementType
    val arrayExprProto = exprToProto(expr.x, inputs, binding)
    val startExprProto = exprToProto(Cast(expr.start, LongType), inputs, binding)
    val lengthExprProto = exprToProto(Cast(expr.length, LongType), inputs, binding)
    // DataFusion list types always have nullable inner elements, so promise
    // ArrayType(elementType, containsNull = true) here even if Spark's
    // expr.dataType reports containsNull = false (e.g. for array(1, 2, 3)).
    val sliceScalarExpr =
      scalarFunctionExprToProtoWithReturnType(
        "spark_array_slice",
        ArrayType(elementType, containsNull = true),
        false,
        arrayExprProto,
        startExprProto,
        lengthExprProto)
    sliceScalarExpr
  }
}

object CometArrayUnion extends CometExpressionSerde[ArrayUnion] {
  override def convert(
      expr: ArrayUnion,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val leftArrayExprProto = exprToProto(expr.children.head, inputs, binding)
    val rightArrayExprProto = exprToProto(expr.children(1), inputs, binding)

    val arraysUnionScalarExpr =
      scalarFunctionExprToProto("array_union", leftArrayExprProto, rightArrayExprProto)
    arraysUnionScalarExpr
  }
}

object CometCreateArray extends CometExpressionSerde[CreateArray] with ArraysBase {
  override def convert(
      expr: CreateArray,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val children = expr.children

    // Handle empty array: return literal directly to avoid DataFusion coerce_types bug
    // when make_array is called with 0 arguments (issue #3338)
    if (children.isEmpty) {
      val emptyArrayLiteral =
        Literal.create(new GenericArrayData(Array.empty[Any]), expr.dataType)
      return exprToProtoInternal(emptyArrayLiteral, inputs, binding)
    }

    // DataFusion's `make_array` asserts strict element-type equality in
    // `MutableArrayData::with_capacities` and panics on a mismatch. Spark's CreateArray is more
    // permissive: its coercion compares element types with `sameType` (nullability ignored), so
    // children that share a surface type but differ in nullability reach here as distinct types.
    // Comet's native runtime types are also frequently MORE nullable than Spark's Catalyst types
    // (`map_entries` forces the entry `value` field nullable, list elements are nullable, ...), so
    // casting to Spark's declared element type does not reliably unify them. Cast every child to a
    // deeply-nullable element type instead (every array/map/struct field nullable at all nesting
    // levels; the cast only widens metadata and never changes values), so `make_array` always sees
    // identical Arrow types. A child whose cast is unsupported declines below.
    val elementType = deepNullable(expr.dataType.asInstanceOf[ArrayType].elementType)
    val childExprs = children.map { c =>
      val unified = if (c.dataType == elementType) c else Cast(c, elementType)
      exprToProtoInternal(unified, inputs, binding)
    }

    if (childExprs.forall(_.isDefined)) {
      scalarFunctionExprToProto("make_array", childExprs: _*)
    } else {
      withFallbackReason(expr, "unsupported arguments for CreateArray")
      None
    }
  }
}

object CometGetArrayItem extends CometExpressionSerde[GetArrayItem] {

  override def convert(
      expr: GetArrayItem,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val ordinalExpr = exprToProtoInternal(expr.ordinal, inputs, binding)

    if (childExpr.isDefined && ordinalExpr.isDefined) {
      val listExtractBuilder = ExprOuterClass.ListExtract
        .newBuilder()
        .setChild(childExpr.get)
        .setOrdinal(ordinalExpr.get)
        .setOneBased(false)
        .setFailOnError(expr.failOnError)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setListExtract(listExtractBuilder)
          .build())
    } else {
      withFallbackReason(expr, "unsupported arguments for GetArrayItem")
      None
    }
  }
}

object CometArrayReverse extends CometExpressionSerde[Reverse] with ArraysBase {
  val unsupportedReason =
    "native reverse does not support arrays whose element type contains binary, struct, or map"

  override def getIncompatibleReasons(): Seq[String] = Seq(unsupportedReason)

  override def getSupportLevel(expr: Reverse): SupportLevel = {
    // Mirror the native impl's element-type support. Report Incompatible (not Unsupported) for
    // element types the native array_reverse cannot handle so the expression routes through the
    // JVM codegen dispatcher (via CometReverse, which mixes in CodegenDispatchFallback) instead
    // of silently falling back to Spark. Previously StructType reported Compatible here while
    // convert rejected it, so such arrays silently fell back.
    if (isTypeSupported(expr.child.dataType)) {
      Compatible(None)
    } else {
      Incompatible(Some(unsupportedReason))
    }
  }

  override def convert(
      expr: Reverse,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // Defensive: only reached under allowIncompatible=true (the default-config Incompatible path
    // routes through the codegen dispatcher before convert). Native array_reverse cannot handle
    // these element types, so decline and let Spark evaluate.
    if (!isTypeSupported(expr.child.dataType)) {
      withFallbackReason(expr, s"child data type not supported: ${expr.child.dataType}")
      return None
    }
    val reverseExprProto = exprToProto(expr.child, inputs, binding)
    val reverseScalarExpr = scalarFunctionExprToProto("array_reverse", reverseExprProto)
    reverseScalarExpr
  }

}

object CometElementAt extends CometExpressionSerde[ElementAt] {

  override def getSupportLevel(expr: ElementAt): SupportLevel = {
    expr.left.dataType match {
      case _: ArrayType => Compatible()
      case MapType(keyType, _, _) => MapKeySupport.keySupport(keyType)
      case _ => Unsupported(Some("Input must be an array or map"))
    }
  }

  override def convert(
      expr: ElementAt,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.left, inputs, binding)
    val ordinalExpr = exprToProtoInternal(expr.right, inputs, binding)

    val baseExpr = expr.left.dataType match {
      case _: MapType =>
        scalarFunctionExprToProto("map_extract", childExpr, ordinalExpr)
      case _ =>
        val defaultExpr =
          expr.defaultValueOutOfBound.flatMap(exprToProtoInternal(_, inputs, binding))

        if (childExpr.isDefined && ordinalExpr.isDefined &&
          defaultExpr.isDefined == expr.defaultValueOutOfBound.isDefined) {
          val arrayExtractBuilder = ExprOuterClass.ListExtract
            .newBuilder()
            .setChild(childExpr.get)
            .setOrdinal(ordinalExpr.get)
            .setOneBased(true)
            .setFailOnError(expr.failOnError)

          defaultExpr.foreach(arrayExtractBuilder.setDefaultValue)

          Some(
            ExprOuterClass.Expr
              .newBuilder()
              .setListExtract(arrayExtractBuilder)
              .build())
        } else {
          withFallbackReason(expr, "unsupported arguments for ElementAt")
          None
        }
    }

    // Spark's ElementAt is a BinaryExpression: for a NULL map/array it returns NULL WITHOUT
    // evaluating the key/index child. Native scalar functions evaluate the key eagerly over the
    // whole batch, so under ANSI (failOnError) a throwing key (e.g. a divide-by-zero) fires even on
    // rows whose map/array is NULL, where Spark short-circuits. When the left can actually be NULL,
    // guard the lookup with CASE WHEN left IS NOT NULL THEN <lookup> ELSE null so the key is only
    // evaluated on the selected rows (DataFusion's CaseExpr filters the batch before the THEN
    // branch), reproducing the short-circuit. Mirrors the CASE-WHEN idiom in CometArrayAppend /
    // CometSize; the ELSE null literal carries the result type, as in CometArraysZip.
    if (expr.failOnError && expr.left.nullable) {
      val isNotNullExpr = createUnaryExpr(
        expr,
        expr.left,
        inputs,
        binding,
        (builder, unaryExpr) => builder.setIsNotNull(unaryExpr))
      val nullLiteralProto = exprToProto(Literal(null, expr.dataType), Seq.empty)
      for {
        base <- baseExpr
        notNull <- isNotNullExpr
        nullLit <- nullLiteralProto
      } yield {
        // The generic serde path attaches this ElementAt's expr_id and QueryContext to the CASE we
        // return here, but the lookup that actually throws under ANSI (ListExtract, for the array
        // case) is nested inside the THEN branch. Attach them to that inner Expr too, so a native
        // INVALID_ARRAY_INDEX_IN_ELEMENT_AT / INVALID_INDEX_OF_ZERO error still renders Spark's
        // `== SQL ... ==` query context. (map_extract ignores expr_id, so the map case is
        // unaffected and never throws an index error anyway.)
        val guardedBase = QueryPlanSerde.attachExprIdAndContext(expr, base)
        val caseWhenExpr = ExprOuterClass.CaseWhen
          .newBuilder()
          .addWhen(notNull)
          .addThen(guardedBase)
          .setElseExpr(nullLit)
          .build()
        ExprOuterClass.Expr
          .newBuilder()
          .setCaseWhen(caseWhenExpr)
          .build()
      }
    } else {
      baseExpr
    }
  }
}

object CometFlatten extends CometExpressionSerde[Flatten] with ArraysBase {

  override def getSupportLevel(expr: Flatten): SupportLevel = childTypesSupportLevel(expr)

  override def convert(
      expr: Flatten,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val flattenExprProto = exprToProto(expr.child, inputs, binding)
    val flattenScalarExpr = scalarFunctionExprToProto("flatten", flattenExprProto)
    flattenScalarExpr
  }
}

object CometArrayFilter extends CometExpressionSerde[ArrayFilter] {

  override def getSupportLevel(expr: ArrayFilter): SupportLevel = Compatible()

  override def convert(
      expr: ArrayFilter,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    expr.function match {
      case LambdaFunction(IsNotNull(v: NamedLambdaVariable), Seq(lambdaVar), _)
          if v.exprId == lambdaVar.exprId =>
        // Fast path: Catalyst desugars `array_compact` to `filter(arr, x -> x IS NOT NULL)`, so
        // restore the native serde here (avoids per-batch JNI). Guard requires the IsNotNull
        // operand to be the lambda variable itself, not a captured column (#4830).
        CometArrayCompact.convert(expr, inputs, binding)
      case _ =>
        // General lambda: run Spark's own evaluation through the codegen dispatcher so the result
        // matches Spark exactly, like the other higher-order functions (`transform`, `exists`).
        // Falls back to Spark when the dispatcher is disabled.
        CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
  }
}

object CometSize extends CometExpressionSerde[Size] {

  override def getSupportLevel(expr: Size): SupportLevel = {
    expr.child.dataType match {
      case _: ArrayType => Compatible()
      case _: MapType => Compatible()
      case other =>
        Unsupported(Some(s"Unsupported child data type: $other"))
    }
  }

  override def convert(
      expr: Size,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val arrayExprProto = exprToProto(expr.child, inputs, binding)
    for {
      isNotNullExprProto <- createIsNotNullExprProto(expr, inputs, binding)
      sizeScalarExprProto <- scalarFunctionExprToProto("size", arrayExprProto)
      emptyLiteralExprProto <- createLiteralExprProto(expr.legacySizeOfNull)
    } yield {
      val caseWhenExpr = ExprOuterClass.CaseWhen
        .newBuilder()
        .addWhen(isNotNullExprProto)
        .addThen(sizeScalarExprProto)
        .setElseExpr(emptyLiteralExprProto)
        .build()
      ExprOuterClass.Expr
        .newBuilder()
        .setCaseWhen(caseWhenExpr)
        .build()
    }
  }

  private def createIsNotNullExprProto(
      expr: Size,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createUnaryExpr(
      expr,
      expr.child,
      inputs,
      binding,
      (builder, unaryExpr) => builder.setIsNotNull(unaryExpr))
  }

  private def createLiteralExprProto(legacySizeOfNull: Boolean): Option[ExprOuterClass.Expr] = {
    val value = if (legacySizeOfNull) -1 else null
    exprToProto(Literal(value, IntegerType), Seq.empty)
  }

}

object CometArrayPosition extends CometExpressionSerde[ArrayPosition] with ArraysBase {

  override def getIncompatibleReasons(): Seq[String] = Seq(nestedFloatIncompatibilityReason)

  override def getSupportLevel(expr: ArrayPosition): SupportLevel = {
    if (expr.children.forall(_.foldable)) {
      // Fall back to Spark for all-literal args so ConstantFolding can handle it.
      Unsupported(Some("all arguments are literals, falling back to Spark"))
    } else {
      childTypesSupportLevel(expr) match {
        case _: Compatible if hasNestedFloatElements(expr.left.dataType) =>
          Incompatible(Some(nestedFloatIncompatibilityReason))
        case level => level
      }
    }
  }

  override def convert(
      expr: ArrayPosition,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val arrayExprProto = exprToProto(expr.left, inputs, binding)
    val elementExprProto = exprToProto(expr.right, inputs, binding)

    // Use spark_array_position which returns Int64 and 0 when not found
    // (matching Spark's behavior)
    val optExpr =
      scalarFunctionExprToProto("spark_array_position", arrayExprProto, elementExprProto)
    optExpr
  }
}

object CometArraysZip extends CometExpressionSerde[ArraysZip] {

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "Not all input data types are supported; falls back to Spark for unsupported types")

  private def isTypeSupported(dt: DataType): Boolean = {
    import DataTypes._
    dt match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
          _: DecimalType | DateType | TimestampType | TimestampNTZType | StringType | NullType |
          BinaryType =>
        true
      case ArrayType(elementType, _) => isTypeSupported(elementType)
      case StructType(fields) => fields.forall(f => isTypeSupported(f.dataType))
      case _ => false
    }
  }

  override def getSupportLevel(expr: ArraysZip): SupportLevel = {
    val inputTypes = expr.children.map(_.dataType).toSet
    for (dt <- inputTypes) {
      if (!isTypeSupported(dt)) {
        return Unsupported(Some(s"Unsupported child data type: $dt"))
      }
    }
    Compatible()
  }

  override def convert(
      expr: ArraysZip,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {

    val exprChildren: Seq[Option[ExprOuterClass.Expr]] =
      expr.children.map(exprToProtoInternal(_, inputs, binding))
    val names: Seq[Any] = expr.names.map(_.eval(EmptyRow))

    // mimic Spark's ArraysZip behavior: returns NULL if any argument is NULL
    val combinedNullCheck = expr.children.map(child => IsNotNull(child)).reduce(And)
    val isNotNullExpr = exprToProtoInternal(combinedNullCheck, inputs, binding)
    val nullLiteralProto = exprToProto(Literal(null, expr.dataType), Seq.empty)

    if (exprChildren.forall(
        _.isDefined) && isNotNullExpr.isDefined && nullLiteralProto.isDefined) {
      val arraysZip: ExprOuterClass.ArraysZip = ExprOuterClass.ArraysZip
        .newBuilder()
        .addAllValues(exprChildren.map(_.get).asJava)
        .addAllNames(names.map(_.toString).asJava)
        .build()

      val caseWhenExpr = ExprOuterClass.CaseWhen
        .newBuilder()
        .addWhen(isNotNullExpr.get)
        .addThen(ExprOuterClass.Expr.newBuilder().setArraysZip(arraysZip).build())
        .setElseExpr(nullLiteralProto.get)
        .build()
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setCaseWhen(caseWhenExpr)
          .build())

    } else {
      withFallbackReason(expr, "unsupported arguments for ArraysZip")
      None
    }
  }
}

trait ArraysBase {

  protected val nestedFloatIncompatibilityReason: String =
    "Nested floating-point elements distinguish `-0.0` from `0.0`, unlike Spark " +
      "(https://github.com/apache/datafusion-comet/issues/5191)"

  protected def hasNestedFloatElements(dt: DataType): Boolean = dt match {
    case ArrayType(elementType: ArrayType, _) =>
      SupportLevel.containsType(elementType, classOf[FloatType], classOf[DoubleType])
    case ArrayType(elementType: StructType, _) =>
      SupportLevel.containsType(elementType, classOf[FloatType], classOf[DoubleType])
    // Map elements cannot reach here: ArraysOverlap and ArrayPosition call
    // TypeUtils.checkForOrderingExpr on the element type, and maps are not orderable,
    // so Spark's analyzer rejects them before planning.
    case _ => false
  }

  def isTypeSupported(dt: DataType): Boolean = {
    import DataTypes._
    dt match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
          _: DecimalType | DateType | TimestampType | TimestampNTZType | StringType =>
        true
      case BinaryType => false
      case ArrayType(elementType, _) => isTypeSupported(elementType)
      case _: StructType =>
        // https://github.com/apache/datafusion-comet/issues/1307
        false
      case _ => false
    }
  }

  /**
   * Support level based on whether every input data type is supported. Returns `Unsupported` for
   * the first unsupported input type, otherwise `Compatible`.
   */
  def childTypesSupportLevel(expr: Expression): SupportLevel =
    expr.children
      .map(_.dataType)
      .collectFirst { case dt if !isTypeSupported(dt) => dt }
      .map(dt => Unsupported(Some(s"data type not supported: $dt")))
      .getOrElse(Compatible())
}

object CometArrayTransform extends CometCodegenDispatch[ArrayTransform]

object CometArrayExists extends CometCodegenDispatch[ArrayExists]

object CometArrayForAll extends CometCodegenDispatch[ArrayForAll]

object CometArrayAggregate extends CometCodegenDispatch[ArrayAggregate]

object CometArraySort extends CometCodegenDispatch[ArraySort]

object CometZipWith extends CometCodegenDispatch[ZipWith]

object CometSequence extends CometExpressionSerde[Sequence] with CodegenDispatchFallback {

  private val temporalUnsupportedReason =
    "date and timestamp element types run through the JVM codegen dispatcher"

  private val unsafeArgUnsupportedReason =
    "sequence arguments must be literals or column references; other shapes run through the " +
      "JVM codegen dispatcher to preserve Spark's per-row null short-circuit"

  override def getSupportLevel(expr: Sequence): SupportLevel = expr.start.dataType match {
    case ByteType | ShortType | IntegerType | LongType =>
      // Spark's codegen for `Sequence` short-circuits per row: any null argument returns null
      // without evaluating the rest. DataFusion evaluates each scalar-UDF argument over the
      // whole batch before calling the outer kernel, so a sub-expression with side effects
      // (a nested call, a `CASE WHEN`, even a zero-arg UDF like `boom()`) could fire on rows
      // Spark's null check would have discarded. A tree-shape "no children" test is not
      // enough — a zero-arg UDF has empty children but still executes. Only literals and
      // column references are safe to lower natively; anything else falls back to the
      // codegen dispatcher, which keeps the whole tree inside Spark's guarded evaluation.
      if (argsAreLiteralsOrRefs(expr)) Compatible()
      else Unsupported(Some(unsafeArgUnsupportedReason))
    case DateType | TimestampType | TimestampNTZType =>
      // Temporal sequences step through timezone/DST/legacy-calendar arithmetic
      // (https://github.com/apache/datafusion-comet/issues/5349), so they stay on the JVM
      // codegen dispatcher.
      Unsupported(Some(temporalUnsupportedReason))
    case other =>
      Unsupported(Some(s"sequence with element type $other is not supported natively"))
  }

  private def argsAreLiteralsOrRefs(expr: Sequence): Boolean = {
    val args = Seq(expr.start, expr.stop) ++ expr.stepOpt
    args.forall {
      case _: Literal | _: Attribute | _: BoundReference => true
      case _ => false
    }
  }

  override def getUnsupportedReasons(): Seq[String] =
    Seq(temporalUnsupportedReason, unsafeArgUnsupportedReason)

  override def convert(
      expr: Sequence,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val startExprProto = exprToProto(expr.start, inputs, binding)
    val stopExprProto = exprToProto(expr.stop, inputs, binding)
    // With no step argument the native kernel computes Spark's per-row default,
    // `start <= stop ? 1 : -1`, which cannot be expressed as a plan-time literal.
    val argProtos = Seq(startExprProto, stopExprProto) ++
      expr.stepOpt.map(exprToProto(_, inputs, binding))
    scalarFunctionExprToProtoWithReturnType(
      "spark_sequence",
      expr.dataType,
      failOnError = false,
      argProtos: _*)
  }
}
