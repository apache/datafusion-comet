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

package org.apache.comet.serde.literals

import java.lang

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, CreateArray, CreateMap, Expression, KnownNullable, Literal, MapFromArrays}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData, TypeUtils}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, DataType, DateType, DayTimeIntervalType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, NullType, ShortType, StringType, StructType, TimestampNTZType, TimestampType, YearMonthIntervalType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import com.google.protobuf.ByteString

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.{CometExpressionSerde, Compatible, ExprOuterClass, LiteralOuterClass, MapKeySupport, SupportLevel, Unsupported}
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType, supportedDataType}
import org.apache.comet.serde.Types.ListLiteral
import org.apache.comet.shims.CometTypeShim

object CometLiteral extends CometExpressionSerde[Literal] with CometTypeShim with Logging {

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "Not all data types are supported for literal values")

  override def getSupportLevel(expr: Literal): SupportLevel = {
    val serializable = expr.dataType match {
      // A null literal carries only its type on the wire, so any type serializeDataType handles.
      case _ if expr.value == null => supportedDataType(expr.dataType, allowComplex = true)
      case ArrayType(elementType, _) => listLiteralElementSupported(elementType)
      case dt => supportedDataType(dt)
    }

    // A shape `canExpandComplexLiteral` admits is rebuilt as a `CreateArray` / `CreateMap` tree.
    if ((serializable && !hasUnwritableValue(expr.value, expr.dataType)) ||
      canExpandComplexLiteral(expr)) {
      Compatible(None)
    } else if (serializable) {
      // The type is fine, so this is a value `convert` could only serialize by changing it.
      Unsupported(Some(s"Unsupported literal value for data type ${expr.dataType}"))
    } else {
      expr.dataType match {
        // ANSI interval types are deliberately kept out of QueryPlanSerde.supportedDataType so
        // they are not claimed as flowing through arbitrary native operators; literals are
        // supported here.
        case _: DayTimeIntervalType | _: YearMonthIntervalType =>
          Compatible(None)
        case dt => Unsupported(Some(s"Unsupported data type $dt"))
      }
    }
  }

  override def convert(
      expr: Literal,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val dataType = expr.dataType
    val value = expr.value

    val expanded = expandComplexLiteral(expr)
    if (expanded.isDefined) {
      return exprToProtoInternal(expanded.get, inputs, binding).orElse {
        withFallbackReason(expr, s"Unsupported data type $dataType")
        None
      }
    }
    val exprBuilder = LiteralOuterClass.Literal.newBuilder()

    if (value == null) {
      exprBuilder.setIsNull(true)
    } else {
      exprBuilder.setIsNull(false)
      dataType match {
        case _: BooleanType => exprBuilder.setBoolVal(value.asInstanceOf[Boolean])
        case _: ByteType => exprBuilder.setByteVal(value.asInstanceOf[Byte])
        case _: ShortType => exprBuilder.setShortVal(value.asInstanceOf[Short])
        case _: IntegerType | _: DateType | _: YearMonthIntervalType =>
          exprBuilder.setIntVal(value.asInstanceOf[Int])
        case _: LongType | _: TimestampType | _: TimestampNTZType | _: DayTimeIntervalType =>
          exprBuilder.setLongVal(value.asInstanceOf[Long])
        case dt if isTimeType(dt) =>
          exprBuilder.setLongVal(value.asInstanceOf[Long])
        case _: FloatType => exprBuilder.setFloatVal(value.asInstanceOf[Float])
        case _: DoubleType => exprBuilder.setDoubleVal(value.asInstanceOf[Double])
        case _: StringType =>
          exprBuilder.setStringVal(value.asInstanceOf[UTF8String].toString)
        case _: DecimalType =>
          // Pass decimal literal as bytes.
          val unscaled = value.asInstanceOf[Decimal].toBigDecimal.underlying.unscaledValue
          exprBuilder.setDecimalVal(com.google.protobuf.ByteString.copyFrom(unscaled.toByteArray))
        case _: BinaryType =>
          val byteStr =
            com.google.protobuf.ByteString.copyFrom(value.asInstanceOf[Array[Byte]])
          exprBuilder.setBytesVal(byteStr)

        case arr: ArrayType =>
          val listLiteralBuilder: ListLiteral.Builder =
            makeListLiteral(value.asInstanceOf[ArrayData].array, arr)
          exprBuilder.setListVal(listLiteralBuilder.build())
          exprBuilder.setDatatype(serializeDataType(dataType).get)
        case dt =>
          withFallbackReason(expr, s"Unexpected datatype '$dt' for literal value '$value'")
          return None
      }
    }

    val dt = serializeDataType(dataType)

    if (dt.isDefined) {
      exprBuilder.setDatatype(dt.get)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setLiteral(exprBuilder)
          .build())
    } else {
      withFallbackReason(expr, s"Unsupported datatype $dataType")
      None
    }

  }

  /** Package-private only so `CometLiteralSuite` can pin it against the gate below. */
  private[serde] def makeListLiteral(
      array: Array[Any],
      arrayType: ArrayType): ListLiteral.Builder = {
    val listLiteralBuilder = ListLiteral.newBuilder()
    // Keep these arms in sync with [[listLiteralElementSupported]].
    arrayType.elementType match {
      case NullType =>
        array.foreach(_ => listLiteralBuilder.addNullMask(true))
      case BooleanType =>
        array.foreach(v => {
          val casted = v.asInstanceOf[lang.Boolean]
          listLiteralBuilder.addBooleanValues(casted)
          listLiteralBuilder.addNullMask(casted != null)
        })
      case ByteType =>
        array.foreach(v => {
          val casted = v.asInstanceOf[lang.Byte]
          listLiteralBuilder.addByteValues(
            if (casted != null) casted.intValue()
            else null.asInstanceOf[Integer])
          listLiteralBuilder.addNullMask(casted != null)
        })
      case ShortType =>
        array.foreach(v => {
          val casted = v.asInstanceOf[lang.Short]
          listLiteralBuilder.addShortValues(
            if (casted != null) casted.intValue()
            else null.asInstanceOf[Integer])
          listLiteralBuilder.addNullMask(casted != null)
        })
      case IntegerType | DateType | _: YearMonthIntervalType =>
        array.foreach(v => {
          val casted = v.asInstanceOf[Integer]
          listLiteralBuilder.addIntValues(casted)
          listLiteralBuilder.addNullMask(casted != null)
        })
      case LongType | TimestampType | TimestampNTZType =>
        array.foreach(v => {
          val casted = v.asInstanceOf[lang.Long]
          listLiteralBuilder.addLongValues(casted)
          listLiteralBuilder.addNullMask(casted != null)
        })
      case FloatType =>
        array.foreach(v => {
          val casted = v.asInstanceOf[lang.Float]
          listLiteralBuilder.addFloatValues(casted)
          listLiteralBuilder.addNullMask(casted != null)
        })
      case DoubleType =>
        array.foreach(v => {
          val casted = v.asInstanceOf[lang.Double]
          listLiteralBuilder.addDoubleValues(casted)
          listLiteralBuilder.addNullMask(casted != null)
        })
      case StringType =>
        array.foreach(v => {
          val casted = v.asInstanceOf[UTF8String]
          listLiteralBuilder.addStringValues(if (casted != null) casted.toString else "")
          listLiteralBuilder.addNullMask(casted != null)
        })
      case _: DecimalType =>
        array
          .foreach(v => {
            val casted =
              v.asInstanceOf[Decimal]
            listLiteralBuilder.addDecimalValues(if (casted != null) {
              com.google.protobuf.ByteString
                .copyFrom(casted.toBigDecimal.underlying.unscaledValue.toByteArray)
            } else ByteString.EMPTY)
            listLiteralBuilder.addNullMask(casted != null)
          })
      case _: BinaryType =>
        array
          .foreach(v => {
            val casted =
              v.asInstanceOf[Array[Byte]]
            listLiteralBuilder.addBytesValues(if (casted != null) {
              com.google.protobuf.ByteString.copyFrom(casted)
            } else ByteString.EMPTY)
            listLiteralBuilder.addNullMask(casted != null)
          })
      case a: ArrayType =>
        array.foreach(v => {
          val casted = v.asInstanceOf[ArrayData]
          listLiteralBuilder.addListValues(if (casted != null) {
            makeListLiteral(casted.array, a)
          } else ListLiteral.newBuilder())
          listLiteralBuilder.addNullMask(casted != null)
        })
    }
    listLiteralBuilder
  }

  /**
   * Element types [[makeListLiteral]] has a branch for. Anything else has to be expanded or
   * declined before `convert` reaches it, or the missing branch raises a `MatchError` mid-plan.
   * `StringType` is matched as a stable identifier, so a non-default collation (whose `equals`
   * compares `collationId`) is declined here rather than falling into the missing branch.
   * `CometLiteralSuite` pins the two together.
   */
  private[serde] def listLiteralElementSupported(dataType: DataType): Boolean = dataType match {
    case NullType | BooleanType | ByteType | ShortType | IntegerType | DateType | LongType |
        TimestampType | TimestampNTZType | FloatType | DoubleType | StringType | BinaryType =>
      true
    case _: DecimalType => true
    // Matched as a type rather than a stable identifier: the start/end fields participate in
    // `equals`, and every (start, end) pair is carried as the same month count.
    case _: YearMonthIntervalType => true
    case ArrayType(elementType, _) => listLiteralElementSupported(elementType)
    case _ => false
  }

  /**
   * Rebuild a folded complex Literal as an equivalent tree of `CreateArray` / `CreateMap` over
   * primitive-typed Literals, or `None` when [[canExpandComplexLiteral]] rejects the shape.
   * `getSupportLevel` probes cheaply with [[canExpandComplexLiteral]], which shares this method's
   * admission checks, so the two cannot diverge and only `convert` materializes the tree. The
   * native `Literal` proto carries scalars and nested `ListLiteral`s but no map values, so a
   * Literal whose type contains a `MapType` has to be expanded before serialization. Teaching the
   * proto to transport maps directly would remove the need for this rewrite and for the declines
   * below: https://github.com/apache/datafusion-comet/issues/1937
   *
   * The rebuilt tree is the tree Spark itself had before `ConstantFolding` collapsed it, down to
   * every container's declared nullability (see [[withNullability]]). That equivalence is the
   * safety property this rewrite rests on: whatever the rebuilt expression does natively is what
   * the same query already does with `ConstantFolding` disabled, so expansion cannot introduce a
   * folding-only behaviour difference. A non-nullable-value map into `map_entries` is handled by
   * the native planner widening its argument rather than declined here.
   *
   * Map key semantics are declined here at admission rather than left to consumers, because not
   * every native map consumer gates on [[MapKeySupport]]; see [[mapKeyTypesExpandable]] for why.
   *
   * Declined shapes:
   *   - Null values, which `convert` serializes as a typed `is_null` literal instead.
   *   - Any map key type native map kernels cannot reproduce Spark equality for (floating-point,
   *     collated string, complex), or whose interpreted ordering is undefined
   *     (`CalendarIntervalType`), at any nesting level. See [[mapKeyTypesExpandable]].
   *   - Arrays whose elements are structs, because [[needsExpansion]] does not walk into a
   *     `StructType`. Native `CreateNamedStruct` builds a 1-row `StructArray` whenever all of its
   *     children are scalars (`values_to_arrays`), which collides with the surrounding batch's
   *     row count, and its proto message carries no type, so Spark's declared field nullability
   *     cannot survive the wire either way. A struct that is only a map value is safe:
   *     `CometCreateMap` hands the whole rebuilt `CreateMap` to the JVM codegen dispatcher, so
   *     Spark's own code builds the struct.
   *   - Folded maps with duplicate keys, see [[hasDuplicateMapKeys]].
   *   - Values no Arrow encoding of the rebuilt tree can carry, see [[hasUnwritableValue]].
   */
  private def expandComplexLiteral(expr: Literal): Option[Expression] = {
    if (!canExpandComplexLiteral(expr)) return None
    expr.dataType match {
      case ArrayType(et, containsNull) =>
        val arr = expr.value.asInstanceOf[ArrayData]
        if (arr.numElements() == 0) {
          Some(emptyTypedArray(et, containsNull))
        } else {
          val elements = (0 until arr.numElements())
            .map(i => withNullability(literalAt(arr, i, et), containsNull))
          Some(CreateArray(elements, useStringTypeWhenEmpty = false))
        }
      case MapType(kt, vt, valueContainsNull) =>
        val mapData = expr.value.asInstanceOf[MapData]
        if (mapData.numElements() == 0) {
          // `MapFromArrays` recovers the declared key/value types from its two array children;
          // a childless `CreateMap` would report `MapType(NullType, NullType)`. Arrow map keys
          // are never null, so the key array is non-nullable.
          Some(
            MapFromArrays(
              emptyTypedArray(kt, containsNull = false),
              emptyTypedArray(vt, valueContainsNull)))
        } else {
          val keys = mapData.keyArray()
          val values = mapData.valueArray()
          val children = (0 until keys.numElements()).flatMap(i =>
            Seq(
              literalAt(keys, i, kt),
              withNullability(literalAt(values, i, vt), valueContainsNull)))
          Some(CreateMap(children, useStringTypeWhenEmpty = false))
        }
      case _ => None
    }
  }

  /**
   * An empty `ArrayType(elementType, containsNull)` value. `CometCreateArray` turns a childless
   * `CreateArray` into an empty `ArrayType(NullType)` list literal (its documented workaround for
   * DataFusion's zero-argument `make_array`), and the cast stamps the declared element type back
   * onto it.
   */
  private def emptyTypedArray(elementType: DataType, containsNull: Boolean): Expression =
    Cast(CreateArray(Nil, useStringTypeWhenEmpty = false), ArrayType(elementType, containsNull))

  /**
   * Admission test for [[expandComplexLiteral]], which gates on this so the two cannot diverge.
   * Split out so `getSupportLevel` can decide without allocating the N `Literal`s `convert` would
   * immediately rebuild. See [[expandComplexLiteral]] for the shapes this declines.
   */
  private def canExpandComplexLiteral(expr: Literal): Boolean = {
    if (expr.value == null || !mapKeyTypesExpandable(expr.dataType)) return false
    val expandableShape = expr.dataType match {
      case ArrayType(et, _) => needsExpansion(et)
      case MapType(kt, _, _) =>
        !hasDuplicateMapKeys(expr.value.asInstanceOf[MapData].keyArray(), kt)
      case _ => false
    }
    // Last, because it is the only check here that walks the whole folded value.
    expandableShape && !hasUnwritableValue(expr.value, expr.dataType)
  }

  /**
   * True when some scalar inside `value` has no faithful Arrow encoding, in which case the
   * literal has to stay on Spark. Two of the copies a Literal takes towards the native side are
   * not total:
   *   - a `UTF8String` holding malformed UTF-8 (`CAST(X'FF' AS STRING)`). A rebuilt `CreateMap`
   *     reaches the JVM codegen dispatcher, whose generated writer copies the bytes verbatim into
   *     a `VarCharVector`; native string kernels then read them through unchecked Arrow FFI and
   *     abort the JVM on a `hint::unreachable_unchecked`, or silently mis-count characters. The
   *     direct paths (`convert`'s `StringType` arm and [[makeListLiteral]]'s) instead go through
   *     `UTF8String.toString`, which substitutes U+FFFD. Neither is fixable on the wire, because
   *     `string_val` and `string_values` are proto `string` fields.
   *   - a `CalendarInterval` beyond about 292 years of elapsed time overflows
   *     `Math.multiplyExact(microseconds, 1000L)` on the way into `IntervalMonthDayNanoVector`,
   *     whose nanosecond field cannot represent it (#5279). Spark itself accepts the value.
   *
   * The same hazards exist for a non-folded `map(...)` / a scanned string column, which never
   * reach this serde; this only keeps a Literal from newly admitting them.
   */
  private def hasUnwritableValue(value: Any, dataType: DataType): Boolean =
    mayHoldUnwritableValue(dataType) && unwritableValueIn(value, dataType)

  /**
   * Type-level precondition for [[hasUnwritableValue]]: only a string or a calendar interval
   * reaches one of its hazardous arms, so a large all-numeric literal is never walked element by
   * element. Applied once per collection rather than once per element.
   */
  private def mayHoldUnwritableValue(dataType: DataType): Boolean =
    SupportLevel.containsType(dataType, classOf[StringType], classOf[CalendarIntervalType])

  private def unwritableValueIn(value: Any, dataType: DataType): Boolean = dataType match {
    case _ if value == null => false
    case _: StringType => !isValidUtf8(value.asInstanceOf[UTF8String])
    case CalendarIntervalType =>
      val micros = value.asInstanceOf[CalendarInterval].microseconds
      micros > MaxArrowIntervalMicros || micros < -MaxArrowIntervalMicros
    case ArrayType(et, _) => unwritableElementIn(value.asInstanceOf[ArrayData], et)
    case MapType(kt, vt, _) =>
      val mapData = value.asInstanceOf[MapData]
      (mayHoldUnwritableValue(kt) && unwritableElementIn(mapData.keyArray(), kt)) ||
      (mayHoldUnwritableValue(vt) && unwritableElementIn(mapData.valueArray(), vt))
    case StructType(fields) =>
      val row = value.asInstanceOf[InternalRow]
      fields.indices.exists { i =>
        val fieldType = fields(i).dataType
        mayHoldUnwritableValue(fieldType) && unwritableValueIn(row.get(i, fieldType), fieldType)
      }
    case _ => false
  }

  private def unwritableElementIn(arr: ArrayData, elementType: DataType): Boolean =
    (0 until arr.numElements()).exists(i =>
      unwritableValueIn(arr.get(i, elementType), elementType))

  /** Largest `CalendarInterval.microseconds` that survives conversion to Arrow nanoseconds. */
  private final val MaxArrowIntervalMicros: Long = Long.MaxValue / 1000

  /**
   * True when every map key type reachable inside `dataType` can be rebuilt and consumed
   * natively. A folded map is rebuilt as a `CreateMap`; once native it may reach a map consumer
   * that has no key-type gate of its own -- `map_contains_key` lowers to
   * `array_contains(map_keys(...), key)`, and neither `map_keys` nor `array_contains` consults
   * [[MapKeySupport]]. A map that is only the value of another map is handed to the JVM
   * dispatcher whole by `CometCreateMap` and never revisits this serde, so a nested unsupported
   * key type would otherwise slip through. Mirror the [[MapKeySupport]] gate here, walking into
   * map values, array elements, and struct fields, so an unsupported key type (floating-point,
   * collated string, complex) declines expansion instead of reaching a native kernel whose key
   * equality disagrees with Spark.
   *
   * Also decline a key type whose interpreted ordering is undefined -- one that recursively
   * contains a `CalendarIntervalType`, whose `PhysicalCalendarIntervalType` ordering throws "does
   * not support ordered operations". `ArrayBasedMapBuilder` dedups such keys by hash equality and
   * never asks for an ordering, but [[hasDuplicateMapKeys]] compares keys through that ordering,
   * so gating here keeps it from being called on a type it cannot order. Such literals fell back
   * before this rewrite too, so declining them matches the prior behaviour.
   */
  private def mapKeyTypesExpandable(dataType: DataType): Boolean = dataType match {
    case MapType(kt, vt, _) =>
      MapKeySupport.keySupport(kt).isInstanceOf[Compatible] &&
      !SupportLevel.containsType(kt, classOf[CalendarIntervalType]) &&
      mapKeyTypesExpandable(vt)
    case ArrayType(et, _) => mapKeyTypesExpandable(et)
    case StructType(fields) => fields.forall(f => mapKeyTypesExpandable(f.dataType))
    case _ => true
  }

  /**
   * True when a Literal of this type has to be expanded rather than serialized, because the
   * native `Literal` proto carries no map values. Walks array nesting only: an array of structs
   * is not expandable (see [[expandComplexLiteral]]), so the walk stops at a `StructType`.
   */
  private def needsExpansion(dataType: DataType): Boolean = dataType match {
    case _: MapType => true
    case ArrayType(et, _) => needsExpansion(et)
    case _ => false
  }

  /** Element `i` of `arr` as a Literal of type `dt`, or a null Literal for a null slot. */
  private def literalAt(arr: ArrayData, i: Int, dt: DataType): Literal =
    Literal(if (arr.isNullAt(i)) null else arr.get(i, dt), dt)

  /**
   * True when the folded map's key array holds duplicates under Spark's own key equality, in
   * which case rebuilding the value as a `CreateMap` would change semantics the folded `MapData`
   * had already settled: `ArrayBasedMapBuilder` throws `DUPLICATED_MAP_KEY` under
   * `MAP_KEY_DEDUP_POLICY=EXCEPTION` and silently drops the earlier entry under `LAST_WIN`, where
   * the original literal (from `from_json`, or a cast of one) kept both entries.
   *
   * Only reached for key types [[mapKeyTypesExpandable]] admits, i.e. atomic non-floating-point,
   * default-collation `StringType`, and `BinaryType` -- floating-point, collated, complex, and
   * `CalendarIntervalType` keys are already declined before this runs. Every admitted key type is
   * orderable, so `TypeUtils.getInterpretedOrdering` (which `ArrayBasedMapBuilder` itself uses
   * for `BinaryType`) is safe and matches Spark's equality: `Array[Byte]` keys compare by content
   * rather than by identity, and the atomic orderings agree with hash equality. A null key cannot
   * occur in a map Spark built, so treat one as a duplicate and keep the projection on Spark
   * instead of asking the ordering to compare it.
   */
  private def hasDuplicateMapKeys(keys: ArrayData, keyType: DataType): Boolean = {
    if (keys.numElements() < 2) return false
    // Single pass out of the ArrayData; a null slot comes back as null.
    val sorted = keys.toObjectArray(keyType)
    if (sorted.contains(null)) return true
    val ordering = TypeUtils.getInterpretedOrdering(keyType)
    java.util.Arrays.sort(sorted, ordering.asInstanceOf[Ordering[Object]])
    (1 until sorted.length).exists(i => ordering.compare(sorted(i - 1), sorted(i)) == 0)
  }

  /**
   * Wrap `expr` in `KnownNullable` when the container it came out of declares its elements or
   * values nullable, so the rebuilt `Create*` reports exactly the literal's declared
   * `ArrayType.containsNull` / `MapType.valueContainsNull`. Both directions matter:
   *   - when the flag is true, a null slot yields a nullable `Literal` while a populated slot
   *     does not, and DataFusion `make_array` asserts strict Arrow-type equality across siblings
   *     (apache/datafusion#22366), so all children have to agree.
   *   - when the flag is false, widening it would make the rebuilt map disagree with a non-folded
   *     `CreateMap` sibling whose type Spark's own coercion had already accepted, which
   *     `CometCreateArray` no longer normalizes away because DataFusion 54.1 cannot coerce a
   *     `MapType.valueContainsNull` mismatch.
   *
   * A container that declares itself non-nullable never holds a null slot: `CreateArray` and
   * `CreateMap` only report `false` when every child is non-nullable, and Spark's other sources
   * of a complex type (a `CAST` target, a `from_json` schema, `MapType.apply`) default the flag
   * to `true`. `CometKnownNullable` forwards the child on the wire, so runtime is unaffected.
   */
  private def withNullability(expr: Expression, nullable: Boolean): Expression =
    if (nullable && !expr.nullable) KnownNullable(expr) else expr
}
