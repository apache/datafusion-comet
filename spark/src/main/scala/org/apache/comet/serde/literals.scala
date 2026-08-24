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
import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateArray, CreateMap, Expression, KnownNullable, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DayTimeIntervalType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, NullType, ShortType, StringType, TimestampNTZType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import com.google.protobuf.ByteString

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.DataTypeSupport.isComplexType
import org.apache.comet.serde.{CometExpressionSerde, Compatible, ExprOuterClass, LiteralOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, hasNonDefaultStringCollation, isTimeType, serializeDataType, supportedDataType}
import org.apache.comet.serde.Types.ListLiteral

object CometLiteral extends CometExpressionSerde[Literal] with Logging {

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "Not all data types are supported for literal values")

  override def getSupportLevel(expr: Literal): SupportLevel = {

    if (supportedDataType(
        expr.dataType,
        allowComplex = expr.value == null ||

          // Nested literal support for native reader
          // can be tracked https://github.com/apache/datafusion-comet/issues/1937
          (expr.dataType
            .isInstanceOf[ArrayType] && (!isComplexType(
            expr.dataType.asInstanceOf[ArrayType].elementType) || expr.dataType
            .asInstanceOf[ArrayType]
            .elementType
            .isInstanceOf[ArrayType])))) {
      Compatible(None)
    } else if (expandComplexLiteral(expr).isDefined) {
      // Rebuilt as a `CreateArray` / `CreateMap` tree in `convert`.
      Compatible(None)
    } else {
      expr.dataType match {
        case _: DayTimeIntervalType => Compatible(None)
        case _ => Unsupported(Some(s"Unsupported data type ${expr.dataType}"))
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
        case _: IntegerType | _: DateType => exprBuilder.setIntVal(value.asInstanceOf[Int])
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

  private def makeListLiteral(array: Array[Any], arrayType: ArrayType): ListLiteral.Builder = {
    val listLiteralBuilder = ListLiteral.newBuilder()
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
      case IntegerType | DateType =>
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
   * Rebuild a folded complex Literal as an equivalent tree of `CreateArray` / `CreateMap` over
   * primitive-typed Literals, or `None` when the shape cannot be rebuilt. The native `Literal`
   * proto carries scalars and nested `ListLiteral`s but no map values, so a Literal whose type
   * contains a `MapType` has to be expanded before serialization. Teaching the proto to transport
   * maps directly would remove the need for this rewrite and for the declines below:
   * https://github.com/apache/datafusion-comet/issues/1937
   *
   * Declined shapes:
   *   - Null values and empty top-level containers: a synthesized `Create*` with no children
   *     cannot recover the original element type. Empty `ArrayType` literals still serialize via
   *     `makeListLiteral`, which keeps the type.
   *   - `StructType` at any depth. Native `CreateNamedStruct` builds a 1-row `StructArray`
   *     whenever all of its children are scalars (`values_to_arrays`), which collides with the
   *     surrounding batch's row count. Its proto message also carries no type, so Spark's
   *     declared field nullability cannot survive the wire either way.
   *   - Map key types whose Spark equality semantics native lookup cannot honor, see
   *     [[hasUnsafeMapKeyType]].
   *   - Folded maps with duplicate keys. The rebuilt `CreateMap` evaluates through
   *     `ArrayBasedMapBuilder`, which throws under `MAP_KEY_DEDUP_POLICY=EXCEPTION`, where the
   *     original literal had already folded cleanly.
   */
  private def expandComplexLiteral(expr: Literal): Option[Expression] = {
    if (expr.value == null) return None
    expr.dataType match {
      case ArrayType(et, _) if needsExpansion(et) =>
        val arr = expr.value.asInstanceOf[ArrayData]
        if (arr.numElements() == 0) {
          None
        } else {
          val elements = (0 until arr.numElements()).map(i => asNullable(literalAt(arr, i, et)))
          Some(CreateArray(elements, useStringTypeWhenEmpty = false))
        }
      case MapType(kt, vt, _) =>
        val mapData = expr.value.asInstanceOf[MapData]
        val keys = mapData.keyArray()
        if (mapData.numElements() == 0 || hasUnsafeMapKeyType(kt) ||
          hasDuplicateMapKeys(keys, kt)) {
          None
        } else {
          val values = mapData.valueArray()
          val children = (0 until keys.numElements()).flatMap(i =>
            Seq(literalAt(keys, i, kt), asNullable(literalAt(values, i, vt))))
          Some(CreateMap(children, useStringTypeWhenEmpty = false))
        }
      case _ => None
    }
  }

  /**
   * True when a Literal of this type has to be expanded rather than serialized, because the
   * native `Literal` proto carries no map values. Walks array nesting only: a `StructType` is not
   * expandable at all (see [[expandComplexLiteral]]), so the walk stops there.
   */
  private def needsExpansion(dataType: DataType): Boolean = dataType match {
    case _: MapType => true
    case ArrayType(et, _) => needsExpansion(et)
    case _ => false
  }

  /** Element `i` of `arr`, or `null` for a null slot. */
  private def valueAt(arr: ArrayData, i: Int, dt: DataType): Any =
    if (arr.isNullAt(i)) null else arr.get(i, dt)

  /** Element `i` of `arr` as a Literal of type `dt`. */
  private def literalAt(arr: ArrayData, i: Int, dt: DataType): Literal =
    Literal(valueAt(arr, i, dt), dt)

  /**
   * True when the map key type has Spark equality semantics that native map lookup (Arrow
   * bytewise comparison) cannot honor, in which case declining expansion keeps the projection on
   * Spark. `NormalizeFloatingNumbers` makes Spark treat `+0.0` and `-0.0` as the same key and
   * canonicalises NaN, and a non-default collation compares under rules native applies as
   * `UTF8_BINARY`. Both are checked at every nesting level of the key type.
   */
  private def hasUnsafeMapKeyType(kt: DataType): Boolean =
    hasNonDefaultStringCollation(kt) ||
      SupportLevel.containsType(kt, classOf[FloatType], classOf[DoubleType])

  /**
   * True when the folded map's key array holds duplicates under Catalyst internal equality
   * (`UTF8String`, `Decimal`, `ArrayData`, `InternalRow` and boxed primitives all implement
   * `equals` / `hashCode`), which is what `ArrayBasedMapBuilder` uses for the key types reachable
   * here.
   */
  private def hasDuplicateMapKeys(keys: ArrayData, keyType: DataType): Boolean = {
    val n = keys.numElements()
    n > 1 && (0 until n).map(i => valueAt(keys, i, keyType)).distinct.size < n
  }

  /**
   * Wrap a non-null expression in `KnownNullable` so the surrounding `Create*` derives a nullable
   * element / value type. DataFusion `make_array` asserts strict Arrow-type equality across
   * siblings and would panic on a non-nullable child next to a nullable one. Same root cause as
   * the mismatched-type decline in `CometCreateArray` (apache/datafusion#22366).
   * `CometKnownNullable` forwards the child on the wire, so runtime is unaffected.
   */
  private def asNullable(expr: Expression): Expression =
    if (expr.nullable) expr else KnownNullable(expr)
}
