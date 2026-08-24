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
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, isTimeType, serializeDataType, supportedDataType}
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
    } else if (canExpandComplexLiteral(expr)) {
      // Rebuilt as a Create[Array|Map|NamedStruct] tree in `convert`.
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

    if (canExpandComplexLiteral(expr)) {
      return exprToProtoInternal(expandComplexLiteral(value, dataType), inputs, binding).orElse {
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
   * True when a non-null Literal of this type is not encodable in the native `Literal` proto and
   * `convert` should try to rebuild it from primitive-typed Literals. The native proto today
   * carries scalars and nested `ListLiteral`s (arrays of arrays / arrays of scalars). It does not
   * carry Map values, so any Literal whose type contains a `MapType` needs to be expanded before
   * serialization.
   *
   * `StructType` (at any nesting depth) is deliberately excluded. Native `CometCreateNamedStruct`
   * (`spark/src/main/scala/org/apache/comet/serde/structs.scala`) uses `values_to_arrays` in
   * `native/spark-expr/src/struct_funcs/create_named_struct.rs`, which returns a 1-row
   * `StructArray` whenever all children are scalar values. That collides with the row count of
   * the surrounding batch and fails `make_array`'s length check. Field nullability is likewise
   * inferred from the concrete child expressions, and `CometKnownNullable`
   * (`spark/src/main/scala/org/apache/comet/serde/contraintExpressions.scala:99`) drops the tag
   * on the wire, so wrapping a non-null child in `KnownNullable` does not carry across. Fall back
   * to Spark for those shapes.
   */
  private def needsExpansion(dataType: DataType): Boolean = dataType match {
    case _: MapType => true
    case ArrayType(et, _) => needsExpansion(et)
    case _ => false
  }

  /**
   * True when the Literal is a non-null complex value that we can rebuild from primitive Literals
   * via [[expandComplexLiteral]]. Empty top-level containers are excluded because a synthesized
   * `Create[Array|Map]` with no children cannot recover the original element type. A folded
   * `MapData` with duplicate keys is also excluded: Spark's `CreateMap.eval`
   * (`sql/catalyst/.../complexTypeCreator.scala:250`) feeds every entry through
   * `ArrayBasedMapBuilder`, which throws under the default `MAP_KEY_DEDUP_POLICY=EXCEPTION`
   * (`sql/catalyst/.../util/ArrayBasedMapBuilder.scala`). Rebuilding a folded literal that came
   * from `from_json` or a similar source would then throw where the original literal had executed
   * cleanly.
   */
  private def canExpandComplexLiteral(expr: Literal): Boolean = {
    if (expr.value == null) return false
    expr.dataType match {
      case at: ArrayType if needsExpansion(at) =>
        expr.value.asInstanceOf[ArrayData].numElements() > 0
      case MapType(kt, _, _) =>
        val mapData = expr.value.asInstanceOf[MapData]
        mapData.numElements() > 0 && !hasDuplicateMapKeys(mapData.keyArray(), kt)
      case _ => false
    }
  }

  /**
   * True when the folded map's key array contains any duplicate values. Uses Catalyst internal
   * equality (`UTF8String`, `Decimal`, `ArrayData`, `InternalRow`, and boxed primitives all
   * implement `equals` / `hashCode`), matching `ArrayBasedMapBuilder`'s own dedup semantics.
   */
  private def hasDuplicateMapKeys(keys: ArrayData, keyType: DataType): Boolean = {
    val n = keys.numElements()
    if (n < 2) return false
    val seen = new java.util.HashSet[Any](n)
    var i = 0
    while (i < n) {
      val k = if (keys.isNullAt(i)) null else keys.get(i, keyType)
      if (!seen.add(k)) return true
      i += 1
    }
    false
  }

  /**
   * Rebuild a folded complex Literal as an equivalent tree of `CreateArray` / `CreateMap` over
   * primitive-typed Literals. Callers must gate on [[canExpandComplexLiteral]] so `dataType` is
   * one of the two complex cases and top-level containers are non-empty and have unique keys.
   */
  private def expandComplexLiteral(value: Any, dataType: DataType): Expression =
    dataType match {
      case ArrayType(et, _) =>
        val arr = value.asInstanceOf[ArrayData]
        val elems = (0 until arr.numElements()).map { i =>
          if (arr.isNullAt(i)) Literal(null, et)
          else asNullable(Literal(arr.get(i, et), et))
        }
        CreateArray(elems, useStringTypeWhenEmpty = false)
      case MapType(kt, vt, _) =>
        val mapData = value.asInstanceOf[MapData]
        val keys = mapData.keyArray()
        val vals = mapData.valueArray()
        val children = (0 until keys.numElements()).flatMap { i =>
          val k = if (keys.isNullAt(i)) Literal(null, kt) else Literal(keys.get(i, kt), kt)
          val v =
            if (vals.isNullAt(i)) Literal(null, vt)
            else asNullable(Literal(vals.get(i, vt), vt))
          Seq(k, v)
        }
        CreateMap(children, useStringTypeWhenEmpty = false)
      case other =>
        throw new IllegalStateException(s"expandComplexLiteral called on $other")
    }

  /**
   * Wrap a non-null expression in `KnownNullable` so a surrounding `Create*` sees every sibling
   * as nullable. DataFusion `make_array` asserts strict Arrow-type equality across siblings and
   * would panic if a folded literal produced a non-nullable child next to a nullable one.
   * `CometKnownNullable` forwards the child on the wire, so runtime is unaffected.
   */
  private def asNullable(expr: Expression): Expression =
    if (expr.nullable) expr else KnownNullable(expr)
}
