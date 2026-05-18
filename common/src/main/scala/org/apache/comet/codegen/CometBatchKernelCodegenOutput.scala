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

package org.apache.comet.codegen

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types._

import org.apache.comet.CometArrowAllocator

/**
 * Output-side emitters for the codegen kernel: [[allocateOutput]], [[emitOutputWriter]]
 * (top-level write entry), [[emitWrite]] (recursive per-type write), the output vector-class
 * lookup. Paired with [[CometBatchKernelCodegenInput]] on the read side.
 */
private[codegen] object CometBatchKernelCodegenOutput {

  /**
   * Allocate an Arrow output vector matching `dataType`. Delegates to [[Utils.toArrowField]] +
   * `Field.createVector` for the Spark -> Arrow mapping (handles `MapVector`'s non-null-key and
   * non-null-entries invariants).
   *
   * `estimatedBytes` pre-sizes the data buffer for variable-length scalar outputs; ignored for
   * non-`BaseVariableWidthVector` roots, and not propagated into nested var-width children (those
   * get default sizing because the parent's `allocateNew` resets child buffers).
   *
   * TODO(nested-varwidth-sizing): thread the estimate into nested var-width children. Arrow
   * Java's child-vector hints are allocator-level, so this needs a small recursion or a heuristic
   * that overshoots root size into known-leaf children.
   *
   * TODO(cached-write-buffer-addrs): mirror the input emitter's `_valueAddr` / `_offsetAddr`
   * caching. Cache buffer addresses at `process` setup and emit `Platform.putByte` /
   * `Platform.copyMemory` for VarChar / VarBinary / Decimal scalar outputs, bypassing `setSafe`'s
   * realloc check. Depends on pre-allocated buffers (above).
   *
   * Closes the vector on any failure so a partially-initialized tree doesn't leak buffers.
   */
  def allocateOutput(
      dataType: DataType,
      name: String,
      numRows: Int,
      estimatedBytes: Int = -1): FieldVector =
    allocateOutput(
      Utils.toArrowField(name, dataType, nullable = true, "UTC"),
      numRows,
      estimatedBytes)

  /** Variant that takes a pre-computed Arrow `Field`, letting hot-path callers cache it. */
  def allocateOutput(field: Field, numRows: Int, estimatedBytes: Int): FieldVector = {
    val vec = field.createVector(CometArrowAllocator).asInstanceOf[FieldVector]
    try {
      vec.setInitialCapacity(numRows)
      vec match {
        case v: BaseVariableWidthVector if estimatedBytes > 0 =>
          v.allocateNew(estimatedBytes.toLong, numRows)
        case _ =>
          vec.allocateNew()
      }
      vec
    } catch {
      case t: Throwable =>
        try vec.close()
        catch {
          case _: Throwable => ()
        }
        throw t
    }
  }

  /**
   * Returns `(concreteVectorClassName, batchSetup, perRowSnippet)`. `output` is cast to the
   * concrete class in `process`'s prelude so `emitWrite`'s complex-type branches can hoist child
   * casts off `output` without re-casting per row.
   */
  def emitOutputWriter(
      dataType: DataType,
      valueTerm: String,
      ctx: CodegenContext): (String, String, String) = {
    val cls = outputVectorClass(dataType)
    val emit = emitWrite("output", "i", valueTerm, dataType, ctx)
    (cls, emit.setup, emit.perRow)
  }

  /** Concrete Arrow vector class name for the output type, used to cast `outRaw` once. */
  private def outputVectorClass(dataType: DataType): String = dataType match {
    case BooleanType => classOf[BitVector].getName
    case ByteType => classOf[TinyIntVector].getName
    case ShortType => classOf[SmallIntVector].getName
    case IntegerType => classOf[IntVector].getName
    case LongType => classOf[BigIntVector].getName
    case FloatType => classOf[Float4Vector].getName
    case DoubleType => classOf[Float8Vector].getName
    case _: DecimalType => classOf[DecimalVector].getName
    case _: StringType => classOf[VarCharVector].getName
    case BinaryType => classOf[VarBinaryVector].getName
    case DateType => classOf[DateDayVector].getName
    case TimestampType => classOf[TimeStampMicroTZVector].getName
    case TimestampNTZType => classOf[TimeStampMicroVector].getName
    case _: ArrayType => classOf[ListVector].getName
    case _: StructType => classOf[StructVector].getName
    case _: MapType => classOf[MapVector].getName
    case other =>
      throw new UnsupportedOperationException(
        s"CometBatchKernelCodegen.outputVectorClass: unsupported output type $other")
  }

  /**
   * Composable write emitter. Returns an [[OutputEmit]] whose `setup` declares once-per-batch
   * typed child-vector casts and whose `perRow` writes `source` into `targetVec` at `idx`.
   * `targetVec` is assumed pre-cast to the right Arrow class (root prelude or a parent's setup).
   *
   * Scalars emit `perRow` only; complex types emit both. Inner setup bubbles up so deep child
   * casts land at the batch prelude.
   */
  private def emitWrite(
      targetVec: String,
      idx: String,
      source: String,
      dataType: DataType,
      ctx: CodegenContext): OutputEmit = dataType match {
    case BooleanType =>
      OutputEmit("", s"$targetVec.set($idx, $source ? 1 : 0);")
    case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | DateType |
        TimestampType | TimestampNTZType =>
      // Spark codegen emits the matching primitive Java type; Arrow `set` overloads accept it.
      OutputEmit("", s"$targetVec.set($idx, $source);")
    case dt: DecimalType =>
      // DecimalOutputShortFastPath: precision <= 18 fits in a signed long, so pass the unscaled
      // value to `setSafe(int, long)` and skip the BigDecimal allocation.
      val write =
        if (dt.precision <= Decimal.MAX_LONG_DIGITS) {
          s"$targetVec.setSafe($idx, $source.toUnscaledLong());"
        } else {
          s"$targetVec.setSafe($idx, $source.toJavaBigDecimal());"
        }
      OutputEmit("", write)
    case _: StringType =>
      // Utf8OutputOnHeapShortcut: when the UTF8String is on-heap (Spark's string functions
      // allocate results on-heap), pass its backing byte[] directly to `setSafe`, skipping the
      // `getBytes()` allocation. Off-heap falls back to `getBytes()`.
      //
      // TODO(utf8-unsafe-write): output-side equivalent of `UTF8String.fromAddress`. Coupled
      // with `cached-write-buffer-addrs` and a pre-allocated buffer.
      val bBase = ctx.freshName("utfBase")
      val bLen = ctx.freshName("utfLen")
      val bArr = ctx.freshName("utfArr")
      OutputEmit(
        "",
        s"""Object $bBase = $source.getBaseObject();
           |int $bLen = $source.numBytes();
           |if ($bBase instanceof byte[]) {
           |  $targetVec.setSafe($idx, (byte[]) $bBase,
           |      (int) ($source.getBaseOffset()
           |          - org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET),
           |      $bLen);
           |} else {
           |  byte[] $bArr = $source.getBytes();
           |  $targetVec.setSafe($idx, $bArr, 0, $bArr.length);
           |}""".stripMargin)
    case BinaryType =>
      OutputEmit("", s"$targetVec.setSafe($idx, $source, 0, $source.length);")
    case ArrayType(elementType, containsNull) =>
      // Spark's `doGenCode` for ArrayType produces an `ArrayData` value. Iterate elements,
      // write each into the `ListVector`'s child, bracket with `startNewValue`/`endValue`. The
      // element write recurses through `emitWrite` on the child vector so any supported scalar
      // becomes a valid element. Nested complex types compose. `targetVec` is a `ListVector` at
      // the call site; only its data vector needs casting (in setup).
      //
      // NullableElementElision: when `containsNull == false` drop the `isNullAt` guard at
      // source level rather than relying on JIT folding.
      val childVar = ctx.freshName("outListChild")
      val childClass = outputVectorClass(elementType)
      val arrVar = ctx.freshName("arr")
      val nVar = ctx.freshName("n")
      val childIdx = ctx.freshName("cidx")
      val jVar = ctx.freshName("j")
      val elemSource = emitSpecializedGetterExpr(arrVar, jVar, elementType)
      val inner = emitWrite(childVar, s"$childIdx + $jVar", elemSource, elementType, ctx)
      val setup =
        (s"$childClass $childVar = ($childClass) $targetVec.getDataVector();" +:
          Seq(inner.setup).filter(_.nonEmpty)).mkString("\n")
      val elementWrite = if (containsNull) {
        s"""if ($arrVar.isNullAt($jVar)) {
           |    $childVar.setNull($childIdx + $jVar);
           |  } else {
           |    ${inner.perRow}
           |  }""".stripMargin
      } else {
        inner.perRow
      }
      val perRow =
        s"""org.apache.spark.sql.catalyst.util.ArrayData $arrVar = $source;
           |int $nVar = $arrVar.numElements();
           |int $childIdx = $targetVec.startNewValue($idx);
           |for (int $jVar = 0; $jVar < $nVar; $jVar++) {
           |  $elementWrite
           |}
           |$targetVec.endValue($idx, $nVar);""".stripMargin
      OutputEmit(setup, perRow)
    case st: StructType =>
      // Spark's `doGenCode` for StructType produces an `InternalRow`. Typed child-vector casts
      // hoist to setup; the per-row body references the hoisted names.
      //
      // For non-nullable fields, drop the `row.isNullAt($fi)` guard at source level so HotSpot
      // emits a straight write path per field rather than a branch.
      val rowVar = ctx.freshName("row")
      val perField = st.fields.zipWithIndex.map { case (field, fi) =>
        val childVar = ctx.freshName("outStructChild")
        val childClass = outputVectorClass(field.dataType)
        val childDecl =
          s"$childClass $childVar = ($childClass) $targetVec.getChildByOrdinal($fi);"
        val fieldSource = emitSpecializedGetterExpr(rowVar, fi.toString, field.dataType)
        val inner = emitWrite(childVar, idx, fieldSource, field.dataType, ctx)
        val write =
          if (!field.nullable) {
            inner.perRow
          } else {
            s"""if ($rowVar.isNullAt($fi)) {
               |  $childVar.setNull($idx);
               |} else {
               |  ${inner.perRow}
               |}""".stripMargin
          }
        val perFieldSetup = (Seq(childDecl) ++ Seq(inner.setup).filter(_.nonEmpty)).mkString("\n")
        (perFieldSetup, write)
      }
      val setup = perField.map(_._1).mkString("\n")
      val perFieldWrites = perField.map(_._2).mkString("\n")
      val perRow =
        s"""org.apache.spark.sql.catalyst.InternalRow $rowVar = $source;
           |$targetVec.setIndexDefined($idx);
           |$perFieldWrites""".stripMargin
      OutputEmit(setup, perRow)
    case mt: MapType =>
      // Spark's `doGenCode` for MapType produces a `MapData`. Typed child-vector casts for the
      // entries struct and the key/value children hoist to setup.
      //
      // Per-row: read keyArray/valueArray, open via `startNewValue(idx)`, write each pair into
      // the entries struct (key always non-null per Spark/Arrow invariant; value guarded on
      // `valueContainsNull`), close via `endValue(idx, n)`.
      val entriesVar = ctx.freshName("outMapEntries")
      val keyVar = ctx.freshName("outMapKey")
      val valVar = ctx.freshName("outMapVal")
      val mapSrc = ctx.freshName("mapSrc")
      val keyArr = ctx.freshName("keyArr")
      val valArr = ctx.freshName("valArr")
      val nVar = ctx.freshName("n")
      val childIdx = ctx.freshName("cidx")
      val jVar = ctx.freshName("j")
      val structClass = classOf[StructVector].getName
      val keyClass = outputVectorClass(mt.keyType)
      val valClass = outputVectorClass(mt.valueType)
      val keySrcExpr = emitSpecializedGetterExpr(keyArr, jVar, mt.keyType)
      val valSrcExpr = emitSpecializedGetterExpr(valArr, jVar, mt.valueType)
      val keyEmit = emitWrite(keyVar, s"$childIdx + $jVar", keySrcExpr, mt.keyType, ctx)
      val valEmit = emitWrite(valVar, s"$childIdx + $jVar", valSrcExpr, mt.valueType, ctx)
      val setup =
        (Seq(
          s"$structClass $entriesVar = ($structClass) $targetVec.getDataVector();",
          s"$keyClass $keyVar = ($keyClass) $entriesVar.getChildByOrdinal(0);",
          s"$valClass $valVar = ($valClass) $entriesVar.getChildByOrdinal(1);") ++
          Seq(keyEmit.setup, valEmit.setup).filter(_.nonEmpty)).mkString("\n")
      val valueWrite = if (mt.valueContainsNull) {
        s"""if ($valArr.isNullAt($jVar)) {
           |    $valVar.setNull($childIdx + $jVar);
           |  } else {
           |    ${valEmit.perRow}
           |  }""".stripMargin
      } else {
        valEmit.perRow
      }
      val perRow =
        s"""org.apache.spark.sql.catalyst.util.MapData $mapSrc = $source;
           |org.apache.spark.sql.catalyst.util.ArrayData $keyArr = $mapSrc.keyArray();
           |org.apache.spark.sql.catalyst.util.ArrayData $valArr = $mapSrc.valueArray();
           |int $nVar = $mapSrc.numElements();
           |int $childIdx = $targetVec.startNewValue($idx);
           |for (int $jVar = 0; $jVar < $nVar; $jVar++) {
           |  $entriesVar.setIndexDefined($childIdx + $jVar);
           |  ${keyEmit.perRow}
           |  $valueWrite
           |}
           |$targetVec.endValue($idx, $nVar);""".stripMargin
      OutputEmit(setup, perRow)
    case other =>
      throw new UnsupportedOperationException(
        s"CometBatchKernelCodegen.emitWrite: unsupported output type $other")
  }

  /**
   * Java expression that reads a typed value out of a `SpecializedGetters` (both `ArrayData` and
   * `InternalRow` implement it). Used by [[emitWrite]] to source each element/field for its
   * recursive inner write.
   */
  private def emitSpecializedGetterExpr(target: String, idx: String, elemType: DataType): String =
    elemType match {
      case BooleanType => s"$target.getBoolean($idx)"
      case ByteType => s"$target.getByte($idx)"
      case ShortType => s"$target.getShort($idx)"
      case IntegerType | DateType => s"$target.getInt($idx)"
      case LongType | TimestampType | TimestampNTZType => s"$target.getLong($idx)"
      case FloatType => s"$target.getFloat($idx)"
      case DoubleType => s"$target.getDouble($idx)"
      case dt: DecimalType => s"$target.getDecimal($idx, ${dt.precision}, ${dt.scale})"
      case _: StringType => s"$target.getUTF8String($idx)"
      case BinaryType => s"$target.getBinary($idx)"
      case ArrayType(_, _) => s"$target.getArray($idx)"
      case _: MapType => s"$target.getMap($idx)"
      case _: StructType =>
        val numFields = elemType.asInstanceOf[StructType].fields.length
        s"$target.getStruct($idx, $numFields)"
      case other =>
        throw new UnsupportedOperationException(
          s"CometBatchKernelCodegen.emitSpecializedGetterExpr: unsupported type $other")
    }

  /** `setup` is once-per-batch (typed child-vector casts); `perRow` runs per row. */
  private case class OutputEmit(setup: String, perRow: String)
}
