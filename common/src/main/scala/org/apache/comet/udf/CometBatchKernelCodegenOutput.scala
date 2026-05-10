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

package org.apache.comet.udf

import org.apache.arrow.vector.{BigIntVector, BitVector, DateDayVector, DecimalVector, FieldVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TimeStampMicroTZVector, TimeStampMicroVector, TinyIntVector, VarBinaryVector, VarCharVector}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampNTZType, TimestampType}

import org.apache.comet.CometArrowAllocator

/**
 * Output-side emitters for the Arrow-direct codegen kernel. Everything that writes a computed
 * value into an Arrow output vector lives here: [[allocateOutput]], [[emitOutputWriter]] (the
 * entry point for the kernel's top-level write), [[emitWrite]] (recursive per-type write), the
 * output vector-class lookup, and the output-side type-support gate.
 *
 * Paired with [[CometBatchKernelCodegenInput]], which handles the symmetric input side.
 */
private[udf] object CometBatchKernelCodegenOutput {

  /**
   * Output types [[allocateOutput]] and [[emitOutputWriter]] can materialize. Recursive: complex
   * types are supported when their children are.
   */
  def isSupportedOutputType(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType => true
    case FloatType | DoubleType => true
    case _: DecimalType => true
    case _: StringType | _: BinaryType => true
    case DateType | TimestampType | TimestampNTZType => true
    case ArrayType(inner, _) => isSupportedOutputType(inner)
    case st: StructType => st.fields.forall(f => isSupportedOutputType(f.dataType))
    case mt: MapType =>
      isSupportedOutputType(mt.keyType) && isSupportedOutputType(mt.valueType)
    case _ => false
  }

  /**
   * Allocate an Arrow output vector matching `dataType`. Delegates field and vector construction
   * to [[Utils.toArrowField]] + `Field.createVector`, which is the pattern the rest of Comet uses
   * to go Spark -> Arrow and handles complex-type wiring (including Arrow's non-null-key and
   * non-null-entries invariants on `MapVector`).
   *
   * For variable-length scalar outputs (`StringType`, `BinaryType`), callers can pass
   * `estimatedBytes` to pre-size the data buffer and avoid `setSafe` reallocation mid-loop. The
   * hint is only applied when the root vector is `VarCharVector` or `VarBinaryVector`; inside a
   * `ListVector` / `StructVector` / `MapVector`, the parent's `allocateNew` reallocates child
   * buffers at default size, so a leaf hint would be lost.
   *
   * Closes the vector on any failure between construction and return so a partially-initialized
   * tree does not leak buffers back to the allocator.
   */
  def allocateOutput(
      dataType: DataType,
      name: String,
      numRows: Int,
      estimatedBytes: Int = -1): FieldVector = {
    val field = Utils.toArrowField(name, dataType, nullable = true, "UTC")
    val vec = field.createVector(CometArrowAllocator).asInstanceOf[FieldVector]
    try {
      vec.setInitialCapacity(numRows)
      vec match {
        case v: VarCharVector if estimatedBytes > 0 =>
          v.allocateNew(estimatedBytes.toLong, numRows)
        case v: VarBinaryVector if estimatedBytes > 0 =>
          v.allocateNew(estimatedBytes.toLong, numRows)
        case _ =>
          vec.allocateNew()
      }
      vec
    } catch {
      case t: Throwable =>
        try vec.close()
        catch { case _: Throwable => () }
        throw t
    }
  }

  /**
   * Split output for a complex-type write: `setup` holds once-per-batch declarations (typed
   * child-vector casts) and lives outside the per-row for-loop; `perRow` holds the statements
   * executed for each row. Scalar writes have empty setup.
   */
  private case class OutputEmit(setup: String, perRow: String)

  /**
   * Returns `(concreteVectorClassName, batchSetup, perRowSnippet)` for the expression's output
   * type at the root of the generated kernel. `output` is already cast to
   * `concreteVectorClassName` in `process`'s prelude, so `emitWrite`'s complex-type branches can
   * hoist child casts straight off `output` without re-casting it per row.
   */
  def emitOutputWriter(
      dataType: DataType,
      valueTerm: String,
      ctx: CodegenContext): (String, String, String) = {
    val cls = outputVectorClass(dataType)
    val emit = emitWrite("output", "i", valueTerm, dataType, ctx)
    (cls, emit.setup, emit.perRow)
  }

  /**
   * Concrete Arrow vector class name for the given output type. The name is used to cast `outRaw`
   * to the right type at the top of the generated `process` method, so that subsequent writes
   * through `emitWrite` can call vector-specific methods without further casts.
   */
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
   * typed child-vector casts (hoisted above the `process` for-loop) and whose `perRow` writes the
   * value produced by `source` into `targetVec` at index `idx`. `targetVec` is assumed to be
   * already typed to the concrete Arrow vector class for `dataType` at the call site (via the
   * prelude cast in `process` for the root, or via a setup cast declared by the caller for nested
   * children).
   *
   * Scalars emit `perRow` only; complex types (`ArrayType` / `StructType` / `MapType`) emit both
   * setup (child-vector casts) and perRow (loops, null guards, recursive writes). Inner
   * `emitWrite` calls return their own setup, which the outer caller concatenates so child-of-
   * child casts bubble up to the batch prelude.
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
      // All scalar primitives and date/time types share the direct `set(idx, value)` shape.
      // Spark's codegen already emits the correct primitive Java type for each; Arrow's
      // typed vectors accept the matching primitive in their `set` overloads.
      OutputEmit("", s"$targetVec.set($idx, $source);")
    case dt: DecimalType =>
      // Optimization: DecimalOutputShortFastPath.
      // For precision <= 18 the unscaled value fits in a signed long; pass it straight to
      // `DecimalVector.setSafe(int, long)` and skip the `java.math.BigDecimal` allocation
      // `setSafe(int, BigDecimal)` requires. For p > 18 the BigDecimal path is unavoidable.
      val write =
        if (dt.precision <= 18) s"$targetVec.setSafe($idx, $source.toUnscaledLong());"
        else s"$targetVec.setSafe($idx, $source.toJavaBigDecimal());"
      OutputEmit("", write)
    case _: StringType =>
      // Optimization: Utf8OutputOnHeapShortcut.
      // `UTF8String` is internally a `(base, offset, numBytes)` view. When the base is a
      // `byte[]` (common case: Spark string functions allocate results on-heap), pass the
      // existing byte[] directly to `VarCharVector.setSafe(int, byte[], int, int)` via the
      // encoded offset and skip the redundant `getBytes()` allocation. Off-heap passthrough
      // (rare on output side) falls back to `getBytes()`.
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
      // Spark's BinaryType value is already a `byte[]`.
      OutputEmit("", s"$targetVec.setSafe($idx, $source, 0, $source.length);")
    case ArrayType(elementType, _) =>
      // Complex-type output: recursive per-row write.
      // Spark's `doGenCode` for ArrayType-returning expressions produces an `ArrayData` value
      // (usually `GenericArrayData` / `UnsafeArrayData`). We iterate its elements, write each
      // one into the Arrow `ListVector`'s child, and bracket with `startNewValue` /
      // `endValue`. The element write recurses through `emitWrite` on the list's child vector,
      // so any scalar we support becomes a valid array element. Nested complex types (Array of
      // Array, Array of Struct) work by the same recursion. `targetVec` is a `ListVector` at
      // the call site (either `output` at root or a hoisted child cast); we only need to cast
      // its data vector, and that cast goes into setup.
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
      val perRow =
        s"""org.apache.spark.sql.catalyst.util.ArrayData $arrVar = $source;
           |int $nVar = $arrVar.numElements();
           |int $childIdx = $targetVec.startNewValue($idx);
           |for (int $jVar = 0; $jVar < $nVar; $jVar++) {
           |  if ($arrVar.isNullAt($jVar)) {
           |    $childVar.setNull($childIdx + $jVar);
           |  } else {
           |    ${inner.perRow}
           |  }
           |}
           |$targetVec.endValue($idx, $nVar);""".stripMargin
      OutputEmit(setup, perRow)
    case st: StructType =>
      // Complex-type output: recursive per-row write to a StructVector.
      // Spark's `doGenCode` for StructType-returning expressions produces an `InternalRow`
      // value (`GenericInternalRow` / `UnsafeRow` / ScalaUDF encoder output). Typed child-vector
      // casts are hoisted to setup (once per batch); the per-row body references the hoisted
      // names. `StructVector` writes are flat-indexed (same `$idx` as the struct's outer slot).
      //
      // Branchless optimization: for each field whose `nullable == false` on the
      // [[StructType]], we skip the `row.isNullAt($fi)` guard at source level. Non-nullable
      // fields in Spark are a contract that the producer does not emit nulls for that field,
      // and matching that contract here lets HotSpot emit a straight write path per field
      // rather than a branch.
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
      // Complex-type output: recursive per-row write to a MapVector.
      // Spark's `doGenCode` for MapType-returning expressions produces a `MapData` value
      // (`ArrayBasedMapData` / `UnsafeMapData` / ScalaUDF encoder output). Typed child-vector
      // casts for the entries struct and the key/value children are hoisted to setup (once per
      // batch); the per-row body references them.
      //
      // Per-row shape:
      //   1. Read keyArray / valueArray from the MapData source.
      //   2. Open a new map entry via `startNewValue(idx)`; returns the base index into the
      //      entries StructVector for this row's key/value pairs.
      //   3. For each key/value pair: set the entries struct slot defined (map values can be
      //      null, but the struct slot itself is defined), write the key (always non-null by
      //      Spark/Arrow invariant), then write the value with a null-guard on
      //      `vals.isNullAt(j)`. Both writes recurse through `emitWrite`.
      //   4. Close the map entry with `endValue(idx, n)`.
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
      val perRow =
        s"""org.apache.spark.sql.catalyst.util.MapData $mapSrc = $source;
           |org.apache.spark.sql.catalyst.util.ArrayData $keyArr = $mapSrc.keyArray();
           |org.apache.spark.sql.catalyst.util.ArrayData $valArr = $mapSrc.valueArray();
           |int $nVar = $mapSrc.numElements();
           |int $childIdx = $targetVec.startNewValue($idx);
           |for (int $jVar = 0; $jVar < $nVar; $jVar++) {
           |  $entriesVar.setIndexDefined($childIdx + $jVar);
           |  ${keyEmit.perRow}
           |  if ($valArr.isNullAt($jVar)) {
           |    $valVar.setNull($childIdx + $jVar);
           |  } else {
           |    ${valEmit.perRow}
           |  }
           |}
           |$targetVec.endValue($idx, $nVar);""".stripMargin
      OutputEmit(setup, perRow)
    case other =>
      throw new UnsupportedOperationException(
        s"CometBatchKernelCodegen.emitWrite: unsupported output type $other")
  }

  /**
   * Java expression that reads a typed value out of a Spark `SpecializedGetters` reference (which
   * both `ArrayData` and `InternalRow` implement) at a given ordinal/index. Used by the
   * `ArrayType` and `StructType` branches of [[emitWrite]] to source each element / field for its
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
}
