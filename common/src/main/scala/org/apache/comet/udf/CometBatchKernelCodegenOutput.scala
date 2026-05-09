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
import org.apache.arrow.vector.complex.{ListVector, StructVector}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampNTZType, TimestampType}

import org.apache.comet.CometArrowAllocator

/**
 * Output-side emitters for the Arrow-direct codegen kernel. Everything that writes a computed
 * value into an Arrow output vector lives here: [[allocateOutput]], [[outputWriter]] (the entry
 * point for the kernel's top-level write), [[emitWrite]] (recursive per-type write), the output
 * vector-class lookup, and the output-side type-support gate.
 *
 * Paired with [[CometBatchKernelCodegenInput]], which handles the symmetric input side.
 */
private[udf] object CometBatchKernelCodegenOutput {

  /**
   * Output types [[allocateOutput]] and [[outputWriter]] can materialize. Recursive: an
   * `ArrayType(inner)` is supported when `inner` is supported, so once we add Map their gate here
   * controls the cascade. `canHandle` uses this predicate so the serde fallback lines up with
   * what the emitter can actually produce.
   */
  def isSupportedOutputType(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType => true
    case FloatType | DoubleType => true
    case _: DecimalType => true
    case _: StringType | _: BinaryType => true
    case DateType | TimestampType | TimestampNTZType => true
    case ArrayType(inner, _) => isSupportedOutputType(inner)
    case st: StructType => st.fields.forall(f => isSupportedOutputType(f.dataType))
    // MapType: deliberately gated off until map output support lands. Flip to a recursive check
    // once `emitWrite` has a case for it.
    case _ => false
  }

  /**
   * Allocate an Arrow output vector matching the expression's `dataType`. Types map to the same
   * Arrow vector classes Comet uses elsewhere (see
   * `org.apache.spark.sql.comet.execution.arrow.ArrowWriters.createFieldWriter`) so writers on
   * the producer and consumer sides stay aligned. Timestamps pick `UTC` as the vector's timezone
   * string; Spark's internal representation is UTC microseconds regardless of session TZ, and the
   * value is the same long either way.
   *
   * For variable-length output types (`StringType`, `BinaryType`), callers can pass
   * `estimatedBytes` to pre-size the data buffer. This avoids `setSafe` reallocations mid-loop
   * when the default per-row estimate is too small (common on regex-replace-style workloads where
   * output size tracks input size). If the estimate is low, `setSafe` still handles growth
   * correctly; if it's high, the extra capacity is freed when the vector is closed.
   */
  def allocateOutput(
      dataType: DataType,
      name: String,
      numRows: Int,
      estimatedBytes: Int = -1): FieldVector =
    dataType match {
      case BooleanType =>
        val v = new BitVector(name, CometArrowAllocator)
        v.allocateNew(numRows)
        v
      case ByteType =>
        val v = new TinyIntVector(name, CometArrowAllocator)
        v.allocateNew(numRows)
        v
      case ShortType =>
        val v = new SmallIntVector(name, CometArrowAllocator)
        v.allocateNew(numRows)
        v
      case IntegerType =>
        val v = new IntVector(name, CometArrowAllocator)
        v.allocateNew(numRows)
        v
      case LongType =>
        val v = new BigIntVector(name, CometArrowAllocator)
        v.allocateNew(numRows)
        v
      case FloatType =>
        val v = new Float4Vector(name, CometArrowAllocator)
        v.allocateNew(numRows)
        v
      case DoubleType =>
        val v = new Float8Vector(name, CometArrowAllocator)
        v.allocateNew(numRows)
        v
      case dt: DecimalType =>
        val v = new DecimalVector(name, CometArrowAllocator, dt.precision, dt.scale)
        v.allocateNew(numRows)
        v
      case _: StringType =>
        val v = new VarCharVector(name, CometArrowAllocator)
        if (estimatedBytes > 0) {
          v.allocateNew(estimatedBytes.toLong, numRows)
        } else {
          v.allocateNew(numRows)
        }
        v
      case BinaryType =>
        val v = new VarBinaryVector(name, CometArrowAllocator)
        if (estimatedBytes > 0) {
          v.allocateNew(estimatedBytes.toLong, numRows)
        } else {
          v.allocateNew(numRows)
        }
        v
      case DateType =>
        val v = new DateDayVector(name, CometArrowAllocator)
        v.allocateNew(numRows)
        v
      case TimestampType =>
        val v = new TimeStampMicroTZVector(name, CometArrowAllocator, "UTC")
        v.allocateNew(numRows)
        v
      case TimestampNTZType =>
        val v = new TimeStampMicroVector(name, CometArrowAllocator)
        v.allocateNew(numRows)
        v
      case ArrayType(inner, _) =>
        // Complex-type output: allocate a ListVector with a freshly allocated inner vector of
        // the element type. The inner vector's own `allocateOutput` run sets up its buffers
        // (including the pre-sized byte estimate for variable-length element types). After
        // allocating the inner, we install it as the ListVector's data vector via
        // `addOrGetVector` and reserve `numRows` entries on the outer list (the offsets +
        // validity buffers).
        val list = new ListVector(
          name,
          CometArrowAllocator,
          FieldType.nullable(ArrowType.List.INSTANCE),
          null)
        val innerVec = allocateOutput(inner, s"$name.element", numRows, estimatedBytes)
        list.initializeChildrenFromFields(java.util.Collections.singletonList(innerVec.getField))
        // Transfer the freshly-allocated inner vector's buffers into the list's data-vector
        // slot. `addOrGetVector` is the standard Arrow pattern for attaching a pre-allocated
        // child; transferTo copies the buffer ownership without data copy.
        val dataVec = list.getDataVector.asInstanceOf[FieldVector]
        innerVec.makeTransferPair(dataVec).transfer()
        innerVec.close()
        list.setInitialCapacity(numRows)
        list.allocateNew()
        list
      case st: StructType =>
        // Complex-type output: allocate a StructVector with N typed children, one per field.
        // Mirrors the ArrayType pattern: pre-allocate each child recursively, install them via
        // `initializeChildrenFromFields`, then transfer each child's buffers into the struct's
        // slot. Each child's outer `name` includes the field name so Arrow field metadata and
        // downstream tooling (Arrow JSON, dictionary encoders) see the Spark field naming.
        val struct = new StructVector(
          name,
          CometArrowAllocator,
          FieldType.nullable(ArrowType.Struct.INSTANCE),
          null)
        val childVectors =
          st.fields.map(f =>
            allocateOutput(f.dataType, s"$name.${f.name}", numRows, estimatedBytes))
        val childFieldList = new java.util.ArrayList[Field]()
        childVectors.foreach(v => childFieldList.add(v.getField))
        struct.initializeChildrenFromFields(childFieldList)
        childVectors.zipWithIndex.foreach { case (childVec, ord) =>
          val dst = struct.getChildByOrdinal(ord).asInstanceOf[FieldVector]
          childVec.makeTransferPair(dst).transfer()
          childVec.close()
        }
        struct.setInitialCapacity(numRows)
        struct.allocateNew()
        struct
      case other =>
        throw new UnsupportedOperationException(
          s"CometBatchKernelCodegen: unsupported output type $other")
    }

  /**
   * Returns `(concreteVectorClassName, writeJavaSnippet)` for the expression's output type at the
   * root of the generated kernel. The snippet assumes `output` is already cast to the concrete
   * vector class, `i` is the current row index, and `$valueTerm` is the Java expression holding
   * the bound expression's evaluated value. Delegates to [[emitWrite]] for the actual snippet,
   * passing `"output"` and `"i"` as the root target and index. Kept as a separate entry point
   * because the orchestrator needs both the vector class (for the cast at the top of `process`)
   * and the snippet.
   */
  def outputWriter(
      dataType: DataType,
      valueTerm: String,
      ctx: CodegenContext): (String, String) = {
    val cls = outputVectorClass(dataType)
    val snippet = emitWrite("output", "i", valueTerm, dataType, ctx)
    (cls, snippet)
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
    case other =>
      throw new UnsupportedOperationException(
        s"CometBatchKernelCodegen.outputVectorClass: unsupported output type $other")
  }

  /**
   * Composable write emitter. Returns a Java snippet that writes the value produced by `source`
   * into vector `targetVec` at index `idx`, specialized on the Spark `dataType`.
   *
   * Compositional: the `ArrayType` and `StructType` cases emit recursive per-row writes whose
   * per-element / per-field writes recurse back into `emitWrite` with the child vector as the new
   * target. `MapType` case is not yet implemented and throws; adding it later is a case addition,
   * not a structural change, because the recursion already flows through this function.
   *
   * For scalar types the snippet emits the direct write, including the decimal short-value fast
   * path ([[DecimalOutputShortFastPath]]) and the UTF8 on-heap shortcut
   * ([[Utf8OutputOnHeapShortcut]]).
   */
  private def emitWrite(
      targetVec: String,
      idx: String,
      source: String,
      dataType: DataType,
      ctx: CodegenContext): String = dataType match {
    case BooleanType =>
      s"$targetVec.set($idx, $source ? 1 : 0);"
    case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | DateType |
        TimestampType | TimestampNTZType =>
      // All scalar primitives and date/time types share the direct `set(idx, value)` shape.
      // Spark's codegen already emits the correct primitive Java type for each; Arrow's
      // typed vectors accept the matching primitive in their `set` overloads.
      s"$targetVec.set($idx, $source);"
    case dt: DecimalType =>
      // Optimization: DecimalOutputShortFastPath.
      // For precision <= 18 the unscaled value fits in a signed long; pass it straight to
      // `DecimalVector.setSafe(int, long)` and skip the `java.math.BigDecimal` allocation
      // `setSafe(int, BigDecimal)` requires. For p > 18 the BigDecimal path is unavoidable.
      if (dt.precision <= 18) {
        s"$targetVec.setSafe($idx, $source.toUnscaledLong());"
      } else {
        s"$targetVec.setSafe($idx, $source.toJavaBigDecimal());"
      }
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
         |}""".stripMargin
    case BinaryType =>
      // Spark's BinaryType value is already a `byte[]`.
      s"$targetVec.setSafe($idx, $source, 0, $source.length);"
    case ArrayType(elementType, _) =>
      // Complex-type output: recursive per-row write.
      // Spark's `doGenCode` for ArrayType-returning expressions produces an `ArrayData` value
      // (usually `GenericArrayData` / `UnsafeArrayData`). We iterate its elements, write each
      // one into the Arrow `ListVector`'s child, and bracket with `startNewValue` /
      // `endValue`. The element write recurses through `emitWrite` on the list's child vector,
      // so any scalar we support becomes a valid array element. Nested complex types (Array of
      // Array, Array of Struct) work by the same recursion.
      val listVar = ctx.freshName("list")
      val childVar = ctx.freshName("child")
      val arrVar = ctx.freshName("arr")
      val nVar = ctx.freshName("n")
      val childIdx = ctx.freshName("cidx")
      val jVar = ctx.freshName("j")
      val listClass = classOf[ListVector].getName
      val childClass = outputVectorClass(elementType)
      val elemSource = specializedGetterExpr(arrVar, jVar, elementType)
      val innerWrite = emitWrite(childVar, s"$childIdx + $jVar", elemSource, elementType, ctx)
      s"""$listClass $listVar = ($listClass) $targetVec;
         |$childClass $childVar = ($childClass) $listVar.getDataVector();
         |org.apache.spark.sql.catalyst.util.ArrayData $arrVar = $source;
         |int $nVar = $arrVar.numElements();
         |int $childIdx = $listVar.startNewValue($idx);
         |for (int $jVar = 0; $jVar < $nVar; $jVar++) {
         |  if ($arrVar.isNullAt($jVar)) {
         |    $childVar.setNull($childIdx + $jVar);
         |  } else {
         |    $innerWrite
         |  }
         |}
         |$listVar.endValue($idx, $nVar);""".stripMargin
    case st: StructType =>
      // Complex-type output: recursive per-row write to a StructVector.
      // Spark's `doGenCode` for StructType-returning expressions produces an `InternalRow`
      // value (`GenericInternalRow` / `UnsafeRow` / ScalaUDF encoder output). We cast each
      // typed child vector once per row at the top of the snippet (no runtime dispatch per
      // field write) and emit one write per field, recursing through `emitWrite` on the
      // child vector. `StructVector` writes are flat-indexed (same `$idx` as the struct's
      // outer slot), so the field write uses `$idx` directly.
      //
      // Branchless optimization: for each field whose `nullable == false` on the
      // [[StructType]], we skip the `row.isNullAt($fi)` guard at source level. Non-nullable
      // fields in Spark are a contract that the producer does not emit nulls for that field,
      // and matching that contract here lets HotSpot emit a straight write path per field
      // rather than a branch.
      val structVar = ctx.freshName("struct")
      val rowVar = ctx.freshName("row")
      val structClass = classOf[StructVector].getName
      val perField = st.fields.zipWithIndex.map { case (field, fi) =>
        val childVar = ctx.freshName("child")
        val childClass = outputVectorClass(field.dataType)
        val decl =
          s"$childClass $childVar = ($childClass) $structVar.getChildByOrdinal($fi);"
        val fieldSource = specializedGetterExpr(rowVar, fi.toString, field.dataType)
        val innerWrite = emitWrite(childVar, idx, fieldSource, field.dataType, ctx)
        val write =
          if (!field.nullable) {
            innerWrite
          } else {
            s"""if ($rowVar.isNullAt($fi)) {
               |  $childVar.setNull($idx);
               |} else {
               |  $innerWrite
               |}""".stripMargin
          }
        (decl, write)
      }
      val childDecls = perField.map(_._1).mkString("\n")
      val perFieldWrites = perField.map(_._2).mkString("\n")
      s"""$structClass $structVar = ($structClass) $targetVec;
         |org.apache.spark.sql.catalyst.InternalRow $rowVar = $source;
         |$structVar.setIndexDefined($idx);
         |$childDecls
         |$perFieldWrites""".stripMargin
    case _: MapType =>
      throw new UnsupportedOperationException(
        "CometBatchKernelCodegen.emitWrite: MapType output not yet implemented")
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
  private def specializedGetterExpr(target: String, idx: String, elemType: DataType): String =
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
          s"CometBatchKernelCodegen.specializedGetterExpr: unsupported type $other")
    }
}
