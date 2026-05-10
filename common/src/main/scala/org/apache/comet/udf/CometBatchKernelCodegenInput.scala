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

import scala.collection.mutable

import org.apache.arrow.vector.{BigIntVector, BitVector, DateDayVector, DecimalVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TimeStampMicroTZVector, TimeStampMicroVector, TinyIntVector, VarBinaryVector, VarCharVector}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampNTZType, TimestampType}

import org.apache.comet.udf.CometBatchKernelCodegen.{ArrayColumnSpec, ArrowColumnSpec, MapColumnSpec, ScalarColumnSpec, StructColumnSpec}
import org.apache.comet.vector.CometPlainVector

/**
 * Input-side emitters for the Arrow-direct codegen kernel. Everything that generates source for
 * reading Arrow input into Spark's typed getter surface lives here: kernel field declarations,
 * per-batch input casts, top-level typed-getter switches, nested `InputArray_${path}` /
 * `InputStruct_${path}` / `InputMap_${path}` classes at every complex level, and the input-side
 * type-support gate.
 *
 * ==Path encoding for nested complex types==
 *
 * Each position in a spec tree has a unique path string, used as the suffix on typed vector
 * fields and as the identifier on nested classes. Starting from the column ordinal:
 *
 *   - root: `col${ord}`
 *   - array element of `P`: `${P}_e`
 *   - struct field `fi` of `P`: `${P}_f${fi}`
 *   - map key of `P`: `${P}_k`
 *   - map value of `P`: `${P}_v`
 *
 * ==Nested-class composition==
 *
 * A nested class at path `P` represents a Spark `ArrayData`, `InternalRow`, or `MapData` view of
 * its Arrow vector. For any complex child one level down, the class holds a pre-allocated
 * instance of the corresponding inner nested class and routes `getArray` / `getStruct` / `getMap`
 * / `keyArray` / `valueArray` calls to that instance after resetting it. N-deep nesting falls out
 * of this: each level only knows about its immediate children.
 *
 * ==Unified reset protocol==
 *
 * `InputArray_${path}` and `InputMap_${path}` classes both take `reset(int startIdx, int length)`
 * and simply capture the slice. Callers (kernel top-level switches, outer complex-getter routers,
 * map `keyArray` / `valueArray` returns) compute `(startIdx, length)` from the appropriate parent
 * offsets before calling `reset`. This unifies the view shape across list-backed arrays and map
 * key/value slices. Structs stay flat-indexed: `InputStruct_${path}` has `reset(int rowIdx)` that
 * just captures the outer row index.
 *
 * Paired with [[CometBatchKernelCodegenOutput]], which handles the symmetric output side.
 */
private[udf] object CometBatchKernelCodegenInput {

  /**
   * Input types the kernel has a typed getter for. Recursive: `ArrayType(inner)` supported when
   * `inner` is supported; `StructType` when every field is; `MapType` when key and value types
   * are both supported.
   */
  def isSupportedInputType(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType => true
    case FloatType | DoubleType => true
    case _: DecimalType => true
    case _: StringType | _: BinaryType => true
    case DateType | TimestampType | TimestampNTZType => true
    case ArrayType(inner, _) => isSupportedInputType(inner)
    case st: StructType => st.fields.forall(f => isSupportedInputType(f.dataType))
    case mt: MapType => isSupportedInputType(mt.keyType) && isSupportedInputType(mt.valueType)
    case _ => false
  }

  /**
   * Emit the kernel's typed vector-field declarations for every level of every input column's
   * spec tree. Top-level complex columns additionally get an instance-field declaration for the
   * pre-allocated nested class. Instance fields for nested-class children one level down live
   * inside the parent nested class.
   */
  def emitInputFieldDecls(inputSchema: Seq[ArrowColumnSpec]): String = {
    val lines = new mutable.ArrayBuffer[String]()
    inputSchema.zipWithIndex.foreach { case (spec, ord) =>
      val path = s"col$ord"
      collectVectorFieldDecls(path, spec, topLevel = true, lines)
      collectTopLevelInstanceDecl(path, spec, lines)
    }
    lines.mkString("\n  ")
  }

  /**
   * Primitive Arrow vector classes that we wrap in [[CometPlainVector]] at the kernel's input-
   * cast time. `CometPlainVector.get*` reads use `Platform.get*` against a `final long` buffer
   * address, so JIT inlines them to branchless reads with no per-call `ArrowBuf` dereference.
   * `CometPlainVector.getBoolean` also includes a bit-packed data-byte cache that collapses 8
   * sequential bit reads to 1 byte read.
   *
   * Not wrapped: `DecimalVector` (kernel emits inline unsafe reads keyed on compile-time
   * precision, so the fast/slow split stays branchless in the emitted Java rather than branching
   * at runtime inside `CometPlainVector.getDecimal`), `VarCharVector` / `VarBinaryVector` (kernel
   * emits inline unsafe reads to avoid the redundant `isNullAt` check inside
   * `CometPlainVector.getUTF8String` / `getBinary`).
   */
  private val primitiveArrowClasses: Set[Class[_]] = Set(
    classOf[BitVector],
    classOf[TinyIntVector],
    classOf[SmallIntVector],
    classOf[IntVector],
    classOf[BigIntVector],
    classOf[Float4Vector],
    classOf[Float8Vector],
    classOf[DateDayVector],
    classOf[TimeStampMicroVector],
    classOf[TimeStampMicroTZVector])

  private def wrapsInCometPlainVector(cls: Class[_]): Boolean =
    primitiveArrowClasses.contains(cls)

  /**
   * Non-wrapped scalar columns that want a cached data-buffer address for inline unsafe reads.
   * `DecimalVector` uses it for the short-precision fast path (`Platform.getLong`);
   * `VarCharVector` / `VarBinaryVector` use it as the base address for `UTF8String.fromAddress` /
   * `Platform.copyMemory`. See the unsafe-emitter block at the bottom of this file for why we
   * inline rather than reuse `CometPlainVector`.
   */
  private def needsValueAddrField(cls: Class[_]): Boolean =
    cls == classOf[DecimalVector] ||
      cls == classOf[VarCharVector] ||
      cls == classOf[VarBinaryVector]

  /** Variable-width columns also want the offset-buffer address cached for `Platform.getInt`. */
  private def needsOffsetAddrField(cls: Class[_]): Boolean =
    cls == classOf[VarCharVector] || cls == classOf[VarBinaryVector]

  private val cometPlainVectorName: String = classOf[CometPlainVector].getName

  private def collectVectorFieldDecls(
      path: String,
      spec: ArrowColumnSpec,
      topLevel: Boolean,
      out: mutable.ArrayBuffer[String]): Unit = spec match {
    case sc: ScalarColumnSpec =>
      // CometPlainVector wrapping and cached-address fields apply only at the kernel's top
      // level. Nested-class children stay on Arrow typed fields because their generated method
      // bodies (inside `InputArray_*` / `InputStruct_*` / `InputMap_*`) call Arrow-style
      // `.isNull(i)` / `.get(i)`; converting those too is Phase D.
      val fieldClass =
        if (topLevel && wrapsInCometPlainVector(sc.vectorClass)) cometPlainVectorName
        else sc.vectorClass.getName
      out += s"private $fieldClass $path;"
      if (topLevel && needsValueAddrField(sc.vectorClass)) {
        out += s"private long ${path}_valueAddr;"
      }
      if (topLevel && needsOffsetAddrField(sc.vectorClass)) {
        out += s"private long ${path}_offsetAddr;"
      }
    case ar: ArrayColumnSpec =>
      out += s"private ${classOf[ListVector].getName} $path;"
      collectVectorFieldDecls(s"${path}_e", ar.element, topLevel = false, out)
    case st: StructColumnSpec =>
      out += s"private ${classOf[StructVector].getName} $path;"
      st.fields.zipWithIndex.foreach { case (f, fi) =>
        collectVectorFieldDecls(s"${path}_f$fi", f.child, topLevel = false, out)
      }
    case mp: MapColumnSpec =>
      out += s"private ${classOf[MapVector].getName} $path;"
      // Key and value vectors live at `${P}_k_e` / `${P}_v_e` so the `InputArray_${P}_k` /
      // `InputArray_${P}_v` synthetic classes (which follow the array-element convention of
      // reading from `${path}_e`) resolve their element reads correctly.
      collectVectorFieldDecls(s"${path}_k_e", mp.key, topLevel = false, out)
      collectVectorFieldDecls(s"${path}_v_e", mp.value, topLevel = false, out)
  }

  private def collectTopLevelInstanceDecl(
      path: String,
      spec: ArrowColumnSpec,
      out: mutable.ArrayBuffer[String]): Unit = spec match {
    case _: ScalarColumnSpec => ()
    case _: ArrayColumnSpec =>
      out += s"private final InputArray_$path ${path}_arrayData = new InputArray_$path();"
    case _: StructColumnSpec =>
      out += s"private final InputStruct_$path ${path}_structData = new InputStruct_$path();"
    case _: MapColumnSpec =>
      out += s"private final InputMap_$path ${path}_mapData = new InputMap_$path();"
  }

  /**
   * Emit the per-batch cast statements. For a map column, casts the outer `MapVector`, then casts
   * the inner `StructVector` (via a local variable) to extract key and value children via
   * `getChildByOrdinal(0)` / `(1)`. For arrays, casts the outer `ListVector` and recurses via
   * `getDataVector()`. For structs, casts the outer `StructVector` and recurses via
   * `getChildByOrdinal(fi)`.
   */
  def emitInputCasts(inputSchema: Seq[ArrowColumnSpec]): String = {
    val lines = new mutable.ArrayBuffer[String]()
    inputSchema.zipWithIndex.foreach { case (spec, ord) =>
      val path = s"col$ord"
      collectCasts(path, spec, s"inputs[$ord]", topLevel = true, lines)
    }
    lines.mkString("\n    ")
  }

  private def collectCasts(
      path: String,
      spec: ArrowColumnSpec,
      source: String,
      topLevel: Boolean,
      out: mutable.ArrayBuffer[String]): Unit = spec match {
    case sc: ScalarColumnSpec =>
      if (topLevel && wrapsInCometPlainVector(sc.vectorClass)) {
        // Wrap in CometPlainVector so per-row reads go through Platform.get* against a final
        // long buffer address. JIT inlines the one-liner getters, treating the address as a
        // register-cached constant across the process loop. useDecimal128 = true matches Spark's
        // 128-bit decimal storage.
        out += s"this.$path = new $cometPlainVectorName($source, true);"
      } else {
        out += s"this.$path = (${sc.vectorClass.getName}) $source;"
      }
      // Address caching applies only at the kernel top level; nested-class reads still go
      // through Arrow typed getters (Phase D).
      if (topLevel && needsValueAddrField(sc.vectorClass)) {
        out += s"this.${path}_valueAddr = this.$path.getDataBuffer().memoryAddress();"
      }
      if (topLevel && needsOffsetAddrField(sc.vectorClass)) {
        out += s"this.${path}_offsetAddr = this.$path.getOffsetBuffer().memoryAddress();"
      }
    case ar: ArrayColumnSpec =>
      out += s"this.$path = (${classOf[ListVector].getName}) $source;"
      collectCasts(s"${path}_e", ar.element, s"this.$path.getDataVector()", topLevel = false, out)
    case st: StructColumnSpec =>
      out += s"this.$path = (${classOf[StructVector].getName}) $source;"
      st.fields.zipWithIndex.foreach { case (f, fi) =>
        collectCasts(
          s"${path}_f$fi",
          f.child,
          s"this.$path.getChildByOrdinal($fi)",
          topLevel = false,
          out)
      }
    case mp: MapColumnSpec =>
      // MapVector's data vector is a StructVector with key at child 0 and value at child 1.
      // Grab the struct through a local var and pull out the typed children. The key / value
      // vectors live at the `_k_e` / `_v_e` paths so the synthetic `InputArray_${P}_k` /
      // `InputArray_${P}_v` classes read them via the standard array-element convention.
      val structLocal = s"${path}__mapStruct"
      out += s"this.$path = (${classOf[MapVector].getName}) $source;"
      out += s"${classOf[StructVector].getName} $structLocal = " +
        s"(${classOf[StructVector].getName}) this.$path.getDataVector();"
      collectCasts(
        s"${path}_k_e",
        mp.key,
        s"$structLocal.getChildByOrdinal(0)",
        topLevel = false,
        out)
      collectCasts(
        s"${path}_v_e",
        mp.value,
        s"$structLocal.getChildByOrdinal(1)",
        topLevel = false,
        out)
  }

  /**
   * Emit the kernel's typed-getter overrides. Spark's `InternalRow` provides the base virtual
   * method; the `@Override` on a final class gives the JIT enough information to devirtualize.
   * Each getter switches on the column ordinal so the call site (with an inlined constant ordinal
   * from `BoundReference.genCode`) folds down to a single branch.
   *
   * `decimalTypeByOrdinal` lets the decimal getter specialize per ordinal: when a
   * `BoundReference` of `DecimalType(precision <= 18)` is the only decimal read at that ordinal,
   * the emitted case skips the `BigDecimal` allocation and reads the unscaled long directly.
   *
   * TODO(unsafe-readers): primitive `v.get(i)` performs a bounds check that is redundant given `i
   * in [0, numRows)`. See `docs/source/contributor-guide/jvm_udf_dispatch.md#open-items`.
   */
  def emitTypedGetters(
      inputSchema: Seq[ArrowColumnSpec],
      decimalTypeByOrdinal: Map[Int, Option[DecimalType]]): String = {
    val withOrd = inputSchema.zipWithIndex

    val isNullCases = withOrd.map { case (spec, ord) =>
      if (!spec.nullable) {
        s"      case $ord: return false;"
      } else {
        // CometPlainVector exposes `isNullAt`; Arrow-typed fields expose `isNull`. Both check
        // the validity bitmap with the same semantics.
        val method = spec.vectorClass match {
          case cls if wrapsInCometPlainVector(cls) => "isNullAt"
          case _ => "isNull"
        }
        s"      case $ord: return this.col$ord.$method(this.rowIdx);"
      }
    }

    val booleanCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[BitVector] =>
        s"      case $ord: return this.col$ord.getBoolean(this.rowIdx);"
    }
    val byteCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[TinyIntVector] =>
        s"      case $ord: return this.col$ord.getByte(this.rowIdx);"
    }
    val shortCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[SmallIntVector] =>
        s"      case $ord: return this.col$ord.getShort(this.rowIdx);"
    }
    val intCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord)
          if cls == classOf[IntVector] || cls == classOf[DateDayVector] =>
        s"      case $ord: return this.col$ord.getInt(this.rowIdx);"
    }
    val longCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord)
          if cls == classOf[BigIntVector] ||
            cls == classOf[TimeStampMicroVector] ||
            cls == classOf[TimeStampMicroTZVector] =>
        s"      case $ord: return this.col$ord.getLong(this.rowIdx);"
    }
    val floatCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[Float4Vector] =>
        s"      case $ord: return this.col$ord.getFloat(this.rowIdx);"
    }
    val doubleCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[Float8Vector] =>
        s"      case $ord: return this.col$ord.getDouble(this.rowIdx);"
    }
    val decimalCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[DecimalVector] =>
        val known = decimalTypeByOrdinal.getOrElse(ord, None)
        val valueAddr = s"this.col${ord}_valueAddr"
        val slowField = s"this.col$ord"
        val fastPath = emitDecimalFastBodyUnsafe(valueAddr, "this.rowIdx", "        ")
        val slowPath = emitDecimalSlowBody(slowField, "this.rowIdx", "        ")
        val body = known match {
          case Some(dt) if dt.precision <= 18 => fastPath
          case Some(_) => slowPath
          case None =>
            s"""        if (precision <= 18) {
               |$fastPath
               |        } else {
               |$slowPath
               |        }""".stripMargin
        }
        s"""      case $ord: {
           |$body
           |      }""".stripMargin
    }
    val binaryCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[VarBinaryVector] =>
        s"""      case $ord: {
           |${emitBinaryBodyUnsafe(
            s"this.col${ord}_valueAddr",
            s"this.col${ord}_offsetAddr",
            "this.rowIdx",
            "        ")}
           |      }""".stripMargin
    }
    val utf8Cases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[VarCharVector] =>
        s"""      case $ord: {
           |${emitUtf8BodyUnsafe(
            s"this.col${ord}_valueAddr",
            s"this.col${ord}_offsetAddr",
            "this.rowIdx",
            "        ")}
           |      }""".stripMargin
    }

    Seq(
      emitOrdinalSwitch("public boolean isNullAt(int ordinal)", "isNullAt", isNullCases),
      emitOrdinalSwitch("public boolean getBoolean(int ordinal)", "getBoolean", booleanCases),
      emitOrdinalSwitch("public byte getByte(int ordinal)", "getByte", byteCases),
      emitOrdinalSwitch("public short getShort(int ordinal)", "getShort", shortCases),
      emitOrdinalSwitch("public int getInt(int ordinal)", "getInt", intCases),
      emitOrdinalSwitch("public long getLong(int ordinal)", "getLong", longCases),
      emitOrdinalSwitch("public float getFloat(int ordinal)", "getFloat", floatCases),
      emitOrdinalSwitch("public double getDouble(int ordinal)", "getDouble", doubleCases),
      emitOrdinalSwitch(
        "public org.apache.spark.sql.types.Decimal getDecimal(" +
          "int ordinal, int precision, int scale)",
        "getDecimal",
        decimalCases),
      emitOrdinalSwitch("public byte[] getBinary(int ordinal)", "getBinary", binaryCases),
      emitOrdinalSwitch(
        "public org.apache.spark.unsafe.types.UTF8String getUTF8String(int ordinal)",
        "getUTF8String",
        utf8Cases)).mkString
  }

  /**
   * Build a per-ordinal map of the `DecimalType` observed on `BoundReference`s in the bound
   * expression. Used by [[emitTypedGetters]] to emit a compile-time-specialized `getDecimal` case
   * per ordinal.
   */
  def decimalPrecisionByOrdinal(boundExpr: Expression): Map[Int, Option[DecimalType]] = {
    boundExpr
      .collect {
        case b: BoundReference if b.dataType.isInstanceOf[DecimalType] =>
          b.ordinal -> b.dataType.asInstanceOf[DecimalType]
      }
      .groupBy(_._1)
      .map { case (ord, pairs) =>
        val distinct = pairs.map(_._2).toSet
        ord -> (if (distinct.size == 1) Some(distinct.head) else None)
      }
  }

  /**
   * Emit every nested class needed for every complex level of every input column. For an
   * `ArrayColumnSpec` we emit `InputArray_${path}`; for a `StructColumnSpec`
   * `InputStruct_${path}`; for a `MapColumnSpec` `InputMap_${path}` plus the `InputArray` classes
   * for the key and value slices (because Spark's `MapData.keyArray()` / `valueArray()` return
   * `ArrayData` - same view shape as any other array).
   */
  def emitNestedClasses(inputSchema: Seq[ArrowColumnSpec]): String = {
    val out = new mutable.ArrayBuffer[String]()
    inputSchema.zipWithIndex.foreach { case (spec, ord) =>
      collectNestedClasses(s"col$ord", spec, out)
    }
    out.mkString("\n")
  }

  private def collectNestedClasses(
      path: String,
      spec: ArrowColumnSpec,
      out: mutable.ArrayBuffer[String]): Unit = spec match {
    case _: ScalarColumnSpec => ()
    case ar: ArrayColumnSpec =>
      out += emitArrayClass(path, ar)
      collectNestedClasses(s"${path}_e", ar.element, out)
    case st: StructColumnSpec =>
      out += emitStructClass(path, st)
      st.fields.zipWithIndex.foreach { case (f, fi) =>
        collectNestedClasses(s"${path}_f$fi", f.child, out)
      }
    case mp: MapColumnSpec =>
      out += emitMapClass(path, mp)
      // Emit InputArray_${path}_k and InputArray_${path}_v - the ArrayData views returned by
      // `MapData.keyArray()` / `valueArray()`. They follow the standard array-element
      // convention: each reads from `${classPath}_e` which maps to the key / value vector
      // emitted at `${path}_k_e` / `${path}_v_e` by [[collectVectorFieldDecls]]. Instance
      // fields for complex key / value elements (one level deeper) live inside these array
      // classes via [[instanceDeclaration]].
      out += emitArrayClass(
        s"${path}_k",
        ArrayColumnSpec(nullable = true, elementSparkType = mp.keySparkType, element = mp.key))
      out += emitArrayClass(
        s"${path}_v",
        ArrayColumnSpec(
          nullable = true,
          elementSparkType = mp.valueSparkType,
          element = mp.value))
      // Recurse into the key / value specs at their canonical paths (${path}_k_e /
      // ${path}_v_e) so nested complex keys / values get their own nested classes.
      collectNestedClasses(s"${path}_k_e", mp.key, out)
      collectNestedClasses(s"${path}_v_e", mp.value, out)
  }

  // ---------------------------------------------------------------------------------------------
  // Shared helpers for complex-getter routing. A "list-backed child reset" computes
  // `(startIdx, length)` for an inner instance from a ListVector / MapVector's offsets at a
  // parent-provided index and calls `reset(startIdx, length)`.
  // ---------------------------------------------------------------------------------------------

  private def emitListBackedChildReset(
      parentVectorPath: String,
      indexExpr: String,
      innerInstanceField: String): String =
    s"""        int __idx = $indexExpr;
       |        int __s = $parentVectorPath.getElementStartIndex(__idx);
       |        int __e = $parentVectorPath.getElementEndIndex(__idx);
       |        $innerInstanceField.reset(__s, __e - __s);""".stripMargin

  /**
   * Emit one `InputArray_${path}` nested class. Unified slice-based reset: callers pass
   * `(startIdx, length)` directly.
   *
   * Key/value arrays of a map share this exact shape - the instance fields for their complex
   * elements (if any) are emitted from [[emitArrayElementGetter]]; the vector fields they read
   * from are at `${path}_e` (following the array-element path convention), which maps to
   * `col${N}_k_e` or `col${N}_v_e` when the array represents a map key/value slice.
   *
   * NOTE: when this class is used for a map's key or value view and the underlying key/value is
   * scalar, there is no `${path}_e` vector field - the map's key/value vector sits at `${path}`
   * itself (e.g. `col0_k`). See [[emitArrayElementGetter]] for how that is handled: scalar
   * element emission reads from `${path}_e`, but for map views the element vector IS the path
   * itself. We rename the element path in [[emitMapClass]] below.
   */
  private def emitArrayClass(path: String, spec: ArrayColumnSpec): String = {
    val baseClassName = classOf[CometArrayData].getName
    val elemPath = s"${path}_e"
    val innerInstance = instanceDeclaration(elemPath, spec.element)
    val isNullAt =
      s"""      @Override
         |      public boolean isNullAt(int i) {
         |        return $elemPath.isNull(startIndex + i);
         |      }""".stripMargin
    val elementGetter = emitArrayElementGetter(path, spec)
    s"""  private final class InputArray_$path extends $baseClassName {
       |    private int startIndex;
       |    private int length;
       |$innerInstance
       |
       |    void reset(int startIdx, int len) {
       |      this.startIndex = startIdx;
       |      this.length = len;
       |    }
       |
       |    @Override
       |    public int numElements() {
       |      return length;
       |    }
       |
       |$isNullAt
       |
       |$elementGetter
       |  }
       |""".stripMargin
  }

  /**
   * Emit the element getter body for a nested `InputArray_${path}`. Scalar element -> direct
   * typed read. Complex element -> `getArray(i)` / `getStruct(i, n)` / `getMap(i)` that resets
   * the inner instance.
   */
  private def emitArrayElementGetter(path: String, spec: ArrayColumnSpec): String = {
    val elemPath = s"${path}_e"
    spec.element match {
      case _: ScalarColumnSpec =>
        emitArrayElementScalarGetter(spec.elementSparkType, elemPath)
      case _: ArrayColumnSpec =>
        val reset = emitListBackedChildReset(elemPath, "startIndex + i", s"${elemPath}_arrayData")
        s"""      @Override
           |      public org.apache.spark.sql.catalyst.util.ArrayData getArray(int i) {
           |$reset
           |        return ${elemPath}_arrayData;
           |      }""".stripMargin
      case _: StructColumnSpec =>
        s"""      @Override
           |      public org.apache.spark.sql.catalyst.InternalRow getStruct(int i, int numFields) {
           |        ${elemPath}_structData.reset(startIndex + i);
           |        return ${elemPath}_structData;
           |      }""".stripMargin
      case _: MapColumnSpec =>
        val reset = emitListBackedChildReset(elemPath, "startIndex + i", s"${elemPath}_mapData")
        s"""      @Override
           |      public org.apache.spark.sql.catalyst.util.MapData getMap(int i) {
           |$reset
           |        return ${elemPath}_mapData;
           |      }""".stripMargin
    }
  }

  /**
   * Emit the scalar-element getter override for a nested `InputArray_${path}`. Only the getter
   * matching the element type is overridden; any other getter inherits the base class's
   * `UnsupportedOperationException`.
   */
  private def emitArrayElementScalarGetter(elemType: DataType, childField: String): String =
    elemType match {
      case BooleanType =>
        s"""      @Override
         |      public boolean getBoolean(int i) {
         |        return $childField.get(startIndex + i) == 1;
         |      }""".stripMargin
      case ByteType =>
        s"""      @Override
         |      public byte getByte(int i) {
         |        return $childField.get(startIndex + i);
         |      }""".stripMargin
      case ShortType =>
        s"""      @Override
         |      public short getShort(int i) {
         |        return $childField.get(startIndex + i);
         |      }""".stripMargin
      case IntegerType | DateType =>
        s"""      @Override
         |      public int getInt(int i) {
         |        return $childField.get(startIndex + i);
         |      }""".stripMargin
      case LongType | TimestampType | TimestampNTZType =>
        s"""      @Override
         |      public long getLong(int i) {
         |        return $childField.get(startIndex + i);
         |      }""".stripMargin
      case FloatType =>
        s"""      @Override
         |      public float getFloat(int i) {
         |        return $childField.get(startIndex + i);
         |      }""".stripMargin
      case DoubleType =>
        s"""      @Override
         |      public double getDouble(int i) {
         |        return $childField.get(startIndex + i);
         |      }""".stripMargin
      case dt: DecimalType =>
        val body =
          if (dt.precision <= 18) emitDecimalFastBody(childField, "startIndex + i", "        ")
          else emitDecimalSlowBody(childField, "startIndex + i", "        ")
        s"""      @Override
         |      public org.apache.spark.sql.types.Decimal getDecimal(
         |          int i, int precision, int scale) {
         |$body
         |      }""".stripMargin
      case _: StringType =>
        s"""      @Override
         |      public org.apache.spark.unsafe.types.UTF8String getUTF8String(int i) {
         |${emitUtf8Body(childField, "startIndex + i", "        ")}
         |      }""".stripMargin
      case BinaryType =>
        s"""      @Override
         |      public byte[] getBinary(int i) {
         |        return $childField.get(startIndex + i);
         |      }""".stripMargin
      case other =>
        throw new UnsupportedOperationException(
          s"nested ArrayData: unsupported element type $other")
    }

  /**
   * Emit the kernel's `@Override public ArrayData getArray(int ordinal)` method. Each case reads
   * `(startIdx, length)` from the outer `ListVector`'s offsets at the current row and calls the
   * pre-allocated instance's unified `reset(startIdx, length)`.
   */
  def emitGetArrayMethod(inputSchema: Seq[ArrowColumnSpec]): String = {
    val cases = inputSchema.zipWithIndex.collect { case (_: ArrayColumnSpec, ord) =>
      val reset =
        emitListBackedChildReset(s"this.col$ord", "this.rowIdx", s"this.col${ord}_arrayData")
      s"""      case $ord: {
         |$reset
         |        return this.col${ord}_arrayData;
         |      }""".stripMargin
    }
    if (cases.isEmpty) {
      ""
    } else {
      s"""
         |  @Override
         |  public org.apache.spark.sql.catalyst.util.ArrayData getArray(int ordinal) {
         |    switch (ordinal) {
         |${cases.mkString("\n")}
         |      default: throw new UnsupportedOperationException(
         |          "getArray out of range: " + ordinal);
         |    }
         |  }
         |""".stripMargin
    }
  }

  /**
   * Emit one `InputStruct_${path}` nested class. Flat-indexed: `reset(int outerRowIdx)` just
   * captures the index. Scalar getters switch on field ordinal; complex getters route to inner
   * instances (offsets computed for array/map children; rowIdx passed through for struct
   * children).
   */
  private def emitStructClass(path: String, spec: StructColumnSpec): String = {
    val baseClassName = classOf[CometInternalRow].getName
    val innerInstances = spec.fields.zipWithIndex
      .flatMap { case (f, fi) =>
        val fieldPath = s"${path}_f$fi"
        Some(instanceDeclaration(fieldPath, f.child)).filter(_.nonEmpty)
      }
      .mkString("\n")
    val isNullCases = spec.fields.zipWithIndex.map {
      case (f, fi) if !f.nullable =>
        s"        case $fi: return false;"
      case (_, fi) =>
        s"        case $fi: return ${path}_f$fi.isNull(this.rowIdx);"
    }
    val scalarGetters = emitStructScalarGetters(path, spec)
    val complexGetters = emitStructComplexGetters(path, spec)
    s"""  private final class InputStruct_$path extends $baseClassName {
       |    private int rowIdx;
       |$innerInstances
       |
       |    void reset(int outerRowIdx) {
       |      this.rowIdx = outerRowIdx;
       |    }
       |
       |    @Override
       |    public int numFields() {
       |      return ${spec.fields.length};
       |    }
       |
       |    @Override
       |    public boolean isNullAt(int ordinal) {
       |      switch (ordinal) {
       |${isNullCases.mkString("\n")}
       |        default: throw new UnsupportedOperationException(
       |            "InputStruct_$path.isNullAt out of range: " + ordinal);
       |      }
       |    }
       |
       |$scalarGetters
       |$complexGetters
       |  }
       |""".stripMargin
  }

  private def emitStructScalarGetters(path: String, spec: StructColumnSpec): String = {
    val withOrd = spec.fields.zipWithIndex
    val scalarOrd = withOrd.filter { case (f, _) => f.child.isInstanceOf[ScalarColumnSpec] }

    def fieldReadScalar(fi: Int, dt: DataType): String = dt match {
      case BooleanType =>
        s"        case $fi: return ${path}_f$fi.get(this.rowIdx) == 1;"
      case ByteType | ShortType | IntegerType | DateType | LongType | TimestampType |
          TimestampNTZType | FloatType | DoubleType =>
        s"        case $fi: return ${path}_f$fi.get(this.rowIdx);"
      case BinaryType =>
        s"        case $fi: return ${path}_f$fi.get(this.rowIdx);"
      case _: StringType =>
        s"""        case $fi: {
           |${emitUtf8Body(s"${path}_f$fi", "this.rowIdx", "          ")}
           |        }""".stripMargin
      case _: DecimalType =>
        throw new IllegalStateException("decimal handled separately")
      case other =>
        throw new UnsupportedOperationException(
          s"nested InputStruct getter: unsupported field type $other")
    }

    val booleanCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == BooleanType =>
          fieldReadScalar(fi, BooleanType)
      }
    val byteCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == ByteType =>
          fieldReadScalar(fi, ByteType)
      }
    val shortCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == ShortType =>
          fieldReadScalar(fi, ShortType)
      }
    val intCases = scalarOrd.collect {
      case (f, fi) if f.sparkType == IntegerType || f.sparkType == DateType =>
        fieldReadScalar(fi, IntegerType)
    }
    val longCases = scalarOrd.collect {
      case (f, fi)
          if f.sparkType == LongType || f.sparkType == TimestampType ||
            f.sparkType == TimestampNTZType =>
        fieldReadScalar(fi, LongType)
    }
    val floatCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == FloatType =>
          fieldReadScalar(fi, FloatType)
      }
    val doubleCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == DoubleType =>
          fieldReadScalar(fi, DoubleType)
      }
    val binaryCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == BinaryType =>
          fieldReadScalar(fi, BinaryType)
      }
    val utf8Cases = scalarOrd.collect {
      case (f, fi) if f.sparkType.isInstanceOf[StringType] => fieldReadScalar(fi, f.sparkType)
    }

    val decimalCases = scalarOrd.collect {
      case (f, fi) if f.sparkType.isInstanceOf[DecimalType] =>
        val dt = f.sparkType.asInstanceOf[DecimalType]
        val field = s"${path}_f$fi"
        val body =
          if (dt.precision <= 18) emitDecimalFastBody(field, "this.rowIdx", "          ")
          else emitDecimalSlowBody(field, "this.rowIdx", "          ")
        s"""        case $fi: {
           |$body
           |        }""".stripMargin
    }

    Seq(
      structSwitch("public boolean getBoolean(int ordinal)", "getBoolean", booleanCases),
      structSwitch("public byte getByte(int ordinal)", "getByte", byteCases),
      structSwitch("public short getShort(int ordinal)", "getShort", shortCases),
      structSwitch("public int getInt(int ordinal)", "getInt", intCases),
      structSwitch("public long getLong(int ordinal)", "getLong", longCases),
      structSwitch("public float getFloat(int ordinal)", "getFloat", floatCases),
      structSwitch("public double getDouble(int ordinal)", "getDouble", doubleCases),
      structSwitch(
        "public org.apache.spark.sql.types.Decimal getDecimal(" +
          "int ordinal, int precision, int scale)",
        "getDecimal",
        decimalCases),
      structSwitch("public byte[] getBinary(int ordinal)", "getBinary", binaryCases),
      structSwitch(
        "public org.apache.spark.unsafe.types.UTF8String getUTF8String(int ordinal)",
        "getUTF8String",
        utf8Cases)).mkString
  }

  private def emitStructComplexGetters(path: String, spec: StructColumnSpec): String = {
    val getArrayCases = spec.fields.zipWithIndex.collect {
      case (f, fi) if f.child.isInstanceOf[ArrayColumnSpec] =>
        val fieldPath = s"${path}_f$fi"
        val reset = emitListBackedChildReset(fieldPath, "this.rowIdx", s"${fieldPath}_arrayData")
        s"""        case $fi: {
           |$reset
           |          return ${fieldPath}_arrayData;
           |        }""".stripMargin
    }
    val getStructCases = spec.fields.zipWithIndex.collect {
      case (f, fi) if f.child.isInstanceOf[StructColumnSpec] =>
        val fieldPath = s"${path}_f$fi"
        s"""        case $fi: {
           |          ${fieldPath}_structData.reset(this.rowIdx);
           |          return ${fieldPath}_structData;
           |        }""".stripMargin
    }
    val getMapCases = spec.fields.zipWithIndex.collect {
      case (f, fi) if f.child.isInstanceOf[MapColumnSpec] =>
        val fieldPath = s"${path}_f$fi"
        val reset = emitListBackedChildReset(fieldPath, "this.rowIdx", s"${fieldPath}_mapData")
        s"""        case $fi: {
           |$reset
           |          return ${fieldPath}_mapData;
           |        }""".stripMargin
    }
    Seq(
      structSwitch(
        "public org.apache.spark.sql.catalyst.util.ArrayData getArray(int ordinal)",
        "getArray",
        getArrayCases),
      structSwitch(
        "public org.apache.spark.sql.catalyst.InternalRow getStruct(int ordinal, int numFields)",
        "getStruct",
        getStructCases),
      structSwitch(
        "public org.apache.spark.sql.catalyst.util.MapData getMap(int ordinal)",
        "getMap",
        getMapCases)).mkString
  }

  /**
   * Emit one `InputMap_${path}` nested class. Holds the slice `(startIndex, length)` and routes
   * `keyArray()` / `valueArray()` through pre-allocated `InputArray_${path}_k` /
   * `InputArray_${path}_v` instances (emitted by [[collectNestedClasses]]).
   */
  private def emitMapClass(path: String, spec: MapColumnSpec): String = {
    val _ = spec // key/value arrays declared via path convention below
    val baseClassName = classOf[CometMapData].getName
    val keyPath = s"${path}_k"
    val valPath = s"${path}_v"
    s"""  private final class InputMap_$path extends $baseClassName {
       |    private int startIndex;
       |    private int length;
       |    private final InputArray_$keyPath ${keyPath}_arrayData = new InputArray_$keyPath();
       |    private final InputArray_$valPath ${valPath}_arrayData = new InputArray_$valPath();
       |
       |    void reset(int startIdx, int len) {
       |      this.startIndex = startIdx;
       |      this.length = len;
       |    }
       |
       |    @Override
       |    public int numElements() {
       |      return length;
       |    }
       |
       |    @Override
       |    public org.apache.spark.sql.catalyst.util.ArrayData keyArray() {
       |      ${keyPath}_arrayData.reset(this.startIndex, this.length);
       |      return ${keyPath}_arrayData;
       |    }
       |
       |    @Override
       |    public org.apache.spark.sql.catalyst.util.ArrayData valueArray() {
       |      ${valPath}_arrayData.reset(this.startIndex, this.length);
       |      return ${valPath}_arrayData;
       |    }
       |  }
       |""".stripMargin
  }

  /**
   * Emit the kernel's top-level `@Override public MapData getMap(int ordinal)` method when the
   * input schema has at least one map-typed column at the top level; empty string otherwise.
   */
  def emitGetMapMethod(inputSchema: Seq[ArrowColumnSpec]): String = {
    val cases = inputSchema.zipWithIndex.collect { case (_: MapColumnSpec, ord) =>
      val reset =
        emitListBackedChildReset(s"this.col$ord", "this.rowIdx", s"this.col${ord}_mapData")
      s"""      case $ord: {
         |$reset
         |        return this.col${ord}_mapData;
         |      }""".stripMargin
    }
    if (cases.isEmpty) {
      ""
    } else {
      s"""
         |  @Override
         |  public org.apache.spark.sql.catalyst.util.MapData getMap(int ordinal) {
         |    switch (ordinal) {
         |${cases.mkString("\n")}
         |      default: throw new UnsupportedOperationException(
         |          "getMap out of range: " + ordinal);
         |    }
         |  }
         |""".stripMargin
    }
  }

  /**
   * Return the inner-instance field declaration for one complex spec at the given path, or an
   * empty string for a scalar spec. Used inside nested-class bodies to declare pre-allocated
   * child-view instances.
   */
  private def instanceDeclaration(path: String, spec: ArrowColumnSpec): String = spec match {
    case _: ScalarColumnSpec => ""
    case _: ArrayColumnSpec =>
      s"    private final InputArray_$path ${path}_arrayData = new InputArray_$path();"
    case _: StructColumnSpec =>
      s"    private final InputStruct_$path ${path}_structData = new InputStruct_$path();"
    case _: MapColumnSpec =>
      s"    private final InputMap_$path ${path}_mapData = new InputMap_$path();"
  }

  private def structSwitch(methodSig: String, label: String, cases: Seq[String]): String = {
    if (cases.isEmpty) {
      ""
    } else {
      s"""
         |    @Override
         |    $methodSig {
         |      switch (ordinal) {
         |${cases.mkString("\n")}
         |        default: throw new UnsupportedOperationException(
         |            "$label out of range: " + ordinal);
         |      }
         |    }
       """.stripMargin
    }
  }

  /**
   * Emit the kernel's top-level `@Override public InternalRow getStruct(int ordinal, int
   * numFields)` method when the input schema has at least one struct-typed column.
   */
  def emitGetStructMethod(inputSchema: Seq[ArrowColumnSpec]): String = {
    val cases = inputSchema.zipWithIndex.collect { case (_: StructColumnSpec, ord) =>
      s"""      case $ord: {
         |        this.col${ord}_structData.reset(this.rowIdx);
         |        return this.col${ord}_structData;
         |      }""".stripMargin
    }
    if (cases.isEmpty) {
      ""
    } else {
      s"""
         |  @Override
         |  public org.apache.spark.sql.catalyst.InternalRow getStruct(int ordinal, int numFields) {
         |    switch (ordinal) {
         |${cases.mkString("\n")}
         |      default: throw new UnsupportedOperationException(
         |          "getStruct out of range: " + ordinal);
         |    }
         |  }
         |""".stripMargin
    }
  }

  private def emitOrdinalSwitch(methodSig: String, label: String, cases: Seq[String]): String = {
    if (cases.isEmpty) {
      ""
    } else {
      s"""
         |  @Override
         |  $methodSig {
         |    switch (ordinal) {
         |${cases.mkString("\n")}
         |      default: throw new UnsupportedOperationException(
         |          "$label out of range: " + ordinal);
         |    }
         |  }
       """.stripMargin
    }
  }

  // -------------------------------------------------------------------------------------------
  // Scalar-read body templates shared by `emitTypedGetters`, `emitArrayElementScalarGetter`, and
  // `emitStructScalarGetters`. Each helper emits the per-type read statements parameterized on
  // `field` (Java expression for the Arrow vector), `idx` (Java expression for the row/slot),
  // and `ind` (per-line indent prefix). Continuation lines are indented by `ind + "    "`. The
  // caller wraps the result in the appropriate control-flow (switch case or method override).
  // -------------------------------------------------------------------------------------------

  /** Parenthesize `idx` when it contains whitespace, to keep `(long) idx * 16L` well-formed. */
  private def castableIdx(idx: String): String = if (idx.contains(' ')) s"($idx)" else idx

  private def emitDecimalFastBody(field: String, idx: String, ind: String): String = {
    val cont = ind + "    "
    val i = castableIdx(idx)
    s"""${ind}long unscaled = $field.getDataBuffer()
       |$cont.getLong((long) $i * 16L);
       |${ind}return org.apache.spark.sql.types.Decimal$$.MODULE$$
       |$cont.createUnsafe(unscaled, precision, scale);""".stripMargin
  }

  private def emitDecimalSlowBody(field: String, idx: String, ind: String): String = {
    val cont = ind + "    "
    s"""${ind}java.math.BigDecimal bd = $field.getObject($idx);
       |${ind}return org.apache.spark.sql.types.Decimal$$.MODULE$$
       |$cont.apply(bd, precision, scale);""".stripMargin
  }

  private def emitUtf8Body(field: String, idx: String, ind: String): String = {
    val cont = ind + "    "
    s"""${ind}int s = $field.getStartOffset($idx);
       |${ind}int e = $field.getEndOffset($idx);
       |${ind}long addr = $field.getDataBuffer().memoryAddress() + s;
       |${ind}return org.apache.spark.unsafe.types.UTF8String
       |$cont.fromAddress(null, addr, e - s);""".stripMargin
  }

  // -------------------------------------------------------------------------------------------
  // Unsafe variants for top-level scalar columns. Each batch caches the data-buffer address (and
  // offset-buffer address for variable-width) on the kernel, letting per-row reads go through
  // Platform.get* directly without re-dereferencing the Arrow vector's ArrowBuf per call. Nested
  // classes still use the Arrow-buffer variants above until the same address caching lands at
  // nested-level emission.
  //
  // The VarChar / VarBinary unsafe emitters below duplicate what CometPlainVector.getUTF8String
  // and getBinary do today, with two differences: they skip CometPlainVector's internal
  // isNullAt (redundant here because the kernel's caller already handled it) and they read the
  // offset-buffer address from a kernel-cached field rather than re-dereferencing the ArrowBuf.
  // Once apache/datafusion-comet#4280 (offsetBufferAddress caching) and #4279 (validity-bitmap
  // byte cache) land, both differences stop mattering and `emitUtf8BodyUnsafe` /
  // `emitBinaryBodyUnsafe` can be deleted in favor of `CometPlainVector` reuse for variable-
  // width. The decimal-fast variant has its own motivation (compile-time precision
  // specialization) unrelated to those issues.
  // -------------------------------------------------------------------------------------------

  private def emitDecimalFastBodyUnsafe(valueAddr: String, idx: String, ind: String): String = {
    val cont = ind + "    "
    val i = castableIdx(idx)
    s"""${ind}long unscaled = org.apache.spark.unsafe.Platform.getLong(null,
       |$cont$valueAddr + (long) $i * 16L);
       |${ind}return org.apache.spark.sql.types.Decimal$$.MODULE$$
       |$cont.createUnsafe(unscaled, precision, scale);""".stripMargin
  }

  private def emitUtf8BodyUnsafe(
      valueAddr: String,
      offsetAddr: String,
      idx: String,
      ind: String): String = {
    val cont = ind + "    "
    val i = castableIdx(idx)
    s"""${ind}int s = org.apache.spark.unsafe.Platform.getInt(null,
       |$cont$offsetAddr + (long) $i * 4L);
       |${ind}int e = org.apache.spark.unsafe.Platform.getInt(null,
       |$cont$offsetAddr + ((long) $i + 1L) * 4L);
       |${ind}return org.apache.spark.unsafe.types.UTF8String
       |$cont.fromAddress(null, $valueAddr + s, e - s);""".stripMargin
  }

  private def emitBinaryBodyUnsafe(
      valueAddr: String,
      offsetAddr: String,
      idx: String,
      ind: String): String = {
    val cont = ind + "    "
    val i = castableIdx(idx)
    s"""${ind}int s = org.apache.spark.unsafe.Platform.getInt(null,
       |$cont$offsetAddr + (long) $i * 4L);
       |${ind}int e = org.apache.spark.unsafe.Platform.getInt(null,
       |$cont$offsetAddr + ((long) $i + 1L) * 4L);
       |${ind}int len = e - s;
       |${ind}byte[] out = new byte[len];
       |${ind}org.apache.spark.unsafe.Platform.copyMemory(null, $valueAddr + s, out,
       |${cont}org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET, len);
       |${ind}return out;""".stripMargin
  }
}
