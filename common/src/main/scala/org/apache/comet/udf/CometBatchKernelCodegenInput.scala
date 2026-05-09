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

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.{BaseVariableWidthViewVector, BigIntVector, BitVector, DateDayVector, DecimalVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TimeStampMicroTZVector, TimeStampMicroVector, TinyIntVector, VarBinaryVector, VarCharVector, ViewVarBinaryVector, ViewVarCharVector}
import org.apache.arrow.vector.complex.{ListVector, StructVector}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampNTZType, TimestampType}

import org.apache.comet.udf.CometBatchKernelCodegen.{ArrayColumnSpec, ArrowColumnSpec, StructColumnSpec}

/**
 * Input-side emitters for the Arrow-direct codegen kernel. Everything that generates source for
 * reading Arrow input into Spark's typed getter surface lives here: kernel field declarations,
 * per-batch input casts, top-level typed-getter switches, nested `InputArray_colN` /
 * `InputStruct_colN` classes, and the input-side type-support gate.
 *
 * Paired with [[CometBatchKernelCodegenOutput]], which handles the symmetric output side
 * ([[allocateOutput]] / `emitWrite` / the output type surface). Keeping the two sides in separate
 * files makes the type coverage on each side readable at a glance.
 */
private[udf] object CometBatchKernelCodegenInput {

  /**
   * Input types the kernel has a typed getter for. Recursive: `ArrayType(inner)` is supported
   * when `inner` is supported; `StructType` is supported when every field is. `canHandle` uses
   * this to gate the serde fallback.
   */
  def isSupportedInputType(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType => true
    case FloatType | DoubleType => true
    case _: DecimalType => true
    // `_: StringType` rather than `StringType` matches collated variants too (Spark 4.x's
    // `StringType` is a class whose case object is the default UTF8_BINARY instance).
    case _: StringType | _: BinaryType => true
    case DateType | TimestampType | TimestampNTZType => true
    case ArrayType(inner, _) => isSupportedInputType(inner)
    case st: StructType => st.fields.forall(f => isSupportedInputType(f.dataType))
    case _ => false
  }

  /**
   * Emit the kernel's per-column field declarations.
   *
   * For a scalar spec at ordinal N: `private $Class colN;`
   *
   * For an array spec at ordinal N: three fields - the outer `ListVector`, the typed child vector
   * (its element vector class), and a single pre-allocated nested `ArrayData` instance that
   * `getArray(N)` will reset and return row by row:
   * {{{
   *   private ListVector colN;
   *   private $ChildVectorClass colN_child;
   *   private final InputArray_colN colN_arrayData = new InputArray_colN();
   * }}}
   */
  def inputFieldDecls(inputSchema: Seq[ArrowColumnSpec]): String =
    inputSchema.zipWithIndex
      .map {
        case (arr: ArrayColumnSpec, ord) =>
          // Array spec: outer ListVector + typed child vector + pre-allocated ArrayData
          // instance. The instance reference is `final`; what changes per row is its
          // `startIndex`/`length` state, reset by `getArray`.
          val listClass = classOf[ListVector].getName
          val childClass = arr.element.vectorClass.getName
          val instanceType = s"InputArray_col$ord"
          s"""private $listClass col$ord;
             |  private $childClass col${ord}_child;
             |  private final $instanceType col${ord}_arrayData = new $instanceType();""".stripMargin
        case (st: StructColumnSpec, ord) =>
          // Struct spec: outer StructVector + one typed child vector per field + pre-allocated
          // InternalRow instance. The instance reference is `final`; what changes per row is
          // its `rowIdx` state, reset by `getStruct`. Per-field child vector types are baked in
          // at compile time so field reads inside the nested class resolve to concrete getters.
          val structClass = classOf[StructVector].getName
          val childDecls = st.fields.zipWithIndex
            .map { case (f, fi) =>
              val childClass = f.child.vectorClass.getName
              s"private $childClass col${ord}_child_$fi;"
            }
            .mkString("\n  ")
          val instanceType = s"InputStruct_col$ord"
          s"""private $structClass col$ord;
             |  $childDecls
             |  private final $instanceType col${ord}_structData = new $instanceType();""".stripMargin
        case (spec, ord) =>
          s"private ${spec.vectorClass.getName} col$ord;"
      }
      .mkString("\n  ")

  /**
   * Emit the input-cast statements at the top of `process`.
   *
   * Scalar: `this.colN = ($Class) inputs[N];`
   *
   * Array: casts the outer ListVector AND its data vector to the typed child class, storing both.
   * Child vector lookup via `getDataVector` happens once per batch; downstream element reads
   * (inside the nested ArrayData) go through the cached typed field.
   *
   * Struct: casts the outer StructVector AND each of its children to their declared typed
   * classes. Children are read via `getChildByOrdinal(fi)` once per batch.
   */
  def inputCasts(inputSchema: Seq[ArrowColumnSpec]): String =
    inputSchema.zipWithIndex
      .map {
        case (arr: ArrayColumnSpec, ord) =>
          val listClass = classOf[ListVector].getName
          val childClass = arr.element.vectorClass.getName
          s"""this.col$ord = ($listClass) inputs[$ord];
             |    this.col${ord}_child = ($childClass) this.col$ord.getDataVector();""".stripMargin
        case (st: StructColumnSpec, ord) =>
          val structClass = classOf[StructVector].getName
          val childCasts = st.fields.zipWithIndex
            .map { case (f, fi) =>
              val childClass = f.child.vectorClass.getName
              s"this.col${ord}_child_$fi = ($childClass) this.col$ord.getChildByOrdinal($fi);"
            }
            .mkString("\n    ")
          s"""this.col$ord = ($structClass) inputs[$ord];
             |    $childCasts""".stripMargin
        case (spec, ord) =>
          s"this.col$ord = (${spec.vectorClass.getName}) inputs[$ord];"
      }
      .mkString("\n    ")

  /**
   * Emit the kernel's typed-getter overrides. Spark's `InternalRow` provides the base virtual
   * method; the generated `@Override` on a final class gives the JIT enough information to
   * devirtualize. Each getter switches on the column ordinal so the call site (with an inlined
   * constant ordinal from `BoundReference.genCode`) folds down to a single branch.
   *
   * Current coverage: `isNullAt` plus getters for boolean, byte, short, int (including
   * `DateDayVector`), long (including `TimeStampMicroVector` and its TZ variant), float, double,
   * decimal, binary, and UTF8 (for both `VarCharVector` and `ViewVarCharVector`). Widen by adding
   * further vector-class cases to the existing switches.
   *
   * `decimalTypeByOrdinal` lets the decimal getter specialize per ordinal: when a
   * `BoundReference` of `DecimalType(precision <= 18)` is the only decimal read at that ordinal,
   * the emitted case skips the `BigDecimal` allocation entirely and reads the unscaled long
   * directly. See [[decimalPrecisionByOrdinal]] for how that map is derived.
   *
   * TODO(unsafe-readers): today the primitive getter emissions go through Arrow's typed
   * `v.get(i)` which performs bounds checks against the vector's capacity. Inside the kernel's
   * `process` loop we already know `i` is in `[0, numRows)` from the loop invariant, so the
   * bounds check is redundant. Mirror `CometPlainVector`'s pattern by caching each input column's
   * value/validity/offset buffer addresses at `process()` entry and emitting direct
   * `Platform.getInt(null, col0_valueAddr + rowIdx * 4L)` (and analogous `getLong`, `getFloat`,
   * `getDouble`) reads. Saves the bounds check and the ArrowBuf indirection per read. Same idea
   * applies inside the nested `ArrayData` readers added in Milestone 2. Deferred to a follow-up
   * because it touches every primitive case and wants a benchmark confirming the win before we
   * commit.
   */
  def typedInputAccessors(
      inputSchema: Seq[ArrowColumnSpec],
      decimalTypeByOrdinal: Map[Int, Option[DecimalType]]): String = {
    val withOrd = inputSchema.zipWithIndex

    val isNullCases = withOrd.map { case (spec, ord) =>
      if (!spec.nullable) s"      case $ord: return false;"
      else s"      case $ord: return this.col$ord.isNull(this.rowIdx);"
    }

    val booleanCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[BitVector] =>
        s"      case $ord: return this.col$ord.get(this.rowIdx) == 1;"
    }
    val byteCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[TinyIntVector] =>
        s"      case $ord: return this.col$ord.get(this.rowIdx);"
    }
    val shortCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[SmallIntVector] =>
        s"      case $ord: return this.col$ord.get(this.rowIdx);"
    }
    val intCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord)
          if cls == classOf[IntVector] || cls == classOf[DateDayVector] =>
        s"      case $ord: return this.col$ord.get(this.rowIdx);"
    }
    val longCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord)
          if cls == classOf[BigIntVector] ||
            cls == classOf[TimeStampMicroVector] ||
            cls == classOf[TimeStampMicroTZVector] =>
        s"      case $ord: return this.col$ord.get(this.rowIdx);"
    }
    val floatCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[Float4Vector] =>
        s"      case $ord: return this.col$ord.get(this.rowIdx);"
    }
    val doubleCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[Float8Vector] =>
        s"      case $ord: return this.col$ord.get(this.rowIdx);"
    }
    val decimalCases = withOrd.collect {
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[DecimalVector] =>
        // Compile-time specialization on the DecimalType precision known at this ordinal.
        //
        // Arrow's decimal128 stores each value as a 16-byte little-endian two's complement
        // integer. When the unscaled value fits in a signed 64-bit long (precision <= 18, i.e.
        // `Decimal.MAX_LONG_DIGITS`), the low 8 bytes of the slot are the signed long value
        // directly; the upper 8 bytes are sign-extension. Reading those 8 bytes via
        // `ArrowBuf.getLong` (little-endian) and wrapping with `Decimal.createUnsafe` bypasses
        // the `BigDecimal` allocation that `DecimalVector.getObject` performs.
        //
        // `decimalTypeByOrdinal(ord)` tells us which branch to emit: `Some(dt)` with
        // `dt.precision <= 18` emits the fast path only, `Some(dt)` with precision > 18 emits
        // the slow path only, `None` means either the ordinal has no `BoundReference` in the
        // tree or has multiple conflicting DecimalTypes. The `None` case emits the runtime
        // branch as a defensive fallback; it should not normally hit in a well-analyzed plan.
        val known = decimalTypeByOrdinal.getOrElse(ord, None)
        val fastPath =
          s"""        long unscaled = this.col$ord.getDataBuffer()
             |            .getLong((long) this.rowIdx * 16L);
             |        return org.apache.spark.sql.types.Decimal$$.MODULE$$
             |            .createUnsafe(unscaled, precision, scale);""".stripMargin
        val slowPath =
          s"""        java.math.BigDecimal bd = this.col$ord.getObject(this.rowIdx);
             |        return org.apache.spark.sql.types.Decimal$$.MODULE$$
             |            .apply(bd, precision, scale);""".stripMargin
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
      case (ArrowColumnSpec(cls, _), ord)
          if cls == classOf[VarBinaryVector] || cls == classOf[ViewVarBinaryVector] =>
        // Both vectors expose `byte[] get(int)`; the view variant internally handles the inline
        // vs referenced branch. Not zero-copy (byte[] must be heap-allocated) but correct.
        s"      case $ord: return this.col$ord.get(this.rowIdx);"
    }
    val utf8Cases = withOrd.flatMap {
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[VarCharVector] =>
        Some(s"""      case $ord: {
                |        ${classOf[VarCharVector].getName} v = this.col$ord;
                |        int s = v.getStartOffset(this.rowIdx);
                |        int e = v.getEndOffset(this.rowIdx);
                |        long addr = v.getDataBuffer().memoryAddress() + s;
                |        return org.apache.spark.unsafe.types.UTF8String
                |            .fromAddress(null, addr, e - s);
                |      }""".stripMargin)
      case (ArrowColumnSpec(cls, _), ord) if cls == classOf[ViewVarCharVector] =>
        Some(viewUtf8StringCase(ord))
      case _ => None
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
   * expression. For each ordinal the value is:
   *
   *   - `Some(dt)` when every `BoundReference` at that ordinal shares the same `DecimalType`.
   *   - `None` when there are multiple distinct `DecimalType`s at that ordinal (unexpected in a
   *     well-analyzed plan but handled as a defensive fallback).
   *
   * Ordinals that have no `BoundReference` of `DecimalType` simply aren't in the map. Callers
   * should treat absence the same as `None`: use the runtime branch rather than specializing.
   *
   * Used by [[typedInputAccessors]] to emit a compile-time-specialized `getDecimal` case per
   * ordinal (fast path for precision <= 18, slow path otherwise, with a runtime branch only when
   * the precision cannot be determined).
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
   * Emit nested `InputArray_colN` class declarations, one per array-typed input column. Each
   * class is a `final` subclass of [[CometArrayData]] sized for one column (specialized on
   * element type). `reset(rowIdx)` reads the list's offsets; subsequent element reads inline the
   * zero-copy Arrow access for that element type. All unused `ArrayData` getters inherit the base
   * class's `UnsupportedOperationException` throws.
   *
   * Emitted as inner classes of `SpecificCometBatchKernel` so they can reference the outer
   * `col${N}` (the `ListVector`) and `col${N}_child` (the typed child vector) fields directly.
   */
  def nestedArrayClasses(inputSchema: Seq[ArrowColumnSpec]): String =
    inputSchema.zipWithIndex
      .collect { case (spec: ArrayColumnSpec, ord) => emitNestedArrayClass(ord, spec) }
      .mkString("\n")

  /** Emit one `InputArray_colN` nested class for the given array spec. */
  private def emitNestedArrayClass(ord: Int, spec: ArrayColumnSpec): String = {
    val baseClassName = classOf[CometArrayData].getName
    val elementGetter =
      emitNestedArrayElementGetter(spec.elementSparkType, s"col${ord}_child")
    // If the child is non-nullable, `isNullAt` should always return false. When we add
    // structural nullability tracking to the child spec (ArrowColumnSpec.nullable on the
    // element), we'll emit a literal `return false;` here.
    val isNullAt =
      s"""      @Override
         |      public boolean isNullAt(int i) {
         |        return col${ord}_child.isNull(startIndex + i);
         |      }""".stripMargin
    s"""  private final class InputArray_col$ord extends $baseClassName {
       |    private int startIndex;
       |    private int length;
       |
       |    void reset(int rowIdx) {
       |      this.startIndex = col$ord.getElementStartIndex(rowIdx);
       |      this.length = col$ord.getElementEndIndex(rowIdx) - this.startIndex;
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
   * Emit the element-type-specific getter override for a nested `InputArray_colN`. Only the one
   * getter matching the element type is overridden; any other getter the consumer might call
   * inherits the base class's `UnsupportedOperationException`.
   */
  private def emitNestedArrayElementGetter(elemType: DataType, childField: String): String =
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
        // Short-precision fast path mirrors the top-level `getDecimal` specialization: read the
        // low 8 bytes of the decimal128 slot as a signed long and wrap with `createUnsafe`.
        // `getDecimal` is called with precision/scale as parameters by Spark's codegen; our
        // specialization is keyed on the static element type.
        if (dt.precision <= 18) {
          s"""      @Override
           |      public org.apache.spark.sql.types.Decimal getDecimal(
           |          int i, int precision, int scale) {
           |        long unscaled = $childField.getDataBuffer()
           |            .getLong((long) (startIndex + i) * 16L);
           |        return org.apache.spark.sql.types.Decimal$$.MODULE$$
           |            .createUnsafe(unscaled, precision, scale);
           |      }""".stripMargin
        } else {
          s"""      @Override
           |      public org.apache.spark.sql.types.Decimal getDecimal(
           |          int i, int precision, int scale) {
           |        java.math.BigDecimal bd = $childField.getObject(startIndex + i);
           |        return org.apache.spark.sql.types.Decimal$$.MODULE$$
           |            .apply(bd, precision, scale);
           |      }""".stripMargin
        }
      case _: StringType =>
        // Zero-copy UTF8 read via `UTF8String.fromAddress` on the child VarCharVector's data
        // buffer. Mirrors the top-level `getUTF8String` switch case. ViewVarCharVector child
        // support: deferred; the child vector class check at `canHandle` / spec construction
        // time will need to branch for view-format children when added.
        s"""      @Override
         |      public org.apache.spark.unsafe.types.UTF8String getUTF8String(int i) {
         |        int s = $childField.getStartOffset(startIndex + i);
         |        int e = $childField.getEndOffset(startIndex + i);
         |        long addr = $childField.getDataBuffer().memoryAddress() + s;
         |        return org.apache.spark.unsafe.types.UTF8String
         |            .fromAddress(null, addr, e - s);
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
   * Emit the kernel's `@Override public ArrayData getArray(int ordinal)` method when the input
   * schema has at least one array-typed column; empty string otherwise (the base class's default
   * throws, same as all other complex-type getters until they're added).
   *
   * Each case resets the pre-allocated nested-class instance and returns it. Zero allocation per
   * row beyond the mutable-field writes inside `reset`.
   */
  def emitGetArrayMethod(inputSchema: Seq[ArrowColumnSpec]): String = {
    val cases = inputSchema.zipWithIndex.collect { case (_: ArrayColumnSpec, ord) =>
      s"""      case $ord: {
         |        this.col${ord}_arrayData.reset(this.rowIdx);
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
   * Emit nested `InputStruct_colN` class declarations, one per struct-typed input column. Each
   * class is a `final` subclass of [[CometInternalRow]] with per-field typed-getter overrides
   * baked in at compile time. `reset(rowIdx)` captures the outer row index; downstream field
   * reads hit the typed child-vector field directly at that index (struct children are
   * flat-indexed, no offset chain).
   *
   * Emitted as inner classes of `SpecificCometBatchKernel` so they can reference the outer
   * `col${N}` (the `StructVector`) and `col${N}_child_$fi` (the typed child vectors) fields.
   */
  def nestedStructClasses(inputSchema: Seq[ArrowColumnSpec]): String =
    inputSchema.zipWithIndex
      .collect { case (spec: StructColumnSpec, ord) => emitNestedStructClass(ord, spec) }
      .mkString("\n")

  /** Emit one `InputStruct_colN` nested class for the given struct spec. */
  private def emitNestedStructClass(ord: Int, spec: StructColumnSpec): String = {
    val baseClassName = classOf[CometInternalRow].getName
    val isNullCases = spec.fields.zipWithIndex.map {
      case (f, fi) if !f.nullable =>
        s"      case $fi: return false;"
      case (_, fi) =>
        s"      case $fi: return col${ord}_child_$fi.isNull(this.rowIdx);"
    }
    val getters = emitStructFieldGetters(ord, spec)
    s"""  private final class InputStruct_col$ord extends $baseClassName {
       |    private int rowIdx;
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
       |            "InputStruct_col$ord.isNullAt out of range: " + ordinal);
       |      }
       |    }
       |
       |$getters
       |  }
       |""".stripMargin
  }

  /**
   * Emit the typed getter overrides for a nested `InputStruct_colN`. One override per distinct
   * Spark type appearing in the struct's field list (boolean, byte, short, int, long, float,
   * double, decimal, string, binary). Each override switches on the field ordinal whose field
   * type matches. Ordinals whose field type does not match the getter inherit the base class's
   * `UnsupportedOperationException` and never reach runtime because Spark's `doGenCode` for a
   * struct field of type X calls the getter for X with the exact ordinal.
   */
  private def emitStructFieldGetters(ord: Int, spec: StructColumnSpec): String = {
    val withOrd = spec.fields.zipWithIndex

    def readFor(fieldOrd: Int, dt: DataType): Option[String] = dt match {
      case BooleanType =>
        Some(s"      case $fieldOrd: return col${ord}_child_$fieldOrd.get(this.rowIdx) == 1;")
      case ByteType | ShortType | IntegerType | DateType | LongType | TimestampType |
          TimestampNTZType | FloatType | DoubleType =>
        Some(s"      case $fieldOrd: return col${ord}_child_$fieldOrd.get(this.rowIdx);")
      case BinaryType =>
        Some(s"      case $fieldOrd: return col${ord}_child_$fieldOrd.get(this.rowIdx);")
      case _: StringType =>
        // Zero-copy UTF8 read via the child VarCharVector's data buffer. Mirrors the top-level
        // `getUTF8String` switch case.
        Some(s"""      case $fieldOrd: {
             |        int s = col${ord}_child_$fieldOrd.getStartOffset(this.rowIdx);
             |        int e = col${ord}_child_$fieldOrd.getEndOffset(this.rowIdx);
             |        long addr = col${ord}_child_$fieldOrd.getDataBuffer().memoryAddress() + s;
             |        return org.apache.spark.unsafe.types.UTF8String
             |            .fromAddress(null, addr, e - s);
             |      }""".stripMargin)
      case _: DecimalType =>
        // Decimal is handled in a separate override (signature takes precision/scale).
        None
      case _ => None
    }

    // Simple-typed getters (getBoolean / getByte / ... / getUTF8String / getBinary).
    val booleanCases = withOrd.collect {
      case (f, fi) if f.sparkType == BooleanType => readFor(fi, BooleanType).get
    }
    val byteCases = withOrd.collect {
      case (f, fi) if f.sparkType == ByteType => readFor(fi, ByteType).get
    }
    val shortCases = withOrd.collect {
      case (f, fi) if f.sparkType == ShortType => readFor(fi, ShortType).get
    }
    val intCases = withOrd.collect {
      case (f, fi) if f.sparkType == IntegerType || f.sparkType == DateType =>
        readFor(fi, IntegerType).get
    }
    val longCases = withOrd.collect {
      case (f, fi)
          if f.sparkType == LongType || f.sparkType == TimestampType ||
            f.sparkType == TimestampNTZType =>
        readFor(fi, LongType).get
    }
    val floatCases = withOrd.collect {
      case (f, fi) if f.sparkType == FloatType => readFor(fi, FloatType).get
    }
    val doubleCases = withOrd.collect {
      case (f, fi) if f.sparkType == DoubleType => readFor(fi, DoubleType).get
    }
    val binaryCases = withOrd.collect {
      case (f, fi) if f.sparkType == BinaryType => readFor(fi, BinaryType).get
    }
    val utf8Cases = withOrd.collect {
      case (f, fi) if f.sparkType.isInstanceOf[StringType] => readFor(fi, f.sparkType).get
    }

    // Decimal cases: compile-time fast path per ordinal based on the field's declared precision.
    val decimalCases = withOrd.collect {
      case (f, fi) if f.sparkType.isInstanceOf[DecimalType] =>
        val dt = f.sparkType.asInstanceOf[DecimalType]
        val body = if (dt.precision <= 18) {
          s"""        long unscaled = col${ord}_child_$fi.getDataBuffer()
             |            .getLong((long) this.rowIdx * 16L);
             |        return org.apache.spark.sql.types.Decimal$$.MODULE$$
             |            .createUnsafe(unscaled, precision, scale);""".stripMargin
        } else {
          s"""        java.math.BigDecimal bd = col${ord}_child_$fi.getObject(this.rowIdx);
             |        return org.apache.spark.sql.types.Decimal$$.MODULE$$
             |            .apply(bd, precision, scale);""".stripMargin
        }
        s"""      case $fi: {
           |$body
           |      }""".stripMargin
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

  /**
   * Emit one `@Override`-annotated switch method inside an `InputStruct_colN` class. Returns an
   * empty string when the struct has no fields of this getter's type.
   */
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
   * Emit the kernel's `@Override public InternalRow getStruct(int ordinal, int numFields)` method
   * when the input schema has at least one struct-typed column; empty string otherwise (the base
   * class's default throws).
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

  /**
   * Build one `@Override`-annotated switch method. Returns an empty string when no input columns
   * use this getter so the generated class does not carry a dead method override.
   */
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

  /**
   * Emit a zero-copy `getUTF8String` case for a `ViewVarCharVector` column at the given ordinal.
   * Reads the 16-byte view entry directly from the view buffer and either points at the inline
   * bytes (length &lt;= INLINE_SIZE=12) or at the referenced data buffer via `(bufferIndex,
   * offset)` (length &gt; 12). Follows the layout documented on `BaseVariableWidthViewVector` and
   * the reference decode in its `get(index, holder)` method:
   *
   *   - bytes 0..4: length (int, little-endian via ArrowBuf)
   *   - if length &lt;= 12: bytes 4..16 are inline UTF-8 data
   *   - else: bytes 4..8 are the prefix (unused here), 8..12 the data buffer index, 12..16 the
   *     offset into that buffer
   *
   * No `byte[]` allocation; `UTF8String.fromAddress` wraps the Arrow buffer address directly.
   * This is the main reason to route `Utf8View`-shaped columns through the dispatcher rather than
   * fall back to Spark: native `Utf8View` coverage is uneven, and the zero-copy JVM read matches
   * the semantics Spark expects.
   */
  private def viewUtf8StringCase(ord: Int): String = {
    val elementSize = BaseVariableWidthViewVector.ELEMENT_SIZE
    val inlineSize = BaseVariableWidthViewVector.INLINE_SIZE
    val lengthWidth = BaseVariableWidthViewVector.LENGTH_WIDTH
    val prefixPlusLength = lengthWidth + BaseVariableWidthViewVector.PREFIX_WIDTH
    val prefixPlusLengthPlusBufIdx =
      prefixPlusLength + BaseVariableWidthViewVector.BUF_INDEX_WIDTH
    val viewClass = classOf[ViewVarCharVector].getName
    val bufClass = classOf[ArrowBuf].getName
    s"""      case $ord: {
       |        $viewClass v = this.col$ord;
       |        $bufClass viewBuf = v.getDataBuffer();
       |        long entryStart = (long) this.rowIdx * ${elementSize}L;
       |        int length = viewBuf.getInt(entryStart);
       |        long addr;
       |        if (length > $inlineSize) {
       |          int bufIdx = viewBuf.getInt(entryStart + ${prefixPlusLength}L);
       |          int offset = viewBuf.getInt(entryStart + ${prefixPlusLengthPlusBufIdx}L);
       |          // Cast required: Janino does not resolve the `List<ArrowBuf>.get(int)` generic
       |          // return type; without the cast it sees `.get(bufIdx)` as returning Object.
       |          $bufClass dataBuf = ($bufClass) v.getDataBuffers().get(bufIdx);
       |          addr = dataBuf.memoryAddress() + (long) offset;
       |        } else {
       |          addr = viewBuf.memoryAddress() + entryStart + ${lengthWidth}L;
       |        }
       |        return org.apache.spark.unsafe.types.UTF8String.fromAddress(null, addr, length);
       |      }""".stripMargin
  }
}
