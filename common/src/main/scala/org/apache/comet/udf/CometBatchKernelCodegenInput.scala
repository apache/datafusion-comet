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

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.{BaseVariableWidthViewVector, BigIntVector, BitVector, DateDayVector, DecimalVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TimeStampMicroTZVector, TimeStampMicroVector, TinyIntVector, VarBinaryVector, VarCharVector, ViewVarBinaryVector, ViewVarCharVector}
import org.apache.arrow.vector.complex.{ListVector, StructVector}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampNTZType, TimestampType}

import org.apache.comet.udf.CometBatchKernelCodegen.{ArrayColumnSpec, ArrowColumnSpec, ScalarColumnSpec, StructColumnSpec}

/**
 * Input-side emitters for the Arrow-direct codegen kernel. Everything that generates source for
 * reading Arrow input into Spark's typed getter surface lives here: kernel field declarations,
 * per-batch input casts, top-level typed-getter switches, nested `InputArray_${path}` /
 * `InputStruct_${path}` classes at every complex level, and the input-side type-support gate.
 *
 * ==Path encoding for nested complex types==
 *
 * Each position in a spec tree has a unique path string, used as the suffix on typed vector
 * fields and as the identifier on nested classes. Starting from the column ordinal:
 *
 *   - root: `col${ord}`
 *   - array element of `P`: `${P}_e`
 *   - struct field `fi` of `P`: `${P}_f${fi}`
 *
 * Example: `Struct<a: Array<Int>>` at ordinal 0 produces vector fields `col0` (StructVector),
 * `col0_f0` (ListVector for the `a` field), and `col0_f0_e` (IntVector, the list's element
 * vector). Nested classes get the same suffix: `InputStruct_col0`, `InputArray_col0_f0`.
 *
 * ==Nested-class composition==
 *
 * A nested class at path `P` represents a Spark `ArrayData` or `InternalRow` view of its Arrow
 * vector. For any complex element or field one level down (at path `P_e` or `P_f${fi}`), the
 * class holds a pre-allocated instance of the corresponding inner nested class and routes
 * `getArray` / `getStruct` calls to that instance after resetting it. N-deep nesting falls out of
 * this: each level only knows about its immediate child classes; the recursion handles depth.
 *
 * Paired with [[CometBatchKernelCodegenOutput]], which handles the symmetric output side.
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
   * Emit the kernel's typed vector-field declarations for every level of every input column's
   * spec tree. For a scalar leaf, one typed field at the leaf path; for complex nodes, one field
   * for the complex vector itself (`ListVector` or `StructVector`) plus recursive fields for the
   * children. Top-level complex columns additionally get an instance-field declaration for the
   * pre-allocated nested class.
   *
   * Instance fields for nested-class children one level down live inside the parent nested class,
   * not on the kernel; see [[emitNestedClasses]].
   */
  def inputFieldDecls(inputSchema: Seq[ArrowColumnSpec]): String = {
    val lines = new mutable.ArrayBuffer[String]()
    inputSchema.zipWithIndex.foreach { case (spec, ord) =>
      val path = s"col$ord"
      collectVectorFieldDecls(path, spec, lines)
      collectTopLevelInstanceDecl(path, spec, lines)
    }
    lines.mkString("\n  ")
  }

  private def collectVectorFieldDecls(
      path: String,
      spec: ArrowColumnSpec,
      out: mutable.ArrayBuffer[String]): Unit = spec match {
    case sc: ScalarColumnSpec =>
      out += s"private ${sc.vectorClass.getName} $path;"
    case ar: ArrayColumnSpec =>
      out += s"private ${classOf[ListVector].getName} $path;"
      collectVectorFieldDecls(s"${path}_e", ar.element, out)
    case st: StructColumnSpec =>
      out += s"private ${classOf[StructVector].getName} $path;"
      st.fields.zipWithIndex.foreach { case (f, fi) =>
        collectVectorFieldDecls(s"${path}_f$fi", f.child, out)
      }
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
  }

  /**
   * Emit the per-batch cast statements that materialize `inputs[ord]` into the typed vector
   * fields declared by [[inputFieldDecls]], walking the full spec tree. For a scalar leaf, a
   * single cast; for complex nodes, cast the complex vector, then recurse into children via
   * `getDataVector()` for arrays or `getChildByOrdinal(fi)` for structs. All `getDataVector` /
   * `getChildByOrdinal` calls happen once per batch at the top of `process`; the per-row reads
   * inside nested classes go through the cached typed fields.
   */
  def inputCasts(inputSchema: Seq[ArrowColumnSpec]): String = {
    val lines = new mutable.ArrayBuffer[String]()
    inputSchema.zipWithIndex.foreach { case (spec, ord) =>
      val path = s"col$ord"
      collectCasts(path, spec, s"inputs[$ord]", lines)
    }
    lines.mkString("\n    ")
  }

  private def collectCasts(
      path: String,
      spec: ArrowColumnSpec,
      source: String,
      out: mutable.ArrayBuffer[String]): Unit = spec match {
    case sc: ScalarColumnSpec =>
      out += s"this.$path = (${sc.vectorClass.getName}) $source;"
    case ar: ArrayColumnSpec =>
      out += s"this.$path = (${classOf[ListVector].getName}) $source;"
      collectCasts(s"${path}_e", ar.element, s"this.$path.getDataVector()", out)
    case st: StructColumnSpec =>
      out += s"this.$path = (${classOf[StructVector].getName}) $source;"
      st.fields.zipWithIndex.foreach { case (f, fi) =>
        collectCasts(s"${path}_f$fi", f.child, s"this.$path.getChildByOrdinal($fi)", out)
      }
  }

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
   * applies inside the nested `ArrayData` readers. Deferred to a follow-up because it touches
   * every primitive case and wants a benchmark confirming the win before we commit.
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
   * expression. Used by [[typedInputAccessors]] to emit a compile-time-specialized `getDecimal`
   * case per ordinal (fast path for precision <= 18, slow path otherwise, with a runtime branch
   * only when the precision cannot be determined).
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
   * Emit every nested class needed for every complex level of every input column. For each
   * `ArrayColumnSpec` or `StructColumnSpec` reached during a recursive walk of the spec tree,
   * emits one `InputArray_${path}` or `InputStruct_${path}` class with the appropriate reset /
   * getter shape for that level. Nested classes reference each other by name through the
   * path-suffix convention; forward references are fine because they all live inside the same
   * outer `SpecificCometBatchKernel` class.
   */
  def nestedClasses(inputSchema: Seq[ArrowColumnSpec]): String = {
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
  }

  /**
   * Emit one `InputArray_${path}` nested class.
   *
   *   - Holds `startIndex` / `length` captured from the outer list's offsets at row reset time.
   *   - If the element is complex, also holds a pre-allocated inner nested-class instance.
   *   - `reset(int rowIdx)` reads offsets from the vector at `path` (the `ListVector`).
   *   - `isNullAt(int i)` delegates to the element vector's null bit at `startIndex + i`.
   *   - Element getter: for scalar elements, a direct typed read from the typed child vector at
   *     `${path}_e`; for complex elements, routes through the inner instance after
   *     `reset(startIndex + i)`.
   *
   * The element vector's path is always `${path}_e`, matching the naming convention used by
   * [[inputFieldDecls]] / [[inputCasts]].
   */
  private def emitArrayClass(path: String, spec: ArrayColumnSpec): String = {
    val baseClassName = classOf[CometArrayData].getName
    val elemPath = s"${path}_e"
    val innerInstance = spec.element match {
      case _: ScalarColumnSpec => ""
      case _: ArrayColumnSpec =>
        s"    private final InputArray_$elemPath ${elemPath}_arrayData = " +
          s"new InputArray_$elemPath();"
      case _: StructColumnSpec =>
        s"    private final InputStruct_$elemPath ${elemPath}_structData = " +
          s"new InputStruct_$elemPath();"
    }
    // isNullAt reads the null bit on the element vector at the flat slice index. Works whether
    // the element vector is a scalar, another ListVector, or a StructVector - each carries its
    // own validity bitmap over its own rows.
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
       |    void reset(int rowIdx) {
       |      this.startIndex = $path.getElementStartIndex(rowIdx);
       |      this.length = $path.getElementEndIndex(rowIdx) - this.startIndex;
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
   * Emit the element getter body for a nested `InputArray_${path}` class. Scalar elements get a
   * direct typed read from the typed child vector at `${path}_e`. Complex elements (array /
   * struct) get a `getArray` or `getStruct` override that routes through the inner instance after
   * resetting it to the outer slice index `startIndex + i`.
   */
  private def emitArrayElementGetter(path: String, spec: ArrayColumnSpec): String = {
    val elemPath = s"${path}_e"
    spec.element match {
      case _: ScalarColumnSpec =>
        scalarElementGetter(spec.elementSparkType, elemPath)
      case _: ArrayColumnSpec =>
        s"""      @Override
           |      public org.apache.spark.sql.catalyst.util.ArrayData getArray(int i) {
           |        ${elemPath}_arrayData.reset(startIndex + i);
           |        return ${elemPath}_arrayData;
           |      }""".stripMargin
      case st: StructColumnSpec =>
        val _ = st // suppress unused warning; numFields is an argument Spark passes at call site
        s"""      @Override
           |      public org.apache.spark.sql.catalyst.InternalRow getStruct(int i, int numFields) {
           |        ${elemPath}_structData.reset(startIndex + i);
           |        return ${elemPath}_structData;
           |      }""".stripMargin
    }
  }

  /**
   * Emit the element-type-specific getter override for a scalar-element array. Only the one
   * getter matching the element type is overridden; any other getter the consumer might call
   * inherits the base class's `UnsupportedOperationException`.
   */
  private def scalarElementGetter(elemType: DataType, childField: String): String =
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
        // buffer. ViewVarCharVector child support is deferred; the child vector class check at
        // `canHandle` / spec construction time will need to branch for view-format children
        // when added.
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
   * schema has at least one array-typed column at the top level; empty string otherwise.
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
   * Emit one `InputStruct_${path}` nested class.
   *
   *   - Holds `rowIdx` captured from the outer row index at reset time (struct children are
   *     flat-indexed, so no offset chain is needed).
   *   - For each complex field one level down, holds a pre-allocated inner nested-class instance.
   *   - `reset(int outerRowIdx)` just captures the index.
   *   - `isNullAt(int ordinal)` switches on field ordinal; non-nullable fields return literal
   *     `false`, nullable fields delegate to the field's vector.
   *   - Typed scalar getters (one per appearing scalar type) switch on field ordinal.
   *   - Complex getters (`getArray(ordinal)` / `getStruct(ordinal, numFields)`) switch on field
   *     ordinal and route to the appropriate inner instance after resetting it.
   */
  private def emitStructClass(path: String, spec: StructColumnSpec): String = {
    val baseClassName = classOf[CometInternalRow].getName
    val innerInstances = spec.fields.zipWithIndex
      .flatMap { case (f, fi) =>
        val fieldPath = s"${path}_f$fi"
        f.child match {
          case _: ScalarColumnSpec => None
          case _: ArrayColumnSpec =>
            Some(
              s"    private final InputArray_$fieldPath ${fieldPath}_arrayData = " +
                s"new InputArray_$fieldPath();")
          case _: StructColumnSpec =>
            Some(
              s"    private final InputStruct_$fieldPath ${fieldPath}_structData = " +
                s"new InputStruct_$fieldPath();")
        }
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

  /** Emit the scalar-type getter switches for an `InputStruct_${path}` class. */
  private def emitStructScalarGetters(path: String, spec: StructColumnSpec): String = {
    val withOrd = spec.fields.zipWithIndex

    // Scalar fields only; complex fields are handled by emitStructComplexGetters.
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
           |          int s = ${path}_f$fi.getStartOffset(this.rowIdx);
           |          int e = ${path}_f$fi.getEndOffset(this.rowIdx);
           |          long addr = ${path}_f$fi.getDataBuffer().memoryAddress() + s;
           |          return org.apache.spark.unsafe.types.UTF8String
           |              .fromAddress(null, addr, e - s);
           |        }""".stripMargin
      case _: DecimalType =>
        // Decimal is handled in a separate override; the signature takes precision/scale, so
        // emit a per-field case into that switch, not this one.
        throw new IllegalStateException("decimal handled separately")
      case other =>
        throw new UnsupportedOperationException(
          s"nested InputStruct getter: unsupported field type $other")
    }

    val booleanCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == BooleanType => fieldReadScalar(fi, BooleanType)
      }
    val byteCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == ByteType => fieldReadScalar(fi, ByteType)
      }
    val shortCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == ShortType => fieldReadScalar(fi, ShortType)
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
        case (f, fi) if f.sparkType == FloatType => fieldReadScalar(fi, FloatType)
      }
    val doubleCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == DoubleType => fieldReadScalar(fi, DoubleType)
      }
    val binaryCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == BinaryType => fieldReadScalar(fi, BinaryType)
      }
    val utf8Cases = scalarOrd.collect {
      case (f, fi) if f.sparkType.isInstanceOf[StringType] => fieldReadScalar(fi, f.sparkType)
    }

    val decimalCases = scalarOrd.collect {
      case (f, fi) if f.sparkType.isInstanceOf[DecimalType] =>
        val dt = f.sparkType.asInstanceOf[DecimalType]
        val body = if (dt.precision <= 18) {
          s"""          long unscaled = ${path}_f$fi.getDataBuffer()
             |              .getLong((long) this.rowIdx * 16L);
             |          return org.apache.spark.sql.types.Decimal$$.MODULE$$
             |              .createUnsafe(unscaled, precision, scale);""".stripMargin
        } else {
          s"""          java.math.BigDecimal bd = ${path}_f$fi.getObject(this.rowIdx);
             |          return org.apache.spark.sql.types.Decimal$$.MODULE$$
             |              .apply(bd, precision, scale);""".stripMargin
        }
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

  /**
   * Emit the complex-type getter switches (`getArray` / `getStruct`) for an `InputStruct_${path}`
   * class. Cases route to the pre-allocated inner instance after `reset(this.rowIdx)` (struct is
   * flat-indexed, so the child vector's row at our own rowIdx is this field's value).
   */
  private def emitStructComplexGetters(path: String, spec: StructColumnSpec): String = {
    val getArrayCases = spec.fields.zipWithIndex.collect {
      case (f, fi) if f.child.isInstanceOf[ArrayColumnSpec] =>
        val fieldPath = s"${path}_f$fi"
        s"""        case $fi: {
           |          ${fieldPath}_arrayData.reset(this.rowIdx);
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
    Seq(
      structSwitch(
        "public org.apache.spark.sql.catalyst.util.ArrayData getArray(int ordinal)",
        "getArray",
        getArrayCases),
      structSwitch(
        "public org.apache.spark.sql.catalyst.InternalRow getStruct(int ordinal, int numFields)",
        "getStruct",
        getStructCases)).mkString
  }

  /**
   * Emit one `@Override`-annotated switch method inside an `InputStruct_${path}` class. Returns
   * an empty string when the struct has no fields matched by this getter.
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
   * Emit the kernel's top-level `@Override public InternalRow getStruct(int ordinal, int
   * numFields)` method when the input schema has at least one struct-typed column at the top
   * level; empty string otherwise.
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
   * Build one `@Override`-annotated switch method for the top-level kernel. Returns an empty
   * string when no input columns use this getter so the generated class does not carry a dead
   * method override.
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
