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

import scala.collection.mutable

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.types._

import org.apache.comet.codegen.CometBatchKernelCodegen.{ArrayColumnSpec, ArrowColumnSpec, MapColumnSpec, ScalarColumnSpec, StructColumnSpec}
import org.apache.comet.vector.CometPlainVector

/**
 * Input-side emitters for the codegen kernel: typed field declarations, per-batch input casts,
 * top-level typed-getter switches, nested `InputArray_${path}` / `InputStruct_${path}` /
 * `InputMap_${path}` classes per complex level. Paired with [[CometBatchKernelCodegenOutput]].
 *
 * Path encoding. Each position in the spec tree has a unique path string used as a suffix on
 * vector fields and nested classes. From a column ordinal: root `col${ord}`, array element
 * `${P}_e`, struct field `fi` `${P}_f${fi}`, map key `${P}_k`, map value `${P}_v`.
 *
 * Nested-class composition. Each instance is allocated fresh per `getArray(i)` / `getStruct(i,
 * n)` / `getMap(i)` call, with `final` slice fields. Matches Spark's `ColumnarRow` /
 * `ColumnarArray` model: retain-by-reference consumers (e.g. `ArrayDistinct.nullSafeEval`
 * stashing references in an `OpenHashSet`) get distinct identities, and JIT escape analysis
 * usually scalarizes the allocation when the value is consumed locally.
 */
private[codegen] object CometBatchKernelCodegenInput {

  /**
   * Primitive Arrow vector classes wrapped in [[CometPlainVector]] at input-cast time so per-row
   * reads go through `Platform.get*` against a cached buffer address (JIT inlines to branchless
   * reads). Decimal/VarChar/VarBinary stay on the typed Arrow field with cached buffer addresses
   * for inline unsafe reads.
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
  private val cometPlainVectorName: String = classOf[CometPlainVector].getName

  /** Emit kernel typed-vector field declarations for every level of every input column. */
  def emitInputFieldDecls(inputSchema: Seq[ArrowColumnSpec]): String = {
    val lines = new mutable.ArrayBuffer[String]()
    inputSchema.zipWithIndex.foreach { case (spec, ord) =>
      val path = s"col$ord"
      collectVectorFieldDecls(path, spec, lines)
    }
    lines.mkString("\n  ")
  }

  /**
   * Emit per-batch cast statements, recursing through complex types via `getDataVector` / etc.
   */
  def emitInputCasts(inputSchema: Seq[ArrowColumnSpec]): String = {
    val lines = new mutable.ArrayBuffer[String]()
    inputSchema.zipWithIndex.foreach { case (spec, ord) =>
      val path = s"col$ord"
      collectCasts(path, spec, s"inputs[$ord]", lines)
    }
    lines.mkString("\n    ")
  }

  /**
   * Emit typed-getter overrides. Each switches on column ordinal. With the inlined constant
   * ordinal from `BoundReference.genCode`, JIT folds the switch to one branch.
   *
   * `decimalTypeByOrdinal` lets the decimal getter specialize per ordinal: when only a
   * `DecimalType(precision <= 18)` `BoundReference` reads the ordinal, the case skips the
   * `BigDecimal` allocation and reads the unscaled long directly.
   */
  def emitTypedGetters(
      inputSchema: Seq[ArrowColumnSpec],
      decimalTypeByOrdinal: Map[Int, Option[DecimalType]]): String = {
    val withOrd = inputSchema.zipWithIndex

    val isNullCases = withOrd.map { case (spec, ord) =>
      if (!spec.nullable) {
        s"      case $ord: return false;"
      } else {
        // CometPlainVector exposes `isNullAt`; Arrow-typed fields expose `isNull`. Same semantics.
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
          case Some(dt) if dt.precision <= Decimal.MAX_LONG_DIGITS => fastPath
          case Some(_) => slowPath
          case None =>
            s"""        if (precision <= ${Decimal.MAX_LONG_DIGITS}) {
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

  private def wrapsInCometPlainVector(cls: Class[_]): Boolean =
    primitiveArrowClasses.contains(cls)

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

  private def emitDecimalSlowBody(field: String, idx: String, ind: String): String = {
    val cont = ind + "    "
    s"""${ind}java.math.BigDecimal bd = $field.getObject($idx);
       |${ind}return org.apache.spark.sql.types.Decimal$$.MODULE$$
       |$cont.apply(bd, precision, scale);""".stripMargin
  }

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

  /** Parenthesize `idx` when it contains whitespace, to keep `(long) idx * 16L` well-formed. */
  private def castableIdx(idx: String): String = if (idx.contains(' ')) s"($idx)" else idx

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

  /**
   * Per-ordinal map of the `DecimalType` observed on `BoundReference`s. Used by
   * [[emitTypedGetters]] to emit a precision-specialized `getDecimal` case per ordinal.
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
   * Emit nested classes for every complex level of every input column: `InputArray_${path}` for
   * arrays, `InputStruct_${path}` for structs, `InputMap_${path}` plus `InputArray` views for the
   * key/value slices for maps (Spark's `MapData.keyArray()` / `valueArray()` return `ArrayData`).
   */
  def emitNestedClasses(inputSchema: Seq[ArrowColumnSpec]): String = {
    val out = new mutable.ArrayBuffer[String]()
    inputSchema.zipWithIndex.foreach { case (spec, ord) =>
      collectNestedClasses(s"col$ord", spec, out)
    }
    out.mkString("\n")
  }

  /**
   * Top-level `getArray(int ordinal)` switch. Each case reads `(start, length)` from the outer
   * `ListVector` offsets and allocates a fresh `InputArray_col${ord}` view.
   */
  def emitGetArrayMethod(inputSchema: Seq[ArrowColumnSpec]): String = {
    val cases = inputSchema.zipWithIndex.collect { case (_: ArrayColumnSpec, ord) =>
      s"""      case $ord: {
         |        int __idx = this.rowIdx;
         |        int __s = this.col$ord.getElementStartIndex(__idx);
         |        int __e = this.col$ord.getElementEndIndex(__idx);
         |        return new InputArray_col$ord(__s, __e - __s);
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

  /** Top-level `getMap(int ordinal)` switch when the schema has at least one map column. */
  def emitGetMapMethod(inputSchema: Seq[ArrowColumnSpec]): String = {
    val cases = inputSchema.zipWithIndex.collect { case (_: MapColumnSpec, ord) =>
      s"""      case $ord: {
         |        int __idx = this.rowIdx;
         |        int __s = this.col$ord.getElementStartIndex(__idx);
         |        int __e = this.col$ord.getElementEndIndex(__idx);
         |        return new InputMap_col$ord(__s, __e - __s);
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

  /** Top-level `getStruct(int ordinal, int numFields)` switch when the schema has any struct. */
  def emitGetStructMethod(inputSchema: Seq[ArrowColumnSpec]): String = {
    val cases = inputSchema.zipWithIndex.collect { case (_: StructColumnSpec, ord) =>
      s"""      case $ord: return new InputStruct_col$ord(this.rowIdx);""".stripMargin
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
   * Scalar columns that need a cached data-buffer address for inline unsafe reads.
   * `DecimalVector` uses it for the short-precision fast path (`Platform.getLong`);
   * `VarCharVector` / `VarBinaryVector` use it as the base for `UTF8String.fromAddress` /
   * `Platform.copyMemory`.
   */
  private def needsValueAddrField(cls: Class[_]): Boolean =
    cls == classOf[DecimalVector] ||
      cls == classOf[VarCharVector] ||
      cls == classOf[VarBinaryVector]

  /** Variable-width columns also cache the offset-buffer address for `Platform.getInt`. */
  private def needsOffsetAddrField(cls: Class[_]): Boolean =
    cls == classOf[VarCharVector] || cls == classOf[VarBinaryVector]

  /**
   * Java method name for the per-column null check. Primitive scalars wrapped in
   * [[CometPlainVector]] expose `isNullAt`; Arrow typed fields expose `isNull`. Same semantics.
   * Used both by `emitTypedGetters` (for the kernel's `isNullAt` switch) and by
   * `CometBatchKernelCodegen.defaultBody` (for the `NullIntolerant` short-circuit).
   */
  def nullCheckMethod(spec: ArrowColumnSpec): String = spec match {
    case sc: ScalarColumnSpec if wrapsInCometPlainVector(sc.vectorClass) => "isNullAt"
    case _ => "isNull"
  }

  private def collectVectorFieldDecls(
      path: String,
      spec: ArrowColumnSpec,
      out: mutable.ArrayBuffer[String]): Unit = spec match {
    case sc: ScalarColumnSpec =>
      // Primitive scalars wrap in CometPlainVector for JIT-inlined Platform.get* against a
      // cached buffer address. Decimal/VarChar/VarBinary stay on the Arrow typed field with
      // cached data- (and offset-) buffer addresses for inline unsafe reads.
      val fieldClass =
        if (wrapsInCometPlainVector(sc.vectorClass)) cometPlainVectorName
        else sc.vectorClass.getName
      out += s"private $fieldClass $path;"
      if (needsValueAddrField(sc.vectorClass)) {
        out += s"private long ${path}_valueAddr;"
      }
      if (needsOffsetAddrField(sc.vectorClass)) {
        out += s"private long ${path}_offsetAddr;"
      }
    case ar: ArrayColumnSpec =>
      out += s"private ${classOf[ListVector].getName} $path;"
      collectVectorFieldDecls(s"${path}_e", ar.element, out)
    case st: StructColumnSpec =>
      out += s"private ${classOf[StructVector].getName} $path;"
      st.fields.zipWithIndex.foreach { case (f, fi) =>
        collectVectorFieldDecls(s"${path}_f$fi", f.child, out)
      }
    case mp: MapColumnSpec =>
      out += s"private ${classOf[MapVector].getName} $path;"
      // Key/value vectors live at `${P}_k_e` / `${P}_v_e` so the synthetic `InputArray_${P}_k` /
      // `InputArray_${P}_v` classes (which follow the array-element convention of reading from
      // `${path}_e`) resolve correctly.
      collectVectorFieldDecls(s"${path}_k_e", mp.key, out)
      collectVectorFieldDecls(s"${path}_v_e", mp.value, out)
  }

  private def collectCasts(
      path: String,
      spec: ArrowColumnSpec,
      source: String,
      out: mutable.ArrayBuffer[String]): Unit = spec match {
    case sc: ScalarColumnSpec =>
      if (wrapsInCometPlainVector(sc.vectorClass)) {
        out += s"this.$path = new $cometPlainVectorName($source);"
      } else {
        out += s"this.$path = (${sc.vectorClass.getName}) $source;"
      }
      if (needsValueAddrField(sc.vectorClass)) {
        out += s"this.${path}_valueAddr = this.$path.getDataBuffer().memoryAddress();"
      }
      if (needsOffsetAddrField(sc.vectorClass)) {
        out += s"this.${path}_offsetAddr = this.$path.getOffsetBuffer().memoryAddress();"
      }
    case ar: ArrayColumnSpec =>
      out += s"this.$path = (${classOf[ListVector].getName}) $source;"
      collectCasts(s"${path}_e", ar.element, s"this.$path.getDataVector()", out)
    case st: StructColumnSpec =>
      out += s"this.$path = (${classOf[StructVector].getName}) $source;"
      st.fields.zipWithIndex.foreach { case (f, fi) =>
        collectCasts(s"${path}_f$fi", f.child, s"this.$path.getChildByOrdinal($fi)", out)
      }
    case mp: MapColumnSpec =>
      // MapVector's data vector is a StructVector with key at child 0 and value at child 1.
      val structLocal = s"${path}__mapStruct"
      out += s"this.$path = (${classOf[MapVector].getName}) $source;"
      out += s"${classOf[StructVector].getName} $structLocal = " +
        s"(${classOf[StructVector].getName}) this.$path.getDataVector();"
      collectCasts(s"${path}_k_e", mp.key, s"$structLocal.getChildByOrdinal(0)", out)
      collectCasts(s"${path}_v_e", mp.value, s"$structLocal.getChildByOrdinal(1)", out)
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
      out += emitMapClass(path)
      // Emit `InputArray_${path}_k` / `InputArray_${path}_v` (the views returned by
      // `keyArray()` / `valueArray()`). Each reads from `${classPath}_e` per the array-element
      // convention, mapping to the key/value vector at `${path}_k_e` / `${path}_v_e`.
      out += emitArrayClass(
        s"${path}_k",
        ArrayColumnSpec(nullable = true, elementSparkType = mp.keySparkType, element = mp.key))
      out += emitArrayClass(
        s"${path}_v",
        ArrayColumnSpec(
          nullable = true,
          elementSparkType = mp.valueSparkType,
          element = mp.value))
      collectNestedClasses(s"${path}_k_e", mp.key, out)
      collectNestedClasses(s"${path}_v_e", mp.value, out)
  }

  /**
   * Emit one `InputArray_${path}` nested class. Constructor takes `(startIdx, length)` and stores
   * both in `final` fields. Map key/value arrays share this shape.
   */
  private def emitArrayClass(path: String, spec: ArrayColumnSpec): String = {
    val baseClassName = classOf[CometArrayData].getName
    val elemPath = s"${path}_e"
    val isNullAt =
      s"""      @Override
         |      public boolean isNullAt(int i) {
         |        return $elemPath.${nullCheckMethod(spec.element)}(startIndex + i);
         |      }""".stripMargin
    val elementGetter = emitArrayElementGetter(path, spec)
    s"""  private final class InputArray_$path extends $baseClassName {
       |    private final int startIndex;
       |    private final int length;
       |
       |    InputArray_$path(int startIdx, int len) {
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
   * Element-getter body for a nested array. Scalar -> direct typed read. Complex -> allocate a
   * fresh inner view.
   *
   * Reference-typed getters (`getDecimal` / `getUTF8String` / `getBinary` / `getStruct` /
   * `getArray` / `getMap`) prepend `if (isNullAt(i)) return null;` when the element is nullable,
   * because Spark's `CodeGenerator.setArrayElement` only emits the caller-side `isNullAt` check
   * for primitive elements (it relies on the source's getter to return null for reference types,
   * matching `ColumnarArray.getBinary`). Without this guard, expressions like `Flatten.doGenCode`
   * write empty bytes / garbage decimals where Spark expects null.
   */
  private def emitArrayElementGetter(path: String, spec: ArrayColumnSpec): String = {
    val elemPath = s"${path}_e"
    val nullGuard =
      if (spec.element.nullable) "        if (isNullAt(i)) return null;\n"
      else ""
    spec.element match {
      case _: ScalarColumnSpec =>
        emitArrayElementScalarGetter(spec.elementSparkType, elemPath, spec.element.nullable)
      case _: ArrayColumnSpec =>
        s"""      @Override
           |      public org.apache.spark.sql.catalyst.util.ArrayData getArray(int i) {
           |$nullGuard        int __idx = startIndex + i;
           |        int __s = $elemPath.getElementStartIndex(__idx);
           |        int __e = $elemPath.getElementEndIndex(__idx);
           |        return new InputArray_$elemPath(__s, __e - __s);
           |      }""".stripMargin
      case _: StructColumnSpec =>
        s"""      @Override
           |      public org.apache.spark.sql.catalyst.InternalRow getStruct(int i, int numFields) {
           |$nullGuard        return new InputStruct_$elemPath(startIndex + i);
           |      }""".stripMargin
      case _: MapColumnSpec =>
        s"""      @Override
           |      public org.apache.spark.sql.catalyst.util.MapData getMap(int i) {
           |$nullGuard        int __idx = startIndex + i;
           |        int __s = $elemPath.getElementStartIndex(__idx);
           |        int __e = $elemPath.getElementEndIndex(__idx);
           |        return new InputMap_$elemPath(__s, __e - __s);
           |      }""".stripMargin
    }
  }

  /**
   * Scalar-element getter override. Only the getter matching the element type is overridden;
   * other getters inherit the base class's `UnsupportedOperationException`. Reference-typed
   * getters (Decimal / String / Binary) prepend the null guard documented on
   * [[emitArrayElementGetter]].
   */
  private def emitArrayElementScalarGetter(
      elemType: DataType,
      childField: String,
      elementNullable: Boolean): String = {
    val nullGuard =
      if (elementNullable) "        if (isNullAt(i)) return null;\n"
      else ""
    elemType match {
      case BooleanType =>
        s"""      @Override
           |      public boolean getBoolean(int i) {
           |        return $childField.getBoolean(startIndex + i);
           |      }""".stripMargin
      case ByteType =>
        s"""      @Override
           |      public byte getByte(int i) {
           |        return $childField.getByte(startIndex + i);
           |      }""".stripMargin
      case ShortType =>
        s"""      @Override
           |      public short getShort(int i) {
           |        return $childField.getShort(startIndex + i);
           |      }""".stripMargin
      case IntegerType | DateType =>
        s"""      @Override
           |      public int getInt(int i) {
           |        return $childField.getInt(startIndex + i);
           |      }""".stripMargin
      case LongType | TimestampType | TimestampNTZType =>
        s"""      @Override
           |      public long getLong(int i) {
           |        return $childField.getLong(startIndex + i);
           |      }""".stripMargin
      case FloatType =>
        s"""      @Override
           |      public float getFloat(int i) {
           |        return $childField.getFloat(startIndex + i);
           |      }""".stripMargin
      case DoubleType =>
        s"""      @Override
           |      public double getDouble(int i) {
           |        return $childField.getDouble(startIndex + i);
           |      }""".stripMargin
      case dt: DecimalType =>
        val body =
          if (dt.precision <= Decimal.MAX_LONG_DIGITS) {
            emitDecimalFastBodyUnsafe(s"${childField}_valueAddr", "startIndex + i", "        ")
          } else {
            emitDecimalSlowBody(childField, "startIndex + i", "        ")
          }
        s"""      @Override
           |      public org.apache.spark.sql.types.Decimal getDecimal(
           |          int i, int precision, int scale) {
           |$nullGuard$body
           |      }""".stripMargin
      case _: StringType =>
        s"""      @Override
           |      public org.apache.spark.unsafe.types.UTF8String getUTF8String(int i) {
           |$nullGuard${emitUtf8BodyUnsafe(
            s"${childField}_valueAddr",
            s"${childField}_offsetAddr",
            "startIndex + i",
            "        ")}
           |      }""".stripMargin
      case BinaryType =>
        s"""      @Override
           |      public byte[] getBinary(int i) {
           |$nullGuard${emitBinaryBodyUnsafe(
            s"${childField}_valueAddr",
            s"${childField}_offsetAddr",
            "startIndex + i",
            "        ")}
           |      }""".stripMargin
      case other =>
        throw new UnsupportedOperationException(
          s"nested ArrayData: unsupported element type $other")
    }
  }

  /**
   * Emit one `InputStruct_${path}` nested class. Constructor takes `rowIdx` and stores it in a
   * `final` field. Scalar getters switch on field ordinal. Complex getters allocate fresh inner
   * views (offsets computed for array/map children, rowIdx passed through for struct children).
   */
  private def emitStructClass(path: String, spec: StructColumnSpec): String = {
    val baseClassName = classOf[CometInternalRow].getName
    val isNullCases = spec.fields.zipWithIndex.map {
      case (f, fi) if !f.nullable =>
        s"        case $fi: return false;"
      case (f, fi) =>
        s"        case $fi: return ${path}_f$fi.${nullCheckMethod(f.child)}(this.rowIdx);"
    }
    val scalarGetters = emitStructScalarGetters(path, spec)
    val complexGetters = emitStructComplexGetters(path, spec)
    s"""  private final class InputStruct_$path extends $baseClassName {
       |    private final int rowIdx;
       |
       |    InputStruct_$path(int outerRowIdx) {
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

  // Scalar-read body templates parameterized on row-index expression (`idx`), cached buffer
  // addresses (`valueAddr`, `offsetAddr`) for unsafe reads, or the Arrow field for the decimal
  // slow path. `ind` is the per-line indent.
  //
  // TODO(#4280, #4279): once offset-address caching and validity-bitmap byte cache land in
  // CometPlainVector, replace the VarChar/VarBinary unsafe emitters with CometPlainVector reads.

  private def emitStructScalarGetters(path: String, spec: StructColumnSpec): String = {
    val withOrd = spec.fields.zipWithIndex
    val scalarOrd = withOrd.filter { case (f, _) => f.child.isInstanceOf[ScalarColumnSpec] }

    // For nullable reference-typed struct fields, prepend the null guard so `getX(ord)` returns
    // null on null positions (Spark contract for reference types). Same rationale as the array
    // element getter.
    def nullGuardForCase(fi: Int, fieldNullable: Boolean): String =
      if (fieldNullable) s"          if (isNullAt($fi)) return null;\n"
      else ""

    def fieldReadScalar(fi: Int, dt: DataType, fieldNullable: Boolean): String = {
      val guard = nullGuardForCase(fi, fieldNullable)
      dt match {
        case BooleanType =>
          s"        case $fi: return ${path}_f$fi.getBoolean(this.rowIdx);"
        case ByteType =>
          s"        case $fi: return ${path}_f$fi.getByte(this.rowIdx);"
        case ShortType =>
          s"        case $fi: return ${path}_f$fi.getShort(this.rowIdx);"
        case IntegerType | DateType =>
          s"        case $fi: return ${path}_f$fi.getInt(this.rowIdx);"
        case LongType | TimestampType | TimestampNTZType =>
          s"        case $fi: return ${path}_f$fi.getLong(this.rowIdx);"
        case FloatType =>
          s"        case $fi: return ${path}_f$fi.getFloat(this.rowIdx);"
        case DoubleType =>
          s"        case $fi: return ${path}_f$fi.getDouble(this.rowIdx);"
        case BinaryType =>
          s"""        case $fi: {
             |$guard${emitBinaryBodyUnsafe(
              s"${path}_f${fi}_valueAddr",
              s"${path}_f${fi}_offsetAddr",
              "this.rowIdx",
              "          ")}
             |        }""".stripMargin
        case _: StringType =>
          s"""        case $fi: {
             |$guard${emitUtf8BodyUnsafe(
              s"${path}_f${fi}_valueAddr",
              s"${path}_f${fi}_offsetAddr",
              "this.rowIdx",
              "          ")}
             |        }""".stripMargin
        case _: DecimalType =>
          throw new IllegalStateException("decimal handled separately")
        case other =>
          throw new UnsupportedOperationException(
            s"nested InputStruct getter: unsupported field type $other")
      }
    }

    val booleanCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == BooleanType =>
          fieldReadScalar(fi, BooleanType, f.nullable)
      }
    val byteCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == ByteType =>
          fieldReadScalar(fi, ByteType, f.nullable)
      }
    val shortCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == ShortType =>
          fieldReadScalar(fi, ShortType, f.nullable)
      }
    val intCases = scalarOrd.collect {
      case (f, fi) if f.sparkType == IntegerType || f.sparkType == DateType =>
        fieldReadScalar(fi, IntegerType, f.nullable)
    }
    val longCases = scalarOrd.collect {
      case (f, fi)
          if f.sparkType == LongType || f.sparkType == TimestampType ||
            f.sparkType == TimestampNTZType =>
        fieldReadScalar(fi, LongType, f.nullable)
    }
    val floatCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == FloatType =>
          fieldReadScalar(fi, FloatType, f.nullable)
      }
    val doubleCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == DoubleType =>
          fieldReadScalar(fi, DoubleType, f.nullable)
      }
    val binaryCases =
      scalarOrd.collect {
        case (f, fi) if f.sparkType == BinaryType =>
          fieldReadScalar(fi, BinaryType, f.nullable)
      }
    val utf8Cases = scalarOrd.collect {
      case (f, fi) if f.sparkType.isInstanceOf[StringType] =>
        fieldReadScalar(fi, f.sparkType, f.nullable)
    }

    val decimalCases = scalarOrd.collect {
      case (f, fi) if f.sparkType.isInstanceOf[DecimalType] =>
        val dt = f.sparkType.asInstanceOf[DecimalType]
        val field = s"${path}_f$fi"
        val body =
          if (dt.precision <= Decimal.MAX_LONG_DIGITS) {
            emitDecimalFastBodyUnsafe(s"${field}_valueAddr", "this.rowIdx", "          ")
          } else {
            emitDecimalSlowBody(field, "this.rowIdx", "          ")
          }
        val guard = nullGuardForCase(fi, f.nullable)
        s"""        case $fi: {
           |$guard$body
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
    // Same null-guard rationale as `emitArrayElementGetter`.
    def guardLine(fi: Int, fieldNullable: Boolean): String =
      if (fieldNullable) s"          if (isNullAt($fi)) return null;\n"
      else ""
    val getArrayCases = spec.fields.zipWithIndex.collect {
      case (f, fi) if f.child.isInstanceOf[ArrayColumnSpec] =>
        val fieldPath = s"${path}_f$fi"
        s"""        case $fi: {
           |${guardLine(fi, f.nullable)}          int __idx = this.rowIdx;
           |          int __s = $fieldPath.getElementStartIndex(__idx);
           |          int __e = $fieldPath.getElementEndIndex(__idx);
           |          return new InputArray_$fieldPath(__s, __e - __s);
           |        }""".stripMargin
    }
    val getStructCases = spec.fields.zipWithIndex.collect {
      case (f, fi) if f.child.isInstanceOf[StructColumnSpec] =>
        val fieldPath = s"${path}_f$fi"
        if (f.nullable) {
          s"""        case $fi: {
             |${guardLine(
              fi,
              f.nullable)}          return new InputStruct_$fieldPath(this.rowIdx);
             |        }""".stripMargin
        } else {
          s"        case $fi: return new InputStruct_$fieldPath(this.rowIdx);"
        }
    }
    val getMapCases = spec.fields.zipWithIndex.collect {
      case (f, fi) if f.child.isInstanceOf[MapColumnSpec] =>
        val fieldPath = s"${path}_f$fi"
        s"""        case $fi: {
           |${guardLine(fi, f.nullable)}          int __idx = this.rowIdx;
           |          int __s = $fieldPath.getElementStartIndex(__idx);
           |          int __e = $fieldPath.getElementEndIndex(__idx);
           |          return new InputMap_$fieldPath(__s, __e - __s);
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
   * Emit one `InputMap_${path}` nested class. Constructor takes `(start, length)`; `keyArray()` /
   * `valueArray()` allocate fresh `InputArray_${path}_k` / `InputArray_${path}_v` views.
   */
  private def emitMapClass(path: String): String = {
    val baseClassName = classOf[CometMapData].getName
    val keyPath = s"${path}_k"
    val valPath = s"${path}_v"
    s"""  private final class InputMap_$path extends $baseClassName {
       |    private final int startIndex;
       |    private final int length;
       |
       |    InputMap_$path(int startIdx, int len) {
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
       |      return new InputArray_$keyPath(this.startIndex, this.length);
       |    }
       |
       |    @Override
       |    public org.apache.spark.sql.catalyst.util.ArrayData valueArray() {
       |      return new InputArray_$valPath(this.startIndex, this.length);
       |    }
       |  }
       |""".stripMargin
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
}
