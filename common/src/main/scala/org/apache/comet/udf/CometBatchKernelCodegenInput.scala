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
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampNTZType, TimestampType}

import org.apache.comet.udf.CometBatchKernelCodegen.{ArrayColumnSpec, ArrowColumnSpec, MapColumnSpec, ScalarColumnSpec, StructColumnSpec}

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
    case mp: MapColumnSpec =>
      out += s"private ${classOf[MapVector].getName} $path;"
      // Key and value vectors live at `${P}_k_e` / `${P}_v_e` so the `InputArray_${P}_k` /
      // `InputArray_${P}_v` synthetic classes (which follow the array-element convention of
      // reading from `${path}_e`) resolve their element reads correctly.
      collectVectorFieldDecls(s"${path}_k_e", mp.key, out)
      collectVectorFieldDecls(s"${path}_v_e", mp.value, out)
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
    case mp: MapColumnSpec =>
      // MapVector's data vector is a StructVector with key at child 0 and value at child 1.
      // Grab the struct through a local var and pull out the typed children. The key / value
      // vectors live at the `_k_e` / `_v_e` paths so the synthetic `InputArray_${P}_k` /
      // `InputArray_${P}_v` classes read them via the standard array-element convention.
      val structLocal = s"${path}__mapStruct"
      out += s"this.$path = (${classOf[MapVector].getName}) $source;"
      out += s"${classOf[StructVector].getName} $structLocal = " +
        s"(${classOf[StructVector].getName}) this.$path.getDataVector();"
      collectCasts(s"${path}_k_e", mp.key, s"$structLocal.getChildByOrdinal(0)", out)
      collectCasts(s"${path}_v_e", mp.value, s"$structLocal.getChildByOrdinal(1)", out)
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
   * TODO(unsafe-readers): primitive getters go through Arrow's typed `v.get(i)` which performs
   * bounds checks. Inside the kernel's `process` loop `i` is always in `[0, numRows)`, so the
   * check is redundant. Mirror `CometPlainVector`'s pattern (cache validity/value/offset buffer
   * addresses, use direct `Platform.getInt` reads) behind a benchmark.
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
   * case per ordinal.
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
   * Emit the element getter body for a nested `InputArray_${path}`. Scalar element → direct typed
   * read. Complex element → `getArray(i)` / `getStruct(i, n)` / `getMap(i)` that resets the inner
   * instance.
   */
  private def emitArrayElementGetter(path: String, spec: ArrayColumnSpec): String = {
    val elemPath = s"${path}_e"
    spec.element match {
      case _: ScalarColumnSpec =>
        scalarElementGetter(spec.elementSparkType, elemPath)
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
           |          int s = ${path}_f$fi.getStartOffset(this.rowIdx);
           |          int e = ${path}_f$fi.getEndOffset(this.rowIdx);
           |          long addr = ${path}_f$fi.getDataBuffer().memoryAddress() + s;
           |          return org.apache.spark.unsafe.types.UTF8String
           |              .fromAddress(null, addr, e - s);
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

  /**
   * Emit a zero-copy `getUTF8String` case for a `ViewVarCharVector` column at the given ordinal.
   * Reads the 16-byte view entry directly from the view buffer and either points at the inline
   * bytes (length &lt;= INLINE_SIZE=12) or at the referenced data buffer via `(bufferIndex,
   * offset)` (length &gt; 12).
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
