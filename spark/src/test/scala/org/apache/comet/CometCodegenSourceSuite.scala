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

package org.apache.comet

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Add, AddMonths, BoundReference, Coalesce, Concat, CreateArray, CreateMap, DateFormatClass, ElementAt, Expression, GetStructField, LeafExpression, Length, Literal, MakeTimestamp, MicrosToTimestamp, MillisToTimestamp, MonthsBetween, Nondeterministic, Rand, Size, ToUnixTimestamp, Unevaluable, UnixMicros, UnixMillis, UnixSeconds, Upper}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeFormatter, CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.codegen.CometBatchKernelCodegen
import org.apache.comet.codegen.CometBatchKernelCodegen.{ArrayColumnSpec, ArrowColumnSpec, MapColumnSpec, ScalarColumnSpec, StructColumnSpec, StructFieldSpec}
import org.apache.comet.udf.codegen.CometScalaUDFCodegen

// Resolve Arrow vector classes through the codegen object so tests see the same `Class` objects
// the codegen pattern-matches against, regardless of any future shading rearrangement.

/**
 * Generated-source inspection tests. These exercise `CometBatchKernelCodegen.generateSource` and
 * assert on the emitted Java directly, without invoking Janino. The goal is to catch regressions
 * in the optimizations we claim the dispatcher applies:
 *
 *   - `NullIntolerant` short-circuit wraps `ev.code` in `if (any-input-null) { setNull } else {
 *     ev.code; write }`.
 *   - Non-nullable column declaration emits `return false;` from `isNullAt(ord)`, and a
 *     `BoundReference.nullable=false` (Catalyst sets this from schema-declared nullability) makes
 *     Spark's `doGenCode` skip emitting its own `row.isNullAt(ord)` probe entirely.
 *   - Zero-copy string reads route through `UTF8String.fromAddress`.
 *
 * These are the smallest durable tests that the claimed optimizations actually reach the
 * generated Java, and they document the shapes future contributors should preserve.
 */
class CometCodegenSourceSuite extends AnyFunSuite {

  private val varCharVectorClass =
    CometBatchKernelCodegen.vectorClassBySimpleName("VarCharVector")

  private val nullableString = ArrowColumnSpec(varCharVectorClass, nullable = true)
  private val nonNullableString = ArrowColumnSpec(varCharVectorClass, nullable = false)

  private def gen(
      expr: org.apache.spark.sql.catalyst.expressions.Expression,
      specs: ArrowColumnSpec*): String =
    CometBatchKernelCodegen.generateSource(expr, specs.toIndexedSeq).body

  test("NullIntolerant short-circuit uses isNullAt for CometPlainVector-wrapped columns") {
    // Primitive Arrow vectors (timestamp / int / float / ...) are wrapped in `CometPlainVector`
    // at input-cast time. The short-circuit must call `isNullAt(i)`, not `isNull(i)`, otherwise
    // Janino fails to compile the kernel with "method isNull not declared". Verified end-to-end
    // by `CometTemporalExpressionSuite` date_format tests over `TimeStampMicroTZVector` inputs.
    val tsVec = CometBatchKernelCodegen.vectorClassBySimpleName("TimeStampMicroTZVector")
    val spec = ArrowColumnSpec(tsVec, nullable = true)
    val expr = DateFormatClass(
      BoundReference(0, TimestampType, nullable = true),
      Literal(UTF8String.fromString("yyyy-MM-dd EEEE"), StringType),
      Some("UTC"))
    val src = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(spec)).body
    assert(
      src.contains("if (this.col0.isNullAt(i))"),
      s"expected short-circuit to use isNullAt for CometPlainVector-wrapped col0; got:\n$src")
    assert(
      !src.contains("if (this.col0.isNull(i))"),
      s"expected no raw Arrow isNull on the CometPlainVector-wrapped col0; got:\n$src")
  }

  test("non-nullable column emits literal-false isNullAt case") {
    val expr = Length(BoundReference(0, StringType, nullable = false))
    val src = gen(expr, nonNullableString)
    assert(
      src.contains("case 0: return false;"),
      s"expected non-nullable isNullAt to return literal false; got:\n$src")
  }

  test("non-nullable BoundReference elides Spark's own isNullAt probe in the expression body") {
    // When the BoundReference carries `nullable=false` (Catalyst sets this from schema-declared
    // nullability), Spark's `doGenCode` skips the `row.isNullAt(ord)` branch at source level.
    // The dispatcher does not derive runtime nullability anymore. The BoundReference's source
    // flag is the sole signal, and schema-non-null columns get full elision for free.
    val expr = Length(BoundReference(0, StringType, nullable = false))
    val src = gen(expr, nonNullableString)
    assert(
      !src.contains("row.isNullAt(0)"),
      s"expected Spark's BoundReference null probe to be elided; got:\n$src")
  }

  test("nullable column emits delegated isNullAt case") {
    val expr = Length(BoundReference(0, StringType, nullable = true))
    val src = gen(expr, nullableString)
    assert(
      src.contains("case 0: return this.col0.isNull(this.rowIdx);"),
      s"expected nullable isNullAt to delegate to the Arrow vector; got:\n$src")
  }

  test("VarCharVector getUTF8String uses zero-copy fromAddress") {
    val expr = Length(BoundReference(0, StringType, nullable = true))
    val src = gen(expr, nullableString)
    assert(
      src.contains("org.apache.spark.unsafe.types.UTF8String"),
      s"expected UTF8String reference; got:\n$src")
    assert(src.contains(".fromAddress("), s"expected zero-copy fromAddress read; got:\n$src")
  }

  test("NullIntolerant expression emits input-null short-circuit before ev.code") {
    // Upper is NullIntolerant (null in -> null out). Expect the default body to prepend
    // `if (this.col0.isNull(i)) { setNull; } else { ... }` so null rows skip the whole
    // expression eval, not just the setNull write.
    val expr = Upper(BoundReference(0, StringType, nullable = true))
    val src = gen(expr, nullableString)
    assert(
      src.contains("this.col0.isNull(i)"),
      s"expected NullIntolerant short-circuit on input ordinal 0; got:\n$src")
    assert(
      src.contains("output.setNull(i);"),
      s"expected setNull emission for short-circuited null rows; got:\n$src")
  }

  test("NullIntolerant short-circuit emitted when every node is NullIntolerant") {
    // Length(Upper(BoundReference)): Length is NullIntolerant, Upper is NullIntolerant,
    // BoundReference is a leaf. Every path from a leaf to the root propagates nulls, so the
    // short-circuit heuristic ("any input null -> output null") holds.
    val expr = Length(Upper(BoundReference(0, StringType, nullable = true)))
    val src = gen(expr, nullableString)
    assert(
      src.contains("if (this.col0.isNull(i))"),
      s"expected short-circuit on col0 when every node is NullIntolerant; got:\n$src")
  }

  test("NullIntolerant short-circuit skipped when a non-NullIntolerant node breaks the chain") {
    // Concat is not NullIntolerant. Null in some args doesn't necessarily produce a null
    // result. The short-circuit heuristic would be incorrect here (short-circuiting on c0 or c1
    // being null would skip evaluation, but Concat's null handling differs). Expect the
    // default path without the `if (colX.isNull(i) || colY.isNull(i))` wrapper, letting Spark's
    // own `ev.code` handle nulls correctly.
    val nullable1 = ArrowColumnSpec(varCharVectorClass, nullable = true)
    val nullable2 = ArrowColumnSpec(varCharVectorClass, nullable = true)
    val expr = Length(
      Concat(
        Seq(
          BoundReference(0, StringType, nullable = true),
          BoundReference(1, StringType, nullable = true))))
    val src = gen(expr, nullable1, nullable2)
    assert(
      !src.contains("this.col0.isNull(i) || this.col1.isNull(i)"),
      "expected no pre-null short-circuit when Concat breaks the NullIntolerant chain; " +
        s"got:\n$src")
  }

  test("canHandle accepts CodegenFallback expressions (delegates to eval(row))") {
    // CodegenFallback.doGenCode emits ((Expression) references[N]).eval(row) which is the same
    // mechanism that backs HigherOrderFunction support: the eval reads through the kernel's typed
    // Arrow getters via the row alias. Other CodegenFallback expressions (JsonToStructs,
    // StructsToJson, ...) ride the same path.
    val expr = FakeCodegenFallback(BoundReference(0, StringType, nullable = true))
    val reason = CometBatchKernelCodegen.canHandle(expr)
    assert(
      reason.isEmpty,
      s"expected canHandle to accept CodegenFallback; got rejection: ${reason.getOrElse("")}")
  }

  test("canHandle accepts Nondeterministic expressions (per-partition kernel handles state)") {
    // Each cache entry holds one kernel instance with `init(partitionIndex)` called once, so
    // Rand / Uuid / etc. produce the expected per-partition sequences across batches. The
    // previous canHandle rejection was conservative. With that caching in place, accepting
    // Nondeterministic is correct.
    val expr = FakeNondeterministic()
    val reason = CometBatchKernelCodegen.canHandle(expr)
    assert(reason.isEmpty, s"expected canHandle to accept Nondeterministic; got $reason")
  }

  test("canHandle rejects Unevaluable expressions") {
    val expr = FakeUnevaluable()
    val reason = CometBatchKernelCodegen.canHandle(expr)
    assert(reason.isDefined, "expected canHandle to reject Unevaluable")
    assert(
      reason.get.contains("FakeUnevaluable"),
      s"expected reason to name the rejected expression class; got: ${reason.get}")
  }

  test("CSE collapses a repeated subtree to one evaluation in the generated body") {
    // `Add(Length(Upper(c0)), Length(Upper(c0)))` has `Length(Upper(c0))` as a common subtree.
    // Length.doGenCode emits `$value.numChars()` on every Spark version the project targets,
    // which makes it a stable activation marker. Upper's own doGenCode text drifts across
    // versions (Spark 3.5 emits `UTF8String.toUpperCase()`, Spark 4 emits
    // `CollationSupport.Upper.exec*` via collation-aware codegen), so we avoid it as a marker.
    // When CSE fires, `Length(Upper(c0))` compiles into one `subExpr_*` helper whose body calls
    // `numChars()` once. Both uses in the `Add` read the cached result from mutable state.
    // Without CSE, each Add child would emit its own `numChars()` call.
    val upperOrd0 = Upper(BoundReference(0, StringType, nullable = true))
    val lenUpper = Length(upperOrd0)
    val expr = Add(lenUpper, lenUpper)
    val result = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(nullableString))
    val occurrences = "\\.numChars\\(\\)".r.findAllIn(result.body).size
    assert(
      occurrences == 1,
      "expected CSE to collapse repeated Length evaluation to 1 numChars() call, " +
        s"got $occurrences; src=\n${CodeFormatter.format(result.code)}")
    // Additional proof: CSE emitted a `subExpr_` helper method. Without CSE the generator would
    // have inlined the repeated subtree into the main body with no helper at all.
    assert(
      result.body.contains("subExpr_0(row)"),
      s"expected CSE helper invocation; got:\n${CodeFormatter.format(result.code)}")
  }

  test("CSE does not fire on non-deterministic expressions (regression guard)") {
    // `Add(Rand(0), Rand(0))` is two structurally identical non-deterministic subtrees. CSE must
    // not collapse them: each Rand call must produce an independent draw. Spark's CSE
    // (`EquivalentExpressions.updateExprInMap`) filters non-deterministic expressions via
    // `expr.deterministic`, so the two Rands stay separate. This test is a regression guard
    // against Spark ever relaxing that check and against us accidentally applying CSE outside
    // the `generateExpressions` path (which respects the filter). `Rand.doGenCode` emits one
    // `$rng.nextDouble()` call per evaluation, so two Rands produce two `.nextDouble()` calls
    // in the body. One-call output would indicate incorrect CSE.
    val expr = Add(Rand(Literal(0L, LongType)), Rand(Literal(0L, LongType)))
    val result = CometBatchKernelCodegen.generateSource(expr, IndexedSeq.empty)
    val occurrences = "\\.nextDouble\\(\\)".r.findAllIn(result.body).size
    assert(
      occurrences == 2,
      "expected two independent Rand evaluations (no CSE on nondeterministic), " +
        s"got $occurrences; src=\n${CodeFormatter.format(result.code)}")
  }

  test("DecimalVector getDecimal specializes to unscaled-long fast path for short precision") {
    // Mirrors Spark's `UnsafeRow.getDecimal` split at `Decimal.MAX_LONG_DIGITS` (18), done at
    // codegen time rather than at runtime. The dispatcher reads the `BoundReference`'s
    // `DecimalType` at source-generation time and emits only the fast-path branch when
    // `precision <= 18`. The fast path reads the low 8 bytes of the 16-byte Arrow decimal128
    // slot directly as a signed long via `ArrowBuf.getLong` and wraps with
    // `Decimal.createUnsafe`, avoiding the `BigDecimal` allocation `DecimalVector.getObject`
    // would perform. For precision > 18 the generator emits only the slow-path branch
    // (`getObject + Decimal.apply`); see the companion test below.
    val decimalVectorClass = CometBatchKernelCodegen.vectorClassBySimpleName("DecimalVector")
    val spec = ArrowColumnSpec(decimalVectorClass, nullable = true)
    val expr = BoundReference(0, DecimalType(18, 2), nullable = true)
    val result = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(spec))
    assert(
      result.body.contains(".createUnsafe("),
      "expected Decimal.createUnsafe call on fast path; got:\n" +
        CodeFormatter.format(result.code))
    assert(
      result.body.contains("Platform.getLong(") &&
        result.body.contains("this.col0_valueAddr"),
      "expected unsafe Platform.getLong against cached valueAddr; got:\n" +
        CodeFormatter.format(result.code))
    assert(
      !result.body.contains(".getObject("),
      "expected specialized fast path (no BigDecimal fallback branch in source); got:\n" +
        CodeFormatter.format(result.code))
    assert(
      !result.body.contains("if (precision <= 18)"),
      "expected no runtime precision branch for known short-precision column; got:\n" +
        CodeFormatter.format(result.code))
  }

  test("DecimalVector getDecimal specializes to BigDecimal slow path for long precision") {
    // Companion to the fast-path test. For `DecimalType(p, s)` with `p > 18`, the unscaled value
    // can exceed 64 bits, so the generator emits only the `getObject + Decimal.apply` branch.
    // The fast path markers must be absent so the generated source is minimal for this column.
    val decimalVectorClass = CometBatchKernelCodegen.vectorClassBySimpleName("DecimalVector")
    val spec = ArrowColumnSpec(decimalVectorClass, nullable = true)
    val expr = BoundReference(0, DecimalType(38, 10), nullable = true)
    val result = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(spec))
    assert(
      result.body.contains(".getObject(") && result.body.contains(".apply("),
      s"expected BigDecimal slow path; got:\n${CodeFormatter.format(result.code)}")
    assert(
      !result.body.contains(".createUnsafe("),
      "expected no fast-path emission for long-precision column; got:\n" +
        CodeFormatter.format(result.code))
    assert(
      !result.body.contains("if (precision <= 18)"),
      "expected no runtime precision branch for known long-precision column; got:\n" +
        CodeFormatter.format(result.code))
  }

  test("DecimalVector setSafe uses unscaled-long fast path for short-precision output") {
    // The output writer specializes on the root expression's DecimalType precision. For
    // precision <= 18 the Decimal's unscaled long is passed directly to
    // `DecimalVector.setSafe(int, long)`, avoiding the BigDecimal allocation that
    // `toJavaBigDecimal()` performs. Use a simple expression that produces a DecimalType output:
    // `BoundReference(0, DecimalType(18, 2))` has output type DecimalType(18, 2), which is what
    // the generator specializes on.
    val decimalVectorClass = CometBatchKernelCodegen.vectorClassBySimpleName("DecimalVector")
    val spec = ArrowColumnSpec(decimalVectorClass, nullable = true)
    val expr = BoundReference(0, DecimalType(18, 2), nullable = true)
    val result = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(spec))
    assert(
      result.body.contains(".toUnscaledLong()"),
      s"expected toUnscaledLong call on fast path; got:\n${CodeFormatter.format(result.code)}")
    assert(
      !result.body.contains(".toJavaBigDecimal("),
      "expected no BigDecimal allocation for short-precision output; got:\n" +
        CodeFormatter.format(result.code))
  }

  test("DecimalVector setSafe uses BigDecimal slow path for long-precision output") {
    // Companion to the fast-path output test. Precision > 18 can have unscaled values exceeding
    // 64 bits, so the writer must fall back to the BigDecimal path.
    val decimalVectorClass = CometBatchKernelCodegen.vectorClassBySimpleName("DecimalVector")
    val spec = ArrowColumnSpec(decimalVectorClass, nullable = true)
    val expr = BoundReference(0, DecimalType(38, 10), nullable = true)
    val result = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(spec))
    assert(
      result.body.contains(".toJavaBigDecimal("),
      s"expected BigDecimal slow path; got:\n${CodeFormatter.format(result.code)}")
    assert(
      !result.body.contains(".toUnscaledLong()"),
      "expected no unscaled-long write for long-precision output; got:\n" +
        CodeFormatter.format(result.code))
  }

  test("VarCharVector setSafe uses on-heap UTF8String shortcut") {
    // The UTF8String output writer avoids the `byte[] b = $value.getBytes()` allocation when
    // the UTF8String is on-heap by passing its backing byte[] directly to
    // `VarCharVector.setSafe(int, byte[], int, int)`. Spark's string functions allocate their
    // result on-heap, so this path hits for typical string expressions. Off-heap fallback
    // (for passthrough of zero-copy input reads) stays as the else branch.
    //
    // Markers: `getBaseObject()` (inspecting the backing), `instanceof byte[]` (the branch),
    // and `Platform.BYTE_ARRAY_OFFSET` (the on-heap offset math).
    val expr = Upper(BoundReference(0, StringType, nullable = true))
    val result = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(nullableString))
    assert(
      result.body.contains(".getBaseObject()"),
      s"expected UTF8String.getBaseObject call; got:\n${CodeFormatter.format(result.code)}")
    assert(
      result.body.contains("instanceof byte[]"),
      s"expected on-heap instanceof branch; got:\n${CodeFormatter.format(result.code)}")
    assert(
      result.body.contains("Platform.BYTE_ARRAY_OFFSET"),
      "expected on-heap offset math via Platform.BYTE_ARRAY_OFFSET; got:\n" +
        CodeFormatter.format(result.code))
    assert(
      result.body.contains(".getBytes()"),
      s"expected off-heap getBytes fallback; got:\n${CodeFormatter.format(result.code)}")
  }

  test("non-nullable root expression omits the `if (isNull)` branch in default body") {
    // When the bound expression claims `nullable = false`, the default body drops the
    // `if (ev.isNull) output.setNull(i);` guard entirely. `Length` on a non-nullable column is
    // itself non-nullable (Length.nullable = child.nullable = false), so the writer goes
    // straight to the setSafe/set call. This test uses a non-NullIntolerant-short-circuit
    // shape by wrapping Length in Coalesce, so we exercise the default branch of defaultBody
    // rather than the NullIntolerant one. Actually, Length is NullIntolerant, so the NI branch
    // fires. Use an expression that's non-nullable but whose tree is not fully NullIntolerant
    // to hit the default branch. `Coalesce(Seq(Length(col_non_null), Literal(0)))` has
    // nullable=false (Coalesce is non-null when any child is) and Coalesce itself is not
    // NullIntolerant, so the default branch runs. Assert `setNull` is absent.
    val expr = Coalesce(
      Seq(Length(BoundReference(0, StringType, nullable = false)), Literal(0, IntegerType)))
    val result = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(nonNullableString))
    assert(
      !result.body.contains("output.setNull(i);"),
      "expected no setNull for a non-nullable root expression; got:\n" +
        CodeFormatter.format(result.code))
  }

  test("nullable root expression keeps the `if (isNull)` branch in default body") {
    // Baseline: when the root expression is nullable, the setNull branch must still be emitted.
    // Uses Coalesce with a nullable child so the Coalesce itself remains nullable. Guards the
    // NonNullableOutputShortCircuit optimization against over-firing.
    val expr = Coalesce(
      Seq(
        Length(BoundReference(0, StringType, nullable = true)),
        BoundReference(1, IntegerType, nullable = true)))
    val result = CometBatchKernelCodegen.generateSource(
      expr,
      IndexedSeq(
        nullableString,
        ArrowColumnSpec(
          CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
          nullable = true)))
    assert(
      result.body.contains("output.setNull(i);"),
      "expected setNull branch for a nullable root expression; got:\n" +
        CodeFormatter.format(result.code))
  }

  test("nullable NullIntolerant root keeps post-eval isNull guard inside short-circuit (#4554)") {
    // `NullIntolerant` only constrains "null in -> null out". An expression can still set
    // `ev.isNull = true` from non-null inputs — `MakeTimestamp(failOnError = false)` does this in
    // its `doGenCode` catch block when year/month/day/hour/min/sec components are out of range
    // (issue #4554). The dispatcher previously assumed NullIntolerant + non-null inputs implied a
    // non-null output and dropped the post-eval guard; that wrote stale `ev.value` bytes for
    // every invalid row. The short-circuit on input nulls must coexist with a post-eval
    // `if (ev.isNull) setNull` check whenever the expression itself is nullable.
    val expr = MakeTimestamp(
      BoundReference(0, IntegerType, nullable = true),
      BoundReference(1, IntegerType, nullable = true),
      BoundReference(2, IntegerType, nullable = true),
      BoundReference(3, IntegerType, nullable = true),
      BoundReference(4, IntegerType, nullable = true),
      BoundReference(5, DecimalType(8, 6), nullable = true),
      timezone = None,
      timeZoneId = Some("UTC"),
      failOnError = false)
    assert(expr.nullable, "MakeTimestamp(failOnError=false) must be nullable for this test")
    val intCol = ArrowColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
      nullable = true)
    val decCol = ArrowColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("DecimalVector"),
      nullable = true)
    val result = CometBatchKernelCodegen.generateSource(
      expr,
      IndexedSeq(intCol, intCol, intCol, intCol, intCol, decCol))
    val src = result.body
    val formatted = CodeFormatter.format(result.code)
    // Two distinct setNull sites must exist: the input-null short-circuit before `ev.code` runs,
    // and the post-eval guard that propagates `ev.isNull = true` set by MakeTimestamp's catch
    // block on invalid components. Pre-fix there was only one (the input short-circuit).
    val setNullOccurrences = "output\\.setNull\\(i\\);".r.findAllIn(src).length
    assert(
      setNullOccurrences >= 2,
      "expected at least two setNull sites (input short-circuit + post-eval ev.isNull guard); " +
        s"found $setNullOccurrences. Source:\n$formatted")
  }

  test("ArrayType(StringType) output emits ListVector startNewValue/endValue recursion") {
    // CreateArray over a BoundReference(StringType) produces ArrayType(StringType). emitWrite's
    // ArrayType case should emit:
    //   - ListVector cast of output
    //   - child VarCharVector extraction via getDataVector
    //   - startNewValue + per-element loop + endValue
    //   - the per-element write recursing into the StringType case (which uses the UTF8 on-heap
    //     shortcut marker `instanceof byte[]`)
    // Focus markers: ListVector cast, VarCharVector child cast, startNewValue, endValue, and
    // the inner UTF8 shortcut branch.
    val expr =
      CreateArray(
        Seq(BoundReference(0, StringType, nullable = true), Literal.create("x", StringType)))
    val result = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(nullableString))
    val src = result.body
    val formatted = CodeFormatter.format(result.code)
    assert(src.contains("ListVector"), s"expected ListVector in emitted body; got:\n$formatted")
    assert(src.contains(".startNewValue("), s"expected startNewValue call; got:\n$formatted")
    assert(src.contains(".endValue("), s"expected endValue call; got:\n$formatted")
    assert(
      src.contains(".getDataVector()"),
      s"expected child vector extraction; got:\n$formatted")
    assert(
      src.contains("instanceof byte[]"),
      s"expected inner UTF8 on-heap shortcut for string elements; got:\n$formatted")
  }

  test("MapType output emits MapVector startNewValue/endValue + per-pair writes") {
    // CreateMap produces MapType(k, v). emitWrite's MapType case should emit:
    //   - MapVector cast of output
    //   - entries StructVector extraction
    //   - typed key / value child casts via getChildByOrdinal(0) / (1)
    //   - startNewValue / endValue bracketing
    //   - setIndexDefined on each struct entry
    //   - keyArray() / valueArray() retrieval from the MapData source
    // Non-null literals here mean `valueContainsNull == false`, so the value-side null guard is
    // elided. The existence and elision of the `isNullAt` guard are exercised by the dedicated
    // [[NullableElementElision]] tests below.
    val expr = CreateMap(
      Seq(
        Literal.create("a", StringType),
        Literal(1, IntegerType),
        Literal.create("b", StringType),
        Literal(2, IntegerType)))
    val src = CometBatchKernelCodegen.generateSource(expr, IndexedSeq.empty).body
    Seq(
      "MapVector",
      "StructVector",
      ".startNewValue(",
      ".endValue(",
      ".setIndexDefined(",
      ".keyArray()",
      ".valueArray()").foreach { marker =>
      assert(src.contains(marker), s"expected $marker in MapType output emission; got:\n$src")
    }
  }

  test("nested fixed-width map children grow with setSafe, not set (#4539)") {
    // Map<Int, Int> output: both key and value are fixed-width children of the entries struct.
    // Their element count is the data-dependent sum of per-row map sizes, not bounded by numRows,
    // and is unknown until the write loop has evaluated each row, so the writes must use `setSafe`
    // to grow on demand. A bare `set` throws once a row's entries exceed the child's initial
    // capacity (issue #4539: the literal map's third key overflowed the pre-sized IntVector).
    val expr = CreateMap(
      Seq(
        Literal(1, IntegerType),
        Literal(10, IntegerType),
        Literal(2, IntegerType),
        Literal(20, IntegerType)))
    val src = CometBatchKernelCodegen.generateSource(expr, IndexedSeq.empty).body
    assert(
      src.contains(".setSafe("),
      s"expected setSafe for nested fixed-width writes; got:\n$src")
    // `.set(` is a bare fixed-width write; `setSafe(` / `setNull(` / `setIndexDefined(` do not
    // match this literal. There must be none into the nested children.
    assert(
      !src.contains(".set("),
      s"expected no bare fixed-width set into map children; got:\n$src")
  }

  test("top-level scalar output keeps the pre-sized set fast path") {
    // The root output vector is pre-sized to numRows and written once per row, so it uses the
    // bare `set` fast path rather than paying for setSafe's per-write capacity check. This pins
    // the boundary the #4539 fix draws: setSafe is for nested children only.
    val expr = Add(BoundReference(0, IntegerType, nullable = false), Literal(1, IntegerType))
    val intSpec = ArrowColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
      nullable = false)
    val src = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(intSpec)).body
    assert(
      src.contains("output.set("),
      s"expected bare set for the pre-sized root output; got:\n$src")
    assert(
      !src.contains(".setSafe("),
      s"expected no setSafe for a scalar root output; got:\n$src")
  }

  test("ArrayType output elides isNullAt on the element loop when containsNull is false") {
    // CreateArray over only-non-null Literals produces ArrayType(elementType, containsNull=false).
    // The element write should drop the `arr.isNullAt(j)` guard at source level rather than
    // relying on JIT folding.
    val expr = CreateArray(Seq(Literal(1, IntegerType), Literal(2, IntegerType)))
    val src = CometBatchKernelCodegen.generateSource(expr, IndexedSeq.empty).body
    assert(
      !src.contains(".isNullAt("),
      s"expected no isNullAt in element loop when containsNull=false; got:\n$src")
    assert(src.contains(".startNewValue("), s"expected startNewValue still emitted; got:\n$src")
  }

  test("ArrayType output keeps isNullAt on the element loop when containsNull is true") {
    // CreateArray with at least one nullable child produces containsNull=true. The element
    // null-guard must survive.
    val expr =
      CreateArray(Seq(BoundReference(0, IntegerType, nullable = true), Literal(2, IntegerType)))
    val intSpec = ArrowColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
      nullable = true)
    val src = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(intSpec)).body
    assert(
      src.contains(".isNullAt("),
      s"expected isNullAt in element loop when containsNull=true; got:\n$src")
  }

  test("MapType output keeps value isNullAt when valueContainsNull is true") {
    // ElementAt with safe-index selection produces a nullable Int. Wrapping the value column in
    // a CreateMap with that nullable Int makes valueContainsNull=true. The value-side null-guard
    // must survive.
    val expr =
      CreateMap(
        Seq(Literal.create("a", StringType), BoundReference(0, IntegerType, nullable = true)))
    val intSpec = ArrowColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
      nullable = true)
    val src = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(intSpec)).body
    assert(
      src.contains(".isNullAt("),
      s"expected isNullAt on the value-write branch when valueContainsNull=true; got:\n$src")
  }

  test("ArrayType(StringType) input emits InputArray_col0 nested class with UTF8 child getter") {
    // Array input with string elements: the kernel must expose a `getArray(0)` that hands Spark's
    // `doGenCode` an `ArrayData` view onto the Arrow `ListVector`'s child `VarCharVector`.
    // Markers: the nested class declaration with a slice constructor, the typed child getter
    // using `fromAddress`, and a `getArray` switch on the ordinal that allocates a fresh view.
    val varCharChildSpec = ScalarColumnSpec(varCharVectorClass, nullable = true)
    val arraySpec =
      ArrayColumnSpec(nullable = true, elementSparkType = StringType, element = varCharChildSpec)
    val expr = Size(BoundReference(0, ArrayType(StringType), nullable = true))
    val src = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(arraySpec)).body

    assert(
      src.contains("class InputArray_col0"),
      s"expected nested ArrayData class for array col0; got:\n$src")
    assert(
      src.contains("InputArray_col0(int startIdx, int len)"),
      s"expected InputArray_col0 to take a slice via constructor; got:\n$src")
    assert(
      src.contains("getElementStartIndex(") && src.contains("getElementEndIndex("),
      s"expected list-offset reads at the call site; got:\n$src")
    assert(
      src.contains("public org.apache.spark.unsafe.types.UTF8String getUTF8String(int i)"),
      s"expected element-type-specific UTF8String getter; got:\n$src")
    assert(
      src.contains(".fromAddress("),
      s"expected zero-copy UTF8 read inside the nested ArrayData; got:\n$src")
    assert(
      src.contains("public org.apache.spark.sql.catalyst.util.ArrayData getArray(int ordinal)"),
      s"expected kernel-level getArray switch; got:\n$src")
    assert(
      src.contains("return new InputArray_col0("),
      s"expected getArray to allocate a fresh InputArray_col0 view; got:\n$src")
  }

  test("ArrayType(IntegerType) input emits primitive int getter in nested class") {
    val intChildSpec = ScalarColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
      nullable = true)
    val arraySpec =
      ArrayColumnSpec(nullable = true, elementSparkType = IntegerType, element = intChildSpec)
    val expr = Size(BoundReference(0, ArrayType(IntegerType), nullable = true))
    val src = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(arraySpec)).body

    assert(
      src.contains("public int getInt(int i)"),
      s"expected primitive int getter on nested array class; got:\n$src")
    // Scalar-element fast path reads directly off the typed child vector. No BigDecimal /
    // fromAddress scaffolding should leak in.
    assert(
      !src.contains(".fromAddress("),
      s"int element getter should not wrap with UTF8 fromAddress; got:\n$src")
  }

  test(
    "ArrayType(DecimalType) short-precision input emits decimal128 fast-path via getLong in " +
      "nested class") {
    val decimalChildSpec = ScalarColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("DecimalVector"),
      nullable = true)
    val arraySpec = ArrayColumnSpec(
      nullable = true,
      elementSparkType = DecimalType(10, 2),
      element = decimalChildSpec)
    val expr =
      ElementAt(
        BoundReference(0, ArrayType(DecimalType(10, 2)), nullable = true),
        Literal(1, IntegerType))
    val src = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(arraySpec)).body

    // Fast path markers: reads the low 8 bytes of the decimal128 slot via getLong + createUnsafe.
    // The slow path would go through getObject + Decimal.apply.
    assert(
      src.contains(".getLong(") && src.contains(".createUnsafe("),
      s"expected decimal-input short-precision fast path in nested class; got:\n$src")
    assert(
      !src.contains(".getObject("),
      s"short-precision decimal element should not use BigDecimal slow path; got:\n$src")
  }

  test("ArrayType(DecimalType) long-precision input emits BigDecimal slow path in nested class") {
    val decimalChildSpec = ScalarColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("DecimalVector"),
      nullable = true)
    val arraySpec = ArrayColumnSpec(
      nullable = true,
      elementSparkType = DecimalType(30, 2),
      element = decimalChildSpec)
    val expr =
      ElementAt(
        BoundReference(0, ArrayType(DecimalType(30, 2)), nullable = true),
        Literal(1, IntegerType))
    val src = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(arraySpec)).body

    assert(
      src.contains(".getObject(") && src.contains("Decimal$.MODULE$"),
      s"expected BigDecimal slow path for p>18 element; got:\n$src")
  }

  private def generate(expr: Expression, specs: IndexedSeq[ArrowColumnSpec]): String =
    CometBatchKernelCodegen.generateSource(expr, specs).body

  test("Array<Array<Int>> emits outer + inner array classes with fresh inner allocation") {
    val innerArray = ArrayColumnSpec(
      nullable = true,
      elementSparkType = IntegerType,
      element = ScalarColumnSpec(
        CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
        nullable = true))
    val outerArray = ArrayColumnSpec(
      nullable = true,
      elementSparkType = ArrayType(IntegerType),
      element = innerArray)
    val expr = Size(BoundReference(0, ArrayType(ArrayType(IntegerType)), nullable = true))
    val src = generate(expr, IndexedSeq(outerArray))
    assert(
      src.contains("class InputArray_col0 ") && src.contains("class InputArray_col0_e "),
      s"expected both outer and inner array classes; got:\n$src")
    assert(
      src.contains("return new InputArray_col0_e("),
      s"expected outer class to allocate a fresh inner array view per call; got:\n$src")
    assert(
      src.contains("public int getInt(int i)"),
      s"expected innermost scalar getter for IntegerType element; got:\n$src")
  }

  test("Array<Struct<a: Int>> emits array class allocating fresh InputStruct_col0_e") {
    val innerStruct = StructColumnSpec(
      nullable = true,
      fields = Seq(
        StructFieldSpec(
          "a",
          IntegerType,
          nullable = true,
          ScalarColumnSpec(
            CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
            nullable = true))))
    val outerArray = ArrayColumnSpec(
      nullable = true,
      elementSparkType = StructType(Seq(StructField("a", IntegerType, nullable = true)).toArray),
      element = innerStruct)
    val elemType = StructType(Seq(StructField("a", IntegerType, nullable = true)).toArray)
    val expr = Size(BoundReference(0, ArrayType(elemType), nullable = true))
    val src = generate(expr, IndexedSeq(outerArray))
    assert(
      src.contains("class InputArray_col0 ") && src.contains("class InputStruct_col0_e "),
      s"expected array-of-struct nested classes; got:\n$src")
    assert(
      src.contains("return new InputStruct_col0_e(startIndex + i)"),
      s"expected array getStruct to allocate a fresh inner struct view; got:\n$src")
  }

  test("Struct<s: Struct<a: Int>> emits outer + inner struct classes") {
    val innerStruct = StructColumnSpec(
      nullable = true,
      fields = Seq(
        StructFieldSpec(
          "a",
          IntegerType,
          nullable = true,
          ScalarColumnSpec(
            CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
            nullable = true))))
    val outerStruct = StructColumnSpec(
      nullable = true,
      fields = Seq(
        StructFieldSpec(
          "s",
          StructType(Seq(StructField("a", IntegerType, nullable = true)).toArray),
          nullable = true,
          innerStruct)))
    val innerType = StructType(Seq(StructField("a", IntegerType, nullable = true)).toArray)
    val outerType = StructType(Seq(StructField("s", innerType, nullable = true)).toArray)
    val expr = GetStructField(
      GetStructField(BoundReference(0, outerType, nullable = true), 0, Some("s")),
      0,
      Some("a"))
    val src = generate(expr, IndexedSeq(outerStruct))
    assert(
      src.contains("class InputStruct_col0 ") && src.contains("class InputStruct_col0_f0 "),
      s"expected outer + inner struct classes; got:\n$src")
    assert(
      src.contains("return new InputStruct_col0_f0(this.rowIdx)"),
      s"expected outer struct getStruct to allocate a fresh inner struct view; got:\n$src")
    assert(
      src.contains("public int getInt(int ordinal)"),
      s"expected innermost getInt on InputStruct_col0_f0; got:\n$src")
  }

  test("Struct<a: Array<Int>> emits struct class allocating fresh InputArray_col0_f0") {
    val innerArray = ArrayColumnSpec(
      nullable = true,
      elementSparkType = IntegerType,
      element = ScalarColumnSpec(
        CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
        nullable = true))
    val outerStruct = StructColumnSpec(
      nullable = true,
      fields = Seq(StructFieldSpec("a", ArrayType(IntegerType), nullable = true, innerArray)))
    val structType =
      StructType(Seq(StructField("a", ArrayType(IntegerType), nullable = true)).toArray)
    val expr = Size(GetStructField(BoundReference(0, structType, nullable = true), 0, Some("a")))
    val src = generate(expr, IndexedSeq(outerStruct))
    assert(
      src.contains("class InputStruct_col0 ") && src.contains("class InputArray_col0_f0 "),
      s"expected struct-of-array nested classes; got:\n$src")
    assert(
      src.contains("return new InputArray_col0_f0("),
      s"expected struct getArray to allocate a fresh inner array view; got:\n$src")
  }

  test("Map<String, Int> emits InputMap_col0 + keyArray / valueArray views") {
    val keySpec = ScalarColumnSpec(varCharVectorClass, nullable = true)
    val valueSpec = ScalarColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
      nullable = true)
    val mapSpec = MapColumnSpec(
      nullable = true,
      keySparkType = StringType,
      valueSparkType = IntegerType,
      key = keySpec,
      value = valueSpec)
    val expr = Size(BoundReference(0, MapType(StringType, IntegerType), nullable = true))
    val src = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(mapSpec)).body
    assert(
      src.contains("class InputMap_col0 "),
      s"expected InputMap_col0 nested class; got:\n$src")
    assert(
      src.contains("class InputArray_col0_k ") && src.contains("class InputArray_col0_v "),
      s"expected key/value array view classes; got:\n$src")
    assert(
      src.contains("return new InputArray_col0_k(this.startIndex, this.length)"),
      s"expected keyArray to allocate a fresh view over the map slice; got:\n$src")
    assert(
      src.contains("return new InputArray_col0_v(this.startIndex, this.length)"),
      s"expected valueArray to allocate a fresh view over the map slice; got:\n$src")
    assert(
      src.contains("public org.apache.spark.sql.catalyst.util.MapData getMap(int ordinal)"),
      s"expected kernel-level getMap switch; got:\n$src")
    assert(
      src.contains("return new InputMap_col0("),
      s"expected getMap to allocate a fresh InputMap_col0 view; got:\n$src")
  }

  test("Map<Array<Int>, Array<String>> emits complex key and complex value views") {
    val keyElem = ScalarColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
      nullable = true)
    val keyArraySpec =
      ArrayColumnSpec(nullable = true, elementSparkType = IntegerType, element = keyElem)
    val valueElem = ScalarColumnSpec(varCharVectorClass, nullable = true)
    val valueArraySpec =
      ArrayColumnSpec(nullable = true, elementSparkType = StringType, element = valueElem)
    val mapSpec = MapColumnSpec(
      nullable = true,
      keySparkType = ArrayType(IntegerType),
      valueSparkType = ArrayType(StringType),
      key = keyArraySpec,
      value = valueArraySpec)
    val expr = Size(
      BoundReference(0, MapType(ArrayType(IntegerType), ArrayType(StringType)), nullable = true))
    val src = CometBatchKernelCodegen.generateSource(expr, IndexedSeq(mapSpec)).body
    // Full chain of nested classes should appear: top-level map view, the key/value array
    // views, and the inner array classes for each complex key/value element.
    Seq(
      "class InputMap_col0 ",
      "class InputArray_col0_k ",
      "class InputArray_col0_v ",
      "class InputArray_col0_k_e ",
      "class InputArray_col0_v_e ").foreach { marker =>
      assert(src.contains(marker), s"expected $marker in emission; got:\n$src")
    }
  }

  /**
   * Null-guard emission for nested reference-typed getters. Spark's
   * `CodeGenerator.setArrayElement` only emits an `isNullAt` check before `update(i, getX(j))`
   * for primitive elements. For reference types it relies on the source's `getX` to return null
   * on null positions itself, matching `ColumnarArray.getBinary`. The emitter prepends `if
   * (isNullAt(...)) return null;` when the element / field is nullable.
   *
   * Runtime regressions for the leaf reference types live in `CometCodegenSuite`; complex-type
   * (Struct/Array/Map) coverage runs through HOFs in `CometCodegenHOFSuite`.
   */
  private val nullableIntStruct = StructColumnSpec(
    nullable = true,
    fields = Seq(
      StructFieldSpec(
        "a",
        IntegerType,
        nullable = true,
        ScalarColumnSpec(
          CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
          nullable = true))))
  private val nullableIntStructType =
    StructType(Seq(StructField("a", IntegerType, nullable = true)).toArray)

  private val nullableIntArray = ArrayColumnSpec(
    nullable = true,
    elementSparkType = IntegerType,
    element = ScalarColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
      nullable = true))

  private val nullableIntStrMap = MapColumnSpec(
    nullable = true,
    keySparkType = IntegerType,
    valueSparkType = StringType,
    key = ScalarColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
      nullable = false),
    value = ScalarColumnSpec(varCharVectorClass, nullable = true))

  test("nested array of nullable Struct emits null guard before allocating InputStruct view") {
    val outer = ArrayColumnSpec(
      nullable = true,
      elementSparkType = nullableIntStructType,
      element = nullableIntStruct)
    val expr = Size(BoundReference(0, ArrayType(nullableIntStructType), nullable = true))
    val src = generate(expr, IndexedSeq(outer))
    assert(
      src.contains("if (isNullAt(i)) return null;") &&
        src.contains("new InputStruct_col0_e(startIndex + i)"),
      s"expected null guard and InputStruct alloc on nullable Struct element; got:\n$src")
  }

  test("nested array of non-nullable Struct elides null guard") {
    // Fully non-nullable inner spec: outer struct nullable=false AND inner Int field
    // nullable=false. Without the inner field also being non-nullable the inner
    // primitive-Int getter wouldn't emit a guard anyway (we only guard reference types), but
    // making everything non-nullable means the broad `!src.contains("if (isNullAt(...))")`
    // assertion verifies "no guards anywhere" rather than passing because the inner happens
    // to be a primitive we don't guard.
    val nonNullableInner = StructColumnSpec(
      nullable = false,
      fields = Seq(
        StructFieldSpec(
          "a",
          IntegerType,
          nullable = false,
          ScalarColumnSpec(
            CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
            nullable = false))))
    val outer = ArrayColumnSpec(
      nullable = true,
      elementSparkType = nullableIntStructType,
      element = nonNullableInner)
    val expr = Size(BoundReference(0, ArrayType(nullableIntStructType), nullable = true))
    val src = generate(expr, IndexedSeq(outer))
    assert(
      src.contains("new InputStruct_col0_e(startIndex + i)"),
      s"sanity: alloc still emitted; got:\n$src")
    assert(
      !src.contains("if (isNullAt(i)) return null;") &&
        !src.contains("if (isNullAt(0)) return null;"),
      s"expected no null guard anywhere on fully non-nullable Struct element; got:\n$src")
  }

  test(
    "nested array of nullable inner Array emits null guard before allocating InputArray view") {
    val outer = ArrayColumnSpec(
      nullable = true,
      elementSparkType = ArrayType(IntegerType),
      element = nullableIntArray)
    val expr = Size(BoundReference(0, ArrayType(ArrayType(IntegerType)), nullable = true))
    val src = generate(expr, IndexedSeq(outer))
    assert(
      src.contains("if (isNullAt(i)) return null;") &&
        src.contains("new InputArray_col0_e(__s, __e - __s)"),
      s"expected null guard and InputArray alloc on nullable Array element; got:\n$src")
  }

  test("nested array of non-nullable inner Array elides null guard") {
    val nonNullableInner = ArrayColumnSpec(
      nullable = false,
      elementSparkType = IntegerType,
      element = ScalarColumnSpec(
        CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
        nullable = false))
    val outer = ArrayColumnSpec(
      nullable = true,
      elementSparkType = ArrayType(IntegerType),
      element = nonNullableInner)
    val expr = Size(BoundReference(0, ArrayType(ArrayType(IntegerType)), nullable = true))
    val src = generate(expr, IndexedSeq(outer))
    assert(
      src.contains("new InputArray_col0_e(__s, __e - __s)"),
      s"sanity: alloc still emitted; got:\n$src")
    assert(
      !src.contains("if (isNullAt(i)) return null;"),
      s"expected no null guard on non-nullable inner Array element; got:\n$src")
  }

  test("nested array of nullable Map emits null guard before allocating InputMap view") {
    val outer = ArrayColumnSpec(
      nullable = true,
      elementSparkType = MapType(IntegerType, StringType),
      element = nullableIntStrMap)
    val expr =
      Size(BoundReference(0, ArrayType(MapType(IntegerType, StringType)), nullable = true))
    val src = generate(expr, IndexedSeq(outer))
    assert(
      src.contains("if (isNullAt(i)) return null;") &&
        src.contains("new InputMap_col0_e(__s, __e - __s)"),
      s"expected null guard and InputMap alloc on nullable Map element; got:\n$src")
  }

  test("nested array of non-nullable Map elides null guard") {
    val nonNullableMap = MapColumnSpec(
      nullable = false,
      keySparkType = IntegerType,
      valueSparkType = StringType,
      key = ScalarColumnSpec(
        CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
        nullable = false),
      value = ScalarColumnSpec(varCharVectorClass, nullable = false))
    val outer = ArrayColumnSpec(
      nullable = true,
      elementSparkType = MapType(IntegerType, StringType),
      element = nonNullableMap)
    val expr =
      Size(BoundReference(0, ArrayType(MapType(IntegerType, StringType)), nullable = true))
    val src = generate(expr, IndexedSeq(outer))
    assert(
      src.contains("new InputMap_col0_e(__s, __e - __s)"),
      s"sanity: alloc still emitted; got:\n$src")
    assert(
      !src.contains("if (isNullAt(i)) return null;"),
      s"expected no null guard on non-nullable Map element; got:\n$src")
  }

  test("struct with nullable struct field emits null guard in getStruct(ordinal) switch") {
    val outerStruct = StructColumnSpec(
      nullable = true,
      fields =
        Seq(StructFieldSpec("s", nullableIntStructType, nullable = true, nullableIntStruct)))
    val outerType =
      StructType(Seq(StructField("s", nullableIntStructType, nullable = true)).toArray)
    val expr = GetStructField(
      GetStructField(BoundReference(0, outerType, nullable = true), 0, Some("s")),
      0,
      Some("a"))
    val src = generate(expr, IndexedSeq(outerStruct))
    assert(
      src.contains("if (isNullAt(0)) return null;") &&
        src.contains("new InputStruct_col0_f0(this.rowIdx)"),
      s"expected null guard and InputStruct alloc for nullable struct field; got:\n$src")
  }

  test("struct with non-nullable struct field elides null guard") {
    val nonNullableInner = StructColumnSpec(
      nullable = false,
      fields = Seq(
        StructFieldSpec(
          "a",
          IntegerType,
          nullable = false,
          ScalarColumnSpec(
            CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
            nullable = false))))
    val outerStruct = StructColumnSpec(
      nullable = true,
      fields =
        Seq(StructFieldSpec("s", nullableIntStructType, nullable = false, nonNullableInner)))
    val outerType =
      StructType(Seq(StructField("s", nullableIntStructType, nullable = false)).toArray)
    val expr = GetStructField(
      GetStructField(BoundReference(0, outerType, nullable = true), 0, Some("s")),
      0,
      Some("a"))
    val src = generate(expr, IndexedSeq(outerStruct))
    assert(
      src.contains("new InputStruct_col0_f0(this.rowIdx)"),
      s"sanity: alloc still emitted; got:\n$src")
    assert(
      !src.contains("if (isNullAt(0)) return null;") &&
        !src.contains("if (isNullAt(i)) return null;"),
      s"expected no null guard anywhere on fully non-nullable struct field; got:\n$src")
  }

  test("struct with nullable array field emits null guard in getArray(ordinal) switch") {
    val outerStruct = StructColumnSpec(
      nullable = true,
      fields =
        Seq(StructFieldSpec("a", ArrayType(IntegerType), nullable = true, nullableIntArray)))
    val outerType =
      StructType(Seq(StructField("a", ArrayType(IntegerType), nullable = true)).toArray)
    val expr =
      Size(GetStructField(BoundReference(0, outerType, nullable = true), 0, Some("a")))
    val src = generate(expr, IndexedSeq(outerStruct))
    assert(
      src.contains("if (isNullAt(0)) return null;") &&
        src.contains("new InputArray_col0_f0(__s, __e - __s)"),
      s"expected null guard and InputArray alloc for nullable array field; got:\n$src")
  }

  test("struct with non-nullable array field elides null guard") {
    val nonNullableInner = ArrayColumnSpec(
      nullable = false,
      elementSparkType = IntegerType,
      element = ScalarColumnSpec(
        CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
        nullable = false))
    val outerStruct = StructColumnSpec(
      nullable = true,
      fields =
        Seq(StructFieldSpec("a", ArrayType(IntegerType), nullable = false, nonNullableInner)))
    val outerType =
      StructType(Seq(StructField("a", ArrayType(IntegerType), nullable = false)).toArray)
    val expr =
      Size(GetStructField(BoundReference(0, outerType, nullable = true), 0, Some("a")))
    val src = generate(expr, IndexedSeq(outerStruct))
    assert(
      src.contains("new InputArray_col0_f0(__s, __e - __s)"),
      s"sanity: alloc still emitted; got:\n$src")
    assert(
      !src.contains("if (isNullAt(0)) return null;") &&
        !src.contains("if (isNullAt(i)) return null;"),
      s"expected no null guard anywhere on fully non-nullable array field; got:\n$src")
  }

  test("struct with nullable map field emits null guard in getMap(ordinal) switch") {
    val outerStruct = StructColumnSpec(
      nullable = true,
      fields = Seq(
        StructFieldSpec(
          "m",
          MapType(IntegerType, StringType),
          nullable = true,
          nullableIntStrMap)))
    val outerType =
      StructType(Seq(StructField("m", MapType(IntegerType, StringType), nullable = true)).toArray)
    val expr = Size(GetStructField(BoundReference(0, outerType, nullable = true), 0, Some("m")))
    val src = generate(expr, IndexedSeq(outerStruct))
    assert(
      src.contains("if (isNullAt(0)) return null;") &&
        src.contains("new InputMap_col0_f0(__s, __e - __s)"),
      s"expected null guard and InputMap alloc for nullable map field; got:\n$src")
  }

  test("struct with non-nullable map field elides null guard") {
    val nonNullableMap = MapColumnSpec(
      nullable = false,
      keySparkType = IntegerType,
      valueSparkType = StringType,
      key = ScalarColumnSpec(
        CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
        nullable = false),
      value = ScalarColumnSpec(varCharVectorClass, nullable = false))
    val outerStruct = StructColumnSpec(
      nullable = true,
      fields = Seq(
        StructFieldSpec("m", MapType(IntegerType, StringType), nullable = false, nonNullableMap)))
    val outerType = StructType(
      Seq(StructField("m", MapType(IntegerType, StringType), nullable = false)).toArray)
    val expr = Size(GetStructField(BoundReference(0, outerType, nullable = true), 0, Some("m")))
    val src = generate(expr, IndexedSeq(outerStruct))
    assert(
      src.contains("new InputMap_col0_f0(__s, __e - __s)"),
      s"sanity: alloc still emitted; got:\n$src")
    assert(
      !src.contains("if (isNullAt(0)) return null;"),
      s"expected no null guard on non-nullable map field; got:\n$src")
  }

  // Bucket 4 datetime expressions routed through CometCodegenDispatch. Each entry pairs a
  // bound Catalyst expression with the Arrow column specs the kernel would see at runtime.
  // The test asserts `generateSource` returns a non-empty body, which means Spark's own
  // `doGenCode` succeeded under the codegen context (no NotImplementedError, no rewrite to
  // a CodegenFallback path).
  test("Bucket 4 datetime expressions produce non-empty generated kernel source") {
    val intCol = ArrowColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("IntVector"),
      nullable = true)
    val longCol = ArrowColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("BigIntVector"),
      nullable = true)
    val decCol = ArrowColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("DecimalVector"),
      nullable = true)
    val dateCol = ArrowColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("DateDayVector"),
      nullable = true)
    val tsCol = ArrowColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("TimeStampMicroTZVector"),
      nullable = true)
    val strCol = ArrowColumnSpec(
      CometBatchKernelCodegen.vectorClassBySimpleName("VarCharVector"),
      nullable = true)

    val cases: Seq[(String, Expression, IndexedSeq[ArrowColumnSpec])] = Seq(
      (
        "AddMonths",
        AddMonths(
          BoundReference(0, DateType, nullable = true),
          BoundReference(1, IntegerType, nullable = true)),
        IndexedSeq(dateCol, intCol)),
      (
        "MonthsBetween",
        MonthsBetween(
          BoundReference(0, TimestampType, nullable = true),
          BoundReference(1, TimestampType, nullable = true),
          Literal(true),
          Some("UTC")),
        IndexedSeq(tsCol, tsCol)),
      (
        "MakeTimestamp",
        MakeTimestamp(
          BoundReference(0, IntegerType, nullable = true),
          BoundReference(1, IntegerType, nullable = true),
          BoundReference(2, IntegerType, nullable = true),
          BoundReference(3, IntegerType, nullable = true),
          BoundReference(4, IntegerType, nullable = true),
          BoundReference(5, DecimalType(8, 6), nullable = true),
          timezone = None,
          timeZoneId = Some("UTC")),
        IndexedSeq(intCol, intCol, intCol, intCol, intCol, decCol)),
      (
        "MillisToTimestamp",
        MillisToTimestamp(BoundReference(0, LongType, nullable = true)),
        IndexedSeq(longCol)),
      (
        "MicrosToTimestamp",
        MicrosToTimestamp(BoundReference(0, LongType, nullable = true)),
        IndexedSeq(longCol)),
      (
        "UnixSeconds",
        UnixSeconds(BoundReference(0, TimestampType, nullable = true)),
        IndexedSeq(tsCol)),
      (
        "UnixMillis",
        UnixMillis(BoundReference(0, TimestampType, nullable = true)),
        IndexedSeq(tsCol)),
      (
        "UnixMicros",
        UnixMicros(BoundReference(0, TimestampType, nullable = true)),
        IndexedSeq(tsCol)),
      (
        "ToUnixTimestamp",
        ToUnixTimestamp(
          BoundReference(0, StringType, nullable = true),
          Literal(UTF8String.fromString("yyyy-MM-dd HH:mm:ss"), StringType),
          Some("UTC")),
        IndexedSeq(strCol)))
    cases.foreach { case (name, expr, specs) =>
      val src = CometBatchKernelCodegen.generateSource(expr, specs).body
      assert(src.nonEmpty, s"$name: expected non-empty generated source")
      assert(
        src.contains("public java.lang.Object generate(Object[] references)"),
        s"$name: generated source missing kernel class entry point")
    }
  }

  test("closure-serialized bytes diverge for failOnError / roundOff / timeZoneId variants") {
    // The dispatcher caches kernels by the closure-serialized bytes of the bound expression
    // (`CacheKey.bytesKey`). For expression classes that carry a runtime-dependent boolean
    // (failOnError, roundOff) or string (timeZoneId), two plan instances differing only in that
    // field must serialize to distinct byte sequences so they receive distinct cache entries.
    // A collision would let a kernel compiled for one variant (e.g. ANSI throw site) silently
    // service a request from the other (non-ANSI null-on-overflow).
    //
    // This test serializes through `JavaSerializer` directly, which is what
    // `SparkEnv.get.closureSerializer` returns at runtime. We don't need a live SparkEnv here;
    // `JavaSerializer` only reads `spark.serializer.objectStreamReset` / `spark.serializer.
    // extraDebugInfo` from the conf and otherwise uses plain `ObjectOutputStream`.
    val serializer = new JavaSerializer(new SparkConf()).newInstance()
    def bytesOf(e: Expression): Array[Byte] = {
      val buffer = serializer.serialize(e)
      val out = new Array[Byte](buffer.remaining())
      buffer.get(out)
      out
    }

    def assertDiverge(label: String, a: Expression, b: Expression): Unit = {
      val ab = bytesOf(a)
      val bb = bytesOf(b)
      assert(
        !java.util.Arrays.equals(ab, bb),
        s"$label: expected serialized bytes to differ for variants that the dispatcher must " +
          s"cache separately, but both produced ${ab.length} identical bytes")
    }

    // failOnError on MakeTimestamp (set by the constructor from SQLConf.ansiEnabled at bind time)
    def makeTs(failOnError: Boolean): Expression =
      MakeTimestamp(
        BoundReference(0, IntegerType, nullable = true),
        BoundReference(1, IntegerType, nullable = true),
        BoundReference(2, IntegerType, nullable = true),
        BoundReference(3, IntegerType, nullable = true),
        BoundReference(4, IntegerType, nullable = true),
        BoundReference(5, DecimalType(8, 6), nullable = true),
        timezone = None,
        timeZoneId = Some("UTC"),
        failOnError = failOnError)
    assertDiverge(
      "MakeTimestamp.failOnError",
      makeTs(failOnError = true),
      makeTs(failOnError = false))

    // failOnError on ToUnixTimestamp
    def toUnix(failOnError: Boolean): Expression =
      ToUnixTimestamp(
        BoundReference(0, StringType, nullable = true),
        Literal(UTF8String.fromString("yyyy-MM-dd HH:mm:ss"), StringType),
        timeZoneId = Some("UTC"),
        failOnError = failOnError)
    assertDiverge(
      "ToUnixTimestamp.failOnError",
      toUnix(failOnError = true),
      toUnix(failOnError = false))

    // roundOff on MonthsBetween (carried as a child Literal node)
    def monthsBetween(roundOff: Boolean): Expression =
      MonthsBetween(
        BoundReference(0, TimestampType, nullable = true),
        BoundReference(1, TimestampType, nullable = true),
        Literal(roundOff),
        Some("UTC"))
    assertDiverge(
      "MonthsBetween.roundOff",
      monthsBetween(roundOff = true),
      monthsBetween(roundOff = false))

    // Session timezone propagates onto TimeZoneAwareExpression via timeZoneId; the dispatcher
    // must not share a kernel across timezones because Spark's doGenCode embeds the resolved
    // ZoneId reference into the generated source.
    def makeTsTz(tz: String): Expression =
      MakeTimestamp(
        BoundReference(0, IntegerType, nullable = true),
        BoundReference(1, IntegerType, nullable = true),
        BoundReference(2, IntegerType, nullable = true),
        BoundReference(3, IntegerType, nullable = true),
        BoundReference(4, IntegerType, nullable = true),
        BoundReference(5, DecimalType(8, 6), nullable = true),
        timezone = None,
        timeZoneId = Some(tz),
        failOnError = false)
    assertDiverge("MakeTimestamp.timeZoneId", makeTsTz("UTC"), makeTsTz("America/New_York"))
  }

  test("CacheKey discriminates on ArrowColumnSpec.nullable") {
    // Structural regression: same expression bytes and same Arrow vector class with different
    // `nullable` must produce non-equal cache keys. The dispatcher today hardcodes `nullable=true`
    // for top-level specs, so the two variants don't both arise from runtime data, but the case
    // class equality contract still has to discriminate so that any future tiered cache or test
    // construction can rely on it. The non-nullable variant's generated source emits a literal
    // `false` from `isNullAt`, distinct codegen output that we never want to silently share with
    // the nullable variant.
    val bytes = java.nio.ByteBuffer.wrap(Array[Byte](1, 2, 3))
    val nullable =
      IndexedSeq[ArrowColumnSpec](ArrowColumnSpec(varCharVectorClass, nullable = true))
    val nonNullable =
      IndexedSeq[ArrowColumnSpec](ArrowColumnSpec(varCharVectorClass, nullable = false))
    val k1 = CometScalaUDFCodegen.CacheKey(bytes, nullable)
    val k2 = CometScalaUDFCodegen.CacheKey(bytes, nonNullable)
    assert(
      k1 != k2,
      "expected nullable=true and nullable=false specs to produce distinct cache keys")
    assert(
      k1.hashCode != k2.hashCode,
      "case-class hashCode should also differ; identical hashCodes would degrade lookup but not " +
        "equality, so the assertion is mainly a sanity check on Spec.hashCode")
  }
}

/**
 * Minimal fake expressions for the `canHandle` rejection tests. Each opts into one of the marker
 * traits whose presence forces a serde-level fallback. Bodies are unreachable; `canHandle` walks
 * the tree structurally.
 */
private case class FakeCodegenFallback(child: Expression)
    extends Expression
    with CodegenFallback {
  override def children: Seq[Expression] = Seq(child)

  override def nullable: Boolean = true

  override def dataType: DataType = StringType

  override def eval(input: InternalRow): Any = null

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = copy(child = newChildren.head)
}

private case class FakeNondeterministic() extends LeafExpression with Nondeterministic {
  override def nullable: Boolean = true

  override def dataType: DataType = IntegerType

  override protected def initializeInternal(partitionIndex: Int): Unit = {}

  override protected def evalInternal(input: InternalRow): Any = 0

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException("test fake; never reaches codegen")
}

private case class FakeUnevaluable() extends LeafExpression with Unevaluable {
  override def nullable: Boolean = true

  override def dataType: DataType = IntegerType
}
