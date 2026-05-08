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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Concat, Expression, LeafExpression, Length, Literal, Nondeterministic, RegExpReplace, RLike, Unevaluable, Upper}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

import org.apache.comet.udf.CometBatchKernelCodegen
import org.apache.comet.udf.CometBatchKernelCodegen.ArrowColumnSpec

// Resolve Arrow vector classes through the codegen object so tests see the same `Class` objects
// the shaded `common` module sees. A direct `classOf[org.apache.arrow.vector.VarCharVector]` here
// would be the unshaded class from the test classpath, which is not `==` to the shaded class the
// production pattern-matches against.

/**
 * Generated-source inspection tests. These exercise `CometBatchKernelCodegen.generateSource` and
 * assert on the emitted Java directly, without invoking Janino. The goal is to catch regressions
 * in the optimizations we claim the dispatcher applies:
 *
 *   - `NullIntolerant` short-circuit wraps `ev.code` in `if (any-input-null) { setNull; } else {
 *     ev.code; write; }`.
 *   - Non-nullable column declaration emits `return false;` from `isNullAt(ord)` and, when the
 *     dispatcher rewrites the `BoundReference`, Spark's `doGenCode` stops emitting its own
 *     `row.isNullAt(ord)` probe.
 *   - Zero-copy string reads route through `UTF8String.fromAddress`.
 *   - The specialized `RegExpReplace` emitter engages for the shape its guard accepts.
 *
 * These are the smallest durable tests that the claimed optimizations actually reach the
 * generated Java, and they document the shapes future contributors should preserve.
 */
class CometCodegenSourceSuite extends AnyFunSuite {

  private val varCharVectorClass =
    CometBatchKernelCodegen.vectorClassBySimpleName("VarCharVector")
  private val viewVarCharVectorClass =
    CometBatchKernelCodegen.vectorClassBySimpleName("ViewVarCharVector")

  private val nullableString = ArrowColumnSpec(varCharVectorClass, nullable = true)
  private val nonNullableString = ArrowColumnSpec(varCharVectorClass, nullable = false)

  private def gen(
      expr: org.apache.spark.sql.catalyst.expressions.Expression,
      specs: ArrowColumnSpec*): String =
    CometBatchKernelCodegen.generateSource(expr, specs.toIndexedSeq).body

  test("non-nullable column emits literal-false isNullAt case") {
    val expr = Length(BoundReference(0, StringType, nullable = false))
    val src = gen(expr, nonNullableString)
    assert(
      src.contains("case 0: return false;"),
      s"expected non-nullable isNullAt to return literal false; got:\n$src")
  }

  test("non-nullable BoundReference elides Spark's own isNullAt probe in the expression body") {
    // When the BoundReference carries `nullable=false`, Spark's `doGenCode` skips the
    // `row.isNullAt(ord)` branch at source level. This is the payoff of the tree-rewrite in
    // `CometCodegenDispatchUDF.lookupOrCompile`: subsequent expressions over the same column
    // compile to tighter source rather than relying on JIT to constant-fold `isNullAt`.
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

  test("ViewVarCharVector getUTF8String branches inline vs referenced without allocating") {
    val viewSpec = ArrowColumnSpec(viewVarCharVectorClass, nullable = true)
    val expr = Length(BoundReference(0, StringType, nullable = true))
    val src = gen(expr, viewSpec)
    // The view case reads the 16-byte view entry and picks inline vs referenced data without a
    // byte[] allocation. Key markers: `viewBuf.getInt(entryStart)` for the length read and the
    // same `fromAddress` wrapper as the plain-VarChar case.
    assert(
      src.contains("viewBuf.getInt(entryStart)"),
      s"expected view entry length read; got:\n$src")
    assert(
      src.contains(".fromAddress("),
      s"expected view case to construct UTF8String via fromAddress; got:\n$src")
  }

  test("NullIntolerant expression emits input-null short-circuit before ev.code") {
    // RLike is NullIntolerant (a null subject returns null, not "did not match"). Expect the
    // default body to prepend `if (this.col0.isNull(i)) { setNull; } else { ... }` so null rows
    // skip the whole regex eval, not just the setNull write.
    val expr =
      RLike(BoundReference(0, StringType, nullable = true), Literal.create("\\d+", StringType))
    val src = gen(expr, nullableString)
    assert(
      src.contains("this.col0.isNull(i)"),
      s"expected NullIntolerant short-circuit on input ordinal 0; got:\n$src")
    assert(
      src.contains("output.setNull(i);"),
      s"expected setNull emission for short-circuited null rows; got:\n$src")
  }

  test("specialized RegExpReplace emitter engages for BoundReference subject") {
    val expr = RegExpReplace(
      subject = BoundReference(0, StringType, nullable = true),
      regexp = Literal.create("\\d+", StringType),
      rep = Literal.create("N", StringType),
      pos = Literal(1, IntegerType))
    val src = gen(expr, nullableString)
    // The specialized path reads bytes directly and runs `Pattern.matcher(...).replaceAll(...)`
    // without detouring through `UTF8String`. Key marker: no `UTF8String` on the subject read
    // inside the loop; instead `inputs` or the typed column field with `.get(i)`.
    assert(
      src.contains(".matcher(") && src.contains(".replaceAll("),
      s"expected specialized Matcher.replaceAll shape; got:\n$src")
    assert(
      src.contains("this.col0.get(i)"),
      s"expected specialized path to read bytes directly from the typed column; got:\n$src")
  }

  test("specialized RegExpReplace declines when subject is not a BoundReference") {
    // Upper breaks the specialization guard; fall through to the default `doGenCode` path.
    val expr = RegExpReplace(
      subject = Upper(BoundReference(0, StringType, nullable = true)),
      regexp = Literal.create("\\d+", StringType),
      rep = Literal.create("N", StringType),
      pos = Literal(1, IntegerType))
    val src = gen(expr, nullableString)
    // The default path routes the subject read through the kernel's getters. Marker of the
    // default path: the Upper child emits `row.getUTF8String(0)` / `row.isNullAt(0)` because
    // `ctx.INPUT_ROW = "row"`.
    assert(
      src.contains("row.getUTF8String(0)") || src.contains("this.getUTF8String(0)"),
      s"expected default path with row/kernel getter invocation; got:\n$src")
  }

  test("NullIntolerant short-circuit emitted when every node is NullIntolerant") {
    // RLike(Upper(BoundReference), Literal): RLike is NullIntolerant, Upper is NullIntolerant,
    // BoundReference and Literal are leaves. Every path from a leaf to the root propagates
    // nulls, so the short-circuit heuristic ("any input null -> output null") holds.
    val expr =
      RLike(
        Upper(BoundReference(0, StringType, nullable = true)),
        Literal.create("x", StringType))
    val src = gen(expr, nullableString)
    assert(
      src.contains("if (this.col0.isNull(i))"),
      s"expected short-circuit on col0 when every node is NullIntolerant; got:\n$src")
  }

  test("NullIntolerant short-circuit skipped when a non-NullIntolerant node breaks the chain") {
    // Concat is not NullIntolerant; null in some args doesn't necessarily produce a null
    // result. The short-circuit heuristic would be incorrect here (short-circuiting on c0 or c1
    // being null would skip evaluation, but Concat's null handling differs). Expect the
    // default path without the `if (colX.isNull(i) || colY.isNull(i))` wrapper, letting Spark's
    // own `ev.code` handle nulls correctly.
    val nullable1 = ArrowColumnSpec(varCharVectorClass, nullable = true)
    val nullable2 = ArrowColumnSpec(varCharVectorClass, nullable = true)
    val expr = RLike(
      Concat(
        Seq(
          BoundReference(0, StringType, nullable = true),
          BoundReference(1, StringType, nullable = true))),
      Literal.create("x", StringType))
    val src = gen(expr, nullable1, nullable2)
    assert(
      !src.contains("this.col0.isNull(i) || this.col1.isNull(i)"),
      "expected no pre-null short-circuit when Concat breaks the NullIntolerant chain; " +
        s"got:\n$src")
  }

  test("canHandle rejects CodegenFallback expressions") {
    val expr = FakeCodegenFallback(BoundReference(0, StringType, nullable = true))
    val reason = CometBatchKernelCodegen.canHandle(expr)
    assert(reason.isDefined, "expected canHandle to reject CodegenFallback")
    assert(
      reason.get.contains("FakeCodegenFallback"),
      s"expected reason to name the rejected expression class; got: ${reason.get}")
  }

  test("canHandle accepts Nondeterministic expressions (per-partition kernel handles state)") {
    // Per-partition kernel instance caching in `CometCodegenDispatchUDF.ensureKernel` advances
    // mutable state across batches in one partition, so Rand/Uuid/etc. produce the expected
    // sequences. The previous canHandle rejection was conservative; with that caching in
    // place, accepting Nondeterministic is correct.
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
