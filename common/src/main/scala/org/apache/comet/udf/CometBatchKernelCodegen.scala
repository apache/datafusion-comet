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

import org.apache.arrow.vector.{BigIntVector, BitVector, DateDayVector, DecimalVector, FieldVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TimeStampMicroTZVector, TimeStampMicroVector, TinyIntVector, ValueVector, VarBinaryVector, VarCharVector, ViewVarBinaryVector, ViewVarCharVector}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, Literal, RegExpReplace, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodegenContext, CodeGenerator, CodegenFallback, ExprCode, GeneratedClass}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StringType}

import org.apache.comet.shims.CometExprTraitShim

/**
 * Compiles a bound [[Expression]] plus an input schema into a specialized [[CometBatchKernel]]
 * that fuses Arrow input reads, expression evaluation, and Arrow output writes into one
 * Janino-compiled method per (expression, schema) pair.
 *
 * Input- and output-side emission live in their own files ([[CometBatchKernelCodegenInput]] and
 * [[CometBatchKernelCodegenOutput]]). This file is the orchestrator: it defines the per-column
 * [[ArrowColumnSpec]] vocabulary, the top-level [[canHandle]] / [[allocateOutput]] / [[compile]]
 * / [[generateSource]] entry points, and the cross-cutting kernel-shape decisions
 * (null-intolerant short-circuit, CSE variant, specialized per-expression emitters). Reading the
 * file split end-to-end shows symmetric input and output type-surface coverage at a glance.
 *
 * ==Compile-time specialization on batch invariants==
 *
 * The dispatcher knows, per input column, the concrete Arrow vector class (e.g.
 * [[VarCharVector]]) and whether the column is nullable. Both are compile-time invariants of the
 * kernel and baked into the generated code as typed fields and fixed branches rather than runtime
 * dispatch. The same expression against a different input schema resolves to a different compiled
 * kernel.
 *
 * The generated kernel '''is''' the `InternalRow` that Spark's `BoundReference.genCode` reads
 * from. `ctx.INPUT_ROW = "row"` and the `process` body aliases `InternalRow row = this;` so
 * Spark's generated `row.getUTF8String(ord)` resolves to the kernel's own typed getter (a final
 * method on a final class with the ordinal known at the call site; JIT devirtualizes and folds
 * the switch). `row` rather than `this` because Spark's `splitExpressions` uses INPUT_ROW as the
 * parameter name of any helper method it emits, and `this` is a reserved Java keyword.
 *
 * Input scope: all scalar Spark types that map to a single Arrow vector, plus `ArrayType(inner)`
 * and `StructType` (recursive, via nested-class emission). See
 * [[CometBatchKernelCodegenInput.isSupportedInputType]] for the authoritative gate and
 * [[CometBatchKernelCodegenInput.typedInputAccessors]] / the nested-class emitters for how each
 * shape is read. Output scope: scalar types plus `ArrayType` and `StructType` (recursive). See
 * [[CometBatchKernelCodegenOutput.isSupportedOutputType]] and
 * [[CometBatchKernelCodegenOutput.allocateOutput]] / `emitWrite`.
 *
 * ==Default path==
 *
 * Reuses Spark's `doGenCode` for expression evaluation. BoundReference reads resolve to typed,
 * constant-ordinal calls into the kernel's own getters.
 *
 * ==Specialized path==
 *
 * A per-expression match case in [[compile]] emits custom Java, bypassing `doGenCode`. Used for
 * expressions whose default-path codegen pays a measurable penalty because Spark's generated code
 * materializes a Java `String` (for example, `java.util.regex.Matcher` requires a
 * `CharSequence`). See [[specializedRegExpReplaceBody]] for the reasoning and the criteria for
 * adding a new specialization.
 *
 * ==Optimization menu==
 *
 * Every optimization the generator applies is compile-time specialized on the bound expression
 * and input schema, so the emitted Java carries only the chosen path at each emission site.
 * Source-level tests in `CometCodegenSourceSuite` assert activation per entry below.
 *
 * Input readers (Arrow to Java values, in [[CometBatchKernelCodegenInput.typedInputAccessors]]):
 *
 *   - `ZeroCopyUtf8Read` for `VarCharVector` / `ViewVarCharVector`: `UTF8String.fromAddress`
 *     wraps Arrow's data-buffer address with no `byte[]` allocation.
 *   - `NonNullableIsNullAtElision` for non-nullable columns: `isNullAt(ord)` returns a literal
 *     `false`, and `CometCodegenDispatchUDF.rewriteBoundReferences` tightens the
 *     `BoundReference.nullable` so Spark's `doGenCode` stops probing too.
 *   - `DecimalInputShortFastPath` for `DecimalType(p, _)` with `p <= 18`: reads the low 8 bytes
 *     of the decimal128 slot as a signed long and wraps with `Decimal.createUnsafe`. Slow path
 *     (`getObject` + `Decimal.apply`) emitted only for `p > 18`.
 *
 * Output writers (Java values to Arrow, in [[CometBatchKernelCodegenOutput]]):
 *
 *   - `DecimalOutputShortFastPath` for `DecimalType(p, _)` with `p <= 18`: passes
 *     `Decimal.toUnscaledLong` to `DecimalVector.setSafe(int, long)`. Slow path via
 *     `toJavaBigDecimal()` emitted only for `p > 18`.
 *   - `Utf8OutputOnHeapShortcut` for `StringType`: when the `UTF8String` base is a `byte[]`,
 *     passes it directly to `VarCharVector.setSafe(int, byte[], int, int)` and skips the
 *     redundant `getBytes()` allocation. Off-heap fallback retains `getBytes()`.
 *   - `PreSizedOutputBuffer` for variable-length output types: the caller passes an
 *     input-size-derived byte estimate to avoid mid-loop reallocation.
 *
 * Kernel shape (in [[defaultBody]] and [[generateSource]]):
 *
 *   - `NullIntolerantShortCircuit`: trees where every node is `NullIntolerant` or a leaf get a
 *     pre-body null check over the union of input ordinals; null rows skip both CSE and
 *     expression evaluation.
 *   - `NonNullableOutputShortCircuit`: bound expressions with `nullable == false` drop the `if
 *     (ev.isNull) setNull` guard and write unconditionally.
 *   - `SubexpressionElimination` (when `spark.sql.subexpressionEliminationEnabled`): common
 *     subtrees become helper methods writing into `addMutableState` fields. See the CSE section
 *     below for why the class-field variant is used.
 *
 * Expression specializers (per-expression custom per-row body, in the `specialized*` family):
 *
 *   - `RegExpReplaceSpecialized`: `RegExpReplace` with a direct `BoundReference` subject,
 *     foldable pattern and replacement, and `pos == 1`. Emits `byte[] -> String -> Matcher`
 *     directly, bypassing the `UTF8String` round-trip that default `doGenCode` forces. See
 *     [[specializedRegExpReplaceBody]] for the full rationale and the criteria for adding new
 *     specializers.
 *
 * ==Subexpression elimination (CSE)==
 *
 * CSE hoists repeated subtrees into a single evaluation per row. Spark exposes two entry points:
 *
 *   - `subexpressionElimination` (via `ctx.generateExpressions(..., doSubexpressionElimination =
 *     true)` + `ctx.subexprFunctionsCode`). Each common subexpression becomes a helper method
 *     that writes its result into class-level mutable state allocated via `addMutableState`. The
 *     main expression's `genCode` references those class fields. This is what
 *     `GeneratePredicate`, `GenerateMutableProjection`, and `GenerateUnsafeProjection` use.
 *   - `subexpressionEliminationForWholeStageCodegen`. CSE results live in local variables
 *     declared in the caller's scope, and the main expression's `genCode` references those
 *     locals. Only safe when no helper method gets extracted between the locals' declaration site
 *     and their use.
 *
 * We use the '''class-field''' variant. The WSCG variant does not work in our shape without
 * additional setup: Spark's arithmetic, string, and decimal expressions internally call
 * `splitExpressionsWithCurrentInputs`, which splits into helper methods unless `currentVars` is
 * non-null. In our kernel `currentVars` is null (we read from a row, not from materialized
 * locals), so those splits fire and the helper bodies cannot see CSE-declared locals in the outer
 * scope. The class-field variant sidesteps this entirely because helper methods can read class
 * fields freely.
 *
 * ==Future WSCG-variant exploration==
 *
 * Making the WSCG variant usable would require:
 *
 *   - Setting `ctx.currentVars = Seq.fill(numInputs)(null)` before CSE. `BoundReference.genCode`
 *     checks `currentVars != null && currentVars(ord) != null`, so an all-null `currentVars` lets
 *     reads fall through to the `INPUT_ROW` path (what we want) while
 *     `splitExpressionsWithCurrentInputs` sees `currentVars != null` and declines to split (also
 *     what we want in that variant).
 *   - Verifying that direct `ctx.splitExpressions` calls (not the `-WithCurrentInputs` wrapper)
 *     in a handful of expressions (`hash`, `Cast`, `collectionOperations`, `ToStringBase`) remain
 *     self-contained. They pass explicit args to their split helpers, so they should be fine, but
 *     that is a per-expression audit.
 *   - Benchmarking. The potential win is that CSE state lives in local variables rather than
 *     class fields, so HotSpot has more freedom to keep values in registers. Whether that wins
 *     over the class-field variant is unclear; CSE state is written once and read 2+ times per
 *     row, and the expression work usually dominates. Not worth doing until a profile shows
 *     class-field access on the hot path.
 *   - If the kernel ever gets integrated into Spark's `WholeStageCodegenExec` pipeline (rather
 *     than standing alone), the WSCG variant becomes the natural fit and this revisit is forced.
 *     Until then, the standalone-kernel shape matches Predicate/Projection/UnsafeRow generators,
 *     which use class-field CSE.
 */
object CometBatchKernelCodegen extends Logging with CometExprTraitShim {

  /**
   * Per-column compile-time invariants. The concrete Arrow vector class and whether the column is
   * nullable are baked into the generated kernel's typed fields and branches. Part of the cache
   * key: different vector classes or nullability produce different kernels.
   *
   * Sealed hierarchy so that complex types (array/map/struct) can carry their nested element
   * shape recursively. Today scalar, array, and struct specs exist; map cases will land as an
   * additional subclass when the emitter covers them. A companion `apply` / `unapply` preserves
   * the original scalar-only construction and extractor shape so existing callers don't need to
   * change.
   */
  sealed trait ArrowColumnSpec {
    def vectorClass: Class[_ <: ValueVector]
    def nullable: Boolean
  }

  object ArrowColumnSpec {

    /** Convenience constructor producing a [[ScalarColumnSpec]]. */
    def apply(vectorClass: Class[_ <: ValueVector], nullable: Boolean): ArrowColumnSpec =
      ScalarColumnSpec(vectorClass, nullable)

    /**
     * Backward-compatible extractor for the common scalar case. Callers that want array / struct
     * / future map specs should pattern match on the subclass directly.
     */
    def unapply(spec: ArrowColumnSpec): Option[(Class[_ <: ValueVector], Boolean)] = spec match {
      case ScalarColumnSpec(c, n) => Some((c, n))
      case _ => None
    }
  }

  /** Scalar column: one Arrow vector class per row slot, no nested structure. */
  final case class ScalarColumnSpec(vectorClass: Class[_ <: ValueVector], nullable: Boolean)
      extends ArrowColumnSpec

  /**
   * Array column: an Arrow `ListVector` wrapping a child spec. `elementSparkType` is the Spark
   * `DataType` of the element so the nested-class getter emitter can choose the right template
   * (e.g. `getUTF8String` for `StringType`, `getInt` for `IntegerType`). The child spec carries
   * the Arrow child vector class. Nested arrays (`Array<Array<...>>`) work by the child being
   * itself an `ArrayColumnSpec`.
   */
  final case class ArrayColumnSpec(
      nullable: Boolean,
      elementSparkType: DataType,
      element: ArrowColumnSpec)
      extends ArrowColumnSpec {
    override def vectorClass: Class[_ <: ValueVector] = classOf[ListVector]
  }

  /**
   * Struct column: an Arrow `StructVector` wrapping N typed child specs. Each entry carries the
   * Spark field name (for schema identification in the cache key), the Spark `DataType` of the
   * field (so per-field emitters pick the right read/write template), the child `ArrowColumnSpec`
   * (so nested shapes like `Struct<Array<...>>` compose by trait-level recursion), and the
   * field's `nullable` bit (so non-nullable fields elide their per-row null check at source
   * level). Nested structs (`Struct<Struct<...>>`) work by the child being itself a
   * `StructColumnSpec`.
   */
  final case class StructColumnSpec(nullable: Boolean, fields: Seq[StructFieldSpec])
      extends ArrowColumnSpec {
    override def vectorClass: Class[_ <: ValueVector] = classOf[StructVector]
  }

  /** One field entry on a [[StructColumnSpec]]. */
  final case class StructFieldSpec(
      name: String,
      sparkType: DataType,
      nullable: Boolean,
      child: ArrowColumnSpec)

  /**
   * Map column: an Arrow `MapVector` (subclass of `ListVector`) whose data vector is a
   * `StructVector` with a key field at ordinal 0 and a value field at ordinal 1. `key` and
   * `value` are themselves `ArrowColumnSpec` so nested shapes (`Map<Struct<...>, Array<X>>`,
   * `Map<Map<...>, ...>`) compose by trait-level recursion. Nullable map entries are controlled
   * per-column by the outer map's validity; nullable keys and values are carried in the child
   * specs' `nullable` bit.
   */
  final case class MapColumnSpec(
      nullable: Boolean,
      keySparkType: DataType,
      valueSparkType: DataType,
      key: ArrowColumnSpec,
      value: ArrowColumnSpec)
      extends ArrowColumnSpec {
    override def vectorClass: Class[_ <: ValueVector] = classOf[MapVector]
  }

  /**
   * Resolve an Arrow vector class by its simple name, using the same classloader the codegen uses
   * internally. Intended for tests: the `common` module shades `org.apache.arrow` to
   * `org.apache.comet.shaded.arrow`, so `classOf[VarCharVector]` at a call site in an unshaded
   * module refers to a different [[Class]] object than the one the codegen compares against.
   * Callers pass a simple name and get back the class the production code actually uses.
   */
  def vectorClassBySimpleName(name: String): Class[_ <: ValueVector] = name match {
    case "BitVector" => classOf[BitVector]
    case "TinyIntVector" => classOf[TinyIntVector]
    case "SmallIntVector" => classOf[SmallIntVector]
    case "IntVector" => classOf[IntVector]
    case "BigIntVector" => classOf[BigIntVector]
    case "Float4Vector" => classOf[Float4Vector]
    case "Float8Vector" => classOf[Float8Vector]
    case "DecimalVector" => classOf[DecimalVector]
    case "DateDayVector" => classOf[DateDayVector]
    case "TimeStampMicroVector" => classOf[TimeStampMicroVector]
    case "TimeStampMicroTZVector" => classOf[TimeStampMicroTZVector]
    case "VarCharVector" => classOf[VarCharVector]
    case "ViewVarCharVector" => classOf[ViewVarCharVector]
    case "VarBinaryVector" => classOf[VarBinaryVector]
    case "ViewVarBinaryVector" => classOf[ViewVarBinaryVector]
    case other => throw new IllegalArgumentException(s"unknown Arrow vector class: $other")
  }

  /**
   * Result of compiling a bound [[Expression]] into a Janino kernel. The `factory` is the Spark
   * [[GeneratedClass]] produced by Janino and is safe to share across threads and partitions: it
   * holds no mutable state. The `freshReferences` closure regenerates the references array each
   * time a new kernel instance is allocated.
   *
   * Why not cache a single `references` array: some expressions (notably [[ScalaUDF]]) embed
   * stateful Spark `ExpressionEncoder` serializers into `references` via `ctx.addReferenceObj`.
   * Those serializers reuse an internal `UnsafeRow` / `byte[]` buffer per `.apply(...)` call and
   * are not thread-safe. If two kernels on different partitions shared one serializer instance,
   * they would race on that buffer and produce garbage. Re-running `genCode` per kernel
   * allocation costs microseconds; Janino compile costs milliseconds. Cache the expensive piece,
   * refresh the cheap one, stay correct.
   *
   * Mirrors Spark `WholeStageCodegenExec`: compile once per plan, instantiate per partition, call
   * `init(partitionIndex)` once, iterate.
   */
  final case class CompiledKernel(factory: GeneratedClass, freshReferences: () => Array[Any]) {
    def newInstance(): CometBatchKernel =
      factory.generate(freshReferences()).asInstanceOf[CometBatchKernel]
  }

  /**
   * Plan-time predicate: can the codegen dispatcher handle this bound expression end to end? If
   * it returns `None`, the serde is free to emit the codegen proto. If it returns `Some(reason)`,
   * the serde must fall back (usually via `withInfo(...) + None`) so Spark runs the expression
   * rather than crashing in the Janino compile at execute time.
   *
   * Checks:
   *   - every `BoundReference`'s data type is in
   *     [[CometBatchKernelCodegenInput.isSupportedInputType]] (i.e. the kernel has a typed getter
   *     for it)
   *   - the overall `expr.dataType` is in [[CometBatchKernelCodegenOutput.isSupportedOutputType]]
   *     (i.e. `allocateOutput` and `emitWrite` know how to materialize it)
   *   - the expression is scalar (no `AggregateFunction`, no generators). These never reach a
   *     scalar serde, but we belt-and-suspenders anyway.
   *
   * Intermediate node types are '''not''' checked. Spark's `doGenCode` materializes intermediates
   * in local variables; only the leaves (which read from the row) and the root (which writes to
   * the output vector) touch Arrow.
   */
  def canHandle(boundExpr: Expression): Option[String] = {
    if (!CometBatchKernelCodegenOutput.isSupportedOutputType(boundExpr.dataType)) {
      return Some(s"codegen dispatch: unsupported output type ${boundExpr.dataType}")
    }
    // Reject expressions that can't be safely compiled or cached:
    //   - AggregateFunction / Generator: non-scalar bridge shape.
    //   - CodegenFallback: opts out of `doGenCode`, which our compile path assumes works.
    //     Passing one in would emit interpreted-eval glue that our kernel can't splice cleanly.
    //   - Unevaluable: unresolved plan markers. Shouldn't reach a serde, but cheap to guard.
    //     `isCodegenInertUnevaluable` lets the shim exclude version-specific leaves that are
    //     `Unevaluable` but never touched by codegen (e.g. Spark 4.0's `ResolvedCollation`, which
    //     lives in `Collate.collation` as a type marker; `Collate.genCode` delegates to its child).
    //
    // Nondeterministic and stateful expressions are accepted: the dispatcher allocates one
    // kernel instance per partition (per `CometCodegenDispatchUDF.ensureKernel`) and calls
    // `init(partitionIndex)` once on partition entry, so per-row state on `Rand`,
    // `MonotonicallyIncreasingID`, etc. advances correctly across batches in the same
    // partition and resets across partitions.
    //
    // `ExecSubqueryExpression` (e.g. `ScalarSubquery`, `InSubqueryExec`) is also accepted, and
    // works correctly via a four-link invariant:
    //   1. The surrounding Comet operator inherits `SparkPlan.waitForSubqueries`, which calls
    //      `updateResult()` on every `ExecSubqueryExpression` in its `expressions` before the
    //      operator's compute path ever reaches the JVM UDF bridge.
    //   2. `ScalarSubquery.result` (and equivalents on other subquery expressions) is a plain
    //      mutable field on the case class. `@volatile` affects cross-thread visibility but
    //      not serializability: Java/Kryo serializers include it.
    //   3. `SparkEnv.closureSerializer` captures the populated `result` value in the bytes
    //      that travel through `CometCodegenDispatchUDF`'s arg-0 transport.
    //   4. The dispatcher's cache key is those exact bytes (see
    //      `CometCodegenDispatchUDF.CacheKey`). Different `result` values produce different
    //      bytes, hence different cache entries, hence a fresh compile per distinct subquery
    //      value. No cross-query staleness.
    //
    // If any of those four links breaks (a different cache-key derivation that drops `result`;
    // a Comet operator that bypasses `waitForSubqueries`; a transport that strips `@volatile`
    // fields), subquery correctness regresses. Keep this invariant intact when refactoring the
    // cache-key or transport layers.
    boundExpr.find {
      case _: org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction => true
      case _: org.apache.spark.sql.catalyst.expressions.Generator => true
      case _: CodegenFallback => true
      case u: Unevaluable if isCodegenInertUnevaluable(u) => false
      case _: Unevaluable => true
      case _ => false
    } match {
      case Some(bad) =>
        return Some(
          s"codegen dispatch: expression ${bad.getClass.getSimpleName} not supported " +
            "(aggregate, generator, codegen-fallback, or unevaluable)")
      case None =>
    }
    val badRef = boundExpr.collectFirst {
      case b: BoundReference if !CometBatchKernelCodegenInput.isSupportedInputType(b.dataType) =>
        b
    }
    badRef.map(b =>
      s"codegen dispatch: unsupported input type ${b.dataType} at ordinal ${b.ordinal}")
  }

  /**
   * Allocate an Arrow output vector matching the expression's `dataType`. Thin forwarder to
   * [[CometBatchKernelCodegenOutput.allocateOutput]]. Kept on this object as part of the public
   * API so external callers (`CometCodegenDispatchUDF`) do not have to know about the internal
   * split.
   */
  def allocateOutput(
      dataType: DataType,
      name: String,
      numRows: Int,
      estimatedBytes: Int = -1): FieldVector =
    CometBatchKernelCodegenOutput.allocateOutput(dataType, name, numRows, estimatedBytes)

  /**
   * Output of [[generateSource]]. `body` is the raw Java source Janino will compile; `code` is
   * the post-`stripOverlappingComments` wrapper Janino actually takes as input; `references` are
   * the runtime objects the generated constructor pulls from via `ctx.addReferenceObj` (cached
   * patterns, replacement strings, etc.). Tests inspect `body` to assert the shape of the
   * generated source. See `CometCodegenSourceSuite` for examples.
   */
  final case class GeneratedSource(body: String, code: CodeAndComment, references: Array[Any])

  /**
   * Generate the Java source for a kernel without compiling it. Factored out of [[compile]] so
   * tests can assert on the emitted source (null short-circuit present, non-nullable `isNullAt`
   * returns literal `false`, specialized emitter engaged, etc.) without paying for Janino.
   */
  def generateSource(
      boundExpr: Expression,
      inputSchema: Seq[ArrowColumnSpec]): GeneratedSource = {
    val ctx = new CodegenContext
    // `BoundReference.genCode` emits `${ctx.INPUT_ROW}.getUTF8String(ord)`. We alias a local
    // `row` to `this` at the top of `process` so those reads resolve to the kernel's own typed
    // getters (virtual dispatch on a concrete final class, JIT devirtualizes + folds the
    // switch). `row` rather than `this` because Spark's `splitExpressions` uses INPUT_ROW as the
    // parameter name of any helper method it emits; `this` is a reserved keyword, so using it
    // as a parameter name produces `private UTF8String helper(InternalRow this)` which Janino
    // rejects.
    ctx.INPUT_ROW = "row"

    val baseClass = classOf[CometBatchKernel].getName
    // Resolve shaded Arrow class names at compile time so generated source
    // matches the abstract method signature after Maven relocation.
    val valueVectorClass = classOf[ValueVector].getName
    val fieldVectorClass = classOf[FieldVector].getName

    // Pick the per-row body. Specialized emitters get priority; the default reuses
    // Spark's doGenCode.
    //
    // TODO(method-size): the per-row body lives inline inside `process`'s for-loop and is not
    // split. Individual `doGenCode` implementations (e.g. `Concat`, `Cast`, `CaseWhen`) call
    // `ctx.splitExpressionsWithCurrentInputs` internally, which does the right thing here
    // because `currentVars == null` and `INPUT_ROW = "row"`: helper methods get `InternalRow
    // row` as a parameter and our kernel aliases `row = this` in `process`, so they resolve
    // reads through our typed getters. The outer `perRowBody` itself, however, is never split.
    // A sufficiently deep composed expression (e.g. multi-level ScalaUDF with heavy encoder
    // converters per level) can push `process` past Janino's 64KB method size limit, at which
    // point compile fails. Mitigation when we hit that ceiling: wrap `perRowBody` in
    // `ctx.splitExpressionsWithCurrentInputs(Seq(perRowBody), funcName = "evalRow",
    // arguments = Seq(...))`. That path is already covered by the `row`-as-`this` alias we
    // install above. Skip it speculatively because today's workloads sit comfortably below the
    // threshold and splitting unconditionally adds a function-call frame per row for the
    // common case.
    val (concreteOutClass, perRowBody) = boundExpr match {
      case rr: RegExpReplace if canSpecializeRegExpReplace(rr) =>
        (classOf[VarCharVector].getName, specializedRegExpReplaceBody(ctx, rr, inputSchema))
      case _ =>
        // Class-field CSE. `generateExpressions` runs `subexpressionElimination` under the
        // hood, which populates `ctx.subexprFunctions` with per-row helper calls that write
        // common subexpression results into `addMutableState`-allocated fields; the returned
        // `ExprCode` then references those fields. `subexprFunctionsCode` is the concatenated
        // helper invocation block, spliced into the per-row body by `defaultBody` (inside the
        // NullIntolerant else-branch when that short-circuit fires, otherwise before
        // `ev.code`). See the "Subexpression elimination" section of the object-level
        // Scaladoc for why we use this variant rather than the WSCG one.
        val ev = if (SQLConf.get.subexpressionEliminationEnabled) {
          ctx.generateExpressions(Seq(boundExpr), doSubexpressionElimination = true).head
        } else {
          boundExpr.genCode(ctx)
        }
        val subExprsCode = ctx.subexprFunctionsCode
        val (cls, snippet) =
          CometBatchKernelCodegenOutput.outputWriter(boundExpr.dataType, ev.value, ctx)
        (cls, defaultBody(boundExpr, ev, snippet, subExprsCode))
    }

    val typedFieldDecls = CometBatchKernelCodegenInput.inputFieldDecls(inputSchema)
    val typedInputCasts = CometBatchKernelCodegenInput.inputCasts(inputSchema)
    val decimalTypeByOrdinal = CometBatchKernelCodegenInput.decimalPrecisionByOrdinal(boundExpr)
    val getters =
      CometBatchKernelCodegenInput.typedInputAccessors(inputSchema, decimalTypeByOrdinal)
    val nested = CometBatchKernelCodegenInput.nestedClasses(inputSchema)
    val getArrayMethod = CometBatchKernelCodegenInput.emitGetArrayMethod(inputSchema)
    val getStructMethod = CometBatchKernelCodegenInput.emitGetStructMethod(inputSchema)
    val getMapMethod = CometBatchKernelCodegenInput.emitGetMapMethod(inputSchema)

    val codeBody =
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  return new SpecificCometBatchKernel(references);
         |}
         |
         |class SpecificCometBatchKernel extends $baseClass {
         |
         |  ${ctx.declareMutableStates()}
         |
         |  $typedFieldDecls
         |  private int rowIdx;
         |
         |  public SpecificCometBatchKernel(Object[] references) {
         |    super(references);
         |    ${ctx.initMutableStates()}
         |  }
         |
         |  @Override
         |  public void init(int partitionIndex) {
         |    ${ctx.initPartition()}
         |  }
         |
         |  $getters
         |  $getArrayMethod
         |  $getStructMethod
         |  $getMapMethod
         |
         |  @Override
         |  public void process(
         |      $valueVectorClass[] inputs,
         |      $fieldVectorClass outRaw,
         |      int numRows) {
         |    $concreteOutClass output = ($concreteOutClass) outRaw;
         |    $typedInputCasts
         |    // Alias the kernel as `row` so Spark-generated `${ctx.INPUT_ROW}.method()` reads
         |    // resolve to the kernel's own typed getters. Helper methods that Spark splits off
         |    // via `splitExpressions` also take `InternalRow row` as a parameter; we pass `this`
         |    // implicitly since callers substitute INPUT_ROW which we've set to `row`.
         |    org.apache.spark.sql.catalyst.InternalRow row = this;
         |    for (int i = 0; i < numRows; i++) {
         |      this.rowIdx = i;
         |      $perRowBody
         |    }
         |  }
         |
         |  ${ctx.declareAddedFunctions()}
         |
         |$nested
         |}
       """.stripMargin

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    GeneratedSource(code.body, code, ctx.references.toArray)
  }

  def compile(boundExpr: Expression, inputSchema: Seq[ArrowColumnSpec]): CompiledKernel = {
    val src = generateSource(boundExpr, inputSchema)
    val (clazz, _) =
      try {
        CodeGenerator.compile(src.code)
      } catch {
        case t: Throwable =>
          logError(
            s"CometBatchKernelCodegen: compile failed for ${boundExpr.getClass.getSimpleName}. " +
              s"Generated source follows:\n${src.body}",
            t)
          throw t
      }
    // One log per unique (expr, schema) compile; the caller caches the result so subsequent
    // batches with the same shape reuse this compile.
    val specialized = boundExpr match {
      case _: RegExpReplace
          if canSpecializeRegExpReplace(boundExpr.asInstanceOf[RegExpReplace]) =>
        " [specialized]"
      case _ => ""
    }
    logInfo(
      s"CometBatchKernelCodegen: compiled ${boundExpr.getClass.getSimpleName}$specialized " +
        s"-> ${boundExpr.dataType}  inputs=" +
        inputSchema
          .map(s => s"${s.vectorClass.getSimpleName}${if (s.nullable) "?" else ""}")
          .mkString(","))
    // Freshen references per kernel allocation. See the `CompiledKernel` scaladoc for why.
    // `generateSource` is pure with respect to its inputs (no hidden state) and produces a
    // layout-compatible references array each time because the expression and schema are
    // fixed.
    val freshReferences: () => Array[Any] = () =>
      generateSource(boundExpr, inputSchema).references
    CompiledKernel(clazz, freshReferences)
  }

  /**
   * Can this `RegExpReplace` instance be handled by the specialized emitter? Requires a direct
   * column reference as subject, non-null foldable pattern and replacement, and offset of 1.
   * Other shapes fall back to the default `doGenCode` path.
   */
  private def canSpecializeRegExpReplace(rr: RegExpReplace): Boolean = {
    val subjectIsBound =
      rr.subject.isInstanceOf[BoundReference] && rr.subject.dataType == StringType
    val patternOk =
      rr.regexp.foldable && rr.regexp.dataType == StringType && rr.regexp.eval() != null
    val replOk = rr.rep.foldable && rr.rep.dataType == StringType && rr.rep.eval() != null
    val posIsOne = rr.pos match {
      case Literal(v: Int, _) => v == 1
      case _ => false
    }
    subjectIsBound && patternOk && replOk && posIsOne
  }

  /**
   * Emit the per-row body for `RegExpReplace`. Per-row shape: read Arrow subject bytes, decode to
   * Java `String`, run `Matcher.replaceAll` with a cached `Pattern` and the replacement String,
   * re-encode to bytes, write to Arrow.
   *
   * ==Why this specialization exists==
   *
   * The default path runs `boundExpr.genCode(ctx)` and wraps it with kernel-side getter reads and
   * a `UTF8String -> bytes -> Arrow` write. For `RegExpReplace` specifically, Spark's generated
   * code does not stay in `UTF8String` space: `java.util.regex.Matcher` requires a
   * `CharSequence`, so the generated code materializes a Java `String` from the input
   * `UTF8String` (a UTF-8 decode, allocating a `char[]`), runs the matcher, then wraps the result
   * String back into a `UTF8String` (a UTF-8 encode, allocating a `byte[]`). The per-row shape
   * is:
   *
   * {{{
   *   default:  Arrow bytes -> UTF8String -> String -> Matcher ->
   *              String -> UTF8String -> bytes -> Arrow
   * }}}
   *
   * On a wide-match workload (every character of the row gets replaced, so the output is the full
   * row length), the round trip added ~44% per-row cost versus a tight byte-oriented loop with
   * shape:
   *
   * {{{
   *   specialized:  Arrow bytes -> String -> Matcher -> String -> bytes -> Arrow
   * }}}
   *
   * This specialization emits the byte-oriented shape directly. No `UTF8String` appears in the
   * generated per-row loop. The expression remains a first-class citizen of the dispatcher
   * (plan-time serde, schema-keyed caching, zero-config for the caller).
   *
   * ==When to add a specialization==
   *
   * The general rule: specialize when an expression's `doGenCode` output shape forces conversions
   * that an Arrow-aware byte-oriented implementation does not pay. The common case is expressions
   * whose implementation requires a Java `String` (anything using `java.util.regex` and some
   * `DateTimeFormatter` expressions), because Spark's `UTF8String <-> String` round-trip is not
   * free for wide outputs. Keep specializations minimal so comparisons stay honest. Avoid
   * layering speculative optimizations; let the default-path optimization menu handle the common
   * cases.
   */
  private def specializedRegExpReplaceBody(
      ctx: CodegenContext,
      rr: RegExpReplace,
      inputSchema: Seq[ArrowColumnSpec]): String = {
    val subjectOrd = rr.subject.asInstanceOf[BoundReference].ordinal
    val subjectClass = inputSchema(subjectOrd).vectorClass
    require(
      subjectClass == classOf[VarCharVector] || subjectClass == classOf[ViewVarCharVector],
      "specializedRegExpReplaceBody expects VarCharVector or ViewVarCharVector at ordinal " +
        s"$subjectOrd, got ${subjectClass.getSimpleName}")

    val patternStr = rr.regexp.eval().toString
    val replStr = rr.rep.eval().toString
    val compiledPattern = java.util.regex.Pattern.compile(patternStr)

    // addReferenceObj adds a class-level field initialized from references[] in the constructor,
    // so the Pattern and replacement String are resolved once, not per row.
    val patternRef =
      ctx.addReferenceObj("pattern", compiledPattern, "java.util.regex.Pattern")
    val replRef = ctx.addReferenceObj("replacement", replStr, "java.lang.String")

    val sb = ctx.freshName("sb")
    val s = ctx.freshName("s")
    val r = ctx.freshName("r")
    val rb = ctx.freshName("rb")

    s"""
       |if (this.col$subjectOrd.isNull(i)) {
       |  output.setNull(i);
       |} else {
       |  byte[] $sb = this.col$subjectOrd.get(i);
       |  String $s = new String($sb, java.nio.charset.StandardCharsets.UTF_8);
       |  String $r = $patternRef.matcher($s).replaceAll($replRef);
       |  byte[] $rb = $r.getBytes(java.nio.charset.StandardCharsets.UTF_8);
       |  output.setSafe(i, $rb, 0, $rb.length);
       |}
     """.stripMargin
  }

  /**
   * Per-row body for the default (non-specialized) path.
   *
   * For expressions that implement the `NullIntolerant` marker trait (null in any input -> null
   * output), emits a short-circuit that skips expression evaluation entirely when any input
   * column is null in the current row. This saves the full `ev.code` cost for null rows, not just
   * the output setNull call. Does not change behavior, only performance.
   *
   * For other expressions, the standard shape applies: evaluate the expression, then check
   * `ev.isNull` to decide between `setNull` and a write. Null semantics are handled internally by
   * Spark's generated `ev.code`.
   *
   * `subExprsCode` is the CSE helper-invocation block (see the "Subexpression elimination"
   * section of the object-level Scaladoc). It writes common subexpression results into class
   * fields that `ev.code` reads, so it must run before `ev.code`. In the NullIntolerant short-
   * circuit case it is placed inside the else branch, skipping CSE evaluation for null rows as
   * well as main-body evaluation. In the default case it precedes `ev.code`. Empty string when
   * CSE is disabled or the tree has no common subexpressions.
   */
  private def defaultBody(
      boundExpr: Expression,
      ev: ExprCode,
      writeSnippet: String,
      subExprsCode: String): String = {
    boundExpr match {
      case _ if isNullIntolerant(boundExpr) && allNullIntolerant(boundExpr) =>
        // Every node from root to leaf is either NullIntolerant or a leaf. That transitively
        // guarantees "any BoundReference null at this row -> whole expression null", so we can
        // short-circuit on the union of input ordinals. Breaking the chain with a non-null-
        // propagating node like `coalesce` or `if` produces the wrong result (coalesce(null,x)
        // is x, not null), so the check above rejects those shapes and falls through to the
        // default branch which runs Spark's own null-aware ev.code.
        val inputOrdinals =
          boundExpr.collect { case b: BoundReference => b.ordinal }.distinct
        val nullCheck =
          if (inputOrdinals.isEmpty) "false"
          else inputOrdinals.map(ord => s"this.col$ord.isNull(i)").mkString(" || ")
        s"""
           |if ($nullCheck) {
           |  output.setNull(i);
           |} else {
           |  $subExprsCode
           |  ${ev.code}
           |  $writeSnippet
           |}
         """.stripMargin
      case _ =>
        // Optimization: NonNullableOutputShortCircuit.
        // When the bound expression declares `nullable = false`, the `if (ev.isNull)` branch is
        // dead and HotSpot may or may not fold it (it depends on whether the expression's
        // `doGenCode` made `ev.isNull` a `FalseLiteral` or a variable whose value is
        // false-at-runtime but not a compile-time constant from Spark's side). Drop the guard
        // at source level so we don't depend on JIT folding and keep the generated body
        // minimal.
        if (!boundExpr.nullable) {
          s"""
             |$subExprsCode
             |${ev.code}
             |$writeSnippet
           """.stripMargin
        } else {
          s"""
             |$subExprsCode
             |${ev.code}
             |if (${ev.isNull}) {
             |  output.setNull(i);
             |} else {
             |  $writeSnippet
             |}
           """.stripMargin
        }
    }
  }

  /**
   * True iff every node in the expression tree is either `NullIntolerant` or a leaf we can safely
   * consider null-propagating (`BoundReference` and `Literal`). Used to gate the `NullIntolerant`
   * short-circuit in [[defaultBody]]: the short-circuit collects `BoundReference` ordinals from
   * the whole tree and skips `ev.code` when any of them is null, which is only correct when every
   * path from a leaf to the root propagates nulls. A non- propagating node (`Coalesce`, `If`,
   * `CaseWhen`, `Concat`, etc.) anywhere in the tree invalidates this assumption: `coalesce(null,
   * x)` is `x`, not null, so pre-nulling on any input null would produce the wrong result.
   */
  private def allNullIntolerant(expr: Expression): Boolean =
    !expr.exists {
      case _: BoundReference | _: Literal => false
      case other => !isNullIntolerant(other)
    }
}
