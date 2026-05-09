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
import org.apache.arrow.vector.{BaseVariableWidthViewVector, BigIntVector, BitVector, DateDayVector, DecimalVector, FieldVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TimeStampMicroTZVector, TimeStampMicroVector, TinyIntVector, ValueVector, VarBinaryVector, VarCharVector, ViewVarBinaryVector, ViewVarCharVector}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, Literal, RegExpReplace, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodegenContext, CodeGenerator, CodegenFallback, ExprCode, GeneratedClass}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampNTZType, TimestampType}

import org.apache.comet.CometArrowAllocator
import org.apache.comet.shims.CometExprTraitShim

/**
 * Compiles a bound [[Expression]] plus an input schema into a specialized [[CometBatchKernel]]
 * that fuses Arrow input reads, expression evaluation, and Arrow output writes into one
 * Janino-compiled method per (expression, schema) pair.
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
 * Input scope: all scalar Spark types that map to a single Arrow vector, covering `BitVector`,
 * `TinyIntVector`, `SmallIntVector`, `IntVector`, `BigIntVector`, `Float4Vector`, `Float8Vector`,
 * `DecimalVector`, `VarCharVector` and `ViewVarCharVector`, `VarBinaryVector` and
 * `ViewVarBinaryVector`, `DateDayVector`, and the timestamp variants `TimeStampMicroVector` and
 * `TimeStampMicroTZVector`. Output scope: all scalar Spark types that map to a single Arrow
 * vector (Boolean, Byte, Short, Int, Long, Float, Double, Decimal, String, Binary, Date,
 * Timestamp, TimestampNTZ). Widen inputs by adding cases to [[typedInputAccessors]]; widen
 * outputs by adding cases to [[outputWriter]] and [[allocateOutput]].
 *
 * ==Default path==
 *
 * Reuses Spark's `doGenCode` for expression evaluation. BoundReference reads resolve to typed,
 * constant-ordinal calls into the kernel's own getters.
 *
 * ==Specialized path==
 *
 * A per-expression match case in [[compile]] emits custom Java, bypassing `doGenCode`. Used for
 * expressions whose default-path codegen pays a measurable penalty versus hand-coded because
 * Spark's generated code materializes a Java `String` (for example, `java.util.regex.Matcher`
 * requires a `CharSequence`). See [[specializedRegExpReplaceBody]] for the reasoning and the
 * criteria for adding a new specialization.
 *
 * ==Universal boundary optimizations==
 *
 * Applied to every compiled kernel regardless of expression class. Current set:
 *
 *   - '''Zero-copy UTF8String reads''' ([[typedInputAccessors]]). `getUTF8String` wraps Arrow's
 *     native data buffer address directly via `UTF8String.fromAddress`. Skips the `byte[]`
 *     allocation that `VarCharVector.get(i)` would pay.
 *   - '''Pre-sized string output buffers''' ([[allocateOutput]]). For variable-length output
 *     types, the caller passes an input-size-derived byte estimate to avoid mid-loop reallocation
 *     in `setSafe`.
 *   - '''`NullIntolerant` short-circuit''' ([[defaultBody]]). For expressions that implement
 *     Spark's `NullIntolerant` marker trait (null in any input -> null output), the emitter
 *     prepends an input-nullity pre-check that skips expression evaluation entirely for null
 *     rows, not just the output write.
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
   * nullable are both baked into the generated kernel's typed fields and branches. Part of the
   * cache key: different vector classes or nullability produce different kernels.
   */
  final case class ArrowColumnSpec(vectorClass: Class[_ <: ValueVector], nullable: Boolean)

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
   *   - every `BoundReference`'s data type is in [[isSupportedInputType]] (i.e. the kernel has a
   *     typed getter for it)
   *   - the overall `expr.dataType` is in [[isSupportedOutputType]] (i.e. `allocateOutput` and
   *     `outputWriter` know how to materialize it)
   *   - the expression is scalar (no `AggregateFunction`, no generators). These never reach a
   *     scalar serde, but we belt-and-suspenders anyway.
   *
   * Intermediate node types are '''not''' checked. Spark's `doGenCode` materializes intermediates
   * in local variables; only the leaves (which read from the row) and the root (which writes to
   * the output vector) touch Arrow.
   */
  def canHandle(boundExpr: Expression): Option[String] = {
    if (!isSupportedOutputType(boundExpr.dataType)) {
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
      case b: BoundReference if !isSupportedInputType(b.dataType) => b
    }
    badRef.map(b =>
      s"codegen dispatch: unsupported input type ${b.dataType} at ordinal ${b.ordinal}")
  }

  /**
   * Input types the kernel has a typed getter for. Widen when [[typedInputAccessors]] adds cases.
   */
  private def isSupportedInputType(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType => true
    case FloatType | DoubleType => true
    case _: DecimalType => true
    // `_: StringType` rather than `StringType` matches collated variants too (Spark 4.x's
    // `StringType` is a class whose case object is the default UTF8_BINARY instance).
    case _: StringType | _: BinaryType => true
    case DateType | TimestampType | TimestampNTZType => true
    case _ => false
  }

  /** Output types [[allocateOutput]] and [[outputWriter]] can materialize. */
  private def isSupportedOutputType(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType => true
    case FloatType | DoubleType => true
    case _: DecimalType => true
    case _: StringType | _: BinaryType => true
    case DateType | TimestampType | TimestampNTZType => true
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
      case other =>
        throw new UnsupportedOperationException(
          s"CometBatchKernelCodegen: unsupported output type $other")
    }

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
        val (cls, snippet) = outputWriter(boundExpr.dataType, ev.value)
        (cls, defaultBody(boundExpr, ev, snippet, subExprsCode))
    }

    val typedFieldDecls = inputFieldDecls(inputSchema)
    val typedInputCasts = inputCasts(inputSchema)
    val decimalTypeByOrdinal = decimalPrecisionByOrdinal(boundExpr)
    val getters = typedInputAccessors(inputSchema, decimalTypeByOrdinal)

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

  /** Emit `private $Class col$ord;` declarations, one per input column. */
  private def inputFieldDecls(inputSchema: Seq[ArrowColumnSpec]): String =
    inputSchema.zipWithIndex
      .map { case (spec, ord) => s"private ${spec.vectorClass.getName} col$ord;" }
      .mkString("\n")

  /** Emit `this.col$ord = ($Class) inputs[$ord];` casts at the top of `process`. */
  private def inputCasts(inputSchema: Seq[ArrowColumnSpec]): String =
    inputSchema.zipWithIndex
      .map { case (spec, ord) =>
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
   */
  private def typedInputAccessors(
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
  private def decimalPrecisionByOrdinal(boundExpr: Expression): Map[Int, Option[DecimalType]] = {
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
   * Emit the per-row body for `RegExpReplace`. Matches the hand-coded `RegExpReplaceUDF` loop:
   * read Arrow subject bytes, decode to Java `String`, run `Matcher.replaceAll` with a cached
   * `Pattern` and the replacement String, re-encode to bytes, write to Arrow.
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
   * On the `replace_wide_match` benchmark (every character of the row gets replaced, so the
   * output is the full row length), this added ~44% per-row cost versus the hand-coded
   * `RegExpReplaceUDF`, which has the shape:
   *
   * {{{
   *   hand-coded:  Arrow bytes -> String -> Matcher -> String -> bytes -> Arrow
   * }}}
   *
   * This specialization emits the hand-coded shape directly. No `UTF8String` appears in the
   * generated per-row loop. Performance becomes equivalent to the hand-coded UDF while the
   * expression remains a first-class citizen of the dispatcher (plan-time serde, schema-keyed
   * caching, zero-config for the caller).
   *
   * ==When to add a specialization==
   *
   * The general rule: specialize when an expression's `doGenCode` output shape forces conversions
   * that the Arrow-aware hand-coded equivalent does not pay. The common case is expressions whose
   * implementation requires a Java `String` (anything using `java.util.regex` and some
   * `DateTimeFormatter` expressions), because Spark's `UTF8String <-> String` round-trip is not
   * free for wide outputs. Specializations should match the hand-coded implementation shape and
   * nothing more, so the comparison stays honest. Avoid layering optimizations beyond what the
   * hand-coded path does in the same file.
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

  /**
   * Returns `(concreteVectorClassName, writeJavaSnippet)` for the expression's output type. The
   * snippet assumes `output` is already cast to the concrete vector class, `i` is the current row
   * index, and `$valueTerm` is the Java expression holding the bound expression's evaluated value
   * (a primitive, `UTF8String`, `byte[]`, or Spark `Decimal` depending on `dataType`).
   */
  private def outputWriter(dataType: DataType, valueTerm: String): (String, String) =
    dataType match {
      case BooleanType =>
        // BitVector.set takes int; 0 or 1 encodes false/true.
        (classOf[BitVector].getName, s"output.set(i, $valueTerm ? 1 : 0);")
      case ByteType =>
        (classOf[TinyIntVector].getName, s"output.set(i, $valueTerm);")
      case ShortType =>
        (classOf[SmallIntVector].getName, s"output.set(i, $valueTerm);")
      case IntegerType =>
        (classOf[IntVector].getName, s"output.set(i, $valueTerm);")
      case LongType =>
        (classOf[BigIntVector].getName, s"output.set(i, $valueTerm);")
      case FloatType =>
        (classOf[Float4Vector].getName, s"output.set(i, $valueTerm);")
      case DoubleType =>
        (classOf[Float8Vector].getName, s"output.set(i, $valueTerm);")
      case _: DecimalType =>
        // Spark `Decimal.toJavaBigDecimal()` allocates a `java.math.BigDecimal`. DecimalVector's
        // `setSafe(int, BigDecimal)` copies the unscaled bytes into the fixed-width buffer.
        // Cheaper paths exist (unscaled-long fast-path for short decimals, direct buffer writes
        // for longer ones) but require branching on `Decimal.toUnscaledLong` success. Defer.
        (classOf[DecimalVector].getName, s"output.setSafe(i, $valueTerm.toJavaBigDecimal());")
      case _: StringType =>
        // UTF8String.getBytes returns a fresh byte[]; setSafe copies into the Arrow data buffer.
        (
          classOf[VarCharVector].getName,
          s"byte[] b = $valueTerm.getBytes(); output.setSafe(i, b, 0, b.length);")
      case BinaryType =>
        // BoundReference produces a `byte[]` directly for BinaryType.
        (
          classOf[VarBinaryVector].getName,
          s"output.setSafe(i, $valueTerm, 0, $valueTerm.length);")
      case DateType =>
        // Days since epoch; Spark's codegen for DateType values is plain `int`.
        (classOf[DateDayVector].getName, s"output.set(i, $valueTerm);")
      case TimestampType =>
        // Microseconds since epoch, UTC. Spark's codegen produces `long`.
        (classOf[TimeStampMicroTZVector].getName, s"output.set(i, $valueTerm);")
      case TimestampNTZType =>
        (classOf[TimeStampMicroVector].getName, s"output.set(i, $valueTerm);")
      case other =>
        throw new UnsupportedOperationException(
          s"CometBatchKernelCodegen: unsupported output type $other")
    }
}
