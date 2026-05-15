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

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, Literal, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.shims.CometExprTraitShim

/**
 * Compiles a bound [[Expression]] plus an input schema into a [[CometBatchKernel]] that fuses
 * Arrow input reads, expression evaluation, and Arrow output writes into one Janino-compiled
 * method per (expression, schema) pair.
 *
 * The kernel is generic over Catalyst expressions; it does not know or assume that the bound tree
 * came from a `ScalaUDF`. Today's only consumer is
 * [[org.apache.comet.udf.codegen.CometScalaUDFCodegen]], but a future consumer (Spark
 * `WholeStageCodegenExec` integration, a non-UDF batch evaluator) can drive this class directly.
 *
 * Constraints: single output vector per kernel (whole projections need a multi-output extension);
 * per-row scalar evaluation only (aggregation, window, generator rejected by [[canHandle]]).
 *
 * Input- and output-side emission live in [[CometBatchKernelCodegenInput]] and
 * [[CometBatchKernelCodegenOutput]]. This file owns the [[ArrowColumnSpec]] vocabulary, the
 * [[canHandle]] / [[allocateOutput]] / [[compile]] / [[generateSource]] entry points, and
 * cross-cutting kernel-shape decisions (null-intolerant short-circuit, CSE variant).
 *
 * The generated kernel '''is''' the `InternalRow` that Spark's `BoundReference.genCode` reads
 * from: `ctx.INPUT_ROW = "row"` plus `InternalRow row = this;` inside `process` routes
 * `row.getUTF8String(ord)` to the kernel's own typed getter (final method, constant ordinal; JIT
 * devirtualizes and folds). `row` rather than `this` because Spark's `splitExpressions` passes
 * INPUT_ROW as a helper-method parameter name and `this` is a reserved Java keyword.
 */
object CometBatchKernelCodegen extends Logging with CometExprTraitShim {

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
    case "VarBinaryVector" => classOf[VarBinaryVector]
    case other => throw new IllegalArgumentException(s"unknown Arrow vector class: $other")
  }

  /**
   * Type surface the kernel covers, on both the input getter side and the output writer side.
   * Recursive: `ArrayType` / `StructType` / `MapType` are supported when their children are.
   * Input and output use a single predicate today; if they ever need to diverge, split this back
   * into per-direction methods.
   */
  def isSupportedDataType(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType => true
    case FloatType | DoubleType => true
    case _: DecimalType => true
    case _: StringType | _: BinaryType => true
    case DateType | TimestampType | TimestampNTZType => true
    case ArrayType(inner, _) => isSupportedDataType(inner)
    case st: StructType => st.fields.forall(f => isSupportedDataType(f.dataType))
    case mt: MapType => isSupportedDataType(mt.keyType) && isSupportedDataType(mt.valueType)
    case _ => false
  }

  /**
   * Count the number of leaf fields (including nested) in a [[DataType]]. Mirrors WSCG's
   * `WholeStageCodegenExec.numOfNestedFields` so the [[canHandle]] threshold check uses the same
   * unit as `spark.sql.codegen.maxFields`.
   */
  private def numOfNestedFields(dataType: DataType): Int = dataType match {
    case st: StructType => st.fields.map(f => numOfNestedFields(f.dataType)).sum
    case m: MapType => numOfNestedFields(m.keyType) + numOfNestedFields(m.valueType)
    case a: ArrayType => numOfNestedFields(a.elementType)
    case _ => 1
  }

  /**
   * Plan-time predicate: can the codegen dispatcher handle this bound expression end to end?
   * `None` greenlights the serde to emit the codegen proto; `Some(reason)` forces a Spark
   * fallback (typically `withInfo(...) + None`) rather than crashing the Janino compile at
   * execute time.
   *
   * Checks every `BoundReference`'s data type and the root `expr.dataType` against
   * [[isSupportedDataType]], and rejects aggregates / generators. Intermediate nodes are not
   * checked: only leaves (row reads) and the root (output write) touch Arrow.
   */
  def canHandle(boundExpr: Expression): Option[String] = {
    if (!isSupportedDataType(boundExpr.dataType)) {
      return Some(s"codegen dispatch: unsupported output type ${boundExpr.dataType}")
    }
    // Mirror WSCG's `spark.sql.codegen.maxFields` gate. Count nested fields in the output type
    // and in every `BoundReference`'s input type. Wide schemas blow the generated class's typed
    // input field count, the typed-getter switch, and the constant pool. Refuse here so the
    // operator falls back to Spark cleanly rather than tripping a Janino compile failure
    // mid-execution (which Comet has no way to recover from).
    val maxFields = SQLConf.get.wholeStageMaxNumFields
    val totalFields = numOfNestedFields(boundExpr.dataType) +
      boundExpr.collect { case b: BoundReference => numOfNestedFields(b.dataType) }.sum
    if (totalFields > maxFields) {
      return Some(
        s"codegen dispatch: too many nested fields ($totalFields > " +
          s"spark.sql.codegen.maxFields=$maxFields)")
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
    // TODO(hof-lambdas): the `CodegenFallback` rule rejects `NamedLambdaVariable`, which flags
    // every higher-order function (`ArrayTransform`, `ArrayAggregate`, `ArrayExists`,
    // `ArrayFilter`, `ZipWith`, `MapFilter`, etc.) as unsupported. The variable is
    // `CodegenFallback` only in isolation; the surrounding HOF binds its `value` field inline
    // as part of its own
    // `doGenCode`, and the resulting Java compiles fine. Loosening this would unlock
    // element-iteration over `Array<Struct>` / `Array<Map>` which today have no fuzz path
    // (`array_max` doesn't apply to non-comparable elements, generators are blocked above). Plan:
    // allow `NamedLambdaVariable` / `LambdaFunction` in the rejection scan; verify the kernel
    // splices the HOF's emitted loop without ctx.references collisions on the lambda holder.
    //
    // Nondeterministic / stateful expressions are accepted: per-partition kernel allocation
    // (`CometScalaUDFCodegen.ensureKernel`) plus a single `init(partitionIndex)` call at
    // partition entry give `Rand` / `MonotonicallyIncreasingID` / etc. correct state across
    // batches and a clean reset across partitions.
    //
    // `ExecSubqueryExpression` (`ScalarSubquery`, `InSubqueryExec`) is accepted via a chain:
    // the surrounding Comet operator's inherited `SparkPlan.waitForSubqueries` populates the
    // subquery's mutable `result` field before evaluation; the closure serializer captures that
    // populated value into the arg-0 bytes; the dispatcher keys its compile cache on those
    // exact bytes, so distinct subquery results produce distinct cache entries with no
    // cross-query staleness. Refactors to the cache-key derivation, the transport, or any
    // Comet operator that bypasses `waitForSubqueries` would break this; preserve it.
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
      case b: BoundReference if !isSupportedDataType(b.dataType) =>
        b
    }
    badRef.map(b =>
      s"codegen dispatch: unsupported input type ${b.dataType} at ordinal ${b.ordinal}")
  }

  /**
   * Allocate an Arrow output vector matching the expression's `dataType`. Thin forwarder to
   * [[CometBatchKernelCodegenOutput.allocateOutput]]. Kept on this object as part of the public
   * API so external callers (`CometScalaUDFCodegen`) do not have to know about the internal
   * split.
   */
  def allocateOutput(
      dataType: DataType,
      name: String,
      numRows: Int,
      estimatedBytes: Int = -1): FieldVector =
    CometBatchKernelCodegenOutput.allocateOutput(dataType, name, numRows, estimatedBytes)

  /** Variant that takes a pre-computed Arrow `Field`, letting hot-path callers cache it. */
  def allocateOutput(field: Field, numRows: Int, estimatedBytes: Int): FieldVector =
    CometBatchKernelCodegenOutput.allocateOutput(field, numRows, estimatedBytes)

  def compile(boundExpr: Expression, inputSchema: Seq[ArrowColumnSpec]): CompiledKernel = {
    val src = generateSource(boundExpr, inputSchema)
    val (clazz, _) =
      try {
        CodeGenerator.compile(src.code)
      } catch {
        case t: Throwable =>
          logError(
            s"CometBatchKernelCodegen: compile failed for ${boundExpr.getClass.getSimpleName}. " +
              s"Generated source follows:\n${CodeFormatter.format(src.code)}",
            t)
          throw t
      }
    logInfo(
      s"CometBatchKernelCodegen: compiled ${boundExpr.getClass.getSimpleName} " +
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
   * Generate the Java source for a kernel without compiling it. Factored out of [[compile]] so
   * tests can assert on the emitted source (null short-circuit present, non-nullable `isNullAt`
   * returns literal `false`, etc.) without paying for Janino.
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

    // Build the per-row body via Spark's doGenCode.
    //
    // `outputSetup` holds once-per-batch declarations (typed child-vector casts for complex
    // output) that `emitOutputWriter` factors out of the per-row body so they do not repeat on
    // every row. Scalar outputs return an empty string here.
    //
    // TODO(method-size): perRowBody is inlined inside process's for-loop and not split.
    // Sufficiently deep trees can exceed Janino's 64KB method size; wrap in
    // ctx.splitExpressionsWithCurrentInputs when hit.
    val (concreteOutClass, outputSetup, perRowBody) = {
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
      val (cls, setup, snippet) =
        CometBatchKernelCodegenOutput.emitOutputWriter(boundExpr.dataType, ev.value, ctx)
      (cls, setup, defaultBody(boundExpr, ev, snippet, subExprsCode))
    }

    val typedFieldDecls = CometBatchKernelCodegenInput.emitInputFieldDecls(inputSchema)
    val typedInputCasts = CometBatchKernelCodegenInput.emitInputCasts(inputSchema)
    val decimalTypeByOrdinal = CometBatchKernelCodegenInput.decimalPrecisionByOrdinal(boundExpr)
    val getters =
      CometBatchKernelCodegenInput.emitTypedGetters(inputSchema, decimalTypeByOrdinal)
    val nested = CometBatchKernelCodegenInput.emitNestedClasses(inputSchema)
    val getArrayMethod = CometBatchKernelCodegenInput.emitGetArrayMethod(inputSchema)
    val getStructMethod = CometBatchKernelCodegenInput.emitGetStructMethod(inputSchema)
    val getMapMethod = CometBatchKernelCodegenInput.emitGetMapMethod(inputSchema)

    val codeBody =
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  return new SpecificCometBatchKernel(references);
         |}
         |
         |final class SpecificCometBatchKernel extends $baseClass {
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
         |    $outputSetup
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

  /**
   * Per-row body for the default path. For `NullIntolerant` expressions (null in any input ->
   * null output), prepends a short-circuit that skips expression evaluation entirely when any
   * input column is null this row, saving the full `ev.code` cost. Otherwise the standard shape:
   * run `ev.code`, then `setNull` or write based on `ev.isNull`.
   *
   * `subExprsCode` is the CSE helper-invocation block; it writes common subexpression results
   * into class fields that `ev.code` reads, so it must run before `ev.code`. Inside the
   * short-circuit it lives in the else branch, skipping CSE for null rows. Empty when CSE is
   * disabled or the tree has none.
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
   * Result of compiling a bound [[Expression]] into a Janino kernel. The Spark-generated
   * `factory` is stateless and safe to share across partitions; `freshReferences` regenerates the
   * references array per kernel allocation.
   *
   * The references array can't be cached because some expressions (notably [[ScalaUDF]]) embed
   * stateful `ExpressionEncoder` serializers via `ctx.addReferenceObj` that reuse an internal
   * `UnsafeRow` / `byte[]` per `.apply(...)`. Sharing one serializer across partition kernels
   * would race on that buffer. Re-running `genCode` is microseconds; Janino compile is
   * milliseconds. Cache the expensive piece, refresh the cheap one.
   *
   * Mirrors Spark `WholeStageCodegenExec`: compile once per plan, instantiate per partition,
   * `init(partitionIndex)`, iterate.
   */
  final case class CompiledKernel(factory: GeneratedClass, freshReferences: () => Array[Any]) {
    def newInstance(): CometBatchKernel =
      factory.generate(freshReferences()).asInstanceOf[CometBatchKernel]
  }

  /**
   * Output of [[generateSource]]. `body` is the raw Java source Janino will compile; `code` is
   * the post-`stripOverlappingComments` wrapper Janino actually takes as input; `references` are
   * the runtime objects the generated constructor pulls from via `ctx.addReferenceObj` (cached
   * patterns, replacement strings, etc.). Tests inspect `body` to assert the shape of the
   * generated source. See `CometCodegenSourceSuite` for examples.
   */
  final case class GeneratedSource(body: String, code: CodeAndComment, references: Array[Any])

  object ArrowColumnSpec {

    /** Convenience constructor producing a [[ScalarColumnSpec]]. */
    def apply(vectorClass: Class[_ <: ValueVector], nullable: Boolean): ArrowColumnSpec =
      ScalarColumnSpec(vectorClass, nullable)

    /**
     * Trait-level extractor that destructures only the scalar case. Pattern-match callers use
     * `case ArrowColumnSpec(cls, nullable)` to filter on scalar specs and pull out their vector
     * class and nullability in one step; complex specs return `None` and skip the case.
     */
    def unapply(spec: ArrowColumnSpec): Option[(Class[_ <: ValueVector], Boolean)] = spec match {
      case ScalarColumnSpec(c, n) => Some((c, n))
      case _ => None
    }
  }
}
