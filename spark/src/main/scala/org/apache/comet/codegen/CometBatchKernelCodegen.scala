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
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, HigherOrderFunction, LambdaFunction, Literal, NamedLambdaVariable, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.shims.CometExprTraitShim

/**
 * Compiles a bound [[Expression]] plus an Arrow input schema into a [[CometBatchKernel]] that
 * fuses Arrow input reads, Spark expression evaluation, and Arrow output writes into one
 * Janino-compiled method per `(expression, schema)` pair.
 *
 * The kernel compiles any bound Catalyst expression. The tree need not be rooted at a `ScalaUDF`.
 * Today's only consumer is [[org.apache.comet.udf.codegen.CometScalaUDFCodegen]].
 *
 * Constraints: one output vector per kernel, per-row scalar evaluation only (aggregate, window,
 * generator are rejected by [[canHandle]]).
 *
 * Input- and output-side emission live in [[CometBatchKernelCodegenInput]] and
 * [[CometBatchKernelCodegenOutput]]. This file owns the [[ArrowColumnSpec]] vocabulary, the
 * [[canHandle]] / [[allocateOutput]] / [[compile]] / [[generateSource]] entry points, and
 * cross-cutting kernel-shape decisions (NullIntolerant short-circuit, CSE variant).
 *
 * The generated kernel is the `InternalRow` that Spark's `BoundReference.genCode` reads from. See
 * [[generateSource]] for how the wiring is set up.
 */
object CometBatchKernelCodegen extends Logging with CometExprTraitShim {

  /**
   * Resolve an Arrow vector class by simple name through the codegen object's own classloader.
   * Tests use this to refer to vector classes via the same classloader the codegen pattern-
   * matches against, in case the test classpath ever diverges from the codegen's (e.g. through
   * future shading rearrangement).
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
   * Type surface the kernel covers on both input and output sides. Recursive: complex types are
   * supported when their children are.
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
   * Mirrors `WholeStageCodegenExec.numOfNestedFields` so [[canHandle]] can reuse
   * `spark.sql.codegen.maxFields`.
   */
  private def numOfNestedFields(dataType: DataType): Int = dataType match {
    case st: StructType => st.fields.map(f => numOfNestedFields(f.dataType)).sum
    case m: MapType => numOfNestedFields(m.keyType) + numOfNestedFields(m.valueType)
    case a: ArrayType => numOfNestedFields(a.elementType)
    case _ => 1
  }

  /**
   * Plan-time predicate. `None` greenlights the serde to emit the codegen proto; `Some(reason)`
   * forces a Spark fallback (typically `withFallbackReason(...) + None`) so the operator falls
   * back cleanly rather than crashing the Janino compile at execute time.
   *
   * Checks every `BoundReference`'s data type and the root `expr.dataType` against
   * [[isSupportedDataType]], rejects aggregates / generators / `CodegenFallback` (other than
   * HOFs, which are admitted), and gates total nested-field count on
   * `spark.sql.codegen.maxFields`.
   */
  def canHandle(boundExpr: Expression): Option[String] = {
    if (!isSupportedDataType(boundExpr.dataType)) {
      return Some(s"codegen dispatch: unsupported output type ${boundExpr.dataType}")
    }
    // Mirror WSCG's `spark.sql.codegen.maxFields` gate. Wide schemas blow the generated class's
    // typed input field count, the typed-getter switch, and the constant pool. Refuse here so the
    // operator falls back to Spark cleanly rather than tripping a Janino compile failure
    // mid-execution (Comet has no recovery for that).
    val maxFields = SQLConf.get.wholeStageMaxNumFields
    val totalFields = numOfNestedFields(boundExpr.dataType) +
      boundExpr.collect { case b: BoundReference => numOfNestedFields(b.dataType) }.sum
    if (totalFields > maxFields) {
      return Some(
        s"codegen dispatch: too many nested fields ($totalFields > " +
          s"spark.sql.codegen.maxFields=$maxFields)")
    }
    // HOFs are `CodegenFallback` but admitted: `CodegenFallback.doGenCode` emits one
    // `((Expression) references[N]).eval(row)` call site per HOF. The kernel dispatches to the
    // HOF's interpreted `eval`, which mutates `NamedLambdaVariable.value` per element and reads
    // the input array through the kernel's typed Arrow getters. Per-task `boundExpr` isolation
    // in `CometScalaUDFCodegen.kernelCache` prevents concurrent partitions from racing on the
    // lambda variable's `AtomicReference`. See `CometCodegenHOFSuite`.
    //
    // Nondeterministic / stateful expressions are accepted: each cache entry holds one kernel
    // instance with a single `init(partitionIndex)` call, so `Rand` / `MonotonicallyIncreasingID`
    // state advances correctly across batches.
    //
    // `ExecSubqueryExpression` (`ScalarSubquery`, `InSubqueryExec`) is accepted: the surrounding
    // Comet operator's inherited `SparkPlan.waitForSubqueries` populates the subquery's
    // `result` field before evaluation. The closure serializer captures that value into the
    // arg-0 bytes, and the dispatcher keys its compile cache on those bytes, so distinct subquery
    // results produce distinct cache entries.
    //
    // `Unevaluable`: rejected by default. `isCodegenInertUnevaluable` exempts version-specific
    // leaves that are `Unevaluable` but never invoked by codegen (e.g. Spark 4.0's
    // `ResolvedCollation` in `Collate.collation`, where `Collate.genCode` delegates to its child).
    boundExpr.find {
      case _: org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction => true
      case _: org.apache.spark.sql.catalyst.expressions.Generator => true
      case _: HigherOrderFunction => false
      case _: LambdaFunction => false
      case _: NamedLambdaVariable => false
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
   * Allocate an Arrow output vector from a pre-built `Field`. Forwards to
   * [[CometBatchKernelCodegenOutput.allocateOutput]].
   */
  def allocateOutput(field: Field, numRows: Int, estimatedBytes: Int): FieldVector =
    CometBatchKernelCodegenOutput.allocateOutput(field, numRows, estimatedBytes)

  /**
   * Spark `DataType` to an Arrow `Field`, resolving mismatches between Arrow Java's default field
   * labels and what Spark / Arrow Rust expect on the FFI boundary.
   */
  def toFfiArrowField(name: String, dataType: DataType, nullable: Boolean): Field =
    CometBatchKernelCodegenOutput.toFfiArrowField(name, dataType, nullable)

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
    logDebug(
      s"CometBatchKernelCodegen: compiled ${boundExpr.getClass.getSimpleName} " +
        s"-> ${boundExpr.dataType}  inputs=" +
        inputSchema
          .map(s => s"${s.vectorClass.getSimpleName}${if (s.nullable) "?" else ""}")
          .mkString(","))
    // ScalaUDF embeds stateful `ExpressionEncoder` serializers via `ctx.addReferenceObj` that
    // reuse internal `UnsafeRow` / `byte[]` buffers per `apply`. Each kernel instance needs its
    // own copy. The closure regenerates the references array per call so the dispatcher can hand
    // a fresh array to every kernel it allocates from this `CompiledKernel`.
    val freshReferences: () => Array[Any] = () =>
      generateSource(boundExpr, inputSchema).references
    CompiledKernel(clazz, freshReferences)
  }

  /**
   * Generate the Java source without compiling it. Tests assert on emitted source (null short-
   * circuit present, non-nullable `isNullAt` returns literal `false`, etc.) without paying for
   * Janino.
   */
  def generateSource(
      boundExpr: Expression,
      inputSchema: Seq[ArrowColumnSpec]): GeneratedSource = {
    canHandle(boundExpr).foreach(reason =>
      throw new IllegalArgumentException(s"CometBatchKernelCodegen.generateSource: $reason"))
    val ctx = new CodegenContext
    // `BoundReference.genCode` emits `${ctx.INPUT_ROW}.getUTF8String(ord)`. Aliasing `row` to
    // `this` at the top of `process` routes those reads to the kernel's typed getters (final
    // class, JIT devirtualizes + folds the switch). `row` rather than `this` because Spark's
    // `splitExpressions` uses `INPUT_ROW` as the parameter name of helper methods it emits;
    // `this` is a reserved keyword and Janino rejects it as a parameter name.
    ctx.INPUT_ROW = "row"

    val baseClass = classOf[CometBatchKernel].getName
    // Resolve Arrow class names at runtime so the generated source matches the method signature
    // the running classloader sees. The packaged Comet jar relocates `org.apache.arrow` to
    // `org.apache.comet.shaded.arrow` (see `spark/pom.xml`); `.getName` picks the right name
    // regardless of whether we run against the shaded jar or the unshaded build output.
    val valueVectorClass = classOf[ValueVector].getName
    val fieldVectorClass = classOf[FieldVector].getName

    // `outputSetup` holds once-per-batch declarations (typed child-vector casts for complex
    // outputs) that `emitOutputWriter` factors out of the per-row body. Scalar outputs return an
    // empty string here.
    //
    // TODO(method-size): perRowBody is inlined inside process's for-loop and not split.
    // Sufficiently deep trees can exceed Janino's 64KB method size. Wrap in
    // ctx.splitExpressionsWithCurrentInputs when hit.
    val (concreteOutClass, outputSetup, perRowBody) = {
      // Class-field CSE. `generateExpressions` runs `subexpressionElimination` under the hood,
      // populating `ctx.subexprFunctions` with per-row helper calls that write common subtree
      // results into `addMutableState` fields. The returned `ExprCode` references those fields.
      // `subexprFunctionsCode` is the concatenated helper invocation block, spliced into the
      // per-row body by `defaultBody`.
      val ev = if (SQLConf.get.subexpressionEliminationEnabled) {
        ctx.generateExpressions(Seq(boundExpr), doSubexpressionElimination = true).head
      } else {
        boundExpr.genCode(ctx)
      }
      val subExprsCode = ctx.subexprFunctionsCode
      val (cls, setup, snippet) =
        CometBatchKernelCodegenOutput.emitOutputWriter(boundExpr.dataType, ev.value, ctx)
      (cls, setup, defaultBody(boundExpr, inputSchema, ev, snippet, subExprsCode))
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
         |    // resolve to the kernel's typed getters. Helper methods that Spark splits via
         |    // `splitExpressions` also take `InternalRow row` as a parameter; `this` flows
         |    // implicitly via INPUT_ROW.
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
   * Per-row body. For `NullIntolerant` expressions where the entire tree propagates nulls,
   * prepends a short-circuit on the union of input ordinals so the whole `ev.code` cost is
   * skipped on null rows. Otherwise the standard shape: run `ev.code`, then `setNull` or write
   * based on `ev.isNull`.
   *
   * `subExprsCode` is the CSE helper-invocation block. It must run before `ev.code`. Inside the
   * short-circuit it lives in the else branch so null rows skip CSE too.
   */
  private def defaultBody(
      boundExpr: Expression,
      inputSchema: Seq[ArrowColumnSpec],
      ev: ExprCode,
      writeSnippet: String,
      subExprsCode: String): String = {
    boundExpr match {
      case _ if isNullIntolerant(boundExpr) && allNullIntolerant(boundExpr) =>
        // Every node from root to leaf is `NullIntolerant` or a leaf, so "any BoundReference null
        // -> whole expression null". A non-null-propagating node like `coalesce` or `if` would
        // make this incorrect (`coalesce(null, x)` is `x`); `allNullIntolerant` rejects those.
        val inputOrdinals =
          boundExpr.collect { case b: BoundReference => b.ordinal }.distinct
        // Primitive Arrow vectors are wrapped in `CometPlainVector` at input-cast time, which
        // exposes `isNullAt(int)` rather than the raw Arrow `isNull(int)`. Pick the right method
        // per ordinal so the short-circuit compiles for timestamp / int / float columns too,
        // not just VarChar / Decimal vectors that stay as raw Arrow types.
        def nullCheckCall(ord: Int): String = {
          val method = CometBatchKernelCodegenInput.nullCheckMethod(inputSchema(ord))
          s"this.col$ord.$method(i)"
        }
        val nullCheck =
          if (inputOrdinals.isEmpty) "false"
          else inputOrdinals.map(nullCheckCall).mkString(" || ")
        // `NullIntolerant` only constrains "any input null -> output null"; it does NOT promise
        // that non-null inputs always produce non-null output. `MakeTimestamp(failOnError=false)`
        // is `NullIntolerant=true` but its `doGenCode` catches `DateTimeException` for invalid
        // year/month/day/hour/min/sec components and sets `ev.isNull = true`. Honor `ev.isNull`
        // post-eval whenever the expression is nullable; skip the guard only when the root is
        // statically non-nullable (`ev.isNull` is then a literal `false`).
        if (boundExpr.nullable) {
          s"""
             |if ($nullCheck) {
             |  output.setNull(i);
             |} else {
             |  $subExprsCode
             |  ${ev.code}
             |  if (${ev.isNull}) {
             |    output.setNull(i);
             |  } else {
             |    $writeSnippet
             |  }
             |}
           """.stripMargin
        } else {
          s"""
             |if ($nullCheck) {
             |  output.setNull(i);
             |} else {
             |  $subExprsCode
             |  ${ev.code}
             |  $writeSnippet
             |}
           """.stripMargin
        }
      case _ =>
        // NonNullableOutputShortCircuit: when `nullable = false`, drop the `if (ev.isNull)`
        // guard at source level rather than relying on JIT folding.
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
   * True iff every node in the tree propagates nulls (`NullIntolerant`, `BoundReference`, or
   * `Literal`). Gates the [[defaultBody]] short-circuit, which is only correct when no node
   * (`Coalesce`, `If`, `CaseWhen`, `Concat`, ...) breaks the propagation chain.
   */
  private def allNullIntolerant(expr: Expression): Boolean =
    !expr.exists {
      case _: BoundReference | _: Literal => false
      case other => !isNullIntolerant(other)
    }

  /**
   * Per-column compile-time invariants. The concrete Arrow vector class and the nullability flag
   * are baked into the generated kernel and form part of the cache key: different vector classes
   * or nullability produce different kernels. The dispatcher hardcodes top-level `nullable=true`
   * (per-batch null density is not part of the cache key); tests reach the non-nullable codegen
   * path by constructing specs directly.
   */
  sealed trait ArrowColumnSpec {
    def vectorClass: Class[_ <: ValueVector]

    def nullable: Boolean
  }

  /** Scalar column: one Arrow vector class per row slot, no nested structure. */
  final case class ScalarColumnSpec(vectorClass: Class[_ <: ValueVector], nullable: Boolean)
      extends ArrowColumnSpec

  /**
   * Array column: an Arrow `ListVector` wrapping a child spec. `elementSparkType` lets the
   * nested-class emitter pick the right read template, and the child carries the Arrow vector
   * class. Nested arrays compose recursively.
   */
  final case class ArrayColumnSpec(
      nullable: Boolean,
      elementSparkType: DataType,
      element: ArrowColumnSpec)
      extends ArrowColumnSpec {
    override def vectorClass: Class[_ <: ValueVector] = classOf[ListVector]
  }

  /**
   * Struct column: an Arrow `StructVector` over N typed children. Each [[StructFieldSpec]]
   * carries the Spark name (cache-key identity), the Spark `DataType`, the child
   * `ArrowColumnSpec`, and the per-field `nullable` bit (lets non-nullable fields elide their
   * per-row null check).
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
   * `StructVector` with key at child 0 and value at child 1. Nested keys and values compose
   * recursively. The child specs' `nullable` field is unused on the read path. Output-side null
   * guards for map values come from `MapType.valueContainsNull` on the Spark `DataType`.
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
   * Compiled kernel handle. `freshReferences` regenerates the references array per kernel
   * allocation because `ScalaUDF` embeds stateful `ExpressionEncoder` serializers that cannot be
   * shared.
   */
  final case class CompiledKernel(factory: GeneratedClass, freshReferences: () => Array[Any]) {
    def newInstance(): CometBatchKernel =
      factory.generate(freshReferences()).asInstanceOf[CometBatchKernel]
  }

  /**
   * Output of [[generateSource]]. Tests inspect `body` to assert the shape of the generated
   * source. See `CometCodegenSourceSuite`.
   */
  final case class GeneratedSource(body: String, code: CodeAndComment, references: Array[Any])

  object ArrowColumnSpec {

    /** Convenience constructor for the scalar case. */
    def apply(vectorClass: Class[_ <: ValueVector], nullable: Boolean): ArrowColumnSpec =
      ScalarColumnSpec(vectorClass, nullable)

    /** Trait-level extractor that destructures only the scalar case. */
    def unapply(spec: ArrowColumnSpec): Option[(Class[_ <: ValueVector], Boolean)] = spec match {
      case ScalarColumnSpec(c, n) => Some((c, n))
      case _ => None
    }
  }
}
