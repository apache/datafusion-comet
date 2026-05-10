<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# JVM UDF dispatch

Comet offloads expressions that lack a native DataFusion implementation, or whose native implementation diverges from Spark's semantics, to JVM-side code that operates on Arrow batches passed through the C Data Interface. This preserves Spark compatibility on expressions that would otherwise force a whole-plan fallback to Spark. The tradeoff is a JNI roundtrip and per-batch JVM execution.

The dispatch path is **Arrow-direct codegen via `CometCodegenDispatchUDF`** - one generic dispatcher that compiles a specialized kernel per bound Spark `Expression` plus input schema. Per-expression specialized emitters inside the dispatcher cover the cases where the default `doGenCode` output pays avoidable conversions; see [Specialized emitters](#specialized-emitters) below.

The JNI bridge (`CometUdfBridge`) and proto schema (`JvmScalarUdf`) are generic enough to carry any `CometUDF` implementation, but the codebase today contains one: `CometCodegenDispatchUDF`.

## Arrow-direct codegen via `CometCodegenDispatchUDF`

One UDF class handles any scalar Spark `Expression` in the supported type surface. For each `(boundExpr, inputSchema)` pair, it compiles a specialized `CometBatchKernel` subclass via Janino that fuses Arrow input reads, expression evaluation, and Arrow output writes into one method. The kernel is cached in a JVM-wide LRU.

### Transport

At plan time the serde binds the expression tree to its leaf `AttributeReference`s, serializes the bound `Expression` via Spark's closure serializer, and emits a `JvmScalarUdf` proto whose argument 0 is a `Literal(bytes, BinaryType)` holding the serialized Expression. Arguments 1..N are the raw data columns the `BoundReference`s refer to, in ordinal order.

At execute time, `CometCodegenDispatchUDF.evaluate` reads the bytes from the `VarBinaryVector` at arg 0, computes a cache key from (bytes, per-column Arrow vector class, per-column nullability), and either reuses a cached `CompiledKernel` or compiles one on the miss path.

The self-describing proto removes the driver-side state the original prototype relied on. Cluster-mode executors deserialize and compile locally.

**Classloader caveat.** The Comet native runtime calls the UDF on a Tokio worker thread whose context classloader may not be Spark's task loader. `SparkEnv.get.closureSerializer.newInstance().deserialize[Expression](bytes)` without an explicit loader fails with `ClassNotFoundException` on Spark's expression classes. The dispatcher passes an explicit loader, falling back to the loader that loaded `Expression` if the thread context is null.

### Compilation

`CometBatchKernelCodegen.compile(boundExpr, inputSchema)` generates a Java source for a `SpecificCometBatchKernel` that:

- Extends `CometBatchKernel`, which extends `CometInternalRow`, which extends Spark's `InternalRow`. The kernel **is** the `InternalRow` that Spark's `BoundReference.genCode` reads from.
- Sets `ctx.INPUT_ROW = "row"` at compile time and aliases `InternalRow row = this;` inside `process`, so Spark's generated body calls `row.getUTF8String(ord)` which resolves to the kernel's own typed getter. The getter is final, the ordinal is constant at the call site, and JIT devirtualizes and folds the switch. `row` rather than `this` because Spark's `splitExpressions` uses INPUT_ROW as a helper-method parameter name and `this` is a reserved Java keyword.
- Carries typed input fields `col0 .. colN`, one per bound column, cast at the top of `process` from the generic `ValueVector[]` to the concrete Arrow class baked in at compile time.
- Emits `isNullAt(ordinal)` and `getUTF8String(ordinal)` overrides whose switch cases are specialized per column. A column marked non-nullable compiles to `return false;`; a `VarCharVector` compiles to a zero-copy `UTF8String.fromAddress` read against the Arrow data buffer; a `ViewVarCharVector` reads the 16-byte view entry, branches inline-vs-referenced, and builds the `UTF8String` without a `byte[]` allocation.
- Overrides `init(int partitionIndex)` with the statements collected by `ctx.addPartitionInitializationStatement`. Non-deterministic expressions (`Rand`, `Randn`, `Uuid`) register statements that reseed mutable state from `partitionIndex`; deterministic expressions leave `init` empty.
- Processes the batch in a tight loop that sets `this.rowIdx = i`, runs the expression body (either `boundExpr.genCode` for the default path or a specialized emitter), and writes to the typed output vector.

### Specialized emitters

For expressions whose `doGenCode` forces conversions that a tighter byte-oriented loop could skip, the dispatcher has per-expression overrides that emit custom Java while staying inside the framework (same cache, same bridge, same serde entry). Today that is `RegExpReplace`: the default path goes `Arrow bytes -> UTF8String -> String -> Matcher -> String -> UTF8String -> bytes -> Arrow` because `java.util.regex.Matcher` requires a `CharSequence`. The specialized emitter writes the byte-oriented shape directly (`Arrow bytes -> String -> Matcher -> String -> bytes -> Arrow`). The `UTF8String` round-trip costs measurable time on wide-match workloads; see `specializedRegExpReplaceBody` for the benchmark rationale.

Precedent for adding new specializations: match when an expression's `doGenCode` pays conversions an Arrow-aware byte-oriented loop would avoid. Keep the specialization minimal (no speculative layering beyond the conversions it exists to skip) so its value over the default path stays legible.

### Caching

Three cache layers compose at three different scopes. None is redundant: collapsing any pair would either lose correctness or pay an avoidable cost.

1. **JVM-wide compile cache.** Value is `CompiledKernel(factory: GeneratedClass, freshReferences: () => Array[Any])`, keyed by `(ByteBuffer.wrap(bytes), IndexedSeq[ArrowColumnSpec])`. Bounded LRU via `Collections.synchronizedMap(LinkedHashMap(accessOrder=true))` with `removeEldestEntry`, capacity 128. Same shape as `IcebergPlanDataInjector.commonCache` in `spark/src/main/scala/org/apache/spark/sql/comet/operators.scala`. Amortizes the Janino compile cost across every thread and every query in the JVM.

2. **Per-thread UDF instance cache.** `CometUdfBridge.INSTANCES` is a `ThreadLocal<Map<Class,CometUDF>>` that hands each task thread its own `CometCodegenDispatchUDF`. Keeps cache layer 3's instance fields safe without synchronization.

3. **Per-partition kernel instance cache.** Plain mutable fields (`activeKernel`, `activeKey`, `activePartition`) on each UDF instance, managed by `ensureKernel`. The compiled `GeneratedClass` produces a kernel instance, and the kernel carries per-row mutable state (`Rand`'s `XORShiftRandom`, `MonotonicallyIncreasingID`'s counter, `addMutableState` fields) that must advance across batches within a partition and reset across partitions. `ensureKernel` allocates a fresh kernel and calls `init(partitionIndex)` only when the partition or cache key changes; otherwise the same kernel handles every batch in the partition.

Matches Spark `WholeStageCodegenExec`: compile once per plan, instantiate per partition, init, iterate.

#### Why `freshReferences` is a closure, not a cached array

`CompiledKernel` holds a closure that regenerates `references: Array[Any]` each time a new kernel is allocated, rather than caching a single shared array. Reason: some expressions (notably `ScalaUDF`) embed stateful Spark `ExpressionEncoder` serializers into `references` via `ctx.addReferenceObj`. Those serializers reuse an internal `UnsafeRow` / `byte[]` buffer per `.apply(...)` call and are not thread-safe. If two kernels on different partitions shared one serializer instance, they would race on that buffer and return garbage.

Re-running `genCode(ctx)` per kernel allocation costs microseconds; Janino compile costs milliseconds. Caching only the expensive piece preserves correctness cheaply. A future optimization would be to distinguish expressions whose references are all immutable (most non-UDF expressions) from those that embed stateful converters, and cache the array in the immutable case; not worth the complexity today.

### Plan-time dispatchability

`CometBatchKernelCodegen.canHandle(boundExpr)` runs at serde time. It returns `None` when the dispatcher can compile the expression, `Some(reason)` when it cannot. Checks:

- Output `dataType` is in the scalar set `allocateOutput` and `outputWriter` cover.
- No `AggregateFunction` or `Generator` anywhere in the tree (scalar-only bridge).
- Every `BoundReference`'s data type is in the input set `typedInputAccessors` has a getter for.

The serde calls `withInfo(original, reason) + None` on a `Some` result, so Spark falls back rather than the kernel compiler crashing at execute time. Intermediate node types are not checked - `doGenCode` materializes them in local variables; only leaves (row reads) and the root (output write) touch Arrow.

### Observability

`CometCodegenDispatchUDF.stats()` returns `DispatcherStats(compileCount, cacheHitCount, cacheSize)`. `hitRate` is derived. `resetStats()` clears the counters (not the cache) for test isolation.

Counters are not yet surfaced anywhere user-visible. Candidates for future wiring: Spark SQL metrics on the hosting operator, a JMX MBean, a Spark accumulator, or a periodic log line.

## User-defined scalar functions (ScalaUDF)

The codegen dispatcher routes scalar `org.apache.spark.sql.catalyst.expressions.ScalaUDF` expressions through the same compile + per-partition-kernel pipeline as the regex serdes. The serde is `CometScalaUDF` in `spark/src/main/scala/org/apache/comet/serde/scalaUdf.scala`, registered in `QueryPlanSerde.miscExpressions`.

Why it works without per-UDF handling: Spark's `ScalaUDF.doGenCode` already emits compilable Java that calls the user function via `ctx.addReferenceObj`. The compile path runs `boundExpr.genCode(ctx)` and picks this up unchanged. The serialized-bytes transport carries the function reference through Spark's closure serializer, the same machinery Spark uses to ship UDFs to executors. Per-partition kernel caching handles `ScalaUDF`'s `stateful=true`.

Without this serde, any `ScalaUDF` in a plan forces Comet to fall back to Spark for the whole plan, losing acceleration on the surrounding operators. With it, scalar UDFs whose types fit the supported surface stay on the Comet path behind one JNI hop.

### What's covered

| What users write                                                | Spark expression class                                 | Route through codegen                                         |
| --------------------------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------- |
| `udf((x: T) => ...)` or `spark.udf.register` (Scala)            | `ScalaUDF`                                             | yes                                                           |
| `spark.udf.register("f", new UDF1[...]{...})` (Java)            | `ScalaUDF` (Spark wraps the Java functional interface) | yes, transparently                                            |
| `CREATE FUNCTION foo AS 'com.example.MyUDF'` (SQL registration) | `ScalaUDF`                                             | yes, if the user class is reachable on the executor classpath |

### What's not covered

| What users write                | Spark expression class                                                            | Why not                                                                              |
| ------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| Aggregate UDF                   | `ScalaAggregator`, `TypedImperativeAggregate`, old `UserDefinedAggregateFunction` | accumulator-based; needs a different bridge contract (accumulate + merge + finalize) |
| Table UDF / generator           | `UserDefinedTableFunction`                                                        | 1 row -> N rows; `canHandle` rejects `Generator`                                     |
| Python `@udf`                   | `PythonUDF`                                                                       | subprocess runtime, not JVM                                                          |
| Pandas `@pandas_udf`            | `PandasUDF`                                                                       | Arrow-via-subprocess runtime                                                         |
| Hive `GenericUDF` / `SimpleUDF` | `HiveGenericUDF` / `HiveSimpleUDF`                                                | separate expression classes; would need their own serde                              |

### Constraints within the ScalaUDF path

- Input and output types must be in the supported surface (see [Type surface](#type-surface)). Nested types (`Struct`, `Array`, `Map`) are supported when their element types are supported.
- The user function must be closure-serializable. This is Spark's own requirement; a function that works with Spark's executor execution works here.
- User functions that touch `TaskContext` internals, accumulators, or broadcast variables in unusual ways may misbehave; the common case works.
- Stateful behavior: per-partition kernel caching resets kernel instance state on partition boundary, matching the contract most user UDFs assume (and matching Spark's own re-instantiation on some paths). UDFs that rely on long-lived JVM-wide state across partitions in the same executor see that state reset more often than before, which is rare and usually a latent bug in the UDF.

### Mode knob interaction

`spark.comet.exec.codegenDispatch.mode` controls routing:

- `auto` (default) and `force`: ScalaUDFs go through the codegen dispatcher.
- `disabled`: `CometScalaUDF.convert` returns `None` and the plan falls back to Spark.

There is no non-codegen Comet path for arbitrary user functions.

## Type surface

### Input (kernel getters)

All scalar Spark types that map to a single Arrow vector:

| Spark type                                | Arrow vector class                                         | `InternalRow` getter                                     |
| ----------------------------------------- | ---------------------------------------------------------- | -------------------------------------------------------- |
| BooleanType                               | BitVector                                                  | `getBoolean`                                             |
| ByteType                                  | TinyIntVector                                              | `getByte`                                                |
| ShortType                                 | SmallIntVector                                             | `getShort`                                               |
| IntegerType, DateType                     | IntVector, DateDayVector                                   | `getInt`                                                 |
| LongType, TimestampType, TimestampNTZType | BigIntVector, TimeStampMicroVector, TimeStampMicroTZVector | `getLong`                                                |
| FloatType                                 | Float4Vector                                               | `getFloat`                                               |
| DoubleType                                | Float8Vector                                               | `getDouble`                                              |
| DecimalType                               | DecimalVector                                              | `getDecimal(ord, precision, scale)`                      |
| StringType                                | VarCharVector, ViewVarCharVector                           | `getUTF8String` (zero-copy via `UTF8String.fromAddress`) |
| BinaryType                                | VarBinaryVector, ViewVarBinaryVector                       | `getBinary` (allocates `byte[]`)                         |

Widening: add cases to `CometBatchKernelCodegen.typedInputAccessors` and accept the new vector classes in `CometCodegenDispatchUDF.evaluate`'s input pattern match.

### Output (writers + allocators)

All scalar Spark types that map to a single Arrow vector: `Boolean`, `Byte`, `Short`, `Int`, `Long`, `Float`, `Double`, `Decimal`, `String`, `Binary`, `Date`, `Timestamp`, `TimestampNTZ`. Mirrors `ArrowWriters.createFieldWriter` so producer and consumer sides stay aligned. Widen by adding cases to `CometBatchKernelCodegen.allocateOutput` and `outputWriter`.

### Complex types

`ArrayType`, `StructType`, and `MapType` are supported as both input and output, including arbitrary nesting (`Array<Array<...>>`, `Array<Struct>`, `Struct<Map>`, `Map<Struct, Array>`, and so on). Each side of the pipeline handles them through recursion over the `ArrowColumnSpec` tree, with a path-suffix naming convention for the emitted fields and nested classes: `_e` for array element, `_f${fi}` for struct field, `_k` / `_v` for map key / value. N-deep nesting falls out of this because every level only knows about its immediate children.

Output side (`CometBatchKernelCodegenOutput.emitWrite`):

- `ArrayType` emits a `ListVector.startNewValue` / per-element loop / `endValue` triple; the per-element write recurses through `emitWrite` on the list's child vector.
- `StructType` casts each typed child vector once per row, writes each field via one recursive `emitWrite` call per field, and skips the `isNullAt` guard on non-nullable fields.
- `MapType` casts the entries `StructVector` once per row, writes each key / value pair with a per-value null guard (keys are non-nullable per Arrow invariant), and brackets with `startNewValue` / `endValue`.
- `allocateOutput` builds the complex `FieldVector` tree and recursively allocates child buffers, pre-sized from the input data-buffer estimate where applicable.

Input side (`CometBatchKernelCodegenInput`):

- Each complex input column produces a final nested class at every level: `InputArray_${path}` extends `CometArrayData`, `InputStruct_${path}` extends `CometInternalRow`, `InputMap_${path}` extends `CometMapData`. The class holds slice state (arrays / maps: `(startIndex, length)`; structs: `rowIdx`) and pre-allocated child-view instances for any complex child. Spark's generated `row.getArray(ord)` / `row.getStruct(ord, n)` / `row.getMap(ord)` resolves to the kernel's switch which resets and returns the pre-allocated instance.
- Scalar element reads go through the typed child-vector field with zero allocation: `UTF8String.fromAddress` for strings, the decimal128 short-precision fast path for `DecimalType(p <= 18)`, primitive direct reads for everything else.

### Out of scope

- Calendar interval types.
- Aggregates, window functions, generators - these need a different bridge signature than `CometUDF.evaluate`.

## Regex family routing

Regex serdes (`rlike`, `regexp_replace`, `regexp_extract`, `regexp_extract_all`, `regexp_instr`, `split` via `StringSplit`) route to codegen dispatch in the default `auto` mode when `spark.comet.exec.regexp.engine=java` (itself the default). Set `spark.comet.exec.codegenDispatch.mode=disabled` to fall back to Spark; set `mode=force` to prefer codegen regardless of the regex engine.

#### Routing matrix

Rows are the six regex-family expressions; columns are `(spark.comet.exec.regexp.engine, spark.comet.exec.codegenDispatch.mode)`. Cells name the path the serde takes. `Spark` means `convert` returns `None` and Spark executes the expression; `codegen` means the generated Janino kernel via `CometCodegenDispatchUDF`; `native Rust` means the DataFusion scalar function.

| Expression              | java, auto | java, force | java, disabled | rust, auto  | rust, force | rust, disabled |
| ----------------------- | ---------- | ----------- | -------------- | ----------- | ----------- | -------------- |
| `rlike`                 | codegen    | codegen     | Spark          | native Rust | codegen     | native Rust    |
| `regexp_replace`        | codegen    | codegen     | Spark          | native Rust | codegen     | native Rust    |
| `regexp_extract`        | codegen    | codegen     | Spark          | Spark       | Spark       | Spark          |
| `regexp_extract_all`    | codegen    | codegen     | Spark          | Spark       | Spark       | Spark          |
| `regexp_instr`          | codegen    | codegen     | Spark          | Spark       | Spark       | Spark          |
| `split` (`StringSplit`) | codegen    | codegen     | Spark          | native Rust | codegen     | native Rust    |

Notes:

- `force` always tries codegen first and only falls back to the non-codegen path if `canHandle` rejects the bound expression. For `rlike` / `regexp_replace` / `StringSplit` with `rust` engine, that fallback is native Rust. The matrix collapses to the common outcome.
- `auto` with the rust engine does not prefer codegen (it would bypass the native Rust path the user explicitly selected), so the `rust, auto` column matches `rust, disabled`.
- `regexp_extract` / `regexp_extract_all` / `regexp_instr` have no native Rust path; `getSupportLevel` declares them unsupported when engine is rust, so the cells read `Spark` regardless of dispatch mode.
- The rust-engine cells also depend on `spark.comet.expr.allow.incompat`: when `false` (default), the incompatibility listed in `getIncompatibleReasons` vetoes the cell and Spark executes the expression. The matrix describes what happens once the expression reaches `convert`.

## Opting a new expression into codegen dispatch

Adding a new Spark expression to the codegen dispatch path is a serde-only change when its input and output types are already in [Type surface](#type-surface). The pattern mirrors the regex-family serdes in `strings.scala` and the `ScalaUDF` serde in `scalaUdf.scala`.

Steps:

1. **Verify type coverage.** `CometBatchKernelCodegen.canHandle(boundExpr)` returns `None` iff every `BoundReference`'s data type is in `isSupportedInputType` and the root data type is in `isSupportedOutputType`. No extra work needed if the expression uses supported types; if not, widen the relevant case in `typedInputAccessors` / `emitWrite` / `allocateOutput` first.

2. **Wrap `convert` in `pickWithMode`.** The serde's `override def convert(...)` routes through `CodegenDispatchSerdeHelpers.pickWithMode(viaCodegen, viaNonCodegen, preferCodegenInAuto)`. `viaCodegen` is the new helper (step 3). `viaNonCodegen` is either an existing native-DataFusion converter or `() => None` when the only Comet-side path is codegen. `preferCodegenInAuto` decides whether `auto` mode tries codegen first; set `true` when codegen is the intended primary path, `false` when the native path takes priority and codegen is a fallback.

3. **Add the codegen helper.** `private def convertViaJvmUdfGenericCodegen(expr, inputs, binding): Option[Expr]`. Structure (same for every adoption):
   - Any per-expression preconditions (literal-pattern check, offset check, etc.) that `canHandle` does not express. Return `None` with `withInfo` on failure so planning falls back cleanly.
   - `val attrs = expr.collect { case a: AttributeReference => a }.distinct` - the bound tree's input columns in ordinal order.
   - `val boundExpr = BindReferences.bindReference(expr, AttributeSeq(attrs))` - binds `AttributeReference` leaves to `BoundReference(ord, dt, nullable)`.
   - `CodegenDispatchSerdeHelpers.serializedExpressionArg(expr, boundExpr, inputs, binding)` - gates on `canHandle`, serializes via Spark's closure serializer, wraps as a `Literal(bytes, BinaryType)` proto arg. Returns `None` and emits `withInfo` when `canHandle` rejects, so callers just `.getOrElse(return None)`.
   - `val dataArgs = attrs.map(a => exprToProtoInternal(a, inputs, binding).getOrElse(return None))` - the raw data columns.
   - `val returnType = serializeDataType(expr.dataType).getOrElse(return None)` - the expression's Spark output type.
   - Build a `JvmScalarUdf` proto with `setClassName(classOf[CometCodegenDispatchUDF].getName)`, `addArgs(exprArg)` followed by `dataArgs.foreach(addArgs)`, `setReturnType`, `setReturnNullable(expr.nullable)`. Wrap in `ExprOuterClass.Expr` and return `Some(...)`.

4. **Decide non-codegen routing.** Three cases in practice:
   - Native DataFusion path exists (e.g. `regexp_replace` with `engine=rust`): keep the existing `convertViaNativeRegex`/equivalent and have `viaNonCodegen` call it.
   - No native path, but there's a meaningful non-codegen alternative: write that converter (rare; only `RLike` was this case historically, now removed).
   - No alternative: `viaNonCodegen = () => None`, and `mode=disabled` falls through to Spark.

5. **Tests.** Add a smoke test in `CometCodegenDispatchSmokeSuite` using `assertCodegenDidWork` around a `checkSparkAnswerAndOperator`, plus `assertKernelSignaturePresent(Seq(classOf[...Vector]), OutputType)` to prove specialization reached the cache. If the expression has a new code path in `emitWrite` or `typedInputAccessors`, also add a source-level marker assertion in `CometCodegenSourceSuite` so future regressions don't silently lose the optimization.

Once wired, the `auto | force | disabled` mode knob applies automatically and users can disable codegen per-session via `spark.comet.exec.codegenDispatch.mode`.

## Optimizations

Every optimization is compile-time specialized on `(bound expression, input schema)`; the emitted Java carries only the selected path at each site. Source-level tests in `CometCodegenSourceSuite` assert that each of these activates where expected.

### Input readers (`CometBatchKernelCodegenInput.typedInputAccessors` and the nested-class emitters)

- **ZeroCopyUtf8Read** for `VarCharVector` / `ViewVarCharVector`. `UTF8String.fromAddress` wraps Arrow's data-buffer address with no `byte[]` allocation. The view case reads the 16-byte view entry, picks inline vs referenced inline, and builds the `UTF8String` without a `byte[]` allocation either.
- **NonNullableIsNullAtElision** for non-nullable columns. `isNullAt(ord)` returns literal `false`, and `CometCodegenDispatchUDF.rewriteBoundReferences` tightens the `BoundReference.nullable` flag so Spark's `doGenCode` stops probing at source level too (not just at JIT time).
- **DecimalInputShortFastPath** for `DecimalType(p, _)` with `p <= 18`. Reads the low 8 bytes of the decimal128 slot as a signed long and wraps with `Decimal.createUnsafe`. The slow path (`getObject` + `Decimal.apply`) is emitted only for `p > 18`.

### Output writers (`CometBatchKernelCodegenOutput`)

- **DecimalOutputShortFastPath** for `DecimalType(p, _)` with `p <= 18`. Passes `Decimal.toUnscaledLong` to `DecimalVector.setSafe(int, long)`. Slow path via `toJavaBigDecimal()` is emitted only for `p > 18`.
- **Utf8OutputOnHeapShortcut** for `StringType`. When the `UTF8String` base is a `byte[]`, passes it directly to `VarCharVector.setSafe(int, byte[], int, int)` and skips the redundant `getBytes()` allocation. Off-heap fallback retains `getBytes()`.
- **PreSizedOutputBuffer** for variable-length output types. The caller passes an input-size-derived byte estimate to avoid `setSafe` reallocations mid-loop.

### Kernel shape (`defaultBody` / `generateSource`)

- **NullIntolerantShortCircuit**. Expression trees where every node is `NullIntolerant` or a leaf get a pre-body null check over the union of input ordinals; null rows skip both CSE evaluation and the main expression body. Correct only when every path from a leaf to the root propagates nulls; breaking the chain with `Coalesce` / `If` / `CaseWhen` / `Concat` falls through to the default branch which runs Spark's own null-aware `ev.code`.
- **NonNullableOutputShortCircuit**. Bound expressions with `nullable == false` drop the `if (ev.isNull) setNull` guard and write unconditionally at source level rather than depending on JIT constant-folding.
- **SubexpressionElimination** (when `spark.sql.subexpressionEliminationEnabled`). Common subtrees become helper methods writing into `addMutableState` fields. Class-field variant for the reason given in [Subexpression elimination (CSE)](#subexpression-elimination-cse) below.

### Per-expression specializers

- **RegExpReplaceSpecialized** for `RegExpReplace` with a direct `BoundReference` subject, foldable non-null pattern and replacement, and `pos == 1`. Emits `byte[] -> String -> Matcher -> String -> byte[]` directly, bypassing the `UTF8String` round-trip that default `doGenCode` forces. `java.util.regex.Matcher` requires a `CharSequence`, so the default path materializes a Java `String` from the input `UTF8String`, runs the matcher, then encodes back to `UTF8String`. The round-trip cost is measurable on wide-match workloads; see `specializedRegExpReplaceBody` for the benchmark rationale.

The general rule for adding a new specialization: specialize when an expression's `doGenCode` pays conversions that an Arrow-aware byte-oriented implementation can skip. The common case is expressions that require a Java `String` (`java.util.regex`, some `DateTimeFormatter` expressions). Keep specializations minimal so comparisons stay honest.

## Subexpression elimination (CSE)

CSE hoists repeated subtrees into a single evaluation per row. Spark exposes two entry points:

- `subexpressionElimination` (via `ctx.generateExpressions(..., doSubexpressionElimination = true)` + `ctx.subexprFunctionsCode`). Each common subexpression becomes a helper method that writes its result into class-level mutable state allocated via `addMutableState`. The main expression's `genCode` references those class fields. This is what `GeneratePredicate`, `GenerateMutableProjection`, and `GenerateUnsafeProjection` use.
- `subexpressionEliminationForWholeStageCodegen`. CSE results live in local variables declared in the caller's scope, and the main expression's `genCode` references those locals. Only safe when no helper method gets extracted between the locals' declaration site and their use.

We use the **class-field** variant. The WSCG variant does not work in our shape without additional setup: Spark's arithmetic, string, and decimal expressions internally call `splitExpressionsWithCurrentInputs`, which splits into helper methods unless `currentVars` is non-null. In our kernel `currentVars` is null (we read from a row, not from materialized locals), so those splits fire and the helper bodies cannot see CSE-declared locals in the outer scope. The class-field variant sidesteps this because helper methods can read class fields freely.

### Future WSCG-variant exploration

Making the WSCG variant usable would require:

- Setting `ctx.currentVars = Seq.fill(numInputs)(null)` before CSE. `BoundReference.genCode` checks `currentVars != null && currentVars(ord) != null`, so an all-null `currentVars` lets reads fall through to the `INPUT_ROW` path (what we want) while `splitExpressionsWithCurrentInputs` sees `currentVars != null` and declines to split.
- Verifying that direct `ctx.splitExpressions` calls (not the `-WithCurrentInputs` wrapper) in a handful of expressions (`hash`, `Cast`, `collectionOperations`, `ToStringBase`) remain self-contained. They pass explicit args to their split helpers, so they should be fine, but that is a per-expression audit.
- Benchmarking. The potential win is that CSE state lives in local variables rather than class fields, so HotSpot has more freedom to keep values in registers. Whether that wins over the class-field variant is unclear; CSE state is written once and read two or more times per row, and the expression work usually dominates. Not worth doing until a profile shows class-field access on the hot path.
- If the kernel ever gets integrated into Spark's `WholeStageCodegenExec` pipeline (rather than standing alone), the WSCG variant becomes the natural fit and this revisit is forced. Until then, the standalone-kernel shape matches Predicate/Projection/UnsafeRow generators, which use class-field CSE.

## Open items

Each item below has a `TODO` in the code at the referenced location. The code-side comment is a short pointer; this section carries the rationale.

### Dictionary-encoded inputs

`CometCodegenDispatchUDF.evaluate` (near the top). Comet's native scan and shuffle paths currently materialize dictionaries before the UDF bridge, so `v.getField.getDictionary != null` is not observed here today. If that invariant is ever relaxed upstream, the cast in `specFor` throws. Two ways to fix it at that point:

- Materialize at the dispatcher via `CDataDictionaryProvider` (see `NativeUtil.importVector`). Simpler.
- Widen `typedInputAccessors` with a dict-index read plus a lookup into the dictionary vector. Faster on high-cardinality dictionaries but adds a cache-key dimension.

### Cache-key hash cost

`CometCodegenDispatchUDF.CacheKey`. `hashCode` walks `bytesKey` once per batch (`equals` again on hash collision). For small expressions (a few KB) this is single-digit microseconds and invisible; for large `ScalaUDF` closures with heavy encoders (tens to hundreds of KB) it could climb to tens of microseconds per batch. If a workload shows this on a profile, three alternatives worth exploring:

1. Driver-side precomputed hash piggybacked through the Arrow transport as a small tag (e.g. 8 bytes). Executor uses the tag directly as the key. O(1) per batch, and the tag is tiny versus the full byte array.
2. Per-UDF-instance byte-identity fast path. `CometCodegenDispatchUDF` is per-thread; the expression is invariant for the life of one task. Memoize the last-seen `(Arrow data buffer address, offset, length)` tuple and skip the HashMap entirely when it matches.
3. Two-level cache with source-string outer tier. Keep bytes-based L1 as today; add an L2 keyed on `generateSource(expr).code.body` that stores only the Janino-compiled class. Captures the "same lambda, different closure identity" cross-query reuse case (e.g. the same `udf((i: Int) => i + 1)` registered across sessions produces identical source but different serialized bytes).

None of these are worth doing until a profile shows lookup in the hot path.

### Unsafe readers skipping Arrow bounds checks

`CometBatchKernelCodegenInput.typedInputAccessors`. Primitive getters go through Arrow's typed `v.get(i)` which performs bounds checks. Inside the kernel's `process` loop `i` is always in `[0, numRows)`, so the check is redundant. Mirror `CometPlainVector`'s pattern (cache validity/value/offset buffer addresses, use direct `Platform.getInt` reads) behind a benchmark.

### Per-row-body method-size splitting

`CometBatchKernelCodegen.generateSource`. The per-row body lives inline inside `process`'s for-loop and is not split. Individual `doGenCode` implementations (`Concat`, `Cast`, `CaseWhen`) call `ctx.splitExpressionsWithCurrentInputs` internally, but the outer per-row body itself is never split. A sufficiently deep composed expression (multi-level ScalaUDF with heavy encoder converters per level) can push `process` past Janino's 64 KB method size limit, at which point compile fails. Mitigation when that ceiling is hit: wrap `perRowBody` in `ctx.splitExpressionsWithCurrentInputs(Seq(perRowBody), funcName = "evalRow", arguments = Seq(...))`. The `row`-as-`this` alias we install in `process` already covers that path. Skipped speculatively because today's workloads sit comfortably below the threshold and splitting unconditionally adds a function-call frame per row for the common case.

### Hoist per-row child casts for complex output

`CometBatchKernelCodegenOutput.emitWrite` for `StructType` and `MapType`. Each per-row body currently re-casts `output` to its concrete vector class and calls `getChildByOrdinal(fi)` + cast for every child on every row. For a struct with N fields and a batch of M rows, that is N*M `ArrayList.get` + `checkcast` pairs; the individual calls are cheap, but for wide structs it is visible. Hoist the outer cast plus the per-field child casts to locals declared above the `for` loop (the output vector is stable for the batch), then reference the hoisted locals inside the per-row body. Same change applies to `MapType` (`mapVar`, `entriesVar`, `keyVar`, `valVar`).

## Known behavioral limitations

- **`regexp_replace` on a collated subject** rejects at plan time: Spark wraps the pattern in `Collate(Literal, ...)` and the current `RegExpReplace` serde requires a bare `Literal`. Serde-side unwrap would unblock this.
- **`rlike` on ICU collations** (`UNICODE_CI` etc.) is a type mismatch in Spark itself (RLike contracts on `UTF8_BINARY`), not a Comet limitation. Binary collations like `UTF8_LCASE` work.
- **Observability sink**. `CometCodegenDispatchUDF.stats()` and `snapshotCompiledSignatures()` are test-facing; not yet wired to Spark SQL metrics, JMX, or periodic logging.
- **DataFusion alignment gaps in the bridge contract**:
  - `arg_fields` - already covered by `ValueVector.getField()` on the JVM side.
  - `return_field` - dispatcher derives it via `boundExpr.dataType`.
  - `config_options` - session-level state like timezone / locale. Not plumbed across JNI. Would matter for TZ-aware or locale-sensitive UDFs.
  - `ColumnarValue::Scalar` return - DataFusion lets a scalar function return one value broadcast to batch length. Arrow Java has no `ScalarValue` equivalent; adding it would need a new JVM wrapper type plus an FFI protocol extension. Small practical payoff (most UDFs produce row-varying output; true constants are folded at plan time), large surface change.
- **Benchmark observation** (`CometScalaUDFCompositionBenchmark`). On plans of shape `Scan -> Project[UDF] -> noop` or `Scan -> Project[UDF] -> SUM`, the dispatcher runs a few percent slower than "dispatcher disabled" (Spark row-based fallback) at 1M rows. Both paths do the same per-row work in the JVM and our path pays an extra JNI hop. The benefit is keeping the surrounding plan columnar when downstream operators would otherwise fall back, a shape the current benchmark does not exercise. A follow-up benchmark with expensive columnar operators around the UDF (filter + hash join + aggregate) would measure the plan-preservation effect.
- **Candidates for specialized emitters beyond `RegExpReplace`**. Other regex-family expressions (`regexp_extract`, `regexp_extract_all`, `regexp_instr`) pay the same `UTF8String <-> String` conversion chain Spark's `doGenCode` forces. `str_to_map` is another candidate. Audit pending.
- **Longer-term: full `WholeStageCodegenExec` integration**. Build a Spark plan tree (`ArrowOutputExec(ProjectExec(ColumnarToRowExec(BatchInputExec)))`) and let Spark's WSCG fuse everything through its own codegen machinery, reusing `CometVector` on the input side. Larger engineering footprint (custom `CodegenSupport` sink, plan construction inside JNI callbacks) but unlocks nested types and every Arrow input type without Comet-side accessor maintenance.

## File map

- `common/src/main/scala/org/apache/comet/udf/CometCodegenDispatchUDF.scala` - dispatcher `CometUDF`, shared LRU, counters, `snapshotCompiledSignatures()`.
- `common/src/main/scala/org/apache/comet/udf/CometBatchKernelCodegen.scala` - Janino-based kernel compiler, `canHandle`, `allocateOutput`, `outputWriter`, `typedInputAccessors`, `CompiledKernel` with `freshReferences` closure.
- `common/src/main/scala/org/apache/comet/udf/CometInternalRow.scala` - abstract `InternalRow` base with throwing defaults for unimplemented getters.
- `common/src/main/scala/org/apache/comet/udf/CometUDF.scala` - `CometUDF.evaluate(inputs, numRows)` contract.
- `common/src/main/java/org/apache/comet/udf/CometBatchKernel.java` - Java abstract base the generated subclass extends.
- `common/src/main/java/org/apache/comet/udf/CometUdfBridge.java` - JNI entry point; plumbs `numRows` through.
- `native/jni-bridge/src/comet_udf_bridge.rs` - JNI method ID lookup for `CometUdfBridge.evaluate`.
- `native/spark-expr/src/jvm_udf/mod.rs` - Rust-side `JvmScalarUdfExpr` calling the JVM bridge.
- `spark/src/main/scala/org/apache/comet/serde/strings.scala` - rlike / regexp_replace / regexp_extract / regexp_extract_all / regexp_instr / string_split serdes, `CodegenDispatchSerdeHelpers` (`canHandle` + serialization).
- `spark/src/main/scala/org/apache/comet/serde/scalaUdf.scala` - `ScalaUDF` serde routing user UDFs through the dispatcher.
- `spark/src/test/scala/org/apache/comet/CometCodegenDispatchSmokeSuite.scala` - smoke tests: mode knob, composition, `ScalaUDF`, type-surface, zero-column, signature assertions.
- `spark/src/test/scala/org/apache/comet/CometCodegenDispatchFuzzSuite.scala` - randomized string fuzz across null densities and a fixed regex pattern set.
- `spark/src/test/scala/org/apache/spark/sql/benchmark/CometScalaUDFCompositionBenchmark.scala` - benchmark comparing Spark, Comet native built-ins, dispatcher-disabled fallback, and codegen dispatch for composed `ScalaUDF` trees.
