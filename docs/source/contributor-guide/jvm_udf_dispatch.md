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
- Sets `ctx.INPUT_ROW = "this"` at compile time, so Spark's generated body calls `this.getUTF8String(ord)` on the kernel itself. The getter is final, the ordinal is constant at the call site, and JIT devirtualizes and folds the switch.
- Carries typed input fields `col0 .. colN`, one per bound column, cast at the top of `process` from the generic `ValueVector[]` to the concrete Arrow class baked in at compile time.
- Emits `isNullAt(ordinal)` and `getUTF8String(ordinal)` overrides whose switch cases are specialized per column. A column marked non-nullable compiles to `return false;`; a `VarCharVector` compiles to a zero-copy `UTF8String.fromAddress` read against the Arrow data buffer; a `ViewVarCharVector` reads the 16-byte view entry, branches inline-vs-referenced, and builds the `UTF8String` without a `byte[]` allocation.
- Overrides `init(int partitionIndex)` with the statements collected by `ctx.addPartitionInitializationStatement`. Non-deterministic expressions (`Rand`, `Randn`, `Uuid`) register statements that reseed mutable state from `partitionIndex`; deterministic expressions leave `init` empty.
- Processes the batch in a tight loop that sets `this.rowIdx = i`, runs the expression body (either `boundExpr.genCode` for the default path or a specialized emitter), and writes to the typed output vector.

### Specialized emitters

For expressions whose `doGenCode` forces conversions that a tighter byte-oriented loop could skip, the dispatcher has per-expression overrides that emit custom Java while staying inside the framework (same cache, same bridge, same serde entry). Today that is `RegExpReplace`: the default path would go `Arrow bytes → UTF8String → String → Matcher → String → UTF8String → bytes → Arrow` because `java.util.regex.Matcher` requires a `CharSequence`. The specialized emitter writes the byte-oriented shape directly (`Arrow bytes → String → Matcher → String → bytes → Arrow`), closing a ~44% gap measured on a wide-match benchmark pattern.

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

Why it works with zero special handling: Spark's `ScalaUDF.doGenCode` already emits compilable Java that calls the user function via `ctx.addReferenceObj`. Our compile path runs `boundExpr.genCode(ctx)` and picks this up for free. The serialized-bytes transport carries the function reference through Spark's closure serializer, which is the same machinery Spark uses to ship UDFs to executors today. Per-partition kernel caching handles `ScalaUDF`'s `stateful=true`.

Before this serde, any `ScalaUDF` in a plan forced Comet to fall back to Spark in full, losing acceleration on the surrounding operators. Now, scalar UDFs whose types fit the supported surface stay on the Comet path and replace row-by-row interpreted evaluation with batch-processed JVM execution behind one JNI hop.

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
| Table UDF / generator           | `UserDefinedTableFunction`                                                        | 1 row → N rows; `canHandle` rejects `Generator`                                      |
| Python `@udf`                   | `PythonUDF`                                                                       | subprocess runtime, not JVM                                                          |
| Pandas `@pandas_udf`            | `PandasUDF`                                                                       | Arrow-via-subprocess runtime                                                         |
| Hive `GenericUDF` / `SimpleUDF` | `HiveGenericUDF` / `HiveSimpleUDF`                                                | separate expression classes; would need their own serde                              |

### Constraints within the ScalaUDF path

- Input and output types must be in the supported scalar surface (see [Type surface](#type-surface)). Nested-typed arguments (`Struct`, `Array`, `Map`) fall through at `canHandle`.
- The user function must be closure-serializable. This is Spark's own requirement; the same function that works with Spark's executor execution works here.
- User functions that touch `TaskContext` internals, accumulators, or broadcast variables in unusual ways may misbehave. Most don't.
- Stateful behavior: our per-partition kernel caching resets kernel instance state on partition boundary, matching the contract most user UDFs assume (and matching Spark's own re-instantiation on some paths). UDFs that rely on long-lived JVM-wide state across partitions in the same executor would see that state reset more often than before - rare and usually a latent bug in the UDF, not a regression from our path.

### Mode knob interaction

`spark.comet.exec.codegenDispatch.mode` controls routing:

- `auto` (default) and `force`: ScalaUDFs go through the codegen dispatcher.
- `disabled`: `CometScalaUDF.convert` returns `None`, so the plan falls back to Spark. This is the "turn this feature off" escape hatch.

There is no non-codegen fallback for arbitrary user functions; codegen dispatch is the only Comet path that can accept them.

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

`ArrayType` is supported as both input and output, including nested `Array<Array<...>>` by recursion. The shape on each side:

- Output: `emitWrite`'s `ArrayType` case emits a `ListVector.startNewValue` / per-element loop / `endValue` triple; each element write recurses through `emitWrite` on the list's child vector. `allocateOutput` builds the `ListVector` with its inner typed data vector pre-allocated from the input's data-buffer size estimate.
- Input: the kernel emits one `InputArray_colN` final class per array-typed input column, extending `CometArrayData`. The class holds `(startIndex, length)` state reset per row from the outer `ListVector`'s offsets; element reads go through the typed child-vector field with zero allocation (`UTF8String.fromAddress` for string elements, the decimal128 short-precision fast path for `DecimalType(p <= 18)`, primitive direct for others). Spark's generated `row.getArray(ord)` resolves to the kernel's `getArray` switch which resets and returns the pre-allocated instance.

`MapType` and `StructType` will plug into the same recursion: `ArrowColumnSpec` is a sealed trait with an `element: ArrowColumnSpec` field on each complex subclass, so N-deep nesting (`Array<Map<String, Array<Int>>>`) compiles by construction once the Map / Struct emitter cases land. Map key types and struct field ordinals are captured in the spec tree alongside the Spark `DataType`, so the nested-class emitters will get the right getter template per level.

### Out of scope

- `MapType` and `StructType` (planned; see above).
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

## Known limitations and future work

### Resolved in this branch

- **Per-batch nullability detection** is now `v.getNullCount != 0` (was conservatively `true`). Kernels for all-non-null batches compile with `isNullAt` returning `false`, and Spark's `BoundReference.genCode` skips the `isNull` branch at source level. The cache key includes nullability so a later nulls-present batch does not hit a nulls-absent compile.
- **Zero-column references** (e.g. `SELECT nondUuid() FROM t` where `nondUuid` is a zero-arg non-deterministic ScalaUDF) now work via an explicit `numRows: Int` parameter on `CometUDF.evaluate`, plumbed through the JNI bridge. Mirrors DataFusion's `ScalarFunctionArgs.number_rows`; lets UDFs know the batch size even when every arg is a scalar literal.
- **`ScalaUDF` routing** covers user-registered Scala/Java UDFs, SQL-registered UDFs, and UDFs composed with other expressions. Type surface includes all scalar Spark primitives plus `StringType` and `BinaryType`. See the ScalaUDF section above.

### Open

- **Dictionary-encoded inputs** are not handled. Comet's native scan and shuffle paths materialize dictionaries before reaching the UDF bridge, so this is not a current failure mode. If the invariant changes upstream, the fix is to materialize at the dispatcher boundary via `CDataDictionaryProvider` (see `NativeUtil.importVector`) or to specialize kernels on dict encoding as a cache-key dimension. A TODO captures this in `CometCodegenDispatchUDF.evaluate`.
- **Mode knob coverage.** `spark.comet.exec.codegenDispatch.mode = auto | disabled | force` is wired into the rlike, regexp_replace, and `ScalaUDF` serdes via `CodegenDispatchSerdeHelpers.pickWithMode`. Other serdes that might benefit from codegen dispatch (once their expression surface expands) should adopt the same pattern.
- **Cross-type fuzz suite.** `CometCodegenDispatchFuzzSuite` exercises rlike and regexp_replace against randomized string inputs at varying null densities. Type-surface coverage is otherwise by the end-to-end `ScalaUDF` smoke tests (primitives + string + binary through SQL). Broader randomized coverage across primitive types and multi-column expressions could land if needed.
- **Observability sink.** `CometCodegenDispatchUDF.stats()` exposes compile / hit / size counters; `snapshotCompiledSignatures()` exposes the per-kernel `(input vector classes, output DataType)` tuples for test assertions. Neither is wired to Spark SQL metrics, JMX, or a periodic log line.
- **DataFusion alignment gaps** in the bridge contract (items we audited but deferred):
  - `arg_fields` (per-arg field metadata) - already covered by `ValueVector.getField()` on the JVM side.
  - `return_field` - the dispatcher derives it via `boundExpr.dataType`.
  - `config_options` - session-level state like timezone / locale. Not currently plumbed across JNI. Would matter for TZ-aware or locale-sensitive UDFs.
  - `ColumnarValue::Scalar` return - DataFusion lets a scalar function return one value broadcast to batch length. Arrow Java has no `ScalarValue` equivalent; adding it would need a new JVM wrapper type plus an FFI protocol extension for "is scalar". Small practical payoff (most UDFs produce row-varying output; true constants are folded at plan time), large surface change. Not planned unless a concrete use case surfaces.
- **Benchmark observation (`CometScalaUDFCompositionBenchmark`).** On plans of shape `Scan → Project[UDF] → noop` or `Scan → Project[UDF] → SUM`, the dispatcher runs ~5-10% slower than "dispatcher disabled" (Spark row-based fallback) at 1M rows. Root cause: on these shapes both paths do the same per-row work in the JVM (Spark's mature `ScalaUDF.doGenCode` output inside our fused loop vs. Spark's own C2R + Project), and our path pays an extra JNI hop. The value proposition is keeping the surrounding plan columnar when downstream operators would otherwise fall back - a shape not captured by the current benchmark. Would be worth a follow-up benchmark with expensive columnar operators around the UDF (filter + hash join + aggregate) to measure the plan-preservation win.
- **Candidates for specialized emitters beyond `RegExpReplace`.** `RegExpReplace` has a specialized emitter that avoids the `Arrow bytes → UTF8String → String → Matcher → String → UTF8String → bytes → Arrow` conversion chain Spark's `doGenCode` forces. Other expressions whose `doGenCode` pays conversions a tighter byte-oriented loop would avoid (notably the rest of the regex family: `regexp_extract`, `regexp_extract_all`, `regexp_instr`, `str_to_map`) may deserve the same treatment. Audit pending.
- **Longer-term: full `WholeStageCodegenExec` integration.** Build a Spark plan tree (`ArrowOutputExec(ProjectExec(ColumnarToRowExec(BatchInputExec)))`) and let Spark's WSCG fuse everything through its own codegen machinery, reusing `CometVector` on the input side. Larger engineering footprint (custom `CodegenSupport` sink, plan construction inside JNI callbacks) but unlocks nested types and every Arrow input type without Comet-side accessor maintenance.

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
