<!--
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

# Local execution development plan

Status: stage 4c checkpoint, experimental local range and Parquet execution with
COUNT/MIN/MAX aggregation, partitioned hash joins, and terminal global sort/limit. Local execution and bridge
tests pass; Spark SQL validation remains deferred at the user's request.
`spark.comet.exec.local.enabled=true` opts in through the existing Comet extension.
The option defaults to false. Unsupported whole queries use ordinary Comet/Spark planning.
The development baseline is local `upstream/main` at `9c7fcc5aa`.

## Execution contract

Local execution keeps Spark SQL analysis and Comet's Spark-compatible semantics,
but gives DataFusion ownership of parallel execution within a single process.
Each query execution owns one fresh physical graph. Every partition uses those
same operator instances, including the channels and state of `RepartitionExec`.
Equivalent plans deserialized separately for Spark tasks cannot implement this
exchange. Sharing is a consequence of query ownership, not a cache optimization.

The graph is distinct for each execution, even when the same DataFrame is acted
on twice. Retry must create a fresh graph. No stateful operator tree is cached
across executions. Spark task attempts must not independently replay a partition
of an already-running native graph.

The initial native API accepts a fully planned graph and a query-scoped DataFusion
`TaskContext`. It does not optimize the graph or verify distribution requirements.
It consumes the execution owner once and returns one result stream. DataFusion's
`execute_stream` coalesces root partitions concurrently, so progress does not
depend on Spark scheduling every output partition simultaneously. Internal
partition parallelism remains intact even though there is one output stream.

The result boundary is unordered for multiple root partitions and does not expose
their IDs. A single ordered root retains its order. Global ordering must be
established by the planner; coalescing is not a sort-preserving merge. The first
Spark bridge must therefore expose one output partition, report its properties
honestly, and reject partition-sensitive operations it cannot preserve. Supporting
arbitrary RDD partition semantics requires a later, explicit output protocol.

EOF, the first stream error, explicit cancellation, and dropping the result stream
release the owner's references to the graph and context. DataFusion tasks abort
asynchronously; dropping is not a cleanup completion barrier. The process-owned
Tokio runtime must remain alive to complete teardown. A runtime must be entered
when execution starts. The JVM bridge will need cancellation that can interrupt a
blocked result pull and must not require that pull to finish before cancelling.

## Module boundaries

- `native/local`: query execution lifecycle, independent of JNI and Spark tasks.
  Reuse DataFusion scheduling, exchange and result streaming rather than building
  another scheduler or exchange implementation.
- `spark-local`: JVM source module for mode selection, whole-query admission,
  query ownership, result bridge, cancellation and SQL metrics. Build-helper
  compiles it into the existing Spark artifact, avoiding a cyclic dependency on
  `CometConf` and `NativeUtil`; its source and tests remain in separate directories.
- Existing expression crates and Arrow bridge: reuse Spark-compatible evaluation
  and columnar interchange. Audit all task-context callbacks before admission.
- Proposed shared planning module: extract only reusable operator construction
  and expression conversion as integration requires it. The local planner owns
  distribution, ordering and exchange translation for the complete graph.
- Existing native core: retain the JNI library entry and distributed execution.
  It may depend on local execution; the local crate must not depend back on core.

Do not copy the entire existing planner or add a mode flag throughout every
operator. Task-bound memory managers, JVM input iterators, per-task plan creation,
and Spark shuffle readers/writers cannot be reused as the local coordinator.

## Stages and verification gates

Complete one stage, stop adding features, run its checks, inspect the final change,
and record the verdict before starting another. A failed or incomplete gate blocks
the next stage. Report a checkpoint to the user after each stage.

### 1. Native query execution foundation

Deliver a workspace crate with single-use graph ownership and a streaming output
boundary. Do not connect it to Spark yet.

Gate:

- Round-robin and hash repartition deliver every input row exactly once.
- Equal keys from different inputs reach the same hash output partition.
- Input and output partitions are executed once against the common graph.
- One runtime thread can execute more partitions than workers; nested exchanges
  also complete on a small multithreaded runtime.
- Empty and single-partition roots work; ordered single-partition output stays ordered.
- EOF, explicit cancellation, early result drop, source failure and global limit
  release graph/input resources, including pending sibling streams.
- Cancelling one query does not cancel another query on the same runtime.
- Native tests, formatting and Clippy pass. Review ownership and output semantics
  before introducing JNI. This gate does not establish Spark SQL compatibility.

Commands, from `native/`:

```shell
cargo test -p datafusion-comet-local --locked
cargo fmt -p datafusion-comet-local --check
cargo clippy -p datafusion-comet-local --all-targets --locked -- -D warnings
```

The crate is in workspace default members so the existing Cargo/nextest CI run
includes its integration tests. No existing native operator or Spark rule is
changed in this stage.

Checkpoint (2026-09-30): stage 1 passed its native gate; development stops here
before starting stage 2. All 12 integration tests passed on the local macOS host
with DataFusion 55.1.0. Each exchange correctness case routes 2,048 distinct rows
from four input partitions through seven outputs; the nested case adds eleven
outputs. The tests verify exact row identity, equal-key routing, once-only
partition execution, and eventual resource reclamation. Cancellation tests wait
for pending input partitions to start before initiating teardown. Formatting,
Clippy with warnings denied, and whitespace checks passed.

The local Cargo runs used cached dependencies (`--offline`) and the existing
development registry mirror override. Cargo.lock only gains the local crate;
dependency versions were not upgraded. No JVM, Spark SQL, benchmark, or low-memory
spill verdict is claimed at this stage. Those gates remain assigned to the stages
that introduce the corresponding behavior.

Design review conclusions: reuse the DataFusion coalescer; keep native execution
single-use; keep the runtime process-owned and the context query-scoped; treat
result partition semantics and interruptible JNI cancellation as explicit stage-2
work. The present API trusts its caller to supply a fresh graph with valid physical
properties. The future local planner must enforce that contract.

### 2. Spark admission, ownership and result bridge

Add the JVM module/build integration and an opt-in entry before the existing Comet
conversion. Initially support Spark 4.1, read-only batch queries, actual in-process
local masters, and AQE disabled. Inspect the application's effective SparkContext,
not a session override of `spark.master`; reject `local-cluster` and remote masters.
Start with a minimal native source and projection query.

Define a query execution handle with an explicit lifetime. Bridge a single output
partition without collecting all results on the driver. Preserve schema and Arrow
ownership; support `collect`, `take` and streaming iteration within the admitted
surface. Freeze relevant SQL configuration per execution. Detect unsupported
plans before starting anything; fallback is a planning decision, never a recovery
after partial native output. If cancellation requires a registry, bound its lifetime
to live query executions and specify the cleanup path before implementing it.

Gate: opt-in/off behavior; local-master and AQE rejection; repeated actions create
fresh graphs; result correctness; empty output; cancellation during a blocked JNI
pull; no Spark shuffle dependency for the admitted query; no native references
retained after close. Build native before JVM tests and never use Maven `-pl`.
Register JVM suites in both CI matrices. Obtain the Spark SQL gate described below.

Stage-2 implementation:

- Enable the ordinary `CometSparkSessionExtensions`, native execution, and
  `spark.comet.exec.local.enabled`; disable AQE. Admission currently requires
  Spark 4.1 and `SparkContext.isLocal`. Streaming and subquery preparation are
  rejected. No additional session extension is required.
- Admit only a complete `RangeExec` with optional direct column/alias projections
  and an optional root `CollectLimitExec` with zero offset. Configuration is frozen
  in the local physical node. Limits are 1,024 native partitions, 1,024 projected
  columns and 65,536 rows per batch. Expressions, filters, aggregations, exchanges,
  file scans and writes remain outside admission.
- One Spark result task creates one fresh native graph per attempt. Native range
  partitions execute through DataFusion on the existing process runtime. A
  sort-preserving merge retains range order: Spark may have removed a redundant
  sort before invoking the Comet rule. This is necessary for both correctness and
  the ordering advertised by the local node.
- The JNI registry stores numeric, non-reused IDs for at most 1,024 live queries.
  Each entry owns a cancellable producer. EOF, errors, early termination and task
  completion remove it. A concurrent pull holds an `Arc`, so close cannot free an
  object still being read. Polling returns pending after 50 ms, allowing Spark
  interruption checks. Cancellation does not acquire the reader mutex.
- Reuse `prepare_output` (including zero-offset normalization) and `NativeUtil`
  for Arrow ownership. Imported buffers remain valid after native query close
  until the JVM closes their batch. The bridge queues at most one batch; this does
  not bound DataFusion operator reservations or the driver's result collection.

`collect`, `take` and `toLocalIterator` retain Spark's action behavior. In particular,
Spark's `toLocalIterator` materializes a result partition, so it does **not** provide
bounded driver-memory streaming with this single-partition boundary. The native to
Spark task iterator streams batches. DataFrame `.rdd` introduces object
deserialization, which is currently unsupported and causes whole-query fallback.
Native worker count is not derived from Spark task slots; matched CPU/memory budgets
and general result iteration remain later-stage work.

Local verification (2026-09-30): 18 native tests and 34 JVM tests passed (13 local
mode tests plus 21 existing iterator lifecycle tests). Tests cover ordered and
descending ranges, empty input, repeated actions, early stop, fallback, task
cancellation, native errors/panics and Arrow lifetime. Strict Clippy passes for
the native core and local crate. The new JVM suite is registered in both CI
matrices. Formatting and CI configuration checks also passed. The user requested
deferring the Spark SQL suite, so no Spark SQL compatibility verdict is claimed.
Development stopped at the stage-2 checkpoint. The subsequent user instruction
authorized stage 3 while retaining the deferral of Spark SQL validation. This is
an explicit exception to the usual gate order, not a Spark SQL compatibility verdict.

### 3. Native scans and shared expression conversion

Reuse Comet's native Parquet capability and supported expression conversion.
Plan all file partitions in the one graph, without Spark task iterators. Add
filter and project. Audit object store configuration, credentials, scan metadata,
timezone, ANSI mode, decimals and null semantics. Keep unsupported callbacks,
UDFs, subqueries and partition-sensitive expressions outside initial admission.

Gate: compare Spark and local execution results on supported types and expressions,
multiple files and empty inputs; verify pushdown does not alter correctness;
confirm whole-query fallback for unsupported plans. Test configuration isolation
between simultaneous queries and obtain the Spark SQL gate.

Stage-3 admission and reuse:

- Only DataSource V1 scans of Spark's built-in Parquet format on `file:` paths are
  admitted. The existing `CometScanRule` validates a copy of the scan before
  `CometNativeScan` serializes it. Static partition pruning and Spark file splitting
  are reused; file groups and scan settings are captured during local planning.
- `spark-local/LocalParquetPlanner` serializes a whole unary scan/filter/project
  tree using existing operator and expression protobufs. Its explicit expression
  allowlist covers references, literals, aliases, arithmetic, comparisons, boolean
  and null predicates, casts, overflow checks, and conditionals. Both the Spark
  expressions and serialized expressions must be admitted: serde can otherwise
  choose JVM codegen callbacks even for familiar expression classes.
- The initial type surface is primitive numeric/boolean/string/binary, decimal,
  date, timestamp and timestamp NTZ. Nested and collated types, UDFs, nondeterministic
  or partition-sensitive expressions, subqueries and metadata expressions fall
  back as a whole. Existing serde compatibility/configuration gates still apply.
- `native/core/src/local/planner.rs` is a local adapter to core's existing
  `PhysicalPlanner`. It reuses native Parquet construction for each file group,
  assembles those scans under a native union, and constructs shared filter/project
  nodes above it. This is one query graph and one Spark result task; no graph is
  deserialized per Spark input task. No shared expression implementation is copied
  or moved into a dependency cycle with `native/local`.
- Every execution owns its DataFusion session configuration, runtime environment
  and object-store registry, while the Tokio worker runtime remains process-owned.
  Timezone, schema matching and scan flags travel in existing scan metadata;
  expression ANSI/timezone behavior travels in existing expression protobufs.
  Batch size and the Comet row-filter pushdown option are also captured. Arbitrary
  `spark.comet.datafusion.*` overrides are not propagated in this initial mode.
- Cloud/custom filesystems, custom `fs.file.impl`, object-store options, encrypted
  reads, bucketed or ordered scans and file metadata columns are excluded. In
  particular, credentials and encryption callbacks do not cross the local boundary.
  These are admission limits, not a claim that remote files cannot eventually be
  read from a single process.
- Parquet output is unordered and uses the stage-2 single-partition result bridge.
  Ordered scans are rejected because Spark may already have removed a sort based
  on their ordering. Range queries retain their separate sort-preserving adapter.
  Structured execution errors reuse `SparkErrorConverter`; query cleanup happens
  before conversion/rethrow. There is no fallback after native output starts.

The admission caps remain 1,024 file groups, 1,024 output columns and batch size
65,536. These bound some execution overhead, not total memory. The driver currently
captures all admitted file metadata in the physical plan. A query memory budget and
spilling policy were deferred to stage 4a; bounded driver result streaming remains deferred.

Checkpoint (2026-09-30): 55 Spark 4.1 JVM tests passed: 22 local execution tests,
21 iterator lifecycle tests and 12 `NativeUtil` tests. The local tests compare
Spark and local results, require a local physical node (so fallback cannot mask a
failure), and cover multi-file scans, split row groups exactly once, static
partition pruning, nulls/decimals/dates/timestamps, pushdown on/off, missing nullable
columns, empty scans, repeated actions, early stop and errors. Two native Parquet
queries with different timezone settings remain live simultaneously; closing one
does not affect the other's results. ANSI overflow retains `CAST_OVERFLOW`.
All 18 native lifecycle/repartition tests, strict Rust Clippy, formatting and CI
configuration checks also passed. Spark 3.5 main/test compilation with
`-Pstrict-warnings` passed; local execution stays disabled on that profile.
Spark SQL remains deliberately unrun. No scan,
join or aggregate performance claim was made at this checkpoint.

### 4. Operators across exchange boundaries

Add hash aggregation and partitioned joins first, each with its own test checkpoint;
then sort and limit. Translate each exchange's distribution and ordering contract.
Hash exchange, single-partition gathering, broadcast build sharing and range/global
ordering need distinct handling. Use Comet-compatible operators and expressions
where Spark semantics differ. Avoid double-inserting exchanges around already
partitioned inputs. Do not assume matching partition counts imply matching hashes.

Establish a query memory budget and spill configuration. Prefer in-memory exchange
without promising all queries stay in memory. Account for native reservations
without borrowing one Spark task's memory consumer for the whole graph.

Gate: cross-partition joins and groups, null/duplicate keys, empty join sides,
global ordering, limit cleanup, one worker, skew, constrained memory and spilling.
Compare results with Spark and inspect plans for eliminated Spark shuffle stages.
Obtain the Spark SQL gate before expanding admission further.

#### 4a. Hash aggregation checkpoint

Implemented grouped and global COUNT, MIN and MAX over admitted Parquet input.
`LocalAggregatePlanner` recognizes a matching final/partial Spark hash aggregation
pair and its optional exchange, including the complete grouping expressions rather
than just partition counts. It replaces the whole tree with one local query.
`spark-local` owns admission; `native/core/src/local/planner.rs` reuses Comet's
aggregate expression builder. A separate `local.proto` describes complete
aggregation on raw input, without importing Spark's partial buffer protocol.

Grouped multi-partition input goes through one shared DataFusion hash
`RepartitionExec` and `AggregateMode::SinglePartitioned`. Global aggregation and
single-partition groups use native coalescing and `AggregateMode::Single`.
Result expressions run as a native projection. Hash buckets remain internal to
DataFusion; Spark sees one result partition and no shuffle exchange. This version
repartitions raw input without a partial-combine optimization or performance claim.

The Parquet path now owns a `FairSpillPool` per query. The internal setting
`spark.comet.exec.local.memoryLimit` defaults to 256 MiB and limits DataFusion
reservations, not process RSS, scan buffers, driver collection or the sum of
concurrent queries. It does not borrow a Spark task memory consumer.
`spark.comet.exec.local.spill.enabled` defaults to true and permits query-owned
spill files in the OS temporary directory. There is no local-mode disk quota yet.
The separate range adapter is unchanged. Disabling spill permits resource errors
under constrained memory; execution errors never trigger fallback after output.

Admission still rejects DISTINCT, SUM/AVG, floating-point grouping keys or MIN/MAX
outputs, extra/nested exchanges, range aggregation, HAVING, sort and joins. Existing
aggregate, filter and projection enablement flags are honored. Unsupported whole
queries retain ordinary Comet/Spark planning.

Checkpoint (2026-09-30): 61 Spark 4.1 JVM tests passed (28 local execution,
21 iterator lifecycle, 12 NativeUtil). Coverage includes multiple input files and
seven hash partitions, grouped/global empty input, null and duplicate keys, skew,
aggregate filters, repeated actions, early limits, whole-query fallback and
resource-error cleanup followed by a successful query. Three native tests force
spill with 204,800 groups, including seven repartition outputs on one Tokio worker
and early stream drop. Results are checked and reservations and spill disk usage
return to zero after teardown. All 18 native lifecycle/repartition tests, Rust
formatting and strict Clippy passed. Spark 3.5 main/test compilation with strict
warnings also passed; local mode remains disabled on that profile. Spark SQL
remains deliberately unrun.

This checkpoint stopped before 4b (partitioned joins). Ordering and broader
semantic gates remained outstanding.

#### 4b. Partitioned hash join checkpoint

`LocalJoinPlanner` admits a single `ShuffledHashJoinExec` over two independently
admitted Parquet/filter/project inputs, optionally followed by a projection.
Each child must have a hash exchange matching every join key in the same order,
with equal partition counts from 1 to 1,024. Keys must be same-type, supported
non-floating-point attribute references. Both input file groups and expression
metadata travel in `LocalJoin`; scans reuse the existing local Parquet builder.
Both sides share one query context, memory pool, runtime environment and result
stream, while each scan retains its own SQL text pool.

Two DataFusion hash repartitions feed a `PartitionMode::Partitioned` hash join.
Spark hash buckets, shuffle files and task scheduling are removed from this query.
Build-side selection is retained; DataFusion's input-swap projection restores the
logical output order. Inner, left/right/full outer, left semi and left anti joins
are supported with ordinary equality (null keys do not match). Output ordering is
empty, and Spark sees one result partition.

This first join checkpoint requires Spark to select shuffled hash join, for
example with a `SHUFFLE_HASH` hint. It does not convert sort-merge or broadcast
joins. Residual join conditions, computed or floating-point keys, nested joins,
aggregates around joins, extra exchanges and range inputs retain whole-query
fallback. Native hash-join and projection enablement flags are honored.

The same reservation budget covers both inputs, both repartitions and the join.
DataFusion 55.1's hash join build does not spill: enabling local spill does not
make an oversized hash table executable. A failed reservation ends the query,
with no mid-execution fallback. Partitioned/spilling join algorithms are deferred;
there is no claim that this checkpoint handles arbitrary joins under low memory.

Verification includes Spark result multiset comparisons for both build sides,
outer/semi/anti joins, duplicate/null keys, empty inputs, composite keys, result
projection, repeated actions, early limits, fallback and reservation failure.
Native tests run seven join partitions on one Tokio worker, checking 65,536
matching rows and reservation/disk cleanup on completion, early drop and error.
Checkpoint (2026-09-30): 66 JVM tests passed (33 local execution, 21 iterator
lifecycle and 12 NativeUtil), together with 18 native lifecycle tests and six
native planner tests (three aggregation/spill and three join tests). Rust
formatting, strict Clippy and Spark 3.5 main/test compilation with strict warnings
passed. Spark SQL validation remains deferred at the user's request.

This checkpoint stopped before 4c (sort and limit). General join coverage and the
broader Spark SQL gate remained outstanding.

#### 4c. Global sort and limit checkpoint

`LocalOutputPlanner` admits terminal global `SortExec`, `TakeOrderedAndProjectExec`
and root `CollectLimitExec` over the existing Parquet, aggregate and hash-join
inputs. A global sort's range exchange is removed only when every sort expression,
direction and null placement matches. The local graph gathers input partitions,
then runs DataFusion `SortExec` and/or `GlobalLimitExec` once for the whole query.
It never concatenates independently sorted partitions as a globally ordered result.
This initial implementation uses a single global sorter rather than a parallel
local-sort/merge optimization; no sort performance claim is made.

`LocalOutput` carries sort expressions with explicit direction/null placement,
optional fetch, skip and a final projection. Spark's physical limit includes the
offset: admission converts it to `fetch = limit - offset`. Top-K retains
`skip + fetch` rows and applies skip once. Offset without limit remains unbounded;
output projection runs after sort/limit. The terminal operators belong to the
same query context and reservation pool as their input graph, including joined
inputs. Existing scan and expression implementations and the result bridge are
reused. There is no Spark shuffle for an admitted query.

The native root has one result partition. Spark's root `outputOrdering` is retained
conservatively, including an empty declaration when Spark does not advertise order
through a projection or collection limit. Actual ordered results are compared
sequence-by-sequence with Spark; ties without a complete ordering key need not
have a stable relative order. Unordered limit results need not select the same rows
as Spark's partition traversal.

Sorting covers the admitted primitive/decimal/date/timestamp expression surface,
excluding floating-point keys. Per-partition sorting, nested limit pipelines and
new terminal operations on the separate range adapter remain outside this
checkpoint. Existing range execution and its root Spark limit wrapper are retained.
Unsupported shapes use the existing path. Native sort, Top-K and projection flags
gate their corresponding terminal operations; disabling native collection limit
retains the existing Spark result wrapper.

Full sort can spill under the existing query spill policy. Disabling spill or
setting a budget below the sort workspace requirement can produce a resource
error; spill does not promise success at arbitrary budgets. Native tests force
spill with 1,048,576 rows from seven input partitions and a 16 MiB reservation
budget on one Tokio worker. They verify global order, actual spill, and return of
reservations/disk usage to zero after completion or early stream drop. A separate
Top-K test checks offset across all seven input partitions. JVM tests compare all
ASC/DESC and NULLS FIRST/LAST combinations, multiple keys, Top-K projection,
aggregate/join output sorting, empty input, offset beyond EOF, repeated actions,
unordered global limit, fallback and resource-error cleanup.

Checkpoint (2026-09-30): 73 JVM tests passed (40 local execution, 21 iterator
lifecycle and 12 NativeUtil), plus 18 native lifecycle tests and nine native
planner tests. Rust formatting, strict Clippy and Spark 3.5 main/test compilation
with strict warnings passed.

Stop at this checkpoint before stage 5 (hardening and performance). Spark SQL
validation remains deferred at the user's request; its gate has not passed.

### 5. Hardening and performance

Exercise concurrent queries, cancellation, input errors, repeated actions, resource
reclamation, and explain/SQL metrics. Benchmark supported TPC-H/TPC-DS queries
against Spark local mode and current Comet local execution with matched CPU/memory
budgets. Record unsupported queries and fallback separately. Measure wall time,
CPU, peak/retained memory, planning time and spill, not just plan construction.

Gate: supported result comparisons pass, no hangs or persistent resource growth,
default-path regression checks pass, and benchmark results justify wider admission.
Do not claim general performance gains from the stage-one native tests.

## Spark SQL validation policy

For stages changing planner, serde, operators or shims, obtain a Spark SQL verdict
before proceeding. Prefer `run-spark-4.1-tests` on the implementation PR for broad
changes; use the relevant `dev/local-ci.sh spark <row>` only when no CI verdict is
available and the row covers the change. Choose one, not both. Inspect the matching
revision's result before treating the gate as passed. Follow `ci.md` regarding the
shared Maven cache and preparation; do not use `SKIP_PREPARE=1` after Comet changes.

## Deferred features

AQE integration, mixed Spark/native islands, independent Spark task retries,
partition-preserving output, arbitrary RDD operations, JVM/Python UDFs, writes,
streaming queries and distributed deployment are not part of the first usable
local mode. Their absence must be reflected in admission and explain output.
