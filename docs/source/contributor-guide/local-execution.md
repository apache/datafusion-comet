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

Status: experimental Spark range bridge; stage-2 implementation and local checks
are complete. Spark SQL validation is deferred at the user's request.
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
Development stops at this stage-2 checkpoint; stage 3 has not started. Resolve the
deferred gate or explicitly revise this plan before expanding admission.

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
