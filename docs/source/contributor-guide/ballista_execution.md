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

# Experimental: Native Execution on Apache DataFusion Ballista

> **Status: experimental / research.** This is an in-progress exploration, not a supported
> feature. Design discussion is tracked in
> [issue #4796](https://github.com/apache/datafusion-comet/issues/4796) and the initial code
> in draft PR [#4800](https://github.com/apache/datafusion-comet/pull/4800). Interfaces,
> configuration, and behavior described here are subject to change.

## Overview

Today Comet accelerates Spark by running native operators **inside Spark executors** via JNI.
This page describes an additional, optional deployment mode being prototyped: running Comet's
native operators on a distributed **Apache DataFusion Ballista** data plane instead of inside
Spark executors.

In this mode the Spark cluster acts as a lightweight **control plane** — the driver plans the
query and hands it off — while all computation happens on Ballista. The existing in-Spark
accelerator is unchanged; this is purely additive.

The feature is **not tied to Spark Connect**. The native side only consumes a serialized Comet
plan; it does not care how the plan was produced. Because Comet's `QueryPlanSerde` emits that
plan from *any* Spark physical plan, the same mechanism supports whole-query offload from a
regular Spark application as well as from a Spark Connect client. Execution is **all-or-nothing**:
a query offloaded to Ballista runs entirely on Ballista and its result terminates at the driver
(or is written out by Ballista). Interleaving Ballista execution with Spark's own distributed
execution within a single job is out of scope.

## Architecture

```
  Spark app / Spark Connect client
              │
              ▼
  ┌─────────────────────────────┐   CONTROL PLANE (Spark driver only)
  │ Spark driver                │   • Catalyst + Comet driver-side plan rules
  │  + Comet plan rules         │   • serialize the Comet Operator protobuf
  │  + Ballista client          │   • submit to Ballista; collect results here
  └──────────────┬──────────────┘   • NO Spark executor tasks run
                 │  Comet plan protobuf
                 ▼
  ┌─────────────────────────────┐   DATA PLANE (Ballista, fully native)
  │ Ballista scheduler          │   • splits into stages / owns shuffle
  │ Ballista executors ×N       │   • rebuild the Comet plan over datafusion-ffi
  │   (Comet-flavored)          │   • run Comet operators + expressions
  └─────────────────────────────┘
```

Two design choices make this mostly integration rather than new invention:

- **`datafusion-ffi` boundary, not co-linking.** Comet and Ballista track DataFusion on
  independent schedules, so their crates are **not** linked together. Comet exposes a native
  plan (built by its own `PhysicalPlanner`) as an `FFI_ExecutionPlan`; the Ballista side consumes
  it as a `ForeignExecutionPlan`. They share only a stable C ABI (compatible within a DataFusion
  major version). This means Comet's `planner.rs`, operators, and expressions are reused as-is,
  with no reimplementation of plan translation.
- **Driver-side offload.** Comet's driver-side rules already build a root `CometNativeExec` that
  holds the whole-query serialized plan. In Ballista mode the driver submits that plan to Ballista
  and returns results at the driver, instead of dispatching an RDD job to Spark executors.

## Components

**Rust** (`native/`):
- `comet_ffi_plan_from_proto` (`datafusion-comet` core) — decodes a Comet `Operator` proto, builds
  the plan with the existing `PhysicalPlanner`, returns an `FFI_ExecutionPlan`.
- `execution::ballista` module (`datafusion-comet` core, gated behind the default-off `ballista`
  Cargo feature — built with `cargo build --features ballista` / `make core-ballista`, so it is
  folded into the single `libcomet` cdylib rather than a separate library):
  - `CometScanExec` — a serializable DataFusion leaf that rebuilds the plan over FFI at execute time.
  - `CometPhysicalCodec` / `CometLogicalCodec` — extension codecs that compose with Ballista's own
    (delegating non-Comet nodes) so Comet plans can be shipped to Ballista executors.
  - `CometTableProvider` — exposes a Comet plan to Ballista as a table.
  - the `Java_org_apache_comet_ballista_NativeBallista_*` JNI entries (driver-side submission).

**JVM** (`spark/`):
- Driver-side offload hook and configuration (see below).

## Configuration (planned / experimental)

| Config | Default | Description |
| --- | --- | --- |
| `spark.comet.exec.ballista.enabled` | `false` | Offload Comet plans to Ballista at the driver instead of executing in Spark executors. |
| `spark.comet.exec.ballista.scheduler.url` | _(unset)_ | External Ballista scheduler to submit to. When unset, an in-process Ballista engine is used. |

## Roadmap

Legend: ✅ done · 🔨 in progress · ⬜ planned

- ✅ **Rust core** — FFI plan export + gated `execution::ballista` module in `datafusion-comet`
  (`CometScanExec`, composed codecs, `CometTableProvider`) folded into `libcomet` behind the
  default-off `ballista` feature, with codec round-trip and standalone distributed tests.
- 🔨 **R1 — driver-side offload (single-stage).** A Spark app runs a query with
  `spark.comet.exec.ballista.enabled=true`; the driver submits the whole Comet plan to Ballista and
  returns results, with zero Spark-executor tasks. First target query: TPC-H Q1.
  - ✅ R1-T1 — JVM → native → in-process Ballista → JVM Arrow round-trip (spike).
  - ✅ R1-T2 — config flag + driver `executeCollect` override.
  - ◐ R1-T3 — offload proven end-to-end on Q1's single-stage subset (scan + date filter + decimal projections), results match Spark, 0 executor tasks. Full Q1 GROUP BY is structurally multi-block → R2.
  - ✅ R1-T4 (R1b) — **external cluster:** a distributed Comet plan submitted to a separate
    `comet-scheduler` process runs on a separate, **JVM-less** `comet-executor`
    process and returns correct results (no Ballista change needed — the config `override_*_codec`
    fields already exist). Config: `spark.comet.exec.ballista.scheduler.url`. Proven at two layers:
    the Rust harness (`ballista_external_cluster.rs`, a `NativeScan`→shuffle→`Filter` plan), and — via
    `CometBallistaExternalClusterQ1Suite` (`-Pspark-4.0`, feature-built `libcomet` + binaries) — a
    **live Spark driver** offloading **full TPC-H Q1's aggregate** to spawned `comet-scheduler` +
    `comet-executor` child processes, results matching Spark row-for-row (incl. decimal scale) with 0
    Spark-executor tasks. The full agg fragment (partial-agg `NativeScan` leaf → hash shuffle →
    final-agg over a `Scan` leaf) runs on the separate JVM-less executor process without a `JAVA_VM`
    panic.
- 🔨 **R2 — multi-stage distribution.** A distributed 2-block `GROUP BY` (Comet partial-agg → Ballista hash shuffle → Comet final-agg) runs offloaded with 0 Spark-executor tasks and correct results — **full TPC-H Q1's aggregate now runs distributed on Ballista and matches Spark.**
  - ✅ R2-T1 (Ballista) — accept a pre-built physical plan for distribution (a `physical_plan` submission variant; its own Ballista branch/PR).
  - ✅ R2-T2 (Comet native) — feed a `ScanExec` leaf from a native `RecordBatchStream` (not only a JVM input).
  - ✅ R2-T3 (`comet-ballista`) — `CometFragmentExec`: a Comet fragment whose `Scan` leaf is fed by DataFusion child streams.
  - ✅ R2-T4 — 2-block `count(*)` single-key distributes across the shuffle; results match Spark, 0 executor tasks.
  - ✅ R2-T5 — **full TPC-H Q1 aggregate distributed** (no `ORDER BY`): `sum`×4, `avg`×3, `count` over decimals grouped by two keys (`l_returnflag`, `l_linestatus`). `avg`'s partial (sum + count) state and decimal partial sums round-trip through Ballista's Arrow IPC shuffle and compose in the Comet final aggregate; results match Spark's own Q1 row-for-row (incl. decimal scale), 0 executor tasks.
  - ⬜ N-block generalization (a trailing `ORDER BY` / range exchange is a third stage — still out of scope).
- ⬜ **JVM-free executor.** Feature-gate the JNI bridge so the native execution crates build without
  `libjvm`, enabling a standalone Ballista executor binary.
- ⬜ **Multi-partition scans.** Map a scan's file groups to multiple partitions (currently a
  `NativeScan` proto encodes a single partition).
- ⬜ **Wider coverage.** Broaden operator/expression coverage; capture real plans from the JVM
  `QueryPlanSerde` for validation.
- ⬜ **Spark Connect front-end.** Package the driver as a Spark Connect server for unchanged clients.

## Known limitations

- Single-stage only in R1 (no distribution yet); plans containing an exchange are rejected.
- Scans are single-partition today.
- Queries with dynamic partition pruning or correlated scalar subqueries may still launch Spark
  executor tasks to resolve those inputs, even in Ballista mode.
- The FFI boundary requires Comet and Ballista to be built against the same DataFusion **major**
  version.
- Comet core links the JNI bridge, so `libjvm` must be present at runtime even where JNI is unused.
- The offload is folded into the single `libcomet` cdylib behind the default-off `ballista` Cargo
  feature (there is no separate `comet-ballista` cdylib / second copy of Comet core), so a
  Comet-on-executor query and an in-process Ballista offload share one `JAVA_VM` and coexist in the
  same JVM without the "JAVA_VM not initialized" panic. Building with the feature is required for
  the offload entries (`make core-ballista`); the default build stays Ballista-free.
- The single-stage `ORDER BY`/range exchange makes Q1's final sort a third stage — out of the current
  2-block scope; sort on the driver, or wait for N-block generalization.

## References

- Proposal and discussion: [issue #4796](https://github.com/apache/datafusion-comet/issues/4796)
- Prototype code: draft PR [#4800](https://github.com/apache/datafusion-comet/pull/4800)
- [Arrow FFI Usage in Comet](ffi.md)
