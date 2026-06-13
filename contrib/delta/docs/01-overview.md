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

# Comet Delta Contrib — Design Overview

> **Read path:** Delta tables are read **natively through delta-kernel-rs**
> ("kernel-read"), enabled by default. `DeltaKernelScanExec` reads each file
> through delta-kernel-rs (0.24 / arrow-58), applying kernel's physical→logical
> transform (column mapping incl. nested, partition injection, row-tracking) and
> the deletion vector itself, and synthesises Delta's virtual columns in-worker.
> The legacy ParquetSource + DV-sweep + stacked-synthetic-columns read path has
> been **removed**; kernel-read is the only read path. For execution detail see
> [10-iceberg-style-kernel-read.md](10-iceberg-style-kernel-read.md).

## Who this is for

You know Spark's DataSource V2 (`TableProvider`, `Scan`, `Batch`, `InputPartition`).
You may not know Comet, may not know `delta-kernel-rs`, and may not have read
Spark's `FileSourceScanExec` internals. This document explains the shape of
the integration in those terms and points to deeper docs for each subsystem.

## What problem this solves

Apache Spark reads Delta tables today by going through Delta's Scala
`DeltaParquetFileFormat` → Spark's vectorised parquet reader → JVM rows. Comet
already replaces step 2 (the vectorised parquet reader) with a native
DataFusion-based reader for plain parquet scans, and the integration with
Spark for that path is well-trodden. The Delta path was the gap: Delta wraps
its parquet reads in JVM-side projections, filters, and Deletion-Vector logic
that Spark's `FileSourceScanExec` doesn't expose cleanly enough for Comet to
slot in underneath.

This contrib bypasses that wrapping. It plans Delta scans with
`delta-kernel-rs` (the official Rust kernel maintained by the Delta team), and
reads each resolved data file through kernel itself — kernel does the parquet
read, applies the physical→logical transform (column mapping, partition
injection, row-tracking materialisation), and applies the deletion vector. Any
remaining Delta "virtual" columns (`__delta_internal_is_row_deleted`,
`row_commit_version`, `_metadata.*`) are synthesised in-worker by
`DeltaKernelScanExec`. The result is end-to-end native execution for Delta
reads, with no Spark-side parquet decoding on the hot path.

## Mental model: a DSv2 substitute scan that fires before DSv2 binding

If you've written a DataSource V2 connector, the natural way to integrate
would be a `TableProvider` returning a custom `Scan`. The reason this PR
doesn't do that:

1. **Delta is a V1 source on the Spark side.** `DeltaTableV2` exposes a V2
   facade but its read path resolves to a V1 `HadoopFsRelation` carrying a
   `DeltaParquetFileFormat`. By the time DSv2 binding would run, the V1 plan
   is already built.
2. **We want to replace the entire scan node, not just the reader.** Delta
   inserts post-scan projections and filters to implement column mapping,
   row-tracking materialisation, and DV filtering. Those need to be
   _recognised_ and _eliminated_, then their semantics re-emitted natively.
3. **Comet's existing plan-rewrite infrastructure already does this for
   plain parquet.** Hooking in at the same layer (`CometScanRule` /
   `CometExecRule`) gives us the same lifecycle, the same fallback
   surface, and the same metric/error wiring.

So instead of a DSv2 scan, this contrib is a **rule that recognises Delta
relations in the logical plan and substitutes them with a native scan node**.
From a black-box viewpoint, the substitute behaves like a DSv2 `Scan`: it
exposes a schema, partitioning, and per-partition work units (file lists), and
it produces Spark `ColumnarBatch`es. Internally those columnar batches are
produced by DataFusion in Rust and shipped to the JVM as Arrow record batches.

## End-to-end flow

```
┌─────────────────────────────────────────────────────────────────────┐
│  SPARK DRIVER (JVM)                                                  │
│                                                                      │
│  Catalyst logical plan                                               │
│       │                                                              │
│       │   DeltaScanRule.transformV1IfDelta (CometScanRule arm)       │
│       ▼                                                              │
│  CometDeltaScanMarker wrapping the original FileSourceScanExec       │
│       │                                                              │
│       │   CometExecRule routes the marker to the Delta serde         │
│       │   CometDeltaNativeScan.convert (proto serde)                 │
│       │     1. JNI → planDeltaScan: kernel enumerates the snapshot   │
│       │        and ships per file a transform_json + DV descriptor   │
│       │        + kernel-built physical/logical Arrow schemas         │
│       │        (DML rewrites: kernel-enumerate + path-filter)        │
│       │     2. Encode into DeltaScan proto (common + per-task)       │
│       ▼                                                              │
│  CometDeltaNativeScanExec; per-partition DeltaScanTask byte arrays   │
│  injected via DeltaPlanDataInjector                                  │
└───────────────────┬─────────────────────────────────────────────────┘
                    │ shipped to executors via Spark task serialisation
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SPARK EXECUTOR (JVM + Rust via JNI)                                 │
│                                                                      │
│  Comet native exec framework                                         │
│       │                                                              │
│       │   delta_scan.rs shim → comet_contrib_delta::planner          │
│       ▼                                                              │
│  DeltaKernelScanExec (contrib/delta/native/src/kernel_scan.rs)       │
│       │                                                              │
│       │   For each file, read parquet via delta-kernel-rs, apply     │
│       │   kernel's transform (CM/partition/row-tracking) + DV, and   │
│       │   produce ALL output columns BY NAME — in-worker synthesis   │
│       │   (data + partitions + row_index/row_id/is_row_deleted/      │
│       │    row_commit_version/_metadata.*)                           │
│       │       ↓                                                      │
│       │   ProjectionExec (reorder via final_output_indices, if any)  │
│       ▼                                                              │
│  Arrow RecordBatch stream → Comet's existing Arrow→JVM bridge        │
└─────────────────────────────────────────────────────────────────────┘
```

The two non-obvious pieces are **the proto split** (a single "common" block
plus per-partition task arrays) and the **in-worker synthesis** done by
`DeltaKernelScanExec`. Both are covered in
[02-planning.md](02-planning.md) and
[03-native-execution.md](03-native-execution.md).

## Two deployment modes from the same codebase

Default builds (no `-Pcontrib-delta` Maven profile, no `contrib-delta` Cargo
feature) ship with zero Delta surface area:

- The reflection bridge in `spark/.../rules/DeltaIntegration.scala` returns the
  "not handled" sentinel at the first classpath lookup and stays that way for
  the JVM lifetime
- The `delta_scan` arm in `native/core/src/execution/planner/delta_scan.rs` is
  `#[cfg(feature = "contrib-delta")]`-gated and compiles out of the dylib
- The proto variant `delta_scan = 118` is present in the schema but never
  emitted

Delta-enabled builds (`-Pcontrib-delta` + `contrib-delta` Cargo feature):

- `contrib/delta/src/main/scala/...` lands on the classpath, including the
  Spark extension that registers `DeltaScanRule`
- `contrib/delta/native/` (the `comet_contrib_delta` crate) is linked into
  `libcomet`, contributing the `Java_…_planDeltaScan` JNI symbol
- The reflection bridge resolves on first call and caches the result

This is the same shape as the Iceberg contrib in this repo. The motivation is
explained in [04-design-decisions.md](04-design-decisions.md); the
operational implications are in
[05-build-and-deploy.md](05-build-and-deploy.md).

## What this contrib does NOT touch

To keep the integration scope tight, this PR deliberately avoids:

- **Writes.** Delta writes still go through Delta's Scala writer. The native
  path is read-only. (`Delete`, `Update`, `Merge` _use_ this read path via
  Spark's regenerated plans, but the write back to `_delta_log` is Delta's
  code.)
- **Delta transaction protocol.** We do not parse `_delta_log` ourselves —
  kernel-rs does that.
- **Catalog logic.** Path-based and metastore-registered tables both work
  because we plug in below the `LogicalRelation`, after Spark / Delta have
  already resolved the table.

## Where to read next

| Topic                                                                   | Document                                         |
| ----------------------------------------------------------------------- | ------------------------------------------------ |
| The planning rule, proto layout, kernel-rs interaction                  | [02-planning.md](02-planning.md)                 |
| The native execution plan tree and synthetic columns                    | [03-native-execution.md](03-native-execution.md) |
| Why an extension rule (not DSv2), why contrib (not core), why kernel-rs | [04-design-decisions.md](04-design-decisions.md) |
| Maven profile, Cargo feature, the publishing dance                      | [05-build-and-deploy.md](05-build-and-deploy.md) |
| Failure handling, Spark fallback, observability                         | [06-fallback-and-ops.md](06-fallback-and-ops.md) |

If you only have time for one more document, read
[04-design-decisions.md](04-design-decisions.md) — it answers the "why didn't
you just…" questions that come up first in review.

---

**Navigation** · [↑ Index](README.md) · Next → [02 Planning](02-planning.md)
