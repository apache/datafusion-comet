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

# Design decisions and rejected alternatives

> **Partly superseded.** The read-path decisions here (ParquetSource, field-id vs
> name resolution, DV-sweep, synthetic-column stacking) reflect the legacy design
> that has since been **replaced** by the "kernel reads" path. The read path's design
> rationale now lives in [10-iceberg-style-kernel-read.md](archive/10-iceberg-style-kernel-read.md)
> and [11-kernel-read-coherence-audit.md](archive/11-kernel-read-coherence-audit.md). The
> driver-side and integration decisions here remain accurate.

This document captures the "why didn't you just…" questions. Each section
states the decision, the alternatives we considered, and the reason we chose
what we chose.

## Why a Spark extension rule, not a DataSource V2 scan?

**Decision.** Hook into `CometScanRule` (Spark `SparkSessionExtensions`)
and substitute the scan node in Catalyst's plan tree, rather than
implementing `TableProvider` / `Scan` against DSv2.

**Alternative.** Register a DSv2 source that takes over Delta reads.

**Why not.** Delta's public surface is a V2 facade (`DeltaTableV2`), but
its actual read path resolves to a V1 `HadoopFsRelation` carrying a
`DeltaParquetFileFormat`. The DSv2 binding has already been done by the
time we see a Delta plan. To intercept earlier would mean either patching
Delta or re-implementing its catalog logic — both of which would extend
this PR's blast radius dramatically.

Hooking at the same layer as the existing Comet parquet rule also reuses
the existing fallback / metric / error mechanics rather than building
parallel ones for DSv2.

## Why a "contrib" tree, not a core module?

**Decision.** Code lives under `contrib/delta/`, gated by the
`-Pcontrib-delta` Maven profile and `contrib-delta` Cargo feature.
Default builds are unaware of the contrib.

**Alternative A.** Make Delta integration first-class — always built,
always on the classpath.

**Why not.** Delta is one of several table-format integrations Comet
will need (Iceberg already in tree, Hudi likely). Each has heavy
transitive deps (`delta-spark`, `delta-kernel-rs`, kernel's own arrow /
object_store pins). Forcing all consumers to take those deps even when
they only want plain parquet is a regression vs the current state.

**Alternative B.** Ship as a separate Maven artifact in a separate repo.

**Why not.** This contrib needs a _small_ set of core touchpoints that
must evolve in lockstep with the contrib (the `PlanDataInjector` and
`CometScanContrib` SPIs, the `OpStruct::ContribScan` envelope, the
native dispatcher arm). Splitting repos would version-couple them
anyway; same-repo is strictly simpler.

## Why kernel-rs, not parsing `_delta_log` ourselves?

**Decision.** Use `delta-kernel-rs` for snapshot resolution, file
listing, DV materialisation, and column-mapping metadata.

**Alternative.** Hand-roll log replay in Rust.

**Why not.** Delta's transaction protocol is a moving target (DVs, row
tracking, type widening, identity columns, …). Maintaining a
hand-rolled parser would be an ongoing tax and a source of subtle
divergence from Delta's own semantics. kernel-rs is the Delta team's
official Rust kernel, tracks the protocol, and is what the Delta team
will direct integrators to use going forward. The cost is a couple of
heavy transitive deps and an arrow-version pin, both of which we
isolate (see below).

## Why a generic proto envelope, not a typed per-contrib variant?

**Decision.** A single permanent `OpStruct::ContribScan { type_url,
value }` envelope carries every out-of-tree contrib scan. Delta packs a
`DeltaScan` into it and the native side dispatches on `type_url`. The
typed `DeltaScanCommon` / `DeltaScanTask` messages still exist — only
the *dispatch* is generic.

**Alternative (and the original decision here).** Give each contrib a
first-class oneof variant, e.g. `DeltaScan delta_scan = 118`.

**Why the reversal.** The typed-variant approach was implemented first
and rejected in review of #4952. Two problems showed up that the
original analysis missed:

1. **Field-number collisions between independent contrib PRs.** The
   Lance contrib (#4633) independently claimed `lance_scan = 118` — the
   same number. Nothing in the typed-variant scheme prevents two
   out-of-tree contribs from colliding, and resolving it means one of
   them rebasing a wire-format change.
2. **Core's oneof grows once per format.** That makes core's proto —
   and therefore core's review surface — a function of how many
   optional contribs exist, which is exactly the coupling the contrib
   split is meant to remove.

The costs cited for the envelope are real but small: dispatch is a
string suffix compare (once per scan operator, not per batch) plus a
`decode` the typed path also pays, and the payload's schema is still
fully described in `operator.proto` — only the *binding* from `type_url`
to message is out-of-band, and it lives in exactly two places
(`DeltaContribScan.TypeUrl` on the JVM, `DELTA_SCAN_TYPE_NAME` in
`delta_scan.rs`).

The envelope's field layout is deliberately identical to
`google.protobuf.Any` so a JVM producer can populate it from
`Any.pack(...)`. It is hand-rolled rather than importing `Any` because
Comet compiles this `.proto` with two toolchains and the Maven
`protoc-jar` plugin cannot resolve the bundled well-known types.

## Why split-mode serialisation (common + per-task)?

**Decision.** The `DeltaScan` proto carries a `DeltaScanCommon` block;
each partition's `DeltaScanTask[]` rides in a per-partition byte array
via `PlanDataInjector`.

**Alternative.** Embed all tasks for all partitions in the operator
proto.

**Why not.** Spark serialises plan closures to every executor as part
of task scheduling. A 5000-file scan over 200 partitions would push
1M task records through that path. The split keeps the common payload
small (KB) and ships only relevant tasks to each partition.

## Why a ServiceLoader SPI in core, not a reflection bridge?

**Decision.** Core declares a format-agnostic `CometScanContrib` trait
(plus the `CometContribScanMarker` marker trait) and discovers
implementations with the JDK `ServiceLoader`. The contrib ships
`DeltaScanRuleContrib` and a
`META-INF/services/org.apache.comet.rules.CometScanContrib` resource.
All Delta logic still lives entirely in `contrib/delta/...`.

**Alternative (and the original decision here).** A `DeltaIntegration`
object in core acting as a thin reflection bridge — `Class.forName` on
the contrib's module, `getMethod` for each entry point, cached in a
`@volatile var`.

**Why the reversal.** The reflection bridge was implemented first and
rejected in review of #4952. The objection that drove the original
decision — "a trait in core would create a compile-time dependency on
the contrib" — is simply not true of a `ServiceLoader` SPI: the trait
lives in core, the *implementation* lives in the contrib, and the
dependency arrow points contrib → core, which is the correct direction.
Nothing has to be in the same JAR. Concretely the SPI is better on every
axis that mattered:

- **Type safety.** Signatures are checked at compile time instead of
  being re-derived by `getMethod` at runtime, where a signature change
  in the contrib degrades to a silent "not present".
- **No per-entry-point plumbing.** The bridge needed a cached binding,
  an invoke, and an exception funnel *per method*; the SPI needs none.
- **Core names no format.** The bridge hard-coded Delta class-name
  strings in core; the trait mentions no contrib at all, which is what
  lets Lance (#4633) reuse the same SPI.
- **Consistency.** Core already used exactly this pattern for
  `PlanDataInjector` (#4700).

The one property worth preserving from the bridge is that a throwing
contrib must not take down the planner: `CometScanContrib` catches
`NonFatal` from each hook, logs, and treats it as a decline — the same
behaviour the bridge's `InvocationTargetException` funnel provided.

## Why cache reflection method handles?

**Decision.** Any remaining reflective lookup resolves `Class.forName`
and `getMethod` exactly once per JVM, caches the result, and reuses it.

**Why.** Reflection lookup is slow enough that doing it per call would
show up in hot paths. Driver-side lookups fire once per query plan;
executor-side ones fire per task. Per-task is hot enough to matter, and
the cache pattern is cheap.

This still applies to the reflection the contrib does into *delta-spark*
(`DeltaReflection`, which avoids a compile-time delta-spark dependency)
and to the S3A credential bridge — we resolve
`S3AUtils.createAWSCredentialProviderList` once and reuse the
`Method` handle for every kernel-rs engine creation. It no longer
applies to core↔contrib dispatch, which is now the `ServiceLoader` SPI
above.

## (Historical) Why did each FileGroup hold one file when synthetics emit?

> **Superseded by kernel-read.** This describes the legacy
> `ParquetSource` + `DeltaSyntheticColumnsExec` stack, which was
> **deleted** (#50, #82). It is preserved only to explain a past
> design choice. The current `DeltaKernelScanExec` reads one file via
> kernel per task and synthesises every output column by name in-worker
> — there is no FileGroup-to-partition-index coupling.

**Decision (legacy).** When any `emit_*` flag was set, the parquet
`FileScanConfig` was built with one `FileGroup` per file.

**Alternative.** Let the parquet reader pack files into shared groups
for better parallelism.

**Why not.** The now-deleted `DeltaSyntheticColumnsExec` indexed
per-partition state vectors
`(deleted_row_indexes, base_row_ids, default_row_commit_versions)`
by the DataFusion partition index. One file per FileGroup meant
"partition index = file index", which made the index lookup correct.
With shared groups, multiple files would map to one partition index and
the lookup would return the wrong file's metadata.

## (Historical) Why a `DeltaDvFilterExec` and not a Spark filter on top?

> **Superseded by kernel-read.** `DeltaDvFilterExec` was deleted
> alongside the legacy read path. DVs are now resolved executor-side
> inside `DeltaKernelScanExec` via kernel's own apply-DV path
> (`dv_reader::read_dv_indexes`); deleted rows are dropped before the
> batch enters the Comet plan. The rationale below explains the original
> choice.

**Decision (legacy).** Filter deleted rows in the native plan, between
`ParquetSource` and the synthetic-column exec.

**Alternative.** Read all rows natively, ship to the JVM, filter there.

**Why not.** DVs can mark significant fractions of a file as deleted
(MERGE-heavy workloads can easily hit 30%+). Filtering natively avoids
serialising and crossing JNI with rows that are about to be dropped.
The same reasoning still holds for the kernel-read path, which applies
the DV before any rows cross JNI.

## Why an engine cache keyed on storage config?

**Decision.** kernel-rs `DefaultEngine` instances are cached on the
driver, keyed by `(scheme, authority, DeltaStorageConfig)`.

**Why.** `DefaultEngine<TokioBackgroundExecutor>` spawns one OS thread
per executor on creation. In ad-hoc query workloads (notebook users
hitting many tables, MERGE-heavy ETL jobs), driver-side engine creation
was happening dozens of times per minute. Without a cache, tokio's
thread-reaper couldn't keep up and ~2h into regression the driver
tripped `pthread_create EAGAIN`.

The key includes the storage config because two queries against the
same bucket with different IAM credentials must NOT share an engine —
the cached engine has those credentials baked in. The
`DeltaStorageConfig` hash captures the relevant credential identity.

## Why S3A credential resolution Scala-side, not Rust-side?

**Decision.** Walk Hadoop's S3A credential chain
(`SimpleAWS` / `TemporaryAWS` / `AssumedRole` / `IAMInstance`) on the
JVM driver, materialise concrete credentials, and pass them into
kernel-rs's engine config.

**Alternative.** Have kernel-rs's object_store resolve the credential
chain itself.

**Why not.** object_store's credential model is its own type system
(`CredentialProvider`); kernel-rs ships with object_store-0.12 pinned
internally. To bridge Hadoop's `AWSCredentialProviderList` into
object_store 0.12 from Rust would require either rewriting Hadoop's
chain in Rust or fragile FFI. Doing it Scala-side, where
`S3AUtils.createAWSCredentialProviderList` is a known entry point, is
mechanical reflection.

The downside is that long-lived sessions with STS-rotating credentials
would not see rotation events until the engine cache evicts. We accept
this for now because (a) the cache is keyed on a snapshot of the config,
which is sufficient for short-lived sessions and (b) eviction-on-401 is
a follow-up.

## Why row-tracking _synthesis_, not "fall back if not materialised"?

**Decision.** When Delta hasn't materialised `row_id` /
`row_commit_version`, synthesise them natively from `baseRowId +
physical_row_index`.

**Alternative.** Fall back to Spark for any plan that wants row tracking
columns on a non-materialised file.

**Why not.** Row tracking on Delta tables that pre-date the row-tracking
feature flag is exactly the case that needs `baseRowId`-based synthesis.
Falling back in that case means tables in mixed-state (some files
materialised, some not — i.e. tables that pre-date row tracking but have
been touched since) would always fall back. The user-visible result is
"row tracking acceleration only works on tables you wrote from scratch
after enabling row tracking", which is a sharp edge.

Synthesising covers both cases uniformly. In the current kernel-read
path the per-file `baseRowId` (and `defaultRowCommitVersion`) are
supplied JVM-side by `RowTrackingAugmentedFileIndex`, and
`DeltaKernelScanExec` emits `row_id` / `row_commit_version` by name from
kernel's per-file transform plus that base — Delta's `GenerateRowIDs`
strategy expands `_metadata.row_id` into these primitives before Comet
ever sees the plan.

## Why a standalone Cargo manifest in `contrib/delta/native`?

**Decision.** `contrib/delta/native/Cargo.toml` is its own manifest, NOT
part of the workspace.

**Alternative.** Add the crate to the workspace `Cargo.toml`.

**Why not.** kernel-rs internally pins arrow-57. Comet core pins
arrow-58. Putting both in the same Cargo workspace forces resolution
through the workspace's resolver, which would force one or the other.
A separate manifest lets the contrib build against its own arrow version
and surface only Arrow C Data Interface pointers across the boundary.

The cost is that you can't `cargo build` from the root and get the
contrib; you build core (with `--features contrib-delta`) and it
re-exports the contrib via static linking. See [05-build-and-deploy.md](05-build-and-deploy.md).

## Why is `CometCreateArray` declined for type mismatches?

**Decision.** When `CreateArray` is asked to build an array from
elements of different concrete types, decline in the planner and let
Spark do it.

**Why.** Upstream DataFusion's `make_array` is strict about element-type
agreement (`apache/datafusion#22366`). Without the decline, valid
Spark queries that build mixed-type arrays would crash native execution.

We will remove this decline once the upstream issue lands.

---

**Navigation** · [← 03 Native execution](03-native-execution.md) · [↑ Index](README.md) · Next → [05 Build and deploy](05-build-and-deploy.md)
