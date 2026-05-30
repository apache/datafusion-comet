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

**Why not.** This contrib needs a *small* set of core touchpoints that
must evolve in lockstep with the contrib (`PlanDataInjector.opStructCase`,
the `OpStruct::DeltaScan` variant, the `contrib_delta_scan` dispatcher
arm). Splitting repos would version-couple them anyway; same-repo is
strictly simpler.

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

## Why a typed proto variant, not a generic envelope?

**Decision.** `OpStruct::DeltaScan` is a first-class variant of the
operator proto, with typed `DeltaScanCommon` and `DeltaScanTask`
messages.

**Alternative.** Add a generic `OpStruct::ContribOp { kind: string,
payload: bytes }` envelope and decode `payload` based on `kind`.

**Why not.** PR #3932 prototyped the envelope approach. Two problems:

1. Every dispatch becomes a string compare + dynamic payload decode.
   With a typed variant, `match` on the `OpStructCase` enum gives
   O(1) dispatch, and the planner gets to use the proto-generated Rust
   struct directly.
2. Loss of schema-level documentation. The proto file becomes
   self-describing for typed variants; the envelope variant requires
   every consumer to maintain its own out-of-band decoder catalogue.

The downside of the typed-variant approach is that adding a new contrib
requires a one-line addition to the proto. That's a one-time cost per
contrib and an obvious place to do code review.

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

## Why reflection bridges, not abstract types in core?

**Decision.** `DeltaIntegration` in core is a thin reflection bridge.
The actual Delta logic lives entirely in `contrib/delta/...`.

**Alternative.** Define a trait `ContribDeltaSupport` in core, have the
contrib implement it, wire via `ServiceLoader` or similar.

**Why not.** A trait in core would either:

- Force the trait to be in core's compile classpath (creating a
  compile-time dependency from core to contrib, the wrong direction), OR
- Force the contrib classes into the same JAR as core (defeating the
  point of contrib being optional)

Reflection bridges achieve the same dispatch with zero compile-time
coupling. The cost is one `Method` handle lookup per JVM, which we
cache in a `@volatile var`.

## Why cache reflection method handles?

**Decision.** Every reflective bridge resolves `Class.forName` and
`getMethod` exactly once per JVM, caches the result, and reuses it.

**Why.** Reflection lookup is slow enough that doing it per call would
show up in hot paths. Driver-side bridges fire once per query plan;
executor-side bridges fire per task. Per-task is hot enough to matter,
and the cache pattern is cheap.

This applies to the S3A credential bridge as well — we resolve
`S3AUtils.createAWSCredentialProviderList` once and reuse the
`Method` handle for every kernel-rs engine creation.

## Why does each FileGroup hold one file when synthetics emit?

**Decision.** When any `emit_*` flag is set, the parquet
`FileScanConfig` is built with one `FileGroup` per file.

**Alternative.** Let the parquet reader pack files into shared groups
for better parallelism.

**Why not.** `DeltaSyntheticColumnsExec` indexes per-partition state
vectors `(deleted_row_indexes, base_row_ids, default_row_commit_versions)`
by the DataFusion partition index. One file per FileGroup means
"partition index = file index", which is what makes the index lookup
correct. With shared groups, multiple files would map to one partition
index and the lookup would return the wrong file's metadata.

We pay a parallelism cost only when synthetics are emitted (which is a
minority of queries — primarily MERGE/UPDATE/DELETE rewrite plans and
queries that explicitly select `row_id`).

## Why a `DeltaDvFilterExec` and not a Spark filter on top?

**Decision.** Filter deleted rows in the native plan, between
`ParquetSource` and the synthetic-column exec.

**Alternative.** Read all rows natively, ship to the JVM, filter there.

**Why not.** DVs can mark significant fractions of a file as deleted
(MERGE-heavy workloads can easily hit 30%+). Filtering natively avoids
serialising and crossing JNI with rows that are about to be dropped.

It also keeps the synthetic-column logic correct: `is_row_deleted` is
populated by walking the same DV index list, so emitting that column
naturally falls out of the same exec we built for filtering.

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

## Why row-tracking *synthesis*, not "fall back if not materialised"?

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

Synthesising covers both cases uniformly. The cost is the per-task
`base_row_id` field in the proto and the per-batch arithmetic in the
synthetic-columns exec.

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
