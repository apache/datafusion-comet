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

# Comet Delta Contrib — Design Documentation

This directory contains the design documentation for the native Delta Lake
scan integration in Comet. It is written for engineers who:

- Know Spark's DataSource V2 interfaces (TableProvider / Scan / Batch /
  InputPartition) at a working level
- Have *not* read Comet's internals before
- Have *not* worked with `delta-kernel-rs` before
- Want to understand the *design*, not just the *code*

If you only have ten minutes, read [01-overview.md](01-overview.md).

## Reading order

| # | Document | Audience |
|---|---|---|
| 01 | [Overview](01-overview.md) | Everyone — start here |
| 02 | [Planning](02-planning.md) | Engineers reviewing the Scala-side planning rule and proto serde |
| 03 | [Native execution](03-native-execution.md) | Engineers reviewing the Rust-side execution plan tree |
| 04 | [Design decisions](04-design-decisions.md) | Reviewers asking "why didn't you just…" — read after 01 |
| 05 | [Build and deploy](05-build-and-deploy.md) | Operators packaging and deploying Comet with Delta support |
| 06 | [Fallback and ops](06-fallback-and-ops.md) | Operators investigating fallbacks and observability |

## One-paragraph summary

This contrib makes Apache Comet read Delta Lake tables natively in Rust
without going through Spark's `DeltaParquetFileFormat`. It plugs into
Comet's existing plan-rewrite rule (`CometScanRule`) via reflection,
recognises Delta `LogicalRelation`s, and substitutes them with a native
scan node. Driver-side, `delta-kernel-rs` resolves the snapshot and
produces a file list; the result is encoded into a typed proto variant
and shipped to executors. Executor-side, a DataFusion `ExecutionPlan`
tree handles parquet reads (with field-ID matching for column mapping),
deletion-vector filtering, column-mapping rename, and synthesis of
Delta's "virtual" columns (`row_id`, `__delta_internal_is_row_deleted`,
etc.). The contrib is gated behind a Maven profile and a Cargo feature;
default Comet builds are unaware of it.

## Conceptual model in one diagram

```
Catalyst plan with Delta LogicalRelation
        │
        ▼  (driver, JVM)
[DeltaScanRule]  ──→  decline?  ──yes──→  Spark's Delta reader (fallback)
        │
        │ no
        ▼
delta-kernel-rs scan resolution
        │
        ▼
DeltaScan proto (common block + per-task arrays)
        │
        ▼  (executor, JNI boundary)
contrib_delta_scan::build_plan (Rust)
        │
        ▼  builds DataFusion ExecutionPlan tree:
ParquetSource
  → ProjectionExec rename       [optional, if CM physical != logical names]
  → DeltaSyntheticColumnsExec   [if any emit_*]
       OR                       (the two are mutually exclusive)
    DeltaDvFilterExec           [else if any task has DV]
  → ProjectionExec reorder      [optional, if synthetics not a suffix]
        │
        ▼
Arrow RecordBatch stream → Spark ColumnarBatch (via Comet's Arrow bridge)
```

## Glossary

- **kernel-rs** — `delta-kernel-rs`, the Delta team's official Rust crate
  for Delta protocol parsing, snapshot resolution, and DV materialisation
- **DV** — Deletion Vector, Delta's mechanism for soft-deletes (a bitmap
  of deleted row indexes stored alongside the parquet files)
- **CM** — Column Mapping, Delta's mechanism for renaming columns without
  rewriting parquet (`name` or `id` mode)
- **Synthetic columns** — Delta's internal "virtual" columns that aren't
  stored in parquet but are computed at read time:
  `__delta_internal_row_index`, `__delta_internal_is_row_deleted`,
  `row_id`, `row_commit_version`
- **Contrib** — A Maven-profile-gated extension to Comet, with no compile
  or runtime impact on default builds (model follows the Iceberg contrib
  in this repo)
- **Plan-rewrite rule** — A Spark `SparkSessionExtensions` strategy that
  pattern-matches against logical plan nodes and produces physical plans;
  Comet's existing pattern, extended here for Delta

## Where the code lives

```
spark/src/main/scala/org/apache/comet/rules/DeltaIntegration.scala   # reflection bridge in core
spark/src/main/scala/org/apache/comet/rules/CometScanRule.scala      # one arm calling DeltaIntegration
native/proto/src/proto/operator.proto                                # DeltaScan proto variant
native/core/src/execution/planner/contrib_delta_scan.rs              # native dispatcher arm

contrib/delta/src/main/scala/org/apache/comet/contrib/delta/...      # all contrib Scala
contrib/delta/native/src/...                                         # all contrib Rust
contrib/delta/dev/diffs/delta/4.1.0.diff                             # regression diff vs Delta 4.1
contrib/delta/dev/run-regression.sh                                  # regression driver
```

The Scala side has roughly 1500 lines of contrib code; the Rust side has
roughly 2500 lines. Core touchpoints (default-build code) total ~40
lines of net new logic plus the proto variant.

## Reviewing this PR

If you are reviewing the PR that introduces this contrib (PR #4366), the
suggested reading sequence is:

1. [01-overview.md](01-overview.md) here
2. [04-design-decisions.md](04-design-decisions.md) here — answers most "why" questions
3. The Code review strategy in the PR body
4. Selected source files per the strategy

The remaining design documents (02, 03, 05, 06) are more useful as
on-ramp material *after* the PR has merged, for engineers picking up the
code later.
