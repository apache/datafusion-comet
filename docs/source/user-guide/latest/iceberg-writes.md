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

# Accelerating Apache Iceberg V2 Writes using Comet

## Overview

Comet's first layer for accelerating Iceberg V2 writes is a **split-operator plan**: Comet
rewrites Iceberg's single `V2ExistingTableWriteExec` command into a pair of operators — a
**committer** and a **writer** — so that AQE and Comet's columnar rules can see
and re-optimise the data sub-query. The actual file-writing work still runs through Iceberg's
stock JVM writer; nothing in this layer changes Iceberg's commit semantics. A further toggle for
delegating the per-task parquet write to iceberg-rust is planned but not part of this layer.

The split-operator plan is off by default; turn it on per session. Iceberg-Java writes the data
unchanged when it's off.

## What changes about the Iceberg plan

### Iceberg's stock V2 plan

```
V2ExistingTableWriteExec(write, query)        ← single V2 command; commit + write together
└── <query>                                    ← data sub-query (scans, shuffles, ...)
```

This is a single command. Spark's `InsertAdaptiveSparkPlan` runs it as a leaf from AQE's
perspective — AQE never sees the data sub-query inside, so Comet's columnar rules can't convert
the scans / shuffles inside it either.

### Comet's split plan

```
IcebergCommitExec(batchWrite, refreshCache)   ← committer; runs BatchWrite.commit()
└── AdaptiveSparkPlanExec                      ← AQE bubble (Spark inserts this)
    └── IcebergWriteExec(batchWrite)    ← writer (UnaryExecNode); per-task write
        └── <query>                            ← data sub-query: now visible to AQE / Comet
```

The committer keeps Iceberg's commit semantics intact. The writer is a normal
`UnaryExecNode`, so AQE wraps it whenever the data sub-query has a shuffle, and Comet's
standard `transformUp` rules can convert the scans / projects / sorts / exchanges inside the
AQE bubble to their Comet counterparts (`CometScan`, `CometProject`, `CometColumnarExchange`,
…).

### What flows between the two execs

The writer produces **one row per Spark task** with a single `BINARY` column:
Java-serialised bytes of an Iceberg `SparkWrite$TaskCommit(DataFile[])`. The committer
`executeCollect()`s that RDD, deserialises each row's bytes back into a `WriterCommitMessage`,
and hands the resulting array to `BatchWrite.commit(messages)` — the same call Iceberg-Java
would have made internally.

## Configuration

Standard Comet + Iceberg setup ([`iceberg.md`](iceberg.md)) plus the split-operator toggle:

```
# Standard Comet / Iceberg wiring
spark.plugins=org.apache.spark.CometPlugin
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.<name>=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.<name>.type=hadoop                              # or hive / glue / rest / ...
spark.sql.catalog.<name>.warehouse=...

# Comet write toggle (off by default)
spark.comet.write.iceberg.splitOperator.enabled=true
```

`IcebergSparkSessionExtensions` is **mandatory on Spark 3.4** for `UPDATE` / `MERGE` on V2
tables — Spark 3.4's stock planner rejects `UPDATE TABLE` on V2 sources, and Iceberg's analyzer
extension (`RewriteUpdateTable` / `RewriteMergeIntoTable`) is what provides the rewrite. On
Spark 3.5+ the extension is optional but recommended (Spark added native row-level operation
support in 3.5). `INSERT INTO` / `INSERT OVERWRITE` / `DELETE FROM` work without the extension
on every Spark version.

If `splitOperator.enabled=false`, Comet leaves Iceberg's stock plan alone — every write goes
straight through Iceberg-Java.

## Spark version compatibility

Copy-on-write coverage is identical across versions (`UPDATE` / `MERGE` on 3.4 requires
`IcebergSparkSessionExtensions`). Merge-on-read (`WriteDelta`) is intentionally left on Spark's
default path: the per-task `DeltaWriter` is row-dispatched and no native acceleration is
planned, so routing it through the split-operator plan would add planning complexity for no
realisable benefit.

| Capability                                  | 3.4   | 3.5 | 4.0 |
|---------------------------------------------|-------|-----|-----|
| `INSERT INTO` (`AppendData`)                | ✅    | ✅  | ✅  |
| `INSERT OVERWRITE` (static + dynamic)       | ✅    | ✅  | ✅  |
| `DELETE FROM` copy-on-write (`ReplaceData`) | ✅    | ✅  | ✅  |
| `UPDATE` / `MERGE INTO` copy-on-write       | ✅[¹] | ✅  | ✅  |

- [¹] Requires `IcebergSparkSessionExtensions` (see Configuration above).

## Tests

- **`CometIcebergWriteActionSuite`** — end-to-end scenarios against a temporary Hadoop catalog
  covering the copy-on-write V2 logical-write variants on the split-operator path, plus parity
  vs Iceberg-Java's writer, the disabled-config fallback, and the commit-once invariant. Passes
  on Spark 3.4 / 3.5 / 4.0 with `IcebergSparkSessionExtensions` loaded.

## Abort behaviour

The writer calls `writer.abort()` per task on failure to release task-level resources.
The committer (`IcebergCommitExec.run()`) calls `BatchWrite.abort(messages)` if `commit()`
raises.

If task writers stage files locally but their commit messages never reach the driver (e.g.
driver crash mid-collect), the staged Parquet files become **unreferenced orphans**. Iceberg's
catalog-level `RemoveOrphanFiles` action reaps these on the next maintenance run. Schedule
`RemoveOrphanFiles` if you want to avoid storage drift on failed writes. This diverges from
Spark's per-task abort behaviour and is a deliberate trade-off in favour of a simpler
driver-side commit loop.

---

# Developer notes

The sections below document architectural decisions, interactions with Spark internals, and
load-bearing implementation details. They're not required reading for *using* the writer — only
for working on it.

## How the split survives AQE re-planning

AQE calls `reOptimize(inputPlan.logicalLink.get)` every time a query stage materialises and
re-runs the planner on the result. The trick is making sure the writer stays in
place and the commit stays exactly one — not zero, not two.

Two design choices keep this stable:

1. **File writer exec uses a stable logical anchor.** `IcebergWriteLogical` is a
   Catalyst `UnaryNode` that wraps the data sub-query plus the `BatchWrite` reference. The
   planner's `setLogicalLink` pins the writer to this anchor (not to the surrounding
   `ReplaceData` / `AppendData` / etc. logical node) so AQE's `reOptimize` re-plans only the
   data query. Without the anchor AQE would either re-fire the surrounding Iceberg write
   logical node (duplicating the commit) or strip the writer away (no write
   happens).
2. **The `BatchWrite` instance is shared between committer and writer.** Iceberg's
   `Write.toBatch()` returns a freshly-constructed `BatchWrite` on every call; for CoW DML the
   instance holds the scan state and emitted-file tracking, so the writer's writer
   factory and the committer **must** operate on the same instance for `OverwriteFiles`'
   serialisable-isolation validation to walk the right snapshot range. The strategy calls
   `toBatch` once and threads the result through both execs.

## Why a custom logical anchor (`IcebergWriteLogical`)

Without `IcebergWriteLogical`, AQE's `reOptimize` would either:

- **Vanish** the writer, if its logical link points at the data sub-query (which
  carries no write semantics). The re-planned tree would have no writer at all.
- **Duplicate** the committer, if the writer's logical link points at the
  surrounding logical write (`ReplaceData` / `AppendData` / …). The strategy would re-fire the
  entire two-op tree on every AQE iteration, each iteration emitting a fresh
  `IcebergCommitExec` — and Iceberg's `OverwriteFiles` validation then sees the prior
  iteration's file as a newly-added conflicting file and fails.

`IcebergWriteLogical` sits between the two and lets the strategy re-emit a fresh
`IcebergWriteExec` on every iteration without touching the committer.

## Iceberg 1.5.2 (Spark 3.4) logical-write skew

Iceberg 1.5.2 (paired with Spark 3.4) ships its own `ReplaceIcebergData` logical node because
Spark 3.4 lacks native row-level operation support. Field shape is identical to Spark 3.5+'s
stock `ReplaceData`, so `IcebergWriteStrategy` matches it by FQCN and the extracted tuple feeds
the same dispatcher.
