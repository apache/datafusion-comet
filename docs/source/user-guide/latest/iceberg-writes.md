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

# Iceberg Writes: Comet's Split-Operator Plan (Experimental)

**This feature is experimental and disabled by default.** Enable it only after validating it
against your own workloads.

## Overview

Spark writes an Iceberg table through a single physical operator that combines data-file
writing with metadata writing, committing, and catalog validation. Because that operator sits
outside Spark's Adaptive Query Execution (AQE), the sub-query feeding the write — the scans,
projects, sorts, and exchanges producing the rows — cannot be re-planned at runtime.

When `spark.comet.write.iceberg.splitOperator.enabled=true`, Comet rewrites eligible Iceberg
writes into two operators:

1. **`IcebergWrite`** — writes the data files on the executors, exactly as iceberg-java does
   today, and returns each task's serialized commit message. This operator and the sub-query
   feeding it run inside AQE.
2. **`IcebergCommit`** — collects the commit messages on the driver and performs the normal
   Iceberg commit (including commit-time validation), outside AQE, exactly once.

Data files are still written by iceberg-java; only the plan shape changes. The split makes the
write's input visible to AQE and to Comet's columnar rules, and it is the groundwork for a
planned follow-up in which Comet writes the data files natively via
[iceberg-rust](https://github.com/apache/iceberg-rust).

## Configuration

Standard Comet + Iceberg setup (see [`iceberg.md`](iceberg.md)) plus the write-side toggle:

```
# Standard Comet / Iceberg wiring
spark.plugins=org.apache.spark.CometPlugin
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.<name>=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.<name>.type=hadoop                          # or hive / glue / rest / ...
spark.sql.catalog.<name>.warehouse=...

# Split-operator plan (experimental, off by default)
spark.comet.write.iceberg.splitOperator.enabled=true
```

## Supported operations

The split-operator plan is supported on every Spark version Comet supports, with identical
coverage on each:

- `INSERT INTO` / DataFrame `append` (`AppendData`)
- `INSERT OVERWRITE`, static and dynamic (`OverwriteByExpression`,
  `OverwritePartitionsDynamic`)
- Copy-on-write `DELETE` / `UPDATE` / `MERGE` (`ReplaceData`)

## When Comet falls back to Spark's write operator

The rewrite is skipped — and the write runs through Spark's stock combined operator — when:

- `spark.comet.write.iceberg.splitOperator.enabled` is `false` (the default);
- the write is not an Iceberg `SparkWrite` (any other V2 data source);
- the table uses merge-on-read: delta writes (Iceberg `WriteDelta`) are not intercepted;
- the write requires Spark's commit coordinator, which Comet's per-task commit protocol does
  not use;
- Comet cannot reflect the Iceberg internals needed to build the two-operator plan (for
  example an unrecognised write class or a `ReplaceData` projection it cannot map).

In every fallback case the write is planned as if Comet were absent; there is no correctness
trade-off, only no plan change.
