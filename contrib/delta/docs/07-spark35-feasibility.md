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

# Spark 3.5 + Delta 3.3 support: status

**Status:** SHIPPED. 49/49 contrib Scala tests pass on BOTH
`spark-3.5 + Delta 3.3.2` AND `spark-4.1 + Delta 4.1.0`. The earlier
feasibility prediction (preserved at the end of this doc for the record)
was too pessimistic about a Spark-4-only `_metadata.row_id` dependency.

## Actual cost

Five small changes; total under 100 lines:

1. **`spark/pom.xml`** — moved `<delta.version>` from the `contrib-delta`
   profile into each Spark profile (`spark-4.1` → `4.1.0`, `spark-3.5` →
   `3.3.2`). When `-Pcontrib-delta` is layered onto a Spark profile, the
   matching Delta version is selected automatically.

2. **`spark/src/main/spark-3.5/.../ShimSparkErrorConverter.scala`** — added
   `wrapNativeParquetError` mirroring the `spark-4.x` shim of the same name.
   `QueryExecutionErrors.cannotReadFilesError(Throwable, String)` has the
   same signature in Spark 3.5 so the implementation is identical. (This
   was a pre-existing Comet-core gap that any branch using this branch's
   per-task file-path threading under Spark 3.5 would hit, not strictly a
   contrib-delta issue.)

3. **`CometDeltaTestBase.scala`** — `SparkSession.builder()` instead of
   `org.apache.spark.sql.classic.SparkSession.builder()`. The `classic`
   subpackage is a Spark 4 addition; the unqualified path works on both
   and resolves to the same classic builder under Spark 4.

4. **`dev/verify-contrib-delta-gate.sh`** — extended to assert that
   `-Pspark-3.5,contrib-delta` pulls `delta-spark:3.x` (not 4.x), in
   addition to the existing `-Pspark-4.1,contrib-delta` → `delta-spark:4.x`
   check. Catches a future regression where someone hardcodes a Delta
   version on the wrong Spark.

5. **Native side: zero changes.** `delta-kernel-rs 0.19` reads both
   Delta 3.x and 4.x log formats. The same libcomet works under either
   Spark version.

## Test status

| Spark + Delta | Suites | Tests | Status |
|---|---|---|---|
| Spark 4.1 + Delta 4.1.0 | 4 (Features, Native, ColumnMapping, Coverage) | 49 | ✅ all pass |
| Spark 3.5 + Delta 3.3.2 | 4 (same) | 49 | ✅ all pass |

Including the row-tracking-unmaterialised `_metadata.row_id` test, the
DV-bearing tables tests, column-mapping name + id modes, and the full
24-test SQL-surface accelerator-coverage matrix.

## How to use

```bash
# Spark 4.1 + Delta 4.1 (default)
mvn -Pspark-4.1,contrib-delta -pl spark -am test

# Spark 3.5 + Delta 3.3
mvn -Pspark-3.5,contrib-delta -pl spark -am test
```

Both share the same libcomet (rebuilt once with `--features contrib-delta`).

## Post-mortem: why the feasibility eval was wrong

The original doc identified `_metadata.row_index` / `_metadata.row_id` as
Spark-4-only and predicted row-tracking tests would fall back on Spark 3.5.
**That prediction was wrong.** What actually happens:

- A user query reads `_metadata.row_id`.
- Delta's `GenerateRowIDs` strategy (present in BOTH Delta 3.x and 4.x)
  expands the reference into
  `coalesce(_row-id-col-<uuid>, base_row_id + _tmp_metadata_row_index)`.
- Each of those synthetics is something **we handle natively** in
  `DeltaSyntheticColumnsExec`:
  - `_row-id-col-<uuid>` — emit NULL via the materialised-row-id prefix matcher
  - `base_row_id` — emit per-file Int64 constant from `AddFile.baseRowId`
  - `_tmp_metadata_row_index` — emit per-file Int64 row-position counter
- Delta's strategy fires before Comet sees the plan, so `_metadata.row_id`
  itself never reaches our scan — only the expanded primitives do.

The lesson: when we wrote the eval, we conflated "Spark 4 added new APIs"
with "Delta's strategy uses those APIs". Delta's strategy actually uses
its OWN intermediate columns (`_tmp_metadata_row_index`, `_row-id-col-`,
`base_row_id`), which exist in both Delta 3.x and 4.x. Spark 4's
`_metadata.row_id` is irrelevant to our path.

## Open follow-ups (not in this PR)

- **CI matrix** — wire `dev/verify-contrib-delta-gate.sh` + the 49-test
  contrib suite into GitHub Actions for both `spark-3.5,contrib-delta`
  and `spark-4.1,contrib-delta`. Modeled on PR #3932's
  `.github/workflows/delta_spark_test.yml`. (Tracked separately.)
- **Delta 3.3 regression diff** — port `dev/diffs/delta/4.1.0.diff` to
  `dev/diffs/delta/3.3.2.diff` so `contrib/delta/dev/run-regression.sh`
  can target Delta 3.3's own test suite.
- **Spark 3.4 + Delta 2.4** — would require more shim work (Delta 2.4
  lacks DV / row-tracking entirely; would be a degraded-coverage tier).
  Not currently planned.

## Original feasibility prediction (preserved for the record)

> | Effort tier | Time | Scope |
> |---|---|---|
> | Minimal viable | 2–3 dev-days | spark-3.5 build + most coverage passing, row-tracking degraded |
> | Production-equivalent | 1–2 dev-weeks | full coverage on Delta 3.3, regression diff ported, all 49 contrib tests green |
> | Full multi-version | 3–4 dev-weeks | spark-3.4 + spark-3.5 + spark-4.x all green, separate Delta versions per Spark, CI matrix |

Actual cost: **one dev session, ~2 hours including the post-mortem
investigation.** The bulk of the predicted complexity — Spark-4-only API
gaps, shim overlay, expected test-coverage degradation — didn't
materialise because the load-bearing assumption (that the contrib leaned
on Spark 4's expanded `_metadata` API) was wrong.
