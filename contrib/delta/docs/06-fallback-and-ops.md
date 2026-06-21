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

# Fallback paths, observability, and operations

## Design philosophy: fail safe, surface why

Anywhere this contrib cannot confidently produce identical results to
Spark's Delta reader, the planner declines and lets Spark execute the
scan unchanged. The decline path is uniform — `DeltaScanRule` attaches a
reason and returns `None`, which cleanly drops the Comet boundary so
vanilla Spark's Delta reader handles the scan:

```scala
withFallbackReason(scan, "<human-readable reason>")
return None  // no Comet marker — Spark's Delta reader runs it
```

`withFallbackReason` attaches the reason to the plan's tag metadata,
which Comet's explain-fallback rendering picks up. Users running
`EXPLAIN EXTENDED` on a fallback-affected query see the reason in the
fallback section, e.g.:

```
- Delta native scan declined: native_delta_compat does not support
  encryption config
```

This is the primary observability surface. Operators investigating "why
didn't Comet take over this query" should look at this section first.

## Decline catalogue

The current set of decline points, grouped by reason class. Each entry
documents WHY the decline exists and what would need to change to remove it.

### Correctness — load-bearing, do not remove

| Decline                               | Reason                                                                                                           | Removal path                                                          |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| DV materialisation failure            | Kernel-rs couldn't read or parse a DV file → we don't know which rows are deleted → we cannot safely return data | Cannot be removed; this is "kernel errored, defer to Spark"           |
| Reflective AddFile extraction failure | Couldn't get the file list from the Delta relation → nothing to scan                                             | Cannot be removed; this is "Delta's reflection surface changed shape" |
| Kernel-rs log-replay error            | Kernel returned an error during snapshot resolution → we don't have an authoritative file list                   | Cannot be removed; same class as above                                |
| Phase 6 reader-feature gate           | Currently an empty list; future kernel-rs versions may return reader-feature names we don't yet understand       | Per-feature evaluation as kernel-rs evolves                           |

### Shared Comet limits (apply to any native scan, not Delta-specific)

| Decline                              | Reason                                                     | Removal path                                         |
| ------------------------------------ | ---------------------------------------------------------- | ---------------------------------------------------- |
| Unsupported encryption KMS config    | Comet core's `CometParquetUtils` rejects                   | Implement KMS bridge in Comet core                   |
| Custom Hadoop FS schemes (`fake://`) | `object_store` has no Hadoop FS plugin layer               | Bridge Hadoop `FileSystem` to `object_store` in Rust |
| `ShortType` under default config     | `CometScanTypeChecker` rejects                             | Flip the default after coverage testing              |
| String collation in schema           | Comet core can't evaluate collation-aware ops yet          | Implement in core expression evaluators              |
| Variant struct in schema             | Arrow-rs has `parquet-variant` but Comet hasn't integrated | Integrate `parquet-variant` in Comet core            |

### External

| Decline                           | Reason                                              | Removal path                           |
| --------------------------------- | --------------------------------------------------- | -------------------------------------- |
| `TahoeLogFileIndexWithCloudFetch` | Databricks-proprietary file index; not in OSS Delta | Wouldn't ship in this PR; DBR-specific |

### Workaround, tracked upstream

| Decline                           | Reason                                                   | Removal path                            |
| --------------------------------- | -------------------------------------------------------- | --------------------------------------- |
| `CreateArray` mixed element types | `apache/datafusion#22366` (`make_array` strict on types) | Remove this decline when upstream lands |

### User off-switches

| Switch                                                            | Effect                                                                                      |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `spark.comet.scan.deltaNative.enabled=false`                      | Decline all Delta scans → Spark's reader                                                    |
| `spark.comet.scan.deltaNative.fallbackOnUnsupportedFeature=false` | Raise an error instead of falling back on an unsupported feature (test aid, default `true`) |
| `spark.comet.exec.enabled=false`                                  | Disable Comet entirely → Spark for everything                                               |

All keys live in
`contrib/delta/src/main/scala/org/apache/comet/contrib/delta/DeltaConf.scala`.

## Removed decline gates

Earlier versions of this contrib declined on broader cases; the move to
the kernel-read path (#50, #82) and a series of gate-unblock sweeps
brought them under native execution. The mechanisms named below for the
DV / synthetic / row-tracking cases reflect the **current** kernel-read
path, not the deleted `ParquetSource` + `DeltaSyntheticColumnsExec` /
`DeltaDvFilterExec` stack that originally retired some of them. Removed
gates:

- **Column-mapping `id` mode** — implemented via Delta-ID → parquet-field-ID
  translation in the planner; the analysis-time physical schema is shipped
  to kernel's `with_schema`
- **General Parquet field-ID matching** — handled by kernel's own
  column-mapping resolution
- **Synthetic / `_metadata` columns** — `DeltaKernelScanExec` synthesises
  every output column by name in-worker (kernel's per-file transform +
  partitions + row-tracking primitives)
- **Deletion vectors** — applied executor-side inside `DeltaKernelScanExec`
  via `dv_reader::read_dv_indexes`; deleted rows dropped before they cross
  JNI
- **`enableRowTracking=false` for `row_id` queries** — `row_id` synthesised
  from per-file `baseRowId` (supplied by `RowTrackingAugmentedFileIndex`)
- **`TahoeBatchFileIndex` (DML rewrites), `CdcAddFileIndex`** — the
  exact-subset file list is passed through to the kernel scan
- **Change Data Feed (`readChangeFeed`)** — read kernel-natively via
  `TableChanges` (`CometDeltaCdfScanExec`, #84), split across partitions
  (#2); no longer declined
- **Schema-change-since-analysis** — the analysis-time read schema is
  shipped to kernel's `with_schema` (#78), so a snapshot that changed
  since analysis still reads correctly

## Operational signals

### Per-query

- `EXPLAIN EXTENDED` — see "Comet Native Plan Info" section for fallback
  reasons (covered above)
- Comet's existing scan metrics (`scan_time_ms`, `output_rows`,
  `output_batches`) work unchanged for `CometDeltaNativeScan` — they're
  reported through the same DataFusion metric mechanism

### Per-driver

- Driver-side engine cache size: not currently exposed; would be a useful
  follow-up metric. The cache lives behind `engine::engine_cache()` (a
  `OnceLock<Mutex<EngineCacheState>>` static, whose `map` is
  `HashMap<EngineKey, (Arc<DeltaEngine>, u64)>` keyed on
  `(scheme, authority, DeltaStorageConfig)`)
- kernel-rs scan-planning time: implicit in `CometDeltaNativeScanExec`'s
  driver-side latency, not separately reported

### Cluster-wide

- `pthread_create EAGAIN` in driver logs would indicate the engine cache
  is leaking — the fix that landed in this PR addresses the known cause
  (per-scan engine creation without caching). If it returns, investigate
  cache eviction policy (currently no TTL) vs. legitimately high storage
  diversity
- `ServiceConfigurationError` in executor logs typically means the
  `comet-spark` JAR being used by the JVM doesn't match the `libcomet`
  dylib being loaded — usually caused by partial upgrades or stale
  classpaths during iteration. The build invariants in
  [05-build-and-deploy.md](05-build-and-deploy.md) cover the correct combinations

## Known-safe configuration changes operators can make

| Config                                                  | Default                      | Notes                                                             |
| ------------------------------------------------------- | ---------------------------- | ----------------------------------------------------------------- |
| `spark.comet.scan.deltaNative.enabled`                  | `true` (when contrib loaded) | Per-query off-switch via SET                                      |
| `spark.comet.scan.deltaNative.dataFileConcurrencyLimit` | `1`                          | Per-Spark-task concurrency reading Delta data files (2–8 typical) |
| `spark.comet.delta.cdf.maxPartitions`                   | `8`                          | Max Spark partitions a Change Data Feed read splits into          |
| `spark.comet.batchSize`                                 | (Comet default)              | Same setting; controls Arrow batch size                           |

Delta-specific tuning is the on/off switch plus the data-file concurrency
and CDF partition-count knobs above (all defined in `DeltaConf.scala`).

## Debug entry points

For investigating contrib behaviour locally:

1. **Decline reasons**: `EXPLAIN EXTENDED` against the affected query
2. **Native plan shape**: enable DataFusion explain via Comet's
   `spark.comet.debug.enabled` — the resulting trace shows the wrapping
   stack actually built for each partition
3. **kernel-rs interaction**: `RUST_LOG=delta_kernel=debug` on the
   executor surfaces snapshot resolution and DV reads
4. **JVM↔Native bridge**: existing Comet log levels; nothing
   Delta-specific

For production investigation:

1. Check the `Comet Native Plan Info` section first
2. Check driver logs for kernel-rs errors (they bubble up as warnings
   before triggering decline)
3. Check the engine cache hasn't been exhausted (driver logs for
   `pthread_create`)
4. Compare a Comet-on vs Comet-off run of the same query if a
   correctness issue is suspected

The regression diffs in `contrib/delta/dev/diffs/` (one per Delta
version: `3.3.2.diff`, `4.0.0.diff`, `4.1.0.diff`, driven by
`contrib/delta/dev/run-regression.sh`) are the canonical reference for
"what should work" — if a Delta upstream test isn't in the diff and isn't
passing with the contrib enabled, that's either a missed decline gate or
a real bug.

---

**Navigation** · [← 05 Build and deploy](05-build-and-deploy.md) · [↑ Index](README.md)
