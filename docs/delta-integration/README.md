# Comet Native Delta Lake Integration

This folder is a *companion* design / implementation document set for the
`delta-kernel-phase-1` branch of Comet. It describes — end to end and in
detail — what the branch ships, how it works mechanically, and which
real-world scenarios it accelerates vs. defers to the stock Spark + Delta
reader.

It is intended to be readable both for:

  - **Comet contributors** who already know the project's planner / serde /
    native-execution layout and want a top-down map of the Delta-specific
    pieces and the trade-offs that landed.
  - **Intermediate / advanced Spark engineers** who don't necessarily know
    Comet internals but want enough context to evaluate, debug, or extend
    Comet's Delta path against their own workloads.

The docs deliberately duplicate (and go beyond) the user-facing material
in `docs/source/user-guide/latest/delta.md` and the contributor notes in
`docs/source/contributor-guide/delta-spark-tests.md`. Those files target a
condensed user / CI audience; this folder is the architectural reference.

## Reading order

1. **[features-and-limitations.md](features-and-limitations.md)** — the
   capability matrix. What works natively, what falls back to Spark, every
   knob exposed via `spark.comet.*`, and every case where Comet
   *deliberately* declines acceleration.
2. **[architecture.md](architecture.md)** — high-level component diagram
   and the end-to-end lifecycle of a Delta query: detection ➜ planning ➜
   serialization ➜ split-mode injection ➜ native execution ➜ DV / column
   mapping post-processing.
3. **[scala-driver-side.md](scala-driver-side.md)** — line-of-sight tour
   of every Scala component on the JVM driver: `CometScanRule` (detection
   + DV-wrapper stripping + row-tracking rewrite), `CometDeltaNativeScan`
   serde (file-list assembly + partition pruning + split-mode protobuf),
   `CometDeltaNativeScanExec` (per-partition serialization + DPP),
   `DeltaPlanDataInjector`, `DeltaReflection`, and the auto-config rule
   that flips Delta's DV strategy.
4. **[native-execution-side.md](native-execution-side.md)** — the Rust
   side: kernel quarantine and JNI boundary, log replay, predicate
   translation, the `OpStruct::DeltaScan` planner arm,
   `DeltaDvFilterExec`, the column-mapping filter rewriter, and the
   `IgnoreMissingFileSource` wrapper.
5. **[wire-format.md](wire-format.md)** — protobuf shapes
   (`DeltaScanCommon`, `DeltaScan`, `DeltaScanTask`, `DeltaScanTaskList`)
   and the *split-mode* serialization scheme that lets per-partition file
   lists travel alongside a single common header.
6. **[edge-cases-and-fallbacks.md](edge-cases-and-fallbacks.md)** — the
   long tail of correctness traps the branch addresses: deletion vectors
   in their three guises, column mapping (including nested physical
   names), row tracking with `baseRowId` synthesis, time travel,
   shallow-clone path quirks, AQE/DPP interaction, streaming progress
   metrics, encryption, `ignoreMissingFiles`.
7. **[development-and-testing.md](development-and-testing.md)** — the
   test surface added by the branch (unit suites, regression suites,
   benchmarks), the `dev/run-delta-*` helper scripts, the per-version
   `dev/diffs/delta/*.diff` patches against the upstream Delta test
   harness, and the GitHub Actions workflows that exercise all of it.

## Status (as of this branch)

The branch is **`delta-kernel-phase-1`**, the first phase of Comet's
native Delta integration. "Phase 1" reflects the historical slice of work
this PR opens with; later phases (DV, column mapping, row tracking,
type widening, CDF, MERGE/UPDATE post-join scans) have been folded into
the same branch, so it ships as a single coherent capability.

The integration is **opt-in**: nothing happens until the user enables
`spark.comet.scan.deltaNative.enabled=true` (and they're on a Spark
session that has Delta on the classpath at all). The fallback behavior
is to leave the scan untouched — so a Comet plugin that turns this flag
*off* behaves identically to a Comet plugin without any of the code in
this branch.

## What this folder does *not* cover

  - Generic Comet operator coverage (project, filter, aggregate, join,
    shuffle, sort, etc.) — those are documented in the main user/contrib
    guides under `docs/source/`.
  - DataFusion ParquetSource internals — touched on briefly in
    `native-execution-side.md` only where Delta-specific behavior layers
    on top.
  - Delta Lake protocol fundamentals — for the transaction-log /
    AddFile / RemoveFile / DeletionVector model, see the upstream Delta
    [PROTOCOL.md](https://github.com/delta-io/delta/blob/master/PROTOCOL.md).

> **Note:** This folder is documentation only; nothing here is referenced
> by build, CI, packaging, or runtime.
