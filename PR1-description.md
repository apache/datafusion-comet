# feat(contrib): introduce contrib extension SPI

Branch: `comet-contrib-spi`
Base: `main`
Commits: 18 (one architectural cluster + four review-fix rounds + a doc-completeness pass + the symmetric-distribution refactor)

## Summary

Adds the infrastructure for *contrib extensions* — self-contained modules that ship as
part of Comet's release but plug in via a stable SPI rather than being hard-wired into
core. Core gains no functional behaviour change; with no contrib feature/profile
enabled, the dispatch hooks are no-ops and `registered_contrib_kinds()` is empty.

The first concrete contrib (`contrib/example/`) is a worked reference: Scala extension
classes, a `CometScanRuleExtension` implementation, a Rust rlib registering a
`ContribOperatorPlanner`, a `META-INF/services/` entry, its own `.proto` schema with
prost-build wiring, and unit tests covering registration, proto decode round-trip, and
the error path. New contrib authors copy this directory layout.

This is the first of a two-PR sequence. **PR2** (a follow-up off `main` after this
lands) will port Comet's existing delta-kernel-rs integration onto this SPI and into
`contrib/delta/`. The SPI shape has been validated end-to-end against a real Delta port
on a separate branch — see the testing section below.

## Distribution model

Both halves of a contrib are bundled into Comet's released artifacts at build time when
their matching flags are enabled. Contribs are not independently distributable — they
ship inside Comet's release.

- **JVM half** — Scala/Java sources under `contrib/<name>/src/main/scala/`, compiled
  INTO `comet-spark.jar` by activating `-Pcontrib-<name>` on `spark/pom.xml`. The
  contrib's `META-INF/services/` entries go along for the ride; ServiceLoader at
  runtime discovers them from inside `comet-spark.jar` itself. The contrib has a tiny
  `<packaging>pom</packaging>` Maven pom that exists solely to enumerate external
  deps (e.g., a Delta contrib's pom would carry `<dependency>io.delta:delta-spark</dependency>`).
- **Native half** — a Rust `rlib` crate (NOT `cdylib`) linked INTO `libcomet` via the
  matching `--features contrib-<name>` Cargo flag on `native/core`. The contrib's
  `#[ctor]` registers its operator planners during library load.

`mvn install -Pcontrib-example && cargo build --features contrib-example` produces a
Comet build that includes the example contrib in both `comet-spark.jar` and `libcomet`.
A vanilla `mvn install && cargo build` produces a build with zero contrib surface.

The wire format between JVM and native uses a single generic envelope on the operator
proto, `ContribOp { kind, payload }`. Core's planner dispatches by `kind`; the contrib's
native crate registers planners against the same `kind` string the contrib's JVM code
writes into the proto.

## Architecture

```
                       JVM                                Native (single libcomet)

  CometSparkSessionExtensions.apply()         #[cfg(feature = "contrib-example")]
    │                                         extern crate comet_contrib_example;
    ├─ injectQueryStagePrepRule(                            │
    │    CometScanRule._apply                               ▼
    │      ├─ CometExtensionRegistry.load()    contrib's #[ctor] runs at lib load
    │      │    (lazy, first invocation)                    │
    │      │    discovers META-INF/services                 ▼
    │      │    inside comet-spark.jar         comet_contrib_spi::registry (ArcSwap)
    │      │                                   { "delta-scan"           → DeltaPlanner,
    │      ├─ preTransform fold pass             "example-no-op"        → NoOpPlanner,
    │      │   (V1 only, gated on                "example-constant-scan"→ ConstantScanPlanner }
    │      │   COMET_NATIVE_SCAN_ENABLED)                   │
    │      │                                                │
    │      └─ per-scan dispatch:               core's `OpStruct::ContribOp` arm:
    │           iterate registered exts          lookup_contrib_planner_by_kind(co.kind)
    │           first match wins                 .plan(ctx, co.payload, children)
    │                                                       │
    └─ injectQueryStagePrepRule(                ctx: ContribPlannerContext, gives the
         CometExecRule._apply                     contrib core's planner services:
           ├─ CometExtensionRegistry.load()       - session_ctx
           │    (lazy mirror of above)            - build_physical_expr
           │                                      - convert_spark_schema
           └─ (allExecs ∪                         - prepare_object_store
               CometExtensionRegistry              - build_parquet_datasource_exec
                 .mergedSerdes)
                .get(op.getClass)
       )
                       ───── wire format ─────
                           OpStruct.contrib_op {
                             kind:    "delta-scan",
                             payload: <contrib-private bytes,
                                       decoded by the contrib's own prost::Message>,
                             reserved 3 to 9;
                           }
```

## What's in this PR (18 commits)

### Foundational SPI (5 commits)

| Commit | What |
|---|---|
| `51eb0fff` | `ContribOp { kind, payload }` proto envelope; Rust planner registry SPI |
| `f448693b` | Native `OpStruct::ContribOp` dispatcher arm in `planner.rs` |
| `f23500df` | Scala SPI: `CometScanRuleExtension`, `CometOperatorSerdeExtension`, `CometExtensionRegistry` |
| `42234b96` | `CometScanRule` (V1 + V2) and `CometExecRule` consult the registry |
| `8b694715` | `CometSparkSessionExtensions.apply` hooks the registry |

### Worked-reference contrib (3 commits)

| Commit | What |
|---|---|
| `d1553b55` | Rust half of `contrib/example/`: rlib crate, `#[ctor]` registration; introduced `native/contrib-spi/` leaf crate to break a cyclic dep |
| `5cb7099a` | JVM half of `contrib/example/`: Scala extension, ServiceLoader entry, integration test |
| `8508ec50` | First version of the contributor guide |

### SPI shape refinements (2 commits)

| Commit | What |
|---|---|
| `e018076d` | Refinements from a Delta-port confidence check: `preTransform` tree-level hook on `CometScanRuleExtension`, proto layer in `contrib/example/`, class-keyed dispatch convention documented |
| `14e49448` | `ContribPlannerContext` trait + `ParquetDatasourceParams` argument bundle (SPI gap #4 — see "Delta-port confidence check" below) |

### Review-fix rounds (4 commits)

| Commit | What |
|---|---|
| `8930b698` | First review pass: test isolation, V2-asymmetry doc, `#[non_exhaustive]` markers, `preTransform` corruption guard, `#[ctor]` panic safety, drop `contrib-example` from default features, gate registry load lazily, cache mergedSerdes, multi-extension dispatch semantics, `prepare_object_store` returns `Path`, gate preTransform on `COMET_NATIVE_SCAN_ENABLED`, 16 MiB payload cap, "none discovered" diagnostic |
| `68fff43f` | Second pass: `CometExecRule` self-loads, stale docstring, dead doc refs, `Display` wildcard fix, corruption-guard rewrite (identity check, class-changing replacements caught), `ContribOp` size guard ordering, encryption-asymmetry positional-arg test, owned `String` for session_timezone, production-canary `#[cfg]` test |
| `e4e6e6c6` | Third pass: `IdentityHashMap` survivors set, `synchronized` load() publication order, empty/whitespace kind rejection, doc accuracy, scope notes |
| `6652963c` | Fourth pass: identity survivors + cost comment, `resetForTesting` synchronized, doc trims, whitespace kind rejection, `#[cfg(not(any(...)))]` form, public `resetForTesting`, wildcard-arm comment, `Display` debug repr, `ContribOp` proto `reserved` block, payload cap doc |

### Contributor guide completeness (2 commits)

| Commit | What |
|---|---|
| `91c40e0a` | Comprehensive rewrite: prerequisites, JVM-side proto guidance, full `plan()` body walkthrough using every `ContribPlannerContext` method, `CometOperatorSerde[T]` contract, diagnostics story, multi-Spark-version, end-to-end testing recipe, Cargo-canary maintenance note |
| `2c46552c` | Second-pass review fixes |

### Architectural pivot to symmetric distribution (2 commits)

| Commit | What |
|---|---|
| `cf5253ed` | Bundle JVM half INTO `comet-spark.jar` via Maven profile. Mirrors the native side's "Cargo feature pulls rlib into libcomet" model. No more separate contrib JARs. ~70 lines of protobuf-shading boilerplate deleted from the contributor guide (shading now handled automatically by `comet-spark`'s existing shade execution). |
| `c7656fcc` | Deps-only pom per contrib so contribs like Delta can pull in external Maven deps (e.g. `delta-spark`) without recreating a Maven reactor cycle. Registry primitive: `RwLock<HashMap>` → `ArcSwap<HashMap>` for lock-free reads on the dispatch hot path. |

## Notable design decisions

### Why bundle into one artifact?

Previously the contrib's JVM half shipped as a separate Maven JAR users would
`--packages` or `--jars` onto their classpath. That asymmetry made no sense given the
native side already requires a Comet rebuild (Cargo feature flag) for the contrib to
work — pretending the JVM half was distributable independently was a fiction. The
symmetric design has one artifact per side, both varying based on which contribs were
enabled at Comet build time. This eliminated the protobuf-shading recipe that
externally-published contrib JARs needed (which was the single biggest source of doc
complexity).

### Why a separate `comet-contrib-spi` Rust crate?

Cycle break. Core would need to depend on contribs (to link them); contribs need core's
trait types (`ContribOperatorPlanner`). Solution: a leaf crate both depend on, with
nothing depending back on core from a contrib.

### Why `ContribPlannerContext`?

Surfaced by the Delta-port confidence check. A real file-scan contrib needs five
core-side facilities: `init_datasource_exec`, `prepare_object_store_with_configs`,
`convert_spark_types_to_arrow_schema`, expression-planning (`create_expr`), and a
`SessionContext` handle. Exposing these as a trait core implements (and contribs use
via `&dyn ContribPlannerContext`) avoids a back-dep on core while giving contribs
everything they need to compose with Comet's tuned parquet path.

### Why `ArcSwap` instead of `RwLock` for the registry?

Reads are on the dispatch hot path; writes happen exclusively during library init from
sequential `#[ctor]`s. The init-once / read-many access pattern is what `ArcSwap` is
designed for. The original `RwLock<HashMap>` would have introduced reader-writer
contention with no actual concurrent-write workload to justify it.

### Why bundling-via-source-injection rather than bundling-via-shaded-JAR?

A separate Maven module per contrib whose JAR gets shaded into `comet-spark.jar`
would form a Maven reactor cycle (contrib's pom depends on `comet-spark` for SPI
types; `comet-spark`'s contrib profile depends on contrib's JAR). Source-injection
avoids the cycle: the contrib's Scala sources are compiled INSIDE `comet-spark`'s
own compilation pass (via `build-helper-maven-plugin`'s `add-source` goal); no
separate compile, no per-contrib JAR, no cycle. External Maven deps (`delta-spark`,
etc.) flow through the contrib's separate `<packaging>pom</packaging>` artifact.

## The Delta-port confidence check

Before opening this PR, I ported Comet's existing Delta integration (~3,200 lines on
the `delta-kernel-phase-1` branch) onto this SPI as a confidence check. The port
itself is not committed here — its purpose was to surface SPI gaps before review.

Four gaps were found, all addressed in this PR:

1. **No tree-level pre-pass hook** → added `CometScanRuleExtension.preTransform`.
2. **No reference for the proto layer in `contrib/example/`** → added a trivial
   `ExampleConstantScan` message, `build.rs`, prost-build wiring, and tests.
3. **Class-keyed serde dispatch convention undocumented** → documented in the
   contributor guide.
4. **`ContribOperatorPlanner::plan` lacked access to core's parquet / expression
   machinery** → introduced `ContribPlannerContext` trait + `ParquetDatasourceParams`
   bundle in commit `14e49448`. The full ~150-line Delta dispatcher body compiled
   clean against the new SPI surface; every trait method was exercised end-to-end.

Full findings live in `PR1-delta-port-findings.md` (not committed; review-prep
artifact).

Net conclusion: the SPI is the right shape for a real consumer. PR2's Delta port can
proceed mechanically with no further SPI surprises expected.

## Review iterations

The branch went through four independent clean-context code review passes (general-purpose
subagent reviews launched fresh on each iteration's HEAD). Each pass surfaced a different
class of issue:

- **Pass 1** (review of `14e49448`, fixed by `8930b698`): 6 blockers + 10 important +
  nits. Test isolation, SemVer markers, V2 asymmetry, `#[ctor]` panic safety, default
  feature leakage, ServiceLoader gating, more.
- **Pass 2** (review of `8930b698`, fixed by `68fff43f`): 3 regressions + 4 polish
  items + 10 new findings. Stale class docstring, dead doc refs, `Display`
  info-loss, corruption-guard class-change bypass.
- **Pass 3** (review of `68fff43f`, fixed by `e4e6e6c6`): 2 regressions + 8 polish.
  Survivors-set false positive, `load()` publication race.
- **Pass 4** (review of `e4e6e6c6`, fixed by `6652963c`): no blockers found; 6 polish.
  Cost-comment accuracy, identity-map upgrade, payload-guard message ordering.

A separate completeness validation of the contributor guide (the doc-only audit pass)
identified ~13 gaps a real contrib author would hit. Closed in `91c40e0a` and
`2c46552c`.

## Build verification

- `cargo check` (default features): green.
- `cargo check --no-default-features`: green; zero contrib surface in the resulting
  `libcomet`.
- `cargo test -p comet-contrib-spi`: 7 tests pass (registry round-trip, scoped
  registration, kinds snapshot, params constructor/setters, `ContribError` Display
  preservation).
- `cargo test -p datafusion-comet --lib -- execution::planner::contrib`: 5 tests pass,
  including the production-canary that asserts default cdylib has no registered
  contribs and the encryption-asymmetry test that catches positional-arg swaps in
  `init_datasource_exec`.
- `cargo test -p comet-contrib-example`: 4 tests pass (ctor registration, decode +
  build, zero rows, bad payload).
- Maven `-Pcontrib-example` activates the profile cleanly; `comet-contrib-example-deps`
  builds first, then `comet-spark` with the contrib's sources injected. No reactor
  cycle.

## What this PR is NOT

- It does NOT migrate Delta / kernel-rs to the new SPI. That's PR2.
- It does NOT exercise `CometOperatorSerdeExtension` from the example contrib (the
  example only demonstrates `CometScanRuleExtension`; the trait surface is documented
  and validated against the Delta-port confidence check).
- The Maven `BanDuplicateClasses` enforcer is no longer overridden per-contrib
  (contribs are no longer separate Maven modules); the rule applies to `comet-spark`
  itself as before.

## How to review

Suggested reading order:

1. `docs/source/contributor-guide/contrib-extensions.md` — author-facing guide; doubles
   as the architectural overview.
2. `native/proto/src/proto/operator.proto` — the `ContribOp` envelope (small, look for
   the new variant on `OpStruct`).
3. `native/contrib-spi/src/lib.rs` — the leaf SPI crate (~370 lines incl. tests).
4. `spark/src/main/scala/org/apache/comet/spi/` — three small files defining the JVM
   SPI.
5. `native/core/src/execution/planner.rs` — the `OpStruct::ContribOp` dispatcher arm
   (~lines 1960–2020).
6. `native/core/src/execution/planner/contrib.rs` — `CorePlannerContext` adapter that
   exposes core's parquet/expression infrastructure to contribs through the SPI trait.
7. `spark/src/main/scala/org/apache/comet/rules/CometScanRule.scala` and
   `CometExecRule.scala` — the integration hooks. Each is a small insertion at the
   top of `_apply`; the `preTransform` fold runs once per plan.
8. `contrib/example/` — the worked reference (deps-pom + Scala source + Cargo crate).
9. `spark/pom.xml`'s `contrib-example` profile — the template for wiring a new
   contrib into the build.

## Risks / follow-ups (tracked for PR2)

- **`CometOperatorSerdeExtension` not yet exercised by a contrib.** The example
  contrib only demonstrates `CometScanRuleExtension`. PR2's Delta port will exercise
  the operator serde path via `CometDeltaNativeScanExec`'s dedicated class.
- **Native test runner needs `libjvm` on the dyld path.** Running
  `cargo test -p datafusion-comet --lib` on macOS requires
  `DYLD_LIBRARY_PATH=$JAVA_HOME/lib/server` (only relevant when the test binary
  transitively links against the JNI crate). Preexisting on `main` — not introduced
  by this PR — but worth documenting.
- **CI matrix should add a `-Pcontrib-example,--features contrib-example` row** so the
  bundling path is exercised in CI on every PR. Today only the slim build is in CI.
