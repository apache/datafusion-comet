# Plan: move delta-kernel-rs integration into a Comet "contrib" extension

Driver: PR review feedback on `delta-kernel-phase-1` asked the Delta integration to live in a
contrib section rather than core so the core stays format-agnostic and the heavyweight
delta-kernel-rs dependency (with its own `object_store_kernel` pin at 0.12 vs core's
`object_store` at 0.13) is opt-in.

User-confirmed decisions:
1. **Two PRs, fully independent of Delta in PR1.**
   - **PR1 — core contrib capabilities.** Branches off a **clean `main`** (NOT off
     `delta-kernel-phase-1`). Adds the SPI traits, the `ContribOp` proto envelope, the
     ServiceLoader/registration machinery, contributor-guide documentation, and a
     **minimum-viable example contrib** (see below) that exercises the SPI end-to-end for
     test coverage. PR1 contains **zero Delta classes, zero references to Delta from core**.
   - **Implementation strategy during PR1 development:** we port the existing Delta code
     onto the new SPI locally as a confidence check that the SPI is shaped right for the
     real use case — but those Delta-porting changes are NOT committed to PR1. They become
     the starting point of PR2's branch.
   - **PR2 — contribute Delta to the new contrib section.** Creates the `contrib/delta/`
     Maven module + Cargo crate + proto crate, lands the ported Delta code from the
     development-time porting step, wires the ServiceLoader entry, and adds the
     conditional-compile CI gate.
2. **Minimum example contrib in PR1 (`contrib/example/`).** A tiny self-contained extension
   that demonstrates every SPI pattern future contrib authors need: its own Cargo crate, a
   Scala `CometScanRuleExtension` plus its serde, a Rust `ContribOperatorPlanner` registered
   via `#[ctor]`, a ServiceLoader META-INF entry, and a unit / integration test that
   exercises the full JVM→native dispatch path. Implementation candidate: a "constant scan"
   contrib that produces a fixed in-memory batch when a test-only marker table type is read.
   ~50–100 lines of code total. Doubles as the worked example in the contributor-guide doc.
3. **Single cdylib, Cargo-feature-gated contribs (revised).** Contrib Rust crates are pulled
   into core's cdylib via optional Cargo dependencies behind feature flags. There is exactly
   ONE native library at runtime; the registry is a single in-process static. Conditional
   compile is preserved at source level via `cargo build --no-default-features` (zero contrib
   code in the cdylib). Release artifacts ship one flavor of the cdylib with all in-tree
   contribs enabled by default; JVM-side conditional compile is fully preserved because the
   contrib's Scala extension is only activated via `META-INF/services/` discovery, which
   requires the contrib JAR on the classpath. Users who don't add the contrib JAR get
   ServiceLoader silence — the native registration sits dormant. See §5 for build wiring.
4. **Iceberg stays in core.** It is treated as a first-class Comet feature; no parallel move.
5. **Conditional compilation is required.** Core's published JAR + Cargo crate must have zero
   Delta surface (since PR1 has none, this is guaranteed at PR1 merge time and only needs to
   be preserved by PR2). The Delta proto definitions live in `contrib/delta/proto/` from day
   one of PR2 — never in core. Core gains the single generic `ContribOp { kind, payload }`
   envelope in PR1 as the dispatch hop. (See §7 "Proto handling" below.)

This plan executes AFTER P1 (rich credentials for log replay) is signed off — P1 lives in the
files being moved and merging the contrib split before P1 would conflict messily.

## 1. Target layout

```
contrib/
  delta/
    pom.xml                                  ← new Maven module (groupId=org.apache.datafusion,
                                                                  artifactId=comet-contrib-delta-spark${spark.version.short}_${scala.binary.version})
    proto/
      src/main/proto/delta_operator.proto    ← (NEW) Delta-specific messages
                                                   (DeltaScan, DeltaScanCommon, DeltaScanTask,
                                                    DeltaScanTaskList, DeltaColumnMapping,
                                                    DeltaPartitionValue) — never lived in
                                                    core's operator.proto.
      Cargo.toml                             ← (NEW) Rust proto crate for contrib delta
      build.rs                               ← runs prost-build on delta_operator.proto
      src/lib.rs                             ← exposes the generated types
    native/
      Cargo.toml                             ← rlib crate (package=comet-contrib-delta).
                                                   Linked INTO core's cdylib via the
                                                   `contrib-delta` Cargo feature on the core
                                                   crate. NOT a cdylib of its own.
      src/
        lib.rs                               ← #[ctor] registration into core's registry,
                                                  module re-exports
        engine.rs                            ← from native/core/src/delta/engine.rs
        error.rs                             ← from native/core/src/delta/error.rs
        jni.rs                               ← from native/core/src/delta/jni.rs
        predicate.rs                         ← from native/core/src/delta/predicate.rs
        scan.rs                              ← from native/core/src/delta/scan.rs
        integration_tests.rs                 ← from native/core/src/delta/integration_tests.rs
        dv_filter.rs                         ← from native/core/src/execution/operators/delta_dv_filter.rs
        planner_ext.rs                       ← (NEW) implements ContribOperatorPlanner;
                                                   decodes kind="delta-scan" payloads via
                                                   the contrib proto crate, builds the
                                                   ExecutionPlan
    spark/
      src/main/scala/org/apache/comet/contrib/delta/
        DeltaScanExtension.scala             ← (NEW) implements CometScanRuleExtension
      src/main/scala/org/apache/comet/delta/
        DeltaReflection.scala                ← moved from spark/.../comet/delta/
      src/main/scala/org/apache/comet/serde/operator/
        CometDeltaNativeScan.scala           ← moved; serialises into ContribOp envelope
      src/main/scala/org/apache/spark/sql/comet/
        CometDeltaNativeScanExec.scala       ← moved
      src/main/resources/META-INF/services/
        org.apache.comet.spi.CometScanRuleExtension   ← lists DeltaScanExtension
    dev/
      diffs/3.3.2.diff                       ← moved from dev/diffs/delta/3.3.2.diff
      run-regression.sh                      ← moved from dev/run-delta-regression.sh
```

Note: contrib JARs no longer ship a native library of their own. The Scala JAR is pure JVM
code + the META-INF/services entry. The native code lives in core's cdylib because contrib's
Rust rlib is conditionally linked into core via the Cargo feature flag (§5).

`contrib/delta/native/` produces `libcomet_contrib_delta_<arch>.dylib` packaged in the contrib
JAR's `META-INF/native/`. The contrib JAR's class init (via ServiceLoader) extracts and loads
this library; core's JNI is unaware.

## 2. Files that move wholesale

These are 100% Delta and have no incoming references from non-Delta callers outside the hook
points listed in §4:

### Rust → `contrib/delta/native/src/`
- `native/core/src/delta/{engine,error,jni,mod,predicate,scan,integration_tests}.rs`
- `native/core/src/execution/operators/delta_dv_filter.rs` → `contrib/delta/native/src/dv_filter.rs`

### Scala
- `spark/.../comet/delta/DeltaReflection.scala`
- `spark/.../serde/operator/CometDeltaNativeScan.scala`
- `spark/.../sql/comet/CometDeltaNativeScanExec.scala`

### Test driver
- `dev/diffs/delta/3.3.2.diff` and `dev/run-delta-regression.sh`

## 3. Code that stays in core unchanged

- `common/.../objectstore/NativeConfig.scala` — generic, used by core + Iceberg + contribs.
- `native/core/src/parquet/objectstore/s3.rs::resolve_static_credentials` — generic (P1
  placed it here intentionally; the contrib delta JNI module consumes it via core's public crate path).
- The Iceberg integration in its entirety.

What CHANGES in core's proto (and only this):

- `native/proto/src/proto/operator.proto` gains a single new variant on `OpStruct`:
  `ContribOp { string kind = 1; bytes payload = 2; }`. This is the envelope that lets core
  dispatch to contribs without referencing their concrete message types.
- All existing Delta-specific proto messages (`DeltaScan`, `DeltaScanCommon`, `DeltaScanTask`,
  `DeltaScanTaskList`, `DeltaColumnMapping`, `DeltaPartitionValue`) are DELETED from
  `operator.proto` and moved to `contrib/delta/proto/src/main/proto/delta_operator.proto`.
  Core's proto build no longer compiles them. Contrib's proto build compiles them in its own
  generated source tree, used only by contrib's Rust crate and Scala module.

## 4. Code that needs extension points

These are the touch-points outside the `delta/` directories that today reference Delta directly.
Each gets replaced by a registration hook so core no longer mentions Delta.

### Rust core

| File | Current Delta refs | After move |
|---|---|---|
| `native/core/src/lib.rs` | `pub mod delta;` (line 70) | removed |
| `native/core/src/execution/jni_api.rs` | 1 reference | removed (Delta JNI moves to contrib's cdylib) |
| `native/core/src/execution/operators/mod.rs` | `mod delta_dv_filter` + re-export | removed |
| `native/core/src/execution/operators/operator_registry.rs` (line 154) | `OpStruct::DeltaScan(_) => None` stub | replaced by generic ContribScan registry lookup |
| `native/core/src/execution/planner.rs` (~31 refs) | full `OpStruct::DeltaScan(...)` arm at ~1320–1572, plus `ColumnMappingFilterRewriter` at 3115–3145 | the arm calls into `ContribOperatorPlanner::plan(payload, children, ctx)` looked up by `OpStruct` discriminator; the rewriter moves to contrib |

**New SPI in core (`native/core/src/execution/planner/contrib.rs`):**

```rust
pub trait ContribOperatorPlanner: Send + Sync {
    fn op_kind(&self) -> ContribOpKind;
    fn plan(
        &self,
        proto: &OpStruct,
        children: Vec<Arc<SparkPlan>>,
        ctx: &PlanCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>, ExecutionError>;
}

pub fn register_contrib_planner(p: Arc<dyn ContribOperatorPlanner>);
pub fn lookup_contrib_planner(op_kind: ContribOpKind) -> Option<Arc<dyn ContribOperatorPlanner>>;
```

`ContribOpKind` is an enum that lists `DeltaScan` (and stays open for future kinds). The
planner.rs match arm becomes a single lookup that dispatches by kind — the *body* of the
match arm moves to contrib's `planner_ext.rs`.

The contrib cdylib's `lib.rs` registers its planner in a `#[ctor]` (the `ctor` crate) or
inside the contrib JNI's library-init JNI call. Either way, registration happens before any
plan executes.

### JVM / Scala core

| File | Current Delta refs | After move |
|---|---|---|
| `spark/.../CometSparkSessionExtensions.scala` | 20 (mostly comments + one or two conf entries) | comments scrubbed; any Delta-specific conf moves into contrib's own `CometConf` extension |
| `spark/.../rules/CometExecRule.scala` | 2 (case for `SCAN_NATIVE_DELTA_COMPAT` + import) | replaced by SPI scan dispatcher that calls registered extensions' serdes |
| `spark/.../rules/CometScanRule.scala` | **149** (the densest Delta-aware site) | the Delta branch of `transformV1Scan` becomes a call into `CometScanRuleExtension`. The `withDeltaColumnMappingMetadata`, `applyRowTrackingRewrite`, input_file_name handling (#75 design A), nested-CM data_schema overlay (#79), DV materialisation for pre-mat FileIndex (#77), and `extractObjectStoreOptions` wiring all move into contrib's `DeltaScanExtension` |

**New SPI in core (`spark/.../comet/spi/`):**

```scala
trait CometScanRuleExtension {
  def name: String
  def matches(relation: HadoopFsRelation): Boolean
  def transform(
      plan: SparkPlan,
      scanExec: FileSourceScanExec,
      session: SparkSession): Option[SparkPlan]
}

trait CometOperatorSerdeExtension {
  def serdes: Seq[(Class[_ <: SparkPlan], CometOperatorSerde[_])]
}

object CometExtensionRegistry {
  // ServiceLoader-based discovery from META-INF/services/.
  // Idempotent; safe to call multiple times.
  def load(): Unit
  def scanExtensions: Seq[CometScanRuleExtension]
  def serdeExtensions: Seq[CometOperatorSerdeExtension]
}
```

`CometSparkSessionExtensions` calls `CometExtensionRegistry.load()` during its extension
registration, which lets the contrib JAR's `META-INF/services/...CometScanRuleExtension` entry
register `DeltaScanExtension`. No code changes in core after that — discovery is by classpath
presence.

## 5. Build wiring

### Maven
- New module entry in root `pom.xml`: `<module>contrib/delta</module>`.
- `contrib/delta/pom.xml`:
  - artifactId `comet-contrib-delta-spark${spark.version.short}_${scala.binary.version}`
  - Depends on `comet-spark`, `comet-common`.
  - Depends on `delta-spark` (provided/test scope — kept consistent with the regression suite).
  - Has its own native-library packaging step that places the contrib cdylib under
    `META-INF/native/` in the JAR.
- `spark/pom.xml` — drop the Delta provided/test dep that's now in contrib.

### Cargo

- New crate `contrib/delta/native/` with `crate-type = ["rlib"]` (NOT cdylib — it links into
  core's cdylib via Cargo feature). Package name `comet-contrib-delta`.
- Cargo dependency `datafusion-comet = { path = "../../../native/core" }` for the public SPI
  types. `delta_kernel`, `object_store_kernel` dependencies live in
  `contrib/delta/native/Cargo.toml`. Core itself does NOT depend on these crates.
- `native/core/Cargo.toml` adds:

  ```toml
  [features]
  default = ["contrib-delta"]               # ship-with-all-contribs by default
  contrib-delta = ["dep:comet-contrib-delta"]

  [dependencies]
  comet-contrib-delta = { path = "../../contrib/delta/native", optional = true }
  ```

- The `default` feature includes every in-tree contrib by name. Future contribs add their
  own feature flag and append to the default list.
- `cargo build --no-default-features` produces a cdylib with zero contrib code, zero
  `delta_kernel`, zero `object_store_kernel`. This is the conditional-compile escape hatch
  for distributors / users who want a slim binary.
- `cargo build` (default) produces a cdylib with all in-tree contribs compiled in. This is
  the shipped release artifact.
- Add `contrib/delta/native` and `contrib/example/native` to root `native/Cargo.toml`'s
  `members` list so they participate in workspace builds; do NOT add to `default-members`
  (they aren't built standalone — they're consumed via core's feature).

### Single-cdylib native registration

- Core's `lib.rs` at top level: `#[cfg(feature = "contrib-delta")] extern crate comet_contrib_delta;`.
  Bringing the crate into the link line triggers its `#[ctor]` constructor at lib init time.
- Each contrib crate registers its `ContribOperatorPlanner` via a `#[ctor]` (the `ctor`
  crate, already idiomatic in the Rust ecosystem for JNI libraries):

  ```rust
  // contrib/delta/native/src/lib.rs
  #[ctor::ctor]
  fn register() {
      datafusion_comet::register_contrib_planner(
          "delta-scan",
          Arc::new(DeltaContribPlanner::new()),
      );
  }
  ```

- Registration happens once per process at lib load, before any plan executes. The registry
  is a `OnceLock<Mutex<HashMap<&'static str, Arc<dyn ContribOperatorPlanner>>>>` in core.
- No cross-DSO concern: contrib's static initializers run in the same DSO as core's
  registry, since the contrib crate is linked into core's cdylib.

### JVM JAR side (unchanged from previous revision)

- Contrib JARs are still separate Maven artifacts.
- Each contrib JAR has its own `META-INF/services/org.apache.comet.spi.CometScanRuleExtension`
  entry listing its extension class.
- `CometSparkSessionExtensions` calls `CometExtensionRegistry.load()` once during init;
  ServiceLoader picks up entries from every contrib JAR on the classpath.
- A user who wants Delta adds the `comet-contrib-delta-spark${spark.version.short}_${scala.binary.version}` JAR
  to their classpath; the JVM-side `DeltaScanExtension` registers. A user who doesn't add
  the contrib JAR gets nothing — the native side's `DeltaContribPlanner` registration sits
  dormant (registered but never dispatched to because no Scala-side serde generates a
  `ContribOp(kind="delta-scan")` payload).
- The contrib's Scala JAR has NO native library packaging — the native code is in core's
  cdylib via the Cargo feature.

### Spotless / scalafmt
- Add `contrib/delta/` patterns to spotless config so the same formatting applies.

## 6. Two-PR ordering

### PR1 — "feat(core): contrib extension SPI and ContribOp dispatch envelope"

Base: clean `main`. NOT off `delta-kernel-phase-1`. Zero Delta in PR1's diff.

PR1 deliverables:

1. **Rust SPI in core.** New `native/core/src/execution/planner/contrib.rs` with
   `ContribOperatorPlanner` trait, `register_contrib_planner`, `lookup_contrib_planner_by_kind`,
   plus a process-level registry static.
2. **Scala SPI in core.** New package `org.apache.comet.spi` with
   `CometScanRuleExtension`, `CometOperatorSerdeExtension`, `CometExtensionRegistry`
   (ServiceLoader-driven). Idempotent `load()` plus accessors.
3. **Proto envelope.** Add `ContribOp { string kind = 1; bytes payload = 2; }` message and a
   new oneof variant `OpStruct.contrib_op`. No removal of any existing variant.
4. **Native planner dispatch for `OpStruct::ContribOp`.** New arm in `planner.rs` that calls
   `lookup_contrib_planner_by_kind(payload.kind)?.plan(payload.payload, children, ctx)`.
   Returns a friendly error if no planner is registered for that kind.
5. **Integration hook in core's `CometScanRule`.** At the top of `transformV1Scan`, after the
   relation has been matched to `HadoopFsRelation` but before the file-format dispatch,
   iterate `CometExtensionRegistry.scanExtensions` and let the first matching one transform
   the scan. Core's existing branches stay untouched — if no extension matches, the existing
   file-format logic runs. Likewise for `CometExecRule`: a generic dispatcher to registered
   `CometOperatorSerdeExtension` entries runs alongside the existing match arms.
6. **`ContribExtensionLoader` wiring in `CometSparkSessionExtensions`.** Call
   `CometExtensionRegistry.load()` once during Comet's extension registration so contrib
   JARs on the classpath are discovered automatically.
7. **Minimum example contrib at `contrib/example/`.** Demonstrates every SPI pattern in
   ~50–100 lines of code. Concrete shape (subject to refinement during implementation):
   - `contrib/example/pom.xml` — Maven module that builds `comet-contrib-example-*.jar`
     with its own META-INF/services entries and bundled cdylib.
   - `contrib/example/native/Cargo.toml` + `lib.rs` — Rust crate that registers a
     `ContribOperatorPlanner` for kind=`"example-constant-scan"`. The planner builds a
     trivial `MemoryExec`/`ValuesExec` returning a hard-coded batch.
   - `contrib/example/spark/.../ExampleConstantScanExtension.scala` — a
     `CometScanRuleExtension` that matches a sentinel test-only marker (e.g. a synthetic
     `TABLE_TYPE=comet-example-constant`) and wraps it into a `ContribOp(kind="example-constant-scan", payload=<small proto>)`.
   - `contrib/example/src/main/resources/META-INF/services/org.apache.comet.spi.CometScanRuleExtension`
     listing the extension class.
   - `contrib/example/spark/src/test/.../ExampleConstantScanSuite.scala` — runs an end-to-end
     query against the example contrib, validates the in-memory result, and crucially exercises
     ServiceLoader discovery + JNI cdylib loading + ContribOp dispatch.
8. **Contributor guide.** New `docs/source/contributor-guide/contrib-extensions.md` walking
   through `contrib/example/` as the worked reference for authoring a new contrib.

PR1 acceptance gates:
- All existing Comet tests pass unchanged (no behavioural change to core's scan/exec paths
  when no extension matches).
- `contrib/example/`'s integration suite passes end-to-end, proving the JNI cdylib loading,
  proto envelope, and dispatch path work.
- Core JAR + Cargo crate continue to have zero Delta surface (they never had any from PR1's
  base of clean main).

### Implementation strategy DURING PR1 development (not committed)

Confidence-check the SPI shape against the real consumer: in a local-only branch (NOT pushed
to PR1's branch), port the existing Delta integration onto the new SPI. Treat anything that
requires SPI changes as feedback on PR1's design — iterate the SPI in PR1's branch until the
Delta port works cleanly. The Delta port itself becomes the starting commit set of PR2's
branch.

This means:
- The SPI ships in PR1 already shaped for Delta's real needs (no surprises in PR2).
- PR1's diff stays Delta-free.
- PR2 has a clean starting point with a known-good port.

### PR2 — "feat(contrib): add delta-kernel-rs integration as a contrib module"

Base: `main` after PR1 has merged.

PR2 deliverables:

1. **Create `contrib/delta/` Maven module + Cargo crate + proto crate** per the layout in §1.
2. **Define Delta proto in contrib.** `contrib/delta/proto/src/main/proto/delta_operator.proto`
   under proto package `datafusion_comet_contrib_delta`. Wire prost-build (Rust) and
   protobuf-maven-plugin (Java/Scala). Core's proto crate never sees these files.
3. **Land the Delta source files** (the locally-ported version from PR1's development phase).
   File-by-file destination map in §2.
4. **Wire contrib's ServiceLoader entry** at
   `contrib/delta/spark/src/main/resources/META-INF/services/org.apache.comet.spi.CometScanRuleExtension`
   listing `org.apache.comet.contrib.delta.DeltaScanExtension`.
5. **Wire contrib's native library packaging.** Maven packages
   `libcomet_contrib_delta.{so,dylib,dll}` under `META-INF/native/<os>/<arch>/`. Contrib's
   `package.scala` (called during ServiceLoader discovery) extracts to a temp dir and
   `System.load()`s. The contrib's cdylib `lib.rs` calls `register_contrib_planner` against
   core's static registry during library init.
6. **Move the regression test driver** to `contrib/delta/dev/`. Update `dev/run-delta-regression.sh`
   path references. Update the GitHub workflow `delta_regression_test.yml` to build
   `-pl contrib/delta -am` before running.
7. **Conditional-compile CI gate.** A CI job that builds core WITHOUT the contrib module
   and confirms `unzip -l comet-spark*.jar | grep -i delta` returns empty.

PR2 acceptance gates:
- Core's JAR contains zero `org.apache.comet.delta.*`,
  `org.apache.comet.serde.operator.CometDelta*`, `org.apache.spark.sql.comet.CometDelta*` classes.
- Core's `datafusion-comet` Cargo crate compiles with `--no-default-features` and contains
  no `delta_kernel`, `object_store_kernel`, or contrib-delta proto types.
- Full Delta regression passes when the contrib JAR is on the test classpath.

## 7. Proto handling (full conditional-compile shape)

The conditional-compile requirement means core's published artifacts must contain ZERO Delta
proto surface. We use a generic envelope + contrib-owned proto definitions:

**Envelope in core (`native/proto/src/proto/operator.proto`):**

```proto
message ContribOp {
  // Stable string identifier the contrib registers under, e.g. "delta-scan".
  string kind = 1;
  // Opaque payload; the dispatched ContribOperatorPlanner decodes it into its own type.
  bytes payload = 2;
}

message OpStruct {
  oneof op_struct {
    // ...existing variants...
    ContribOp contrib_op = N;   // single replacement for the previous DeltaScan variant
  }
}
```

This is the ONLY Delta-related change in core's proto. `ContribOp` itself is format-agnostic
— it carries no Delta-specific fields.

**Contrib-owned proto (`contrib/delta/proto/src/main/proto/delta_operator.proto`):**

The Delta-specific message tree moves here verbatim from core's `operator.proto`:

```proto
package datafusion_comet_contrib_delta;

message DeltaScan { ... }
message DeltaScanCommon { ... }
message DeltaScanTask { ... }
message DeltaScanTaskList { ... }
message DeltaColumnMapping { ... }
message DeltaPartitionValue { ... }
```

Different package name so generated symbols don't collide.

**Build wiring:**

- `contrib/delta/proto/Cargo.toml` is a NEW Cargo crate (`comet-contrib-delta-proto`) with
  `build.rs` running `prost-build` against `delta_operator.proto`. Output: generated Rust
  types under `comet_contrib_delta_proto::*`. The contrib native crate depends on this.
- `contrib/delta/proto/pom.xml` (or merged into `contrib/delta/pom.xml`) wires
  `protobuf-maven-plugin` to compile `delta_operator.proto` to Java sources. Output:
  generated Java/Scala types under `org.apache.comet.contrib.delta.proto.*` or similar.
  Used only inside `contrib/delta/spark/`.
- Core's proto compilation does NOT see `delta_operator.proto`. Core's `datafusion-comet-proto`
  Cargo crate and core's spark JAR have zero Delta classes. Conditional compile satisfied:
  building Comet without the contrib module produces artifacts with no Delta proto surface
  whatsoever.

**Wire-format flow (no behavioural change):**

1. Scala contrib code in `CometDeltaNativeScan.scala` builds its Delta proto tree using the
   contrib-generated Java types.
2. It serialises the root `DeltaScan` message to bytes and wraps them in a `ContribOp`
   envelope: `ContribOp.newBuilder().setKind("delta-scan").setPayload(deltaScanBytes).build()`.
3. Sets that on `OpStruct.contrib_op` and ships the plan to native.
4. Native core's planner sees `OpStruct::ContribOp` and calls
   `lookup_contrib_planner_by_kind("delta-scan")?.plan(payload, children, ctx)`.
5. Contrib native's `planner_ext` decodes `payload` via the contrib proto crate's `prost::Message::decode` into its `DeltaScan` Rust type and builds the `ExecutionPlan`
   exactly as the current `OpStruct::DeltaScan` arm does today.

Single PR atomic ship: the proto envelope, the contrib proto module, and the migration of all
Delta files all land together.

## 8. Decisions locked in

All previously-open questions are now answered. Keeping the section as a decision log so
future readers don't have to reconstruct the rationale.

- **Shared-static across DSOs:** RESOLVED by the single-cdylib + Cargo-feature design (§5).
  Contrib code is conditionally linked INTO core's cdylib at build time. There is exactly
  one DSO at runtime, one static registry, deterministic init order via `#[ctor]`.
- **Spark / Delta versions for PR2:** Delta 3.3.2 / Spark 3.5 only in this release.
  Spark 3.4 / Delta 2.4 and Spark 4.0 / Delta 4.0 are deferred to subsequent contrib PRs.
  `dev/run-delta-regression.sh` already drives this version by default; the contrib pom
  inherits the existing `spark.version.short=3.5` profile pattern from core's other modules.
- **`META-INF/native/` library size:** Non-issue. Contrib JARs do NOT carry a native library;
  native code lives in core's cdylib via Cargo feature flags. Contrib JARs are pure JVM
  artifacts (~tens of KB) plus a ServiceLoader entry.
- **Release flavors:** Single cdylib flavor with all in-tree contribs enabled by default
  (`cargo build` includes `contrib-delta` and `contrib-example`). The
  `cargo build --no-default-features` path remains available for distributors who want a
  zero-contrib binary, but it is not a published release artifact.
- **Example-contrib publication:** `contrib/example/` is a first-class published Maven
  module, built and released alongside the Delta contrib. It is the worked reference for
  the contributor guide and serves as in-CI smoke coverage of the full SPI dispatch path.
