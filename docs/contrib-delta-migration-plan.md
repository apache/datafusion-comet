# Plan: move delta-kernel-rs integration into a Comet "contrib" extension

Driver: PR review feedback on `delta-kernel-phase-1` asked the Delta integration to live in a
contrib section rather than core so the core stays format-agnostic and the heavyweight
delta-kernel-rs dependency (with its own `object_store_kernel` pin at 0.12 vs core's
`object_store` at 0.13) is opt-in.

User-confirmed decisions:
1. **Single PR.** No series of incremental PRs. All structural changes ship together.
2. **Separate extension JAR with its own cdylib.** Delta contrib ships as a fully independent
   artifact whose native library is loaded at runtime when the contrib JAR is on the classpath.
3. **Iceberg stays in core.** It is treated as a first-class Comet feature; no parallel move.
4. **Proto stays as-is.** Keep the existing `DeltaScan` message verbatim; only its
   reader/writer ownership relocates from core to contrib. (See "Proto handling" below.)

This plan executes AFTER P1 (rich credentials for log replay) is signed off — P1 lives in the
files being moved and merging the contrib split before P1 would conflict messily.

## 1. Target layout

```
contrib/
  delta/
    pom.xml                                  ← new Maven module (groupId=org.apache.datafusion,
                                                                  artifactId=comet-contrib-delta-spark${spark.version.short}_${scala.binary.version})
    native/
      Cargo.toml                             ← new Cargo crate (package=comet-contrib-delta)
      src/
        lib.rs                               ← cdylib entry, JNI symbol exports, registration
        engine.rs                            ← from native/core/src/delta/engine.rs
        error.rs                             ← from native/core/src/delta/error.rs
        jni.rs                               ← from native/core/src/delta/jni.rs
        predicate.rs                         ← from native/core/src/delta/predicate.rs
        scan.rs                              ← from native/core/src/delta/scan.rs
        integration_tests.rs                 ← from native/core/src/delta/integration_tests.rs
        dv_filter.rs                         ← from native/core/src/execution/operators/delta_dv_filter.rs
        planner_ext.rs                       ← (NEW) implements ContribOperatorPlanner for OpStruct::DeltaScan
        scan_rule_ext.rs                     ← (NEW) glue called by Scala-side CometScanRuleExtension
    spark/
      src/main/scala/org/apache/comet/contrib/delta/
        DeltaScanExtension.scala             ← (NEW) implements CometScanRuleExtension
        package.scala                        ← (NEW) ServiceLoader registration helpers
      src/main/scala/org/apache/comet/delta/
        DeltaReflection.scala                ← moved from spark/.../comet/delta/
      src/main/scala/org/apache/comet/serde/operator/
        CometDeltaNativeScan.scala           ← moved
      src/main/scala/org/apache/spark/sql/comet/
        CometDeltaNativeScanExec.scala       ← moved
      src/main/resources/META-INF/services/
        org.apache.comet.spi.CometScanRuleExtension   ← lists DeltaScanExtension
    dev/
      diffs/3.3.2.diff                       ← moved from dev/diffs/delta/3.3.2.diff
      run-regression.sh                      ← moved from dev/run-delta-regression.sh
```

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
- The `DeltaScan` / `DeltaScanCommon` / `DeltaScanTask` / `DeltaScanTaskList` proto messages
  in `native/proto/src/proto/operator.proto`. They stay where they are; only their reader and
  writer move to contrib. The proto crate is generic infrastructure (already a top-level Cargo
  member with no Delta logic), so no module change there.

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
- New crate `contrib/delta/native/` with `crate-type = ["cdylib"]`. Package name
  `comet-contrib-delta`.
- Add to root `native/Cargo.toml` `members` and `default-members`.
- Cargo dependency on `datafusion-comet` (the core crate) for the public SPI types
  (`ContribOperatorPlanner`, `register_contrib_planner`, etc).
- `delta_kernel`, `object_store_kernel` dependencies move from `native/core/Cargo.toml` to
  `contrib/delta/native/Cargo.toml`. Core no longer pulls them in.

### JNI symbol packaging and loading
- The contrib JAR's resources contain `META-INF/native/<os>/<arch>/libcomet_contrib_delta.so|.dylib|.dll`.
- A small helper in contrib's `package.scala` (called by `CometExtensionRegistry.load()`
  before the contrib's `DeltaScanExtension` is registered) extracts the platform-specific
  library to a temp dir and calls `System.load()`. Same pattern Comet core already uses for
  its own native lib.
- Contrib's cdylib has a JNI `OnLoad` (or equivalent) that registers its
  `ContribOperatorPlanner` with the core crate's registry. The core crate's registry is a
  `OnceLock<Mutex<Vec<...>>>`, so this works across cdylib boundaries because the contrib
  cdylib links against `datafusion-comet` and shares the static.

  Caveat: Rust statics are per-DSO. The contrib cdylib must link to the core crate *as a
  shared library* (via the same `libcomet.dylib` already loaded by JVM core) — not as a
  Cargo dependency that statically links a second copy of `datafusion-comet`. We achieve
  this with `crate-type = ["cdylib"]` on contrib AND making core's crate available as a
  dylib for the contrib build profile. This is the trickiest item in the plan; if Rust's
  shared-static crossing turns out fiddly, we fall back to passing registration through a
  JNI call: contrib's library-init JNI calls a `register_contrib_delta_planner` Java method
  that uses JNI again to call into core's registry.

### Spotless / scalafmt
- Add `contrib/delta/` patterns to spotless config so the same formatting applies.

## 6. Single-PR ordering inside the same branch

All changes land in one branch, one PR. Inside the branch the work proceeds in this order so
the regression stays green at each intermediate commit (helpful for reviewer bisect and for
our own validation):

1. **Land P1** on `delta-kernel-phase-1` (separate commit, separate validation; already
   pending). This is the precondition for the contrib work.
2. **Add the SPI traits** (`CometScanRuleExtension`, `ContribOperatorPlanner`, etc.) in core
   with NO callers yet — pure additions. Regression unchanged.
3. **Wire `CometScanRule` and `planner.rs` to call the registry**, with an in-tree shim
   `DeltaScanExtension` registered from core. Logic unchanged; only structurally pluggable
   now. Regression unchanged.
4. **Create the new contrib/delta/ module** with the new build wiring (Maven module +
   Cargo crate). Initially empty — just the build skeleton.
5. **Physically move the Delta files** out of core into `contrib/delta/`. Update imports.
   Wire contrib's ServiceLoader file. Drop the in-tree shim from step 3 once the
   contrib module's extension is discovered at load time.
6. **Move the regression test driver** and validate the full Delta regression still passes
   with the contrib jar on the test classpath.

All six steps are in one branch. The user reviews the single PR.

## 7. Proto handling (single-PR shape)

Per user decision: keep the existing `DeltaScan` / `DeltaScanCommon` / `DeltaScanTask` /
`DeltaScanTaskList` proto messages exactly as they are.

- The `.proto` files stay in `native/proto/src/proto/operator.proto`.
- The generated Rust types stay in the `datafusion-comet-proto` crate (already a top-level
  Cargo member, format-agnostic).
- The generated Java/Scala types stay in `spark`'s build output.
- Only the consumers move: contrib's Rust crate imports the generated types from the proto
  crate; contrib's Scala code does likewise.

No deprecation window needed; no backwards-compat shims. If we later want to generalize to
a `ContribScan` envelope for future Hudi/DeltaSharing/etc. contribs we can do it as a
focused follow-up — the SPI is already shaped for that.

## 8. Risks / open questions to confirm with the user before starting

- **Shared-static across DSOs (§5, JNI loading).** Rust statics live per-dylib. Cross-dylib
  registration depends on contrib linking against core as a `dylib` (not a static copy).
  If that turns out to require deeper toolchain work than expected, the fallback is a
  JNI-mediated registration (contrib calls back through Java which calls forward into
  core's JNI). Confirm we're OK with this risk profile before kickoff. If not, we ship the
  JNI-mediated path from day one.
- **Spark Delta version skew.** The contrib JAR currently targets Delta 3.3.2 (Spark 3.5).
  Confirm whether contrib should also produce Spark 3.4 / Delta 2.4 and Spark 4.0 /
  Delta 4.0 variants in this PR or whether those land in follow-ups. (`dev/run-delta-regression.sh`
  already conditionally drives all three.)
- **`META-INF/native/` library size.** Adding a second cdylib roughly doubles the native
  size of the contrib JAR vs core. Confirm acceptable, or whether contrib should ship one
  cdylib per supported OS/arch combo separately (Linux x86_64, Linux aarch64, macOS x86_64,
  macOS aarch64, Windows x86_64).
