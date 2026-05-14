<!--
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

# Authoring a Comet contrib extension

A Comet *contrib* is a self-contained extension that lives alongside core but ships
independently. Contribs add support for a specific table format or operator class without
core having to know about them at build time. The first contrib in the tree is
[`contrib/example/`](https://github.com/apache/datafusion-comet/tree/main/contrib/example);
read it top-to-bottom as the worked reference, then come back here for the architectural
context.

This document covers how the SPI is shaped, which integration points are available, and
the concrete files a new contrib has to ship.

## SPI stability

The contrib SPI surface is currently **alpha** — minor versions may carry breaking
changes during the early-adopter period. Public types in `comet-contrib-spi` and the
Scala SPI traits are marked `#[non_exhaustive]` (or open for inheritance) so additive
changes are minor bumps. Removals and renames will be called out in release notes. Lock
your contrib to a specific Comet patch version until the SPI is declared stable in a
later release.

## Architecture at a glance

Each contrib has two halves that ship as separate artifacts but are wired together at
build time:

- **JVM half** — a separate Maven JAR (`comet-contrib-<name>-spark${spark.version.short}_${scala.binary.version}`)
  containing Scala / Java extension classes. Discovered at runtime via
  `java.util.ServiceLoader` from the contrib JAR's `META-INF/services/` entries.

- **Native half** — a Rust `rlib` crate (NOT `cdylib`) that is **linked INTO core's
  `libcomet`** at build time when the matching Cargo feature on core is enabled. There is
  exactly one Comet native library at runtime; the contrib's `#[ctor]` registers its
  operator planners during library load.

The wire format between JVM and native uses a single generic envelope on the operator
proto, `ContribOp { kind, payload }`. Core's planner dispatches by `kind`; the contrib's
native crate registers planners against the same `kind` string the contrib's JVM code
writes into the proto.

## SPI surface

### JVM side: `org.apache.comet.spi`

| Trait / Object | Purpose |
|---|---|
| `CometScanRuleExtension` | Intercept scan-tree transformation. Override `preTransform` for tree-level rewrites (V1 only — see below); `matchesV1` / `transformV1` for V1 `FileSourceScanExec`; `matchesV2` / `transformV2` for V2 `BatchScanExec`. Dispatch iterates registered extensions in order; the first one whose `match*` returns `true` AND `transform*` returns `Some` wins. `None` means "decline this instance" and dispatch continues to the next matching extension before falling back to core. |
| `CometOperatorSerdeExtension` | Contribute additional `SparkPlan` class → `CometOperatorSerde` mappings to `CometExecRule`. The merged map is computed once at registry load time. Used when the contrib has its own physical operator (e.g., a contrib-specific scan exec) that needs native serialization. Duplicate class keys across contribs are logged as a warning at load. |
| `CometExtensionRegistry` | Process-wide singleton. `load()` is invoked lazily from `CometScanRule._apply` / `CometExecRule.apply` the first time Comet runs against a Comet-enabled session — so Spark sessions that never enable Comet pay zero ServiceLoader cost. Subsequent calls are no-ops. Test-only `resetForTesting()` exists for unit tests that need a clean registry. |

### `preTransform` is V1-only and disabled when scan is off

`CometScanRule` folds every registered extension's `preTransform` over the plan tree
once, before per-scan dispatch begins. The rewritten subtree is what `transformV1`
receives. `transformV2` does **not** receive a plan reference — V2 contribs that need
wrapper-stripping must do that work inside `transformV2` against `scanExec.scan` and
`scanExec.children` directly.

The fold is skipped entirely when `spark.comet.scan.enabled=false`. A contrib's own
Catalyst wrappers (Delta's DV filter, etc.) become load-bearing when Comet's scan is
disabled; stripping them turns into a correctness bug.

`CometScanRule` also logs a warning when a `FileSourceScanExec` is replaced by an
extension whose `matchesV1` returns false against the original scan's relation — a
contrib that trips this warning is rewriting scans it doesn't recognise and may corrupt
other formats' plans. Narrow your pattern match.

### Convention: define your own SparkPlan subclass for serde dispatch

`CometExecRule` dispatches by **class identity** (`op.getClass`) when matching an
operator to its serde. Contribs that need a custom executor (e.g., a contrib-specific
scan exec carrying contrib-private state) should define a dedicated subclass:

```scala
case class CometMyFormatScanExec(...) extends CometScanExec(..., SCAN_NATIVE_DELTA_COMPAT)
```

and register the serde keyed on the new class:

```scala
class MyFormatSerdeExtension extends CometOperatorSerdeExtension {
  override def serdes: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
    Map(classOf[CometMyFormatScanExec] -> CometMyFormatScanSerde)
}
```

Avoid relying on the legacy `scanImpl: String` tag pattern on a generic `CometScanExec`;
that approach has no analogue in the SPI's class-based dispatch and would require core
changes to support.

### Native side: `comet-contrib-spi` crate

| Item | Purpose |
|---|---|
| `trait ContribOperatorPlanner` | Implemented by the contrib's native crate. The `plan(ctx, payload, children) -> Arc<dyn ExecutionPlan>` method receives a `&dyn ContribPlannerContext` (handle to core's planner services), the contrib-private payload bytes from the `ContribOp` envelope, and the already-built native children. |
| `trait ContribPlannerContext` | Implemented by core. Exposes the parquet exec builder (`build_parquet_datasource_exec`), expression planner (`build_physical_expr`), schema conversion (`convert_spark_schema`), object-store registration (`prepare_object_store`), and the `SessionContext` itself. Contribs reach into core through this trait rather than depending on `datafusion-comet` directly. |
| `struct ParquetDatasourceParams` | `#[non_exhaustive]` argument bundle for the parquet exec builder. Construct via `ParquetDatasourceParams::new(required_schema, object_store_url, file_groups)` and chain `with_*` setters. Adding fields in future is a minor SemVer bump. |
| `register_contrib_planner(kind, planner)` | Process-wide registry. Called from the contrib's `#[ctor::ctor]` at library load. |
| `lookup_contrib_planner_by_kind(kind)` | Used by core's planner; contribs rarely call directly. |
| `ContribError` | `#[non_exhaustive]` minimal error type. Core converts to its own `ExecutionError` at the dispatch site. Variants: `Plan(String)`, `BadPayload(String)`, `WrongChildCount { expected: String, actual: usize }`. Pattern matches MUST include a wildcard arm so future variants don't break consumers. |
| `ScopedContribPlannerRegistration` | `#[cfg(any(test, feature = "test-utils"))]` RAII guard for tests that register a planner without polluting the global registry. Drop restores the previous planner. Pair with `#[serial_test::serial]` if your test asserts on `registered_contrib_kinds()`. |

The SPI crate is intentionally a thin leaf: it depends only on `datafusion`,
`datafusion-comet-proto`, and `object_store`. This is what breaks the would-be cyclic
dependency (core links contribs via Cargo feature flags; contribs need the SPI types —
both depend on a third leaf crate instead of each other). No core-typed values cross
the trait boundary.

### Why `ContribOperatorPlanner` is `Send + Sync` but `ContribPlannerContext` isn't

The planner trait is stored in an `Arc` inside a process-wide registry shared across
threads, so `Send + Sync` is load-bearing. The context is short-lived: a `&dyn`
reference passed for the duration of one synchronous `plan()` call, so the bound would
only restrict implementations without adding safety. Notably, core's `PhysicalPlanner`
carries JNI handles that aren't `Send`; requiring `Send` on the context would force an
awkward `Arc<Mutex<...>>` dance for no gain.

Contribs that want to spawn async work during `plan()` must capture only the
`Arc<SessionContext>` (which **is** `Send + Sync`) before crossing a thread boundary —
not the `&dyn ContribPlannerContext` itself.

### Why `payload: &[u8]` instead of `Bytes`

The dispatcher already owns the decoded `ContribOp` proto; passing `&[u8]` is zero-copy
and avoids forcing every contrib to depend on the `bytes` crate. `prost::Message::decode`
accepts `&[u8]` directly. Contribs that want `Bytes` for downstream zero-copy work can
convert with `bytes::Bytes::copy_from_slice(payload)` — a single allocation, at most
once per plan call.

### `ContribError::WrongChildCount` convention

`expected` is a free-form human description; conventionally a phrase like `"exactly 1"`
or `"0 or 1"` so the displayed error reads:
`wrong child count: expected exactly 1, got 2`.

## Required files (mirror `contrib/example/` exactly)

```
contrib/<name>/
  pom.xml                                                          ← Maven module
  src/main/scala/org/apache/comet/contrib/<name>/
    <SomeClass>.scala                                              ← CometScanRuleExtension / CometOperatorSerdeExtension impl
  src/main/resources/META-INF/services/
    org.apache.comet.spi.CometScanRuleExtension                    ← one line per extension class
    org.apache.comet.spi.CometOperatorSerdeExtension               ← (only if you implement serdes)
  src/test/scala/org/apache/comet/contrib/<name>/
    <SomeClass>Suite.scala                                         ← integration test
  native/
    Cargo.toml                                                     ← rlib crate, workspace = "../../../native"
    build.rs                                                       ← runs prost-build over your proto schema
    src/lib.rs                                                     ← ContribOperatorPlanner impl + #[ctor] registration
    src/proto/<your_op>.proto                                      ← contrib-private proto schema, your own package
    src/generated/                                                 ← (gitignored) prost-build output
```

### Proto layer

Each contrib carries its own `.proto` schema defining the message its `ContribOp.payload`
carries. The Scala side serializes that message and sets it on the operator proto's
`contrib_op` envelope; the Rust side `prost::Message::decode`s the same bytes back.
`contrib/example/`'s `ExampleConstantScan { row_count }` is the trivial reference.

Use your own proto **package name** (e.g., `comet.contrib.<name>`) so symbols never
collide with core or with other contribs. Add `contrib/<name>/native/src/generated/` to
the repository `.gitignore` (the build script writes generated `.rs` there each compile).

Plus three edits to existing files:

- **Root `pom.xml`** — add `<module>contrib/<name></module>` so `mvn install` builds the
  contrib.
- **`native/Cargo.toml`** — add `../contrib/<name>/native` to the workspace `members`
  list (NOT `default-members` — contribs are consumed via core's feature flags).
- **`native/core/Cargo.toml`** — add a `contrib-<name>` feature gate and a matching
  optional `dep:` entry. Add the feature to `default = [...]` if you want it on by
  default in release builds.

## Wire-format flow

1. The contrib's Scala code intercepts a `FileSourceScanExec` (or `BatchScanExec`)
   matching its file format.
2. It builds a contrib-private proto message (the payload format is the contrib's
   choice).
3. It wraps the payload bytes in `ContribOp(kind = "<name>-<operator>", payload =
   <bytes>)` and sets that on the operator proto's `op_struct` field.
4. The proto is shipped through JNI to native.
5. Core's native planner sees `OpStruct::ContribOp`, looks up the planner by `kind`,
   calls `planner.plan(payload, children)`.
6. The contrib's native crate decodes `payload` into its own proto type and returns an
   `Arc<dyn ExecutionPlan>`.
7. Core wraps the result in a `SparkPlan` and continues planning.

## `#[ctor]` registration: panic safety + logging

The contrib's native crate registers its planners during library init via
`#[ctor::ctor]`. Two important quirks to get right:

**Panics in `#[ctor]` abort the JVM process** before `JNI_OnLoad` runs, with no
diagnostic on macOS/Linux. Wrap every ctor body in `std::panic::catch_unwind` and emit
a stderr message on failure:

```rust
#[ctor::ctor]
fn register() {
    let _ = std::panic::catch_unwind(|| {
        register_contrib_planner(MY_KIND, Arc::new(MyPlanner));
    })
    .map_err(|panic| {
        eprintln!("comet-contrib-myname: #[ctor] panicked: {panic:?}");
    });
}
```

**`log::*!` macros inside `#[ctor]` are no-ops.** Comet's logger is initialised later,
in `Java_org_apache_comet_NativeBase_init`. Any diagnostic you need from the ctor body
must go through `eprintln!`. The example contrib follows both patterns.

**Cross-platform caveats.** `#[ctor::ctor]` works on Linux / macOS / Windows MSVC, but
the order of ctor execution across rlibs is link-order dependent and not guaranteed
across compiler versions. Your contrib's ctor **MUST NOT** depend on another contrib
already being registered.

## Cargo feature gate

Each contrib's native rlib is wired into core via a feature flag. Build core with:

```bash
# Default release build: zero contrib surface. registered_contrib_kinds() is empty.
cargo build

# Enable a specific contrib explicitly:
cargo build --features contrib-example
# or
cargo build --features contrib-example,contrib-delta

# Verify the slim build path:
cargo build --no-default-features
```

`registered_contrib_kinds()` in a default release build is empty — production
deployments only see the contribs they explicitly opted into. CI matrix should include
a `--no-default-features` row to catch any accidental contrib leakage into core.

The JVM side is **always** conditional: the contrib JAR is its own artifact, and Spark
only picks it up when it's on the classpath. Even with the Cargo feature on, a user
who doesn't add the contrib JAR sees no behaviour change — the contrib's native planner
sits dormant in the registry, waiting for a JVM serde that never calls it.

## Maven JAR packaging

The example contrib ships a thin JAR (no shading). Real contribs SHOULD prefer thin
JARs too. If your contrib must include a third-party library that conflicts with core's
classpath (e.g., a different protobuf-java version), shade the conflicting classes
under your contrib's package prefix (`org.apache.comet.contrib.<name>.shaded.*`) so
classloader collisions stay local. Do not shade `comet-spark` or its transitive
dependencies — those are `provided` scope and the user supplies them.

## Registry implementation note

The native contrib planner registry is currently a `RwLock<HashMap<String, Arc<...>>>`.
Lookups happen once per `ContribOp` plan call; writes happen only during library init.
The implementation may switch to a lock-free primitive (`ArcSwap`) in a future release
if profiling shows the read path matters; the public API stays unchanged either way.

## Payload size cap

The native dispatcher enforces a hard ceiling of **16 MiB** on `ContribOp.payload`. A
malformed JVM-side serde (or one that accidentally accumulates state across plan calls)
producing a larger payload is rejected with a clear error message before the contrib's
`plan()` runs. The cap is intentionally above any plausible file-scan payload (Delta
with ~100k tasks weighs in around 3–4 MiB) and well below "heap pressure" territory;
the value is hardcoded in `native/core/src/execution/planner.rs`. If your contrib has
a legitimate need for a larger payload, file an issue with the size you need and the
use case -- the cap is a guardrail, not a feature.

## Testing

`contrib/example/`'s test suite demonstrates the recommended pattern:

- A unit test that calls `CometExtensionRegistry.load()` and asserts the contrib's
  extension is discovered. This catches packaging mistakes (missing `META-INF/services`,
  wrong class name, etc.).
- Per-method unit tests for the extension's `matches*` and `transform*` logic.

For a contrib with a real native operator, additionally write an integration test that:

- Builds a `ContribOp` payload Scala-side.
- Submits the plan through a real `SparkSession` configured with the contrib JAR on the
  classpath.
- Asserts the contrib's native planner was reached (typically by checking against a
  result the no-op planner would not produce).

Core's own regression suite for the SPI dispatch path uses the example contrib as its
test fixture, so PR1's CI doubles as smoke coverage for any future contribs.

## See also

- [`contrib/example/`](https://github.com/apache/datafusion-comet/tree/main/contrib/example) —
  the worked reference.
- [`native/contrib-spi/`](https://github.com/apache/datafusion-comet/tree/main/native/contrib-spi) —
  the leaf SPI crate.
