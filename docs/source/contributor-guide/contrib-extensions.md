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
independently. Contribs add support for a specific table format or operator class
without core having to know about them at build time.

The first contrib in the tree is
[`contrib/example/`](https://github.com/apache/datafusion-comet/tree/main/contrib/example) —
read it top-to-bottom as the worked reference. This guide adds the architectural context
and walks through every integration point that the example does not exercise.

## Architecture at a glance

Each contrib has two halves that ship as separate artifacts but are wired together at
build time:

- **JVM half** — a separate Maven JAR
  (`comet-contrib-<name>-spark${spark.version.short}_${scala.binary.version}`) containing
  Scala/Java extension classes plus contrib-private generated proto classes. Discovered
  at runtime via `java.util.ServiceLoader` from the contrib JAR's `META-INF/services/`
  entries.
- **Native half** — a Rust `rlib` crate (NOT `cdylib`) that is **linked INTO core's
  `libcomet`** at build time when the matching Cargo feature on core is enabled. There
  is exactly one Comet native library at runtime; the contrib's `#[ctor]` registers its
  operator planners during library load.

The wire format between JVM and native uses a single generic envelope on the operator
proto, `ContribOp { kind, payload }`. Core's planner dispatches by `kind`; the contrib's
native crate registers planners against the same `kind` string the contrib's JVM code
writes into the proto.

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

Plus three edits to existing files (collected under "Wiring into core", below).

### Prerequisites

You need:

- The same toolchain Comet's main build uses: JDK 11+ (Maven build), Rust stable, `protoc`
  (pulled in automatically by `protoc-jar-maven-plugin` and `prost-build`).
- The contrib's `<name>` decided in advance — it becomes a Cargo feature flag
  (`contrib-<name>`), an artifact ID, a JNI symbol prefix if your contrib calls into its
  own Rust, and a `kind` string component for every `ContribOp`. Choose a short, stable
  identifier; renames are breaking.

### `.gitignore`

The generated proto outputs are checked in nowhere:

- `contrib/<name>/native/src/generated/` — Rust prost output. The example contrib's
  `.gitignore` entry is the template.
- `contrib/<name>/target/` — Maven build output (inherits from the repo-root `.gitignore`).

### Workspace placement constraint

`contrib/<name>/native/Cargo.toml` uses `workspace = "../../../native"`. This relative
path assumes contribs live exactly at `<repo>/contrib/<name>/native`. Deeper nesting
breaks the workspace lookup; place the contrib at the documented depth.

## Wiring into core

Three single-line edits to existing files:

1. **Root `pom.xml`** — add `<module>contrib/<name></module>` under the existing
   `<modules>` block so `mvn install` builds the contrib JAR.
2. **`native/Cargo.toml`** — add `../contrib/<name>/native` to the workspace `members`
   list (NOT `default-members` — contribs are consumed via core's feature flags).
3. **`native/core/Cargo.toml`** — add a `contrib-<name>` feature gate and a matching
   optional `dep:` entry, mirroring the `contrib-example` lines:

   ```toml
   [dependencies]
   comet-contrib-<name> = { path = "../../contrib/<name>/native", optional = true }

   [features]
   contrib-<name> = ["dep:comet-contrib-<name>"]
   ```

   Do **not** add the feature to `default = [...]`. Production builds carry zero contrib
   surface by design; users opt in explicitly. (CI matrix builds should add the feature.)

4. **`native/core/src/lib.rs`** — add the matching feature-gated `extern crate` so the
   contrib's `#[ctor]` is linked in when the feature is on:

   ```rust
   #[cfg(feature = "contrib-<name>")]
   extern crate comet_contrib_<name>;
   ```

## Cargo feature gate

```bash
# Default release build: zero contrib surface. registered_contrib_kinds() is empty.
cargo build

# Enable a specific contrib explicitly:
cargo build --features contrib-example
# Multiple at once:
cargo build --features 'contrib-example contrib-<name>'

# Verify the slim build path:
cargo build --no-default-features
```

A core test under `#[cfg(not(any(feature = "contrib-example", ...)))]` asserts
`registered_contrib_kinds()` is empty in the slim build. When you add a new
`contrib-<name>` feature, **extend that test's `cfg` predicate** (see
`native/core/src/execution/planner/contrib.rs`'s `production_build_has_no_contrib_planners_registered`)
so the canary still compiles on your contrib's CI row.

The JVM side is **always** conditional: the contrib JAR is its own Maven artifact, and
Spark only loads it when it's on the classpath. Even with the Cargo feature on, a user
who doesn't add the contrib JAR sees no behaviour change — the contrib's native planner
sits dormant in the registry, waiting for a JVM serde that never calls it.

## SPI stability

The contrib SPI is currently **alpha** — minor Comet versions may carry breaking
changes during the early-adopter period. Concretely:

- `comet-contrib-spi` is workspace-versioned alongside core. A contrib built against
  Comet `0.17.x` is **not** guaranteed to work with Comet `0.18.x` at runtime; the SPI
  traits may evolve. Pin your contrib's `<version>` and `comet-spark` dependency to a
  specific Comet patch version.
- `ParquetDatasourceParams` and `ContribError` are `#[non_exhaustive]` so additive
  changes (new fields / variants) are minor bumps, not breaks. Use
  `ParquetDatasourceParams::new(...)` + `with_*` setters rather than struct-literal
  syntax; consumers of `ContribError` must include a wildcard match arm.
- Scala SPI traits add new methods with default implementations (default `false` /
  `None`). Override only the methods you need; an additive method change is a minor
  bump. Abstract-method additions are breaking and called out in release notes.
- Releases that change the SPI in a breaking way will say so explicitly.

## SPI surface

### JVM side: `org.apache.comet.spi`

| Trait / Object | Purpose |
|---|---|
| `CometScanRuleExtension` | Intercept scan-tree transformation. See subsections below. |
| `CometOperatorSerdeExtension` | Contribute additional `SparkPlan` class → `CometOperatorSerde` mappings to `CometExecRule`. See subsections below. |
| `CometExtensionRegistry` | Process-wide singleton. `load()` is invoked lazily from `CometScanRule._apply` / `CometExecRule._apply` the first time Comet runs against a Comet-enabled session — sessions that never enable Comet pay zero ServiceLoader cost. Subsequent calls are no-ops. `resetForTesting()` (public) clears the registry between tests. |

#### `CometScanRuleExtension`

- `name: String` — human label used in logs and warnings.
- `preTransform(plan, session): SparkPlan` (default identity) — tree-level pre-pass run
  once per plan before per-scan dispatch. **V1-only.** Use it to undo wrapper rewrites
  applied by your format's own Catalyst strategy (Delta's `PreprocessTableWithDVs` is
  the canonical case). Skipped entirely when `spark.comet.scan.enabled=false` — your
  wrappers become load-bearing in that mode and stripping them would be a correctness
  bug. `CometScanRule` logs a warning when an extension replaces a `FileSourceScanExec`
  whose relation it does not claim; this catches accidental cross-format corruption.
- `matchesV1(relation): Boolean` (default `false`) / `transformV1(plan, scanExec, session): Option[SparkPlan]`
  — V1 dispatch. Make `matchesV1` cheap (typically a file-format class probe).
- `matchesV2(scanExec): Boolean` (default `false`) / `transformV2(scanExec, session): Option[SparkPlan]`
  — V2 dispatch. Unlike V1, `transformV2` does **not** receive a plan-tree reference;
  any wrapper-stripping a V2 contrib needs must happen against `scanExec.scan` /
  `scanExec.children` directly.

Dispatch iterates registered extensions in registration order; the first one whose
`match*` returns `true` AND `transform*` returns `Some` wins. `None` from
`transform*` is treated as "decline this instance" and dispatch continues to the next
matching extension before falling back to core.

Pass state from `preTransform` to `transformV1` via Spark's `TreeNodeTag` mechanism —
do NOT use external mutable state, which leaks across plan invocations.

#### `CometOperatorSerdeExtension`

```scala
trait CometOperatorSerdeExtension {
  def name: String
  def serdes: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]]
}
```

Contribs that need a custom physical operator (e.g., a contrib-specific scan exec
carrying contrib-private state) define their own `SparkPlan` subclass and register a
serde keyed on the new class:

```scala
case class CometMyFormatScanExec(...) extends CometNativeExec { /* ... */ }

class MyFormatSerdeExtension extends CometOperatorSerdeExtension {
  override def name: String = "myformat"
  override def serdes: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
    Map(classOf[CometMyFormatScanExec] -> CometMyFormatScanSerde)
}
```

The merged map across all extensions is computed once at registry load time;
`CometExecRule` consults it via `.get(op.getClass)`. Duplicate class keys across
contribs are logged as a warning at load — the convention is **one contrib defines a
class, that contrib owns its serde**.

Avoid relying on the legacy `scanImpl: String` tag pattern on a generic `CometScanExec`
— the SPI dispatches by class, not by tag.

##### `CometOperatorSerde[T <: SparkPlan]` contract

The serde itself lives in `org.apache.comet.serde.CometOperatorSerde` (not in the `spi`
package). Implement four members:

```scala
class CometMyFormatScanSerde extends CometOperatorSerde[CometMyFormatScanExec] {
  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_MYFORMAT_ENABLED)

  override def requiresNativeChildren: Boolean = false

  override def getSupportLevel(op: CometMyFormatScanExec): SupportLevel =
    Compatible(None)

  override def convert(
      op: CometMyFormatScanExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] = {
    // Build your contrib-private payload message and wrap in ContribOp.
    // See "Building a ContribOp envelope" below.
    Some(builder
      .setContribOp(ContribOp.newBuilder()
        .setKind("myformat-scan")
        .setPayload(myPayload.toByteString))
      .build())
  }

  override def createExec(nativeOp: Operator, op: CometMyFormatScanExec): CometNativeExec =
    new CometMyFormatScanExec(nativeOp, op.output, op.child, /* ... */)
}
```

`convert` MUST return `Some(builder.setContribOp(...).build())` for the dispatch to
reach your native planner; returning `None` falls the operator back to Spark.

### Native side: `comet-contrib-spi` crate

| Item | Purpose |
|---|---|
| `trait ContribOperatorPlanner` | Implemented by the contrib's native crate. `plan(ctx, payload, children) -> Arc<dyn ExecutionPlan>` receives a `&dyn ContribPlannerContext` (handle to core's planner services), the contrib-private payload bytes, and the already-built native children. |
| `trait ContribPlannerContext` | Implemented by core. Exposes the parquet exec builder, expression planner, schema conversion, object-store registration, and the `SessionContext` itself. Contribs reach into core through this trait rather than depending on `datafusion-comet` directly. |
| `struct ParquetDatasourceParams` | `#[non_exhaustive]` argument bundle for the parquet exec builder. Construct via `ParquetDatasourceParams::new(required_schema, object_store_url, file_groups)` and chain `with_*` setters. |
| `register_contrib_planner(kind, planner)` | Process-wide registry. Called from the contrib's `#[ctor::ctor]` at library load. |
| `lookup_contrib_planner_by_kind(kind)` | Used by core's planner; contribs rarely call directly. |
| `registered_contrib_kinds()` | Diagnostic snapshot of registered kinds. |
| `ContribError` | `#[non_exhaustive]` error type. Variants: `Plan(String)`, `BadPayload(String)`, `WrongChildCount { expected: String, actual: usize }`. Pattern matches MUST include a wildcard arm. |
| `ScopedContribPlannerRegistration` | (`#[cfg(any(test, feature = "test-utils"))]`) RAII guard that registers a planner for the lifetime of the guard and removes it on drop. Use in unit tests that exercise dispatch without polluting the global registry. |
| `_clear_for_test()` | (`#[cfg(any(test, feature = "test-utils"))]`) Wipes the registry entirely. **Test escape hatch only** — using it in parallel with other registry consumers is unsafe; prefer `ScopedContribPlannerRegistration`. |

The SPI crate depends only on `datafusion`, `datafusion-comet-proto`, and
`object_store`. Core links contribs via Cargo feature flags; contribs depend on the SPI
crate; nothing depends back on core from a contrib — the dependency graph is a DAG.

#### Why `ContribOperatorPlanner` is `Send + Sync` but `ContribPlannerContext` isn't

The planner trait is stored in an `Arc` inside a process-wide registry shared across
threads, so `Send + Sync` is load-bearing. The context is short-lived: a `&dyn`
reference passed for the duration of one synchronous `plan()` call, so the bound would
only restrict implementations without adding safety. Core's `PhysicalPlanner` carries
JNI handles that aren't `Send`; requiring it would force an `Arc<Mutex<...>>` dance
for no gain.

Contribs that want to spawn async work during `plan()` must capture only the
`Arc<SessionContext>` (which **is** `Send + Sync`) before crossing a thread boundary —
not the `&dyn ContribPlannerContext` itself.

#### Why `payload: &[u8]` instead of `Bytes`

The dispatcher already owns the decoded `ContribOp` proto; passing `&[u8]` is zero-copy
and avoids forcing every contrib to depend on the `bytes` crate. `prost::Message::decode`
accepts `&[u8]` directly. Contribs that want `Bytes` for downstream zero-copy work can
convert via `bytes::Bytes::copy_from_slice(payload)` — one allocation, once per plan
call.

#### `ContribError::WrongChildCount` convention

`expected` is a free-form human description; conventionally a phrase like `"exactly 1"`
or `"0 or 1"`. The dispatcher displays:
`wrong child count: expected exactly 1, got 2`.

#### Error message convention

The dispatcher wraps every `ContribError` with `format!("contrib planner {kind:?}: {e}")`,
so contribs should NOT re-prefix their messages with their own `kind`. Write:

```rust
ContribError::Plan(format!("file not found: {path}"))
```

not:

```rust
ContribError::Plan(format!("myformat-scan: file not found: {path}"))  // double prefix
```

## Proto layer

Each contrib carries its own `.proto` schema defining the message its `ContribOp.payload`
carries. Both halves of the contrib generate code from the same `.proto` source:

- **Rust**, in the contrib's `build.rs` via `prost-build`.
- **Java**, in the contrib's `pom.xml` via `protoc-jar-maven-plugin`.

Use your own proto **package name** (e.g., `comet.contrib.<name>`) so symbols never
collide with core or with other contribs. Add `contrib/<name>/native/src/generated/`
to `.gitignore`.

### Proto, native side

`contrib/example/native/build.rs` is the template:

```rust
fn main() -> std::io::Result<()> {
    let out = std::path::PathBuf::from("src/generated");
    std::fs::create_dir_all(&out)?;
    prost_build::Config::new()
        .out_dir(&out)
        .compile_protos(&["src/proto/example_op.proto"], &["src/proto"])?;
    Ok(())
}
```

Note: writing into `src/generated/` rather than `$OUT_DIR` is a deliberate deviation
from idiomatic prost. It lets `lib.rs` do
`include!(concat!("generated/", "comet.contrib.example.rs"))` with a stable filesystem
path — convenient for editor tooling. The file is gitignored.

The contrib's `Cargo.toml` adds `prost-build` to `[build-dependencies]` and `prost`
to `[dependencies]`.

### Proto, JVM side

Comet's main build shades `com.google.protobuf` under `${comet.shade.packageName}.protobuf`
(see the root `pom.xml`'s `<comet.shade.packageName>` property). The generated
`OperatorOuterClass.ContribOp` references the shaded package. Your contrib's
generated Java proto MUST therefore live under the same shade prefix at runtime, or
the dispatcher will refuse `setContribOp(...)` because `ByteString` / `Message` types
won't align.

The simplest path is to add `protoc-jar-maven-plugin` to your contrib `pom.xml`,
generate Java classes during `generate-sources`, and rely on the parent pom's shading
plugin to relocate `com.google.protobuf` consistently:

```xml
<build>
  <plugins>
    <plugin>
      <groupId>com.github.os72</groupId>
      <artifactId>protoc-jar-maven-plugin</artifactId>
      <version>${protoc-jar-maven-plugin.version}</version>
      <executions>
        <execution>
          <phase>generate-sources</phase>
          <goals><goal>run</goal></goals>
          <configuration>
            <protocArtifact>com.google.protobuf:protoc:${protobuf.version}</protocArtifact>
            <inputDirectories>
              <include>native/src/proto</include>
            </inputDirectories>
          </configuration>
        </execution>
      </executions>
    </plugin>
  </plugins>
</build>
```

And depend on `protobuf-java` so the generated classes compile:

```xml
<dependency>
  <groupId>com.google.protobuf</groupId>
  <artifactId>protobuf-java</artifactId>
  <version>${protobuf.version}</version>
  <scope>provided</scope>
</dependency>
```

`provided` scope, not `compile` — the user's classpath already has the shaded
protobuf-java via `comet-spark`.

`contrib/example/` does not exercise this path because its Scala side never builds a
`ContribOp` (the example's tests only validate dispatch wiring, not payload generation).
The first real-format contrib in the tree will be the place this section's snippets
are first exercised against CI.

### Building a `ContribOp` envelope

From your `CometOperatorSerde.convert`:

```scala
import org.apache.comet.serde.OperatorOuterClass.{ContribOp, Operator}
import comet.contrib.myformat.{MyOpProto}  // your generated Java proto

val payload: MyOpProto = MyOpProto.newBuilder()
  .setSomeField(scanState.someField)
  .build()

val envelope = ContribOp.newBuilder()
  .setKind("myformat-scan")
  .setPayload(payload.toByteString)
  .build()

Some(builder.setContribOp(envelope).build())
```

The Rust generated field on the `Operator` enum is called `op_struct` (a `oneof`); the
Java builder method is `Operator.Builder.setContribOp(ContribOp)`. Both correspond to
the same wire-format field — the naming difference is purely the language conventions
of the code generators.

## Wire-format flow

1. Your Scala code intercepts a `FileSourceScanExec` (or `BatchScanExec`) matching your
   format, returning a `CometMyFormatScanExec` from `transformV1`/`transformV2`.
2. `CometExecRule` later picks up the `CometMyFormatScanExec` instance, finds your serde
   via the class-keyed dispatch, and calls `serde.convert(op, builder, childOp...)`.
3. Your `convert` builds a contrib-private proto message (whatever fields you need),
   serializes it, wraps in `ContribOp { kind, payload }`, and stuffs it into the
   operator builder via `setContribOp`.
4. The proto is shipped through JNI to native.
5. Core's native planner sees `OpStruct::ContribOp`, validates `kind` (non-empty,
   under 16 MiB payload, registered), looks up the planner, calls
   `planner.plan(ctx, payload, children)`.
6. Your native crate decodes `payload` into your own proto type and returns an
   `Arc<dyn ExecutionPlan>`. Use `ctx` to reach core's parquet builder, expression
   planner, etc. (see the next section).
7. Core wraps the result in a `SparkPlan` and continues planning.

## Walking a real `plan()` body

The example contrib's planners return `EmptyExec` — none of the `ContribPlannerContext`
methods are exercised. A file-scan contrib's `plan()` typically threads through all of
them:

```rust
use std::sync::Arc;
use comet_contrib_spi::{
    ContribError, ContribOperatorPlanner, ContribPlannerContext, ParquetDatasourceParams,
};
use datafusion::physical_plan::ExecutionPlan;
use prost::Message;

use crate::proto::MyFormatScan;

pub struct MyFormatScanPlanner;

impl ContribOperatorPlanner for MyFormatScanPlanner {
    fn plan(
        &self,
        ctx: &dyn ContribPlannerContext,
        payload: &[u8],
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, ContribError> {
        // 1. Decode your contrib-private payload.
        let scan = MyFormatScan::decode(payload)
            .map_err(|e| ContribError::BadPayload(format!("decode MyFormatScan: {e}")))?;

        // 2. Translate the Spark proto schemas into Arrow schemas via core.
        let required_schema = ctx.convert_spark_schema(&scan.required_schema);
        let data_schema = ctx.convert_spark_schema(&scan.data_schema);
        let partition_schema = ctx.convert_spark_schema(&scan.partition_schema);

        // 3. Lift Catalyst data-filter Exprs into PhysicalExprs core can execute.
        let data_filters = scan
            .data_filters
            .iter()
            .map(|e| ctx.build_physical_expr(e, required_schema.clone()))
            .collect::<Result<Vec<_>, _>>()?;

        // 4. Register the object store. The returned URL is what every PartitionedFile
        //    in your file_groups must use; the returned Path is the canonical key
        //    inside that store, usually the per-file path the contrib uses to set
        //    `partitioned_file.object_meta.location`.
        let any_file_url = scan.tasks
            .first()
            .map(|t| t.file_path.clone())
            .ok_or_else(|| ContribError::Plan("empty file list".into()))?;
        let object_store_options = scan.object_store_options.clone();
        let (object_store_url, _path_template) =
            ctx.prepare_object_store(any_file_url, &object_store_options)?;

        // 5. Build the file_groups: Vec<Vec<PartitionedFile>> with one inner Vec per
        //    desired DataFusion partition.
        let file_groups = build_partitioned_files(&scan.tasks /* contrib's helper */)?;

        // 6. Hand the bundle to core's tuned ParquetSource.
        let exec = ctx.build_parquet_datasource_exec(
            ParquetDatasourceParams::new(
                required_schema.clone(),
                object_store_url,
                file_groups,
            )
            .with_data_schema(data_schema)
            .with_partition_schema(partition_schema)
            .with_data_filters(data_filters)
            .with_session_timezone(&scan.session_timezone)
            .with_case_sensitive(scan.case_sensitive),
        )?;

        // 7. Optionally wrap the parquet exec in contrib-specific operators
        //    (e.g. a Delta DV filter).
        Ok(exec)
    }
}
```

The flow above mirrors what a real Delta or Iceberg port does. Pieces a contrib
typically owns inside itself, NOT exposed through `ContribPlannerContext`:

- Reading the format's transaction log / manifest (kernel-rs for Delta, iceberg-rust
  for Iceberg).
- Resolving file paths to absolute URLs on the driver.
- Computing per-file deletion-vector / equality-delete row indexes.
- Wrapping the parquet exec in a per-row-filter operator if the format needs it.

Use `ctx` for things that already exist inside core (object-store registry, parquet
plumbing, expression planner); reimplement the format-specific parts in your contrib.

## `#[ctor]` registration: panic safety + logging

The contrib's native crate registers its planners during library init via
`#[ctor::ctor]`. Three quirks to get right:

**Panics in `#[ctor]` abort the JVM process** before `JNI_OnLoad` runs, with no
diagnostic on macOS/Linux. Wrap every ctor body in `std::panic::catch_unwind` and
emit a stderr message on failure:

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
must go through `eprintln!`.

**Cross-platform caveats.** `#[ctor::ctor]` works on Linux / macOS / Windows MSVC, but
the order of ctor execution across rlibs is link-order dependent and not guaranteed
across compiler versions. Your contrib's ctor **MUST NOT** depend on another contrib
already being registered.

The corresponding JVM rule: **do not call `CometExtensionRegistry.load()` from a
class's static initializer** (Scala `object` init, or a JVM-level static block). Scala
monitors are reentrant so it won't deadlock, but re-entry would observe the partially-
built state and shadow the in-flight publication.

## Logging conventions

- **From the contrib's Scala code**: use `org.slf4j.Logger` / Comet's `Logging` trait.
  Lifetime-event logs (extension discovered, contrib registered) at INFO; per-plan
  decisions at DEBUG; correctness violations at WARN.
- **From the contrib's Rust `#[ctor]`**: `eprintln!` only (logger not yet initialised).
- **From the contrib's Rust `plan()` body and runtime code**: `log::*` macros. Choose a
  `target:` matching your crate name so users can filter:
  `log::debug!(target: "comet::contrib::myname", "built plan with {n} files")`.
- **Error context**: pre-format error messages with enough context that the dispatcher's
  `contrib planner "myname-scan": <your-message>` wrapper reads sensibly. Do not
  re-prefix with your `kind`.

## Diagnosing a misconfigured contrib

The most common first-hour problem is "I packaged my JAR and it does nothing." Three
signals to check:

- `CometExtensionRegistry` logs at INFO. When discovery runs and finds zero entries,
  it emits:
  ```
  Comet contrib extensions: none discovered on classpath
    (no META-INF/services entries for CometScanRuleExtension or
     CometOperatorSerdeExtension)
  ```
  Confirm your JAR ships the `META-INF/services/...CometScanRuleExtension` file with
  the correct fully-qualified extension class on its own line.
- ServiceLoader instantiation failures are logged at WARN with `Failed to load a
  CometScanRuleExtension entry; skipping`. Causes: missing no-arg constructor on the
  extension class, exception thrown by the constructor.
- `registered_contrib_kinds()` (Rust) returns the kinds currently registered. If your
  contrib's kind is missing under a build that should include it, the Cargo feature is
  off or the `extern crate` in `native/core/src/lib.rs` is missing.

Set the logger for `org.apache.comet.spi.CometExtensionRegistry` to INFO/WARN to surface
both messages.

### Classloader interaction

`CometExtensionRegistry.load()` uses `Thread.currentThread().getContextClassLoader()`
first, with `getClass.getClassLoader` as fallback. Either should see Comet and the
contrib JAR in typical Spark deploy modes (`--jars`, `--packages`, application
classpath). Discovery is **lazy** — triggered the first time `CometScanRule._apply` or
`CometExecRule._apply` runs against a Comet-enabled session. By that point all
`--jars`-injected JARs are on the classpath, so order-of-arrival inside the driver
JVM is not a concern.

## Maven JAR packaging + version pinning

The example contrib ships a thin JAR with no shading. Real contribs SHOULD prefer thin
JARs too. If your contrib must include a third-party library that conflicts with the
user's classpath, shade the conflicting classes under your contrib's package prefix
(`org.apache.comet.contrib.<name>.shaded.*`) so classloader collisions stay local.
Do **not** shade `comet-spark` or its transitive dependencies — those are `provided`
scope and the user supplies them.

`comet-spark`'s shading of `com.google.protobuf` is the one external dep that does
need attention: generated Java classes from your `.proto` reference the shaded
package, which is handled automatically when you use the parent pom's plugin
configuration (the contrib pom inherits the same `<comet.shade.packageName>` property).

### Version pinning

`comet-spark` is `<scope>provided</scope>` in your contrib's pom. Pin the dependency to
the exact Comet patch version your contrib was tested against:

```xml
<dependency>
  <groupId>org.apache.datafusion</groupId>
  <artifactId>comet-spark-spark${spark.version.short}_${scala.binary.version}</artifactId>
  <version>0.17.0</version>  <!-- not ${project.version} unless your contrib is in-tree -->
  <scope>provided</scope>
</dependency>
```

In-tree contribs use `${project.version}`; out-of-tree contribs use the explicit Comet
version they were built against. A contrib built against Comet `0.17.x` is not
guaranteed runtime-compatible with Comet `0.18.x` — the SPI is alpha.

### Multi-Spark-version support

Comet itself ships a per-Spark-minor-version artifact via the
`spark.version.short` Maven profile (`3.4`, `3.5`, `4.0`). Your contrib follows the
same model:

- Pick the matching Spark profile when building (`-Dspark.version.short=3.5`).
- The resulting artifact ID encodes the Spark version
  (`comet-contrib-<name>-spark3.5_2.13`).
- If your contrib must support multiple Spark minor versions, publish one artifact per
  profile, mirroring Comet. Shim code that differs across Spark versions belongs under
  `src/main/scala-${shims.majorVerSrc}/` (see Comet's `common/`/`spark/` modules for
  the existing pattern).

## Testing

`contrib/example/` demonstrates the JVM-side test pattern:

- A unit test that calls `CometExtensionRegistry.resetForTesting()` and `load()`,
  then asserts the contrib's extension is discovered via ServiceLoader. Catches
  packaging mistakes (missing `META-INF/services`, wrong class name).
- Per-method unit tests for the extension's `matches*` / `transform*` logic.

For native unit tests of a `ContribOperatorPlanner`, use `ScopedContribPlannerRegistration`
from `comet-contrib-spi` to install and tear down planners without polluting the
global registry:

```rust
use comet_contrib_spi::ScopedContribPlannerRegistration;

#[test]
fn my_planner_round_trip() {
    let _guard = ScopedContribPlannerRegistration::new(
        "myformat-scan",
        Arc::new(MyFormatScanPlanner),
    );
    // ... exercise dispatch ...
}
```

Pair with `#[serial_test::serial]` if your test asserts on `registered_contrib_kinds()`
(which other tests' guards may be temporarily mutating in parallel).

### End-to-end (Rust + Scala round-trip)

A full integration test wires the Spark plan through real JNI and asserts the contrib's
native planner ran:

1. Build a `SparkSession` configured with `spark.sql.extensions =
   org.apache.comet.CometSparkSessionExtensions` and the contrib JAR on the classpath
   (sbt: `Test/unmanagedClasspath`; Maven: the contrib's own test scope already has it).
2. Submit a query that hits your format's table reader.
3. Inspect the produced physical plan for your contrib's exec class
   (`plan.exists(_.isInstanceOf[CometMyFormatScanExec])`).
4. Run the plan and assert against the result (e.g., a row count that only your native
   planner could produce, distinguishable from a Spark fall-back).

The example contrib's test fixture doubles as smoke coverage for the SPI dispatch path
itself (kind lookup, payload decode, error wrapping) under Comet's own CI when the
`contrib-example` feature is enabled.

## Payload size cap

The native dispatcher enforces a hard ceiling of **16 MiB** on `ContribOp.payload`
(`MAX_CONTRIB_PAYLOAD_BYTES` in `native/core/src/execution/planner.rs`). A malformed
JVM-side serde (or one that accidentally accumulates state across plan calls)
producing a larger payload is rejected with a clear error message before the contrib's
`plan()` runs. The cap is comfortably above any plausible file-scan payload (Delta
with ~100k tasks weighs in around 3–4 MiB) and well below "heap pressure" territory.
If your contrib has a legitimate need for a higher ceiling, file an issue with the
size you need and the use case — the cap is a guardrail, not a feature.

## Registry implementation note

The native contrib planner registry is currently a `RwLock<HashMap<String, Arc<...>>>`.
Lookups happen once per `ContribOp` plan call; writes happen only during library init.
The implementation may switch to a lock-free primitive (`ArcSwap`) in a future release
if profiling shows the read path matters; the public API stays unchanged either way.

## See also

- [`contrib/example/`](https://github.com/apache/datafusion-comet/tree/main/contrib/example) —
  the worked reference.
- [`native/contrib-spi/`](https://github.com/apache/datafusion-comet/tree/main/native/contrib-spi) —
  the leaf SPI crate.
- [`spark/src/main/scala/org/apache/comet/spi/`](https://github.com/apache/datafusion-comet/tree/main/spark/src/main/scala/org/apache/comet/spi) —
  the JVM SPI traits.
