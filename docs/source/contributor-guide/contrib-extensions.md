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
| `CometScanRuleExtension` | Intercept scan-tree transformation. Override `preTransform` for tree-level rewrites (e.g., undoing your format's own Catalyst strategy); `matchesV1` / `transformV1` for V1 `FileSourceScanExec`; `matchesV2` / `transformV2` for V2 `BatchScanExec`. The first matching extension wins, returning `None` falls back to core's existing file-format dispatch. |
| `CometOperatorSerdeExtension` | Contribute additional `SparkPlan` class → `CometOperatorSerde` mappings to `CometExecRule`. Used when the contrib has its own physical operator (e.g. a contrib-specific scan exec) that needs native serialization. |
| `CometExtensionRegistry` | Process-wide singleton. `load()` is called once during `CometSparkSessionExtensions.apply`; subsequent calls are no-ops. Test-only `resetForTesting()` for unit tests that need a clean registry. |

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
| `trait ContribOperatorPlanner` | Implemented by the contrib's native crate. The `plan(payload, children) -> Arc<dyn ExecutionPlan>` method receives the contrib-private payload bytes from the ContribOp envelope and the already-built native children. |
| `register_contrib_planner(kind, planner)` | Process-wide registry. Called from the contrib's `#[ctor::ctor]` at library load. |
| `lookup_contrib_planner_by_kind(kind)` | Used by core's planner; contribs rarely call directly. |
| `ContribError` | Minimal error type. Core converts to its own `ExecutionError` at the dispatch site. |

The SPI crate is intentionally a thin leaf: it has no dependencies on core. This is what
breaks the would-be cyclic dependency (core links contribs via Cargo feature flags;
contribs need the SPI types — both depend on a third leaf crate instead of each other).

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

## Cargo feature gate

Each contrib's native rlib is wired into core via a feature flag. Build core with:

```bash
# Default release build: all in-tree contribs enabled (contrib-example, future ones too)
cargo build

# Slim build: zero contrib code in libcomet
cargo build --no-default-features
```

The JVM side is **always** conditional: the contrib JAR is its own artifact, and Spark
only picks it up when it's on the classpath. So even with the Cargo feature on, a user
who doesn't add the contrib JAR sees no behaviour change — the contrib's native planner
sits dormant in the registry, waiting for a JVM serde that never calls it.

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

- [`docs/contrib-delta-migration-plan.md`](../../../contrib-delta-migration-plan.md) —
  the architectural rationale + the two-PR plan that introduced the SPI.
- [`contrib/example/`](https://github.com/apache/datafusion-comet/tree/main/contrib/example) —
  the worked reference.
- [`native/contrib-spi/`](https://github.com/apache/datafusion-comet/tree/main/native/contrib-spi) —
  the leaf SPI crate.
