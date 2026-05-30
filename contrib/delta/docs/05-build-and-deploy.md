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

# Build, packaging, and deployment

## The two switches

Two things must be enabled together to get Delta acceleration:

| Switch | What it controls |
|---|---|
| Maven: `-Pcontrib-delta` | Scala/Java contrib classes are compiled and packaged into `comet-spark` JAR. The Spark extension is registered. |
| Cargo: `--features contrib-delta` (on `native/core`) | The contrib Rust crate is linked into `libcomet`. The JNI symbol `Java_…_planDeltaScan` is exported. |

Mismatched switches produce a clear failure:

- JAR with contrib, dylib without → first Delta query: `UnsatisfiedLinkError: planDeltaScan`
- JAR without contrib, dylib with → contrib classes simply absent; `DeltaIntegration.transformV1IfDelta` returns `None`; all Delta queries go through Spark's reader

The Maven `verify` phase has no cross-language assertion; getting both
switches set is on the operator.

## Cargo manifest structure

```
native/
├── core/                          # Comet core native code (workspace member)
│   ├── Cargo.toml                 # arrow = "58", with feature contrib-delta = ["delta-contrib-impl"]
│   └── src/execution/planner/contrib_delta_scan.rs
└── proto/                         # Comet proto definitions
    └── Cargo.toml

contrib/delta/native/              # Standalone, NOT a workspace member
├── Cargo.toml                     # arrow = "57" (kernel-rs's pin)
└── src/
    ├── lib.rs
    ├── engine.rs
    ├── scan.rs
    ├── planner.rs
    ├── dv_filter.rs
    ├── synthetic_columns.rs
    └── jni.rs
```

The contrib crate is referenced from `native/core/Cargo.toml` as a path
dependency gated by the `contrib-delta` feature:

```toml
[features]
contrib-delta = ["delta-contrib-impl"]

[dependencies]
delta-contrib-impl = {
    package = "comet-delta-contrib",
    path = "../../contrib/delta/native",
    optional = true,
}
```

`native/core` and `contrib/delta/native` are NOT in the same workspace, so
Cargo resolves their dependencies independently. This is the only way to
keep arrow-57 (kernel-rs's pin) and arrow-58 (Comet core's pin) in the same
final binary without cross-contamination — they end up as distinct crate
graphs and the boundary between them is the Arrow C Data Interface
(stable across versions).

If you `cargo build` directly in `contrib/delta/native/`, you get a `.rlib`
that does nothing useful — there's no JNI entry compiled in until the
parent `native/core` crate enables the `contrib-delta` feature and pulls
this crate in. Always build from `native/core` (or via the `make`
targets / Maven invocations that do so).

## Maven profile

`spark/pom.xml` declares the `contrib-delta` profile, which does two things:

1. Adds `io.delta:delta-spark` at provided scope so the contrib's
   reflective helpers and tests have the Delta types on the classpath at
   compile time
2. Adds `contrib/delta/src/main/scala/` as an extra source directory via
   `build-helper-maven-plugin` so its sources compile into `comet-spark.jar`

```xml
<profile>
  <id>contrib-delta</id>
  <properties>
    <delta.version>4.1.0</delta.version>
  </properties>
  <dependencies>
    <dependency>
      <groupId>io.delta</groupId>
      <artifactId>delta-spark_${scala.binary.version}</artifactId>
      <version>${delta.version}</version>
      <scope>provided</scope>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <artifactId>build-helper-maven-plugin</artifactId>
        <executions>
          <execution>
            <id>add-contrib-delta-source</id>
            <phase>generate-sources</phase>
            <goals><goal>add-source</goal></goals>
            <configuration>
              <sources>
                <source>${project.parent.basedir}/contrib/delta/src/main/scala</source>
              </sources>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</profile>
```

The Maven profile does *not* trigger the native Cargo build — that is a
separate invocation. Operators must remember to pass
`--features contrib-delta` to the Cargo command (or set the equivalent
environment variable used by `make release`) so the dylib and the JAR end
up consistent. The build invariants section below covers the supported
combinations.

The contrib registers its Spark extension reflectively from
`DeltaIntegration` rather than via
`META-INF/services/SparkSessionExtensionsProvider`, so no service file is
required.

## What the `comet-spark` JAR looks like

| File or class | Default build | `-Pcontrib-delta` build |
|---|---|---|
| `org.apache.comet.rules.CometScanRule` | yes | yes |
| `org.apache.comet.rules.DeltaIntegration` | yes (reflective bridge, returns None at runtime) | yes |
| `org.apache.comet.contrib.delta.DeltaScanRule` | absent | present |
| `org.apache.comet.contrib.delta.CometDeltaNativeScan` | absent | present |
| `org.apache.comet.contrib.delta.DeltaPlanDataInjector` | absent | present |
A `default` consumer is therefore entirely free of Delta classes. Running
the default JAR against a Delta workload simply means
`DeltaIntegration.transformV1IfDelta` returns `None` and Spark's
unaccelerated path runs.

## What `libcomet` looks like

The dylib produced by `cargo build --release -p comet --features contrib-delta`
contains:

- All of Comet core
- The contrib Rust code, statically linked
- `Java_org_apache_comet_Native_planDeltaScan` exported

Default build (no feature) omits the contrib code entirely; the dispatcher
in `native/core/src/execution/planner/mod.rs` has a `#[cfg(not(feature =
"contrib-delta"))]` arm that returns a clear error if a `DeltaScan` proto
somehow arrives:

```rust
#[cfg(not(feature = "contrib-delta"))]
OpStruct::DeltaScan(_) => Err(DataFusionError::Plan(
    "DeltaScan operator received but native build does not include contrib-delta feature".into(),
)),
```

In practice this can't fire because the JVM side wouldn't have produced a
`DeltaScan` proto without the contrib classpath, but defense-in-depth.

## How to build and ship

For a Comet binary that supports Delta:

```bash
# Build the native dylib with the contrib feature
cargo build -p comet --features contrib-delta --release

# Build and install the comet-spark JAR with the contrib profile
mvn -Pspark-4.1 -Pcontrib-delta -DskipTests install
```

The two commands are independent — the Maven build doesn't drive the
Cargo build. The regression script `contrib/delta/dev/run-regression.sh`
runs both in the right order, which is the easiest way to keep them in
sync during iteration.

For default (no Delta) builds, omit both switches:

```bash
cargo build -p comet --release
mvn -Pspark-4.1 -DskipTests install
```

## CI matrix expectation

CI should exercise both build paths. Adding a `-Pcontrib-delta` matrix
entry to the existing Spark profile axis is sufficient — the regression
suite then runs against the Delta test diff (`dev/diffs/delta/4.1.0.diff`)
under that matrix entry.

## Local iteration tips

- **Iterate on Scala only**: `mvn -Pspark-4.1 -Pcontrib-delta -DskipTests
  -pl spark -am install` — skips the native build, reuses your existing
  dylib
- **Iterate on Rust only**: build native (`cargo build -p comet --features
  contrib-delta`), then `cp target/release/libcomet.dylib
  spark/target/...` if you want to skip the JAR repack — the contrib
  classes are still wired the same way

The regression script `contrib/delta/dev/run-regression.sh` handles all of
this from scratch but is slow (full install + sbt + JVM forks).

---

**Navigation** · [← 04 Design decisions](04-design-decisions.md) · [↑ Index](README.md) · Next → [06 Fallback and ops](06-fallback-and-ops.md)
