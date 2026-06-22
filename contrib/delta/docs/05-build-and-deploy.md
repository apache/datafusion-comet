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

| Switch                                               | What it controls                                                                                                                         |
| ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Maven: `-Pcontrib-delta`                             | Scala/Java contrib classes are compiled and packaged into `comet-spark` JAR. The Spark extension is registered.                          |
| Cargo: `--features contrib-delta` (on `native/core`) | The contrib Rust crate is linked into `libcomet`. The JNI symbol `Java_org_apache_comet_contrib_delta_Native_planDeltaScan` is exported. |

Mismatched switches produce a clear failure:

- JAR with contrib, dylib without → first Delta query: `UnsatisfiedLinkError: planDeltaScan`
- JAR without contrib, dylib with → contrib classes simply absent; `DeltaIntegration.transformV1IfDelta` returns `None`; all Delta queries go through Spark's reader

The Maven `verify` phase has no cross-language assertion; getting both
switches set is on the operator.

## Cargo manifest structure

```
native/
├── core/                          # Comet core native code (workspace member)
│   ├── Cargo.toml                 # crate name datafusion-comet; feature contrib-delta = ["comet-contrib-delta"]
│   └── src/execution/planner/delta_scan.rs   # contrib-delta-gated dispatcher shim
└── proto/                         # Comet proto definitions
    └── Cargo.toml

contrib/delta/native/              # Standalone, NOT a workspace member
├── Cargo.toml                     # crate name comet-contrib-delta; arrow = "58.1", delta_kernel = "0.24" (arrow-58 feature)
└── src/
    ├── lib.rs
    ├── engine.rs
    ├── scan.rs
    ├── kernel_scan.rs
    ├── planner.rs
    ├── predicate.rs
    ├── dv_reader.rs
    ├── error.rs
    └── jni.rs
```

The contrib crate is referenced from `native/core/Cargo.toml` as a path
dependency gated by the `contrib-delta` feature; the dependency package is
`comet-contrib-delta`.

`native/core` and `contrib/delta/native` are NOT in the same workspace, so
Cargo resolves their dependencies independently. Both crate graphs pin
arrow-58 (kernel-rs is pulled in with its `arrow-58` feature, matching
Comet core), so batches produced by kernel drop straight into the Comet
plan with no Arrow bridge. Keeping the contrib out of the core workspace
isolates its heavy transitive dependency tree (`delta_kernel`,
`object_store`, …) from default builds entirely.

If you `cargo build` directly in `contrib/delta/native/`, you get a `.rlib`
that does nothing useful — there's no JNI entry wired into `libcomet` until
the parent `native/core` crate enables the `contrib-delta` feature and
pulls this crate in. Always build from `native/core` (or via the Maven
invocations that do so).

## Maven profile

`spark/pom.xml` declares the `contrib-delta` profile, which does two things:

1. Adds `io.delta:delta-spark_${scala.binary.version}` at provided scope so
   the contrib's reflective helpers and tests have the Delta types on the
   classpath at compile time
2. Adds `contrib/delta/src/main/scala/` (and the matching test sources) as
   extra source directories via `build-helper-maven-plugin` so its sources
   compile into `comet-spark.jar`

The `delta.version` is NOT pinned by the `contrib-delta` profile itself; each
Spark profile sets its matching `delta.version` (Spark 3.5 → 3.3.2,
Spark 4.0 → 4.0.0, Spark 4.1 → 4.1.0), and the parent POM carries a top-level
`4.1.0` default as a floor for invocations that activate no Spark profile.

```xml
<profile>
  <id>contrib-delta</id>
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
        <groupId>org.codehaus.mojo</groupId>
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

The Maven profile does _not_ trigger the native Cargo build — that is a
separate invocation. Operators must remember to pass
`--features contrib-delta` to the Cargo command (or set
`COMET_FEATURES=contrib-delta`, which `make release` forwards as
`--features=...`) so the dylib and the JAR end up consistent. The sections
below cover the supported combinations.

The contrib registers its Spark extension reflectively from
`DeltaIntegration` rather than via
`META-INF/services/SparkSessionExtensionsProvider`, so no service file is
required.

## What the `comet-spark` JAR looks like

| File or class                                         | Default build                                    | `-Pcontrib-delta` build |
| ----------------------------------------------------- | ------------------------------------------------ | ----------------------- |
| `org.apache.comet.rules.CometScanRule`                | yes                                              | yes                     |
| `org.apache.comet.rules.DeltaIntegration`             | yes (reflective bridge, returns None at runtime) | yes                     |
| `org.apache.comet.contrib.delta.DeltaScanRule`        | absent                                           | present                 |
| `org.apache.comet.contrib.delta.CometDeltaNativeScan` | absent                                           | present                 |
| `org.apache.spark.sql.comet.CometDeltaNativeScanExec` | absent                                           | present                 |
| `org.apache.spark.sql.comet.CometDeltaCdfScanExec`    | absent                                           | present                 |
| `org.apache.spark.sql.comet.DeltaPlanDataInjector`    | absent                                           | present                 |

A `default` consumer is therefore entirely free of Delta classes. Running
the default JAR against a Delta workload simply means
`DeltaIntegration.transformV1IfDelta` returns `None` and Spark's
unaccelerated path runs.

## What `libcomet` looks like

The dylib produced by `cargo build --release -p comet --features contrib-delta`
contains:

- All of Comet core
- The contrib Rust code, statically linked
- `Java_org_apache_comet_contrib_delta_Native_planDeltaScan` (and
  `…_planDeltaReadSchemas`) exported

Default build (no feature) omits the contrib code entirely. The
`OpStruct::DeltaScan` dispatcher arm in
`native/core/src/execution/planner.rs` exists unconditionally; under a
default build its `#[cfg(not(feature = "contrib-delta"))]` branch returns a
clear error if a `DeltaScan` proto somehow arrives:

```rust
#[cfg(not(feature = "contrib-delta"))]
{
    let _ = scan;
    Err(GeneralError(
        "Received a DeltaScan operator but core was built without the \
         `contrib-delta` Cargo feature. Rebuild with both \
         `-Pcontrib-delta` (Maven) and `--features contrib-delta` (Cargo) \
         to enable Delta Lake support."
            .into(),
    ))
}
#[cfg(feature = "contrib-delta")]
{
    delta_scan::plan_delta_scan(self, spark_plan, scan)
}
```

In practice this can't fire because the JVM side wouldn't have produced a
`DeltaScan` proto without the contrib classpath, but defense-in-depth.

## How to build and ship

For a Comet binary that supports Delta:

```bash
# Build the native dylib with the contrib feature
cargo build -p datafusion-comet --features contrib-delta --release

# Build and install the comet-spark JAR with the contrib profile.
# Spark 4.1 requires JDK 17 (java.lang.Record); the parent POM defaults
# java.version=11, so override it when building the Spark 4.1 profile.
mvn -Pspark-4.1 -Pcontrib-delta -DskipTests \
  -Djava.version=17 -Dmaven.compiler.source=17 -Dmaven.compiler.target=17 \
  install
```

The two commands are independent — the Maven build doesn't drive the
Cargo build. The regression script `contrib/delta/dev/run-regression.sh`
runs both in the right order, which is the easiest way to keep them in
sync during iteration.

For default (no Delta) builds, omit both switches:

```bash
cargo build -p datafusion-comet --release
mvn -Pspark-4.1 -DskipTests \
  -Djava.version=17 -Dmaven.compiler.source=17 -Dmaven.compiler.target=17 \
  install
```

## CI

Three GitHub Actions workflows exercise the Delta contrib:

- `.github/workflows/delta_contrib_test.yml` — builds the native library
  with `--features contrib-delta` once, then runs the contrib's own Scala
  suites across a matrix of (Spark 3.5 + Delta 3.3.2), (Spark 4.0 + Delta
  4.0.x), and (Spark 4.1 + Delta 4.1.x). A build-gate job runs
  `dev/verify-contrib-delta-gate.sh` to prove default builds carry zero
  Delta surface.
- `.github/workflows/delta_regression_test.yml` — runs the Delta own-suite
  regression via `contrib/delta/dev/run-regression.sh` (which applies the
  Delta test diff `contrib/delta/dev/diffs/<delta-version>.diff`) across the
  same Spark matrix.

`dev/verify-contrib-delta-gate.sh` asserts three things: a default
`cargo tree` excludes `comet-contrib-delta` / `delta_kernel`; a default
Maven `dependency:list` excludes `io.delta:*` (and per-Spark pinning is
correct: 3.x for Spark 3.5, 4.0.x for Spark 4.0, 4.1.x for Spark 4.1); and
the default `libcomet.dylib` has no Delta external symbols.

## Local iteration tips

- **Iterate on Scala only**: `mvn -Pspark-4.1 -Pcontrib-delta -DskipTests
-pl spark -am -Djava.version=17 -Dmaven.compiler.source=17
-Dmaven.compiler.target=17 install` — skips the native build, reuses your
  existing dylib. Build with `-pl spark` (the contrib folds into the
  `spark` module via `build-helper`); never `-pl contrib/delta`, which is
  not a Maven module.
- **Iterate on Rust only**: build native (`cargo build -p datafusion-comet
--features contrib-delta`), then copy `native/target/release/libcomet.*`
  into the location Maven bundled from (`spark/target/...`) if you want to
  skip the JAR repack — the contrib classes are still wired the same way

The regression script `contrib/delta/dev/run-regression.sh` handles all of
this from scratch but is slow (full install + sbt + JVM forks).

---

**Navigation** · [← 04 Design decisions](04-design-decisions.md) · [↑ Index](README.md) · Next → [06 Fallback and ops](06-fallback-and-ops.md)
