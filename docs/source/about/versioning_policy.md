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

# Versioning Policy

Apache DataFusion Comet follows [semantic versioning](https://semver.org/) with the format
`MAJOR.MINOR.PATCH`. This document describes what each component of a release means, what
compatibility guarantees Comet provides, and how Comet relates to Apache Spark versions.

This policy is inspired by, but is not identical to, the
[Apache Spark versioning policy](https://spark.apache.org/versioning-policy.html). The differences
reflect the fact that Comet is currently pre-1.0 and ships much more frequently than Spark.

## Pre-1.0 Releases

Comet is currently in the `0.x` series. Per semantic versioning, the `0.x` series is considered
unstable:

- A minor release (`0.X.0`) may include breaking changes.
- A patch release (`0.X.Y`, where `Y > 0`) contains bug fixes only.

In particular, the following are explicitly **not** stable in the `0.x` series and may change in any
minor release without prior notice:

- Configuration keys under `spark.comet.*` (names, defaults, and semantics).
- The protobuf format used to serialize query plans between the JVM and the native library.
- Internal Scala, Java, and Rust APIs that are not part of the documented public API.

Where a breaking change is significant, it will be called out in the release notes.

## Compatibility Commitments

The following commitments apply to every Comet release, including the `0.x` series.

### Public Scala and Java API

Public classes and methods (for example, `org.apache.spark.CometPlugin`) are considered part of
Comet's public API. Removing or making source- or binary-incompatible changes to a public API
requires a deprecation cycle: the API must remain available, with a deprecation warning, for at
least one minor release before removal.

Public APIs annotated with `@Unstable` are exempt from this guarantee and may change in any minor
release without a deprecation cycle. The `@Unstable` annotation does not yet exist and will be
introduced as the need arises.

### Query Result Semantics

Expressions and operators whose support level is `Compatible` are expected to produce results that
match Apache Spark. Result differences in `Compatible` items are tracked as bugs and fixed in
subsequent releases.

Items whose support level is `Incompatible` or `Unsupported` have no result-compatibility
guarantees. `Incompatible` items require an explicit per-expression or per-operator opt-in
(for example, `spark.comet.expr.<Name>.allowIncompatible=true`).

For details on per-expression and per-operator support levels, see the
[compatibility guide](../user-guide/latest/compatibility/index.md).

## Apache Spark Version Support

Each Comet release supports the **latest patch version** of every Apache Spark minor release that
Comet targets at the time of release. The currently supported Spark versions are listed on the
[Spark Version Compatibility](../user-guide/latest/compatibility/spark-versions.md) page.

When the upstream Spark project publishes a new patch within a supported minor (for example,
`3.5.8` → `3.5.9`), the next Comet release will pick it up. Older Spark patches within the same
minor are not separately supported.

Dropping support for an entire Spark minor (for example, removing Spark 3.4) is announced in the
release notes of the **preceding** Comet minor release, giving users at least one release of
notice before the change takes effect.

Comet binaries are published per `(Spark minor × Scala binary version)` combination. Users must
select the binary that matches their Spark and Scala installation.

## Release Cadence

Comet targets a `0.X.0` minor release every four to six weeks. Patch releases (`0.X.Y`) are made
on demand, only when a critical bug or security fix needs to ship before the next minor release.

Only the most recent minor release receives patch releases. Comet does not currently backport
fixes to older minor releases; users are expected to upgrade forward.

## Native Library Coupling

Each Comet release ships a JVM jar and a native library that are built and tested together. The
two artifacts must come from the **same Comet release**. Mixing a JVM jar from one Comet release
with a native library from another is unsupported and may fail at runtime due to protobuf or FFI
incompatibilities.

## Road to 1.0

When `1.0.0` ships:

- Strict semantic versioning will apply to the public Scala and Java API: breaking changes will
  only be made in a future major release. APIs annotated with `@Unstable` remain exempt and may
  change in any minor release.
- The stability commitments for configuration keys and the protobuf plan format will be
  re-evaluated and documented as part of the `1.0.0` release.

Tracking and planning for the `1.0.0` release happens in
[issue #4082](https://github.com/apache/datafusion-comet/issues/4082).
