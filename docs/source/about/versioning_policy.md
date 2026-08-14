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
reflect the fact that Comet ships much more frequently than Spark, and that Comet is an accelerator
rather than an engine in its own right: Spark, not Comet, defines what a correct answer looks like.

```{note}
This policy takes effect with the `1.0.0` release. The earlier `0.x` series was unstable in the
sense semantic versioning gives that term: any `0.x` minor release could include breaking changes,
and the guarantees described below do not apply retroactively to it.
```

## Comet's Compatibility Surface

Comet is a plugin rather than a library. Users install it through Spark's plugin system and then
set `spark.comet.*` properties in `spark-defaults.conf`, in a `spark-submit` invocation, or in a
notebook. For the overwhelming majority of users, configuration is the only thing they ever touch,
so Comet's configuration is its primary API and this policy treats it as such.

Comet does also expose a small Java and Scala API, enumerated below, for the cases that
configuration cannot express: the class names Spark itself needs by name, and one service provider
interface that vendors implement. It is small on purpose, and everything outside it is internal.

The following are covered by this versioning policy:

- **Configuration keys under `spark.comet.*`**: their names, types, accepted values, default
  values, and semantics.
- **A small, enumerated public Java and Scala API**: the class names users write into Spark config
  properties, and the S3 credential provider SPI that vendors implement. The full list is in
  [Public Scala and Java API](#public-scala-and-java-api).
- **Query results for expressions and operators whose support level is `Compatible`**, where the
  contract is defined by Apache Spark rather than by Comet. See
  [Query Result Semantics](#query-result-semantics).

The following are internal implementation details. They are not covered by this policy and may
change in any release:

- The protobuf format used to serialize query plans between the JVM and the native library. The
  JVM jar and the native library ship together and are versioned together; see
  [Native Library Coupling](#native-library-coupling).
- Every Scala, Java, and Rust type not on that list, including the internal structure of the
  classes that are on it. See [Everything Else Is Internal](#everything-else-is-internal).
- `EXPLAIN` output, and the shape of the plans Comet produces. Which operators are fused, how a
  native block is partitioned, and how a plan is rendered may all change between releases.
- Metric names and log output.
- Performance characteristics, including which expressions and operators run natively and which
  fall back to Spark. An expression that ran natively in one release may fall back in the next,
  and vice versa. The results stay the same; only the speed changes.

## What Each Version Component Means

### Major Releases

A major release may:

- Remove a configuration key, or change one in a way that is not backward compatible.
- Remove or incompatibly change a member of the enumerated
  [public API](#public-scala-and-java-api), including any change that breaks a vendor jar built
  against an earlier release.
- Remove a `spark.comet.legacy.*` escape hatch, making the newer behavior unconditional.
- Remove a deprecated configuration alias left behind by a rename.

### Minor Releases

A minor release may:

- Add features, operators, expressions, and configuration keys, and make additive changes to the
  public API that keep existing vendor jars working.
- Change existing behavior, provided the change ships with a legacy escape hatch. See
  [Behavior Changes and Legacy Configurations](#behavior-changes-and-legacy-configurations).
- Deprecate configuration keys and public API members, ahead of removal in a later major release.
- Add or remove support for an Apache Spark version. See
  [Apache Spark Version Support](#apache-spark-version-support).

### Patch Releases

A patch release contains bug fixes only. It adds no configuration keys and makes no behavior
changes, with one exception: correctness fixes, which are covered in
[Correctness Fixes Are Not Breaking Changes](#correctness-fixes-are-not-breaking-changes).

## Behavior Changes and Legacy Configurations

A **behavior change** is one where the same query, run over the same data, with the same explicitly
set configuration, produces a different result or a different error than it did in the previous
release.

Comet follows Apache Spark's approach here. A behavior change may ship in a minor release, but only
when all three of the following hold:

1. A boolean configuration key under `spark.comet.legacy.*` restores the previous behavior. It
   defaults to `false`, meaning the new behavior is what users get unless they opt out.
2. The [Upgrade Guide](../user-guide/latest/migration-guide.md) gains an entry that describes the
   change and names the configuration key that reverts it.
3. The release notes for that version call the change out.

The escape hatch is what makes the change safe to ship in a minor release. A user who is broken by
it has a documented, single-property fix available while they adapt, rather than being forced to
pin an old Comet release.

Behavior changes that require this treatment include changing the default value of an existing
configuration key, changing what an existing key's values mean, and changing the semantics of an
`Incompatible` expression or operator whose divergence from Spark users may have come to depend on.

### Lifetime of a Legacy Configuration

A `spark.comet.legacy.*` key is deprecated from the moment it is added. Its purpose is to buy users
time to migrate, not to preserve two behaviors indefinitely.

Such a key may only be removed in a major release. When it is removed, the newer behavior becomes
unconditional and the removal is noted in the upgrade guide.

Contributors adding a legacy configuration should follow
[Changing the Behavior of an Existing Config](../contributor-guide/config_conventions.md#changing-the-behavior-of-an-existing-config)
in the contributor guide.

### Renaming and Removing Configuration Keys

Renaming a configuration key is not a behavior change and does not need a legacy escape hatch. The
old key is kept working as a deprecated alias using the `withAlternative` mechanism described in
[Renaming an Existing Config](../contributor-guide/config_conventions.md#renaming-an-existing-config).
The alias may only be dropped in a major release.

Removing a configuration key outright requires a deprecation cycle: the key must remain available,
with a deprecation warning, for at least one minor release before it is removed in a major release.

## Correctness Fixes Are Not Breaking Changes

Comet's contract is to produce the results that Apache Spark produces. When an expression or
operator whose support level is `Compatible` produces something different from Spark, that is a
bug in Comet, not a behavior that users are entitled to rely on.

Fixing such a bug is a bug fix. It may ship in any release, including a patch release. It does not
require a major version bump, and it does not require a legacy configuration key. Users must not
depend on Comet-specific incorrect results.

Two riders apply:

- Maintainers **may** add a legacy configuration key for a correctness fix with an unusually wide
  blast radius, for example one that changes the results of a common expression across many
  queries. This is a judgment call made case by case, not an obligation.
- When Apache Spark itself changes results for a given Spark version, Comet follows Spark. Tracking
  an upstream change is likewise not a Comet breaking change.

### Query Result Semantics

Expressions and operators whose support level is `Compatible` are expected to produce results that
match Apache Spark. Result differences in `Compatible` items are tracked as bugs and fixed in
subsequent releases, under the rules above.

Items whose support level is `Incompatible` or `Unsupported` have no result-compatibility
guarantees. `Incompatible` items require an explicit per-expression or per-operator opt-in
(for example, `spark.comet.expression.<Name>.allowIncompatible=true`).

For details on per-expression and per-operator support levels, see the
[compatibility guide](../user-guide/latest/compatibility/index.md).

## Public Scala and Java API

Comet is a plugin, not a library, so its public Java and Scala API is deliberately small. Every
member of it carries the `org.apache.comet.annotation.Public` annotation, and it is enumerated in
full below. **Anything not listed here is internal**, whatever its access modifier
says, and is covered by [Everything Else Is Internal](#everything-else-is-internal).

### Class Names Referenced From Configuration

These classes are named as _values_ in Spark configuration properties. Users do not compile against
them; they write the fully qualified name into a config string.

| Class                                  | Named in                             | Purpose                                               |
| -------------------------------------- | ------------------------------------ | ----------------------------------------------------- |
| `org.apache.spark.CometPlugin`         | `spark.plugins`                      | Installs Comet.                                       |
| `org.apache.comet.ExtendedExplainInfo` | `spark.sql.extendedExplainProviders` | Adds Comet fallback explanations to `EXPLAIN` output. |

Renaming or removing one of these class names breaks user configuration in exactly the way renaming
a `spark.comet.*` key does, so it is treated on the same terms: a deprecation cycle, then removal in
a major release. Their internal structure, by contrast, carries no guarantee.

### The S3 Credential Provider SPI

The classes in `org.apache.comet.cloud.s3` are a service provider interface. Vendors implement it to
supply AWS credentials to Comet's native S3 readers, compiling against Comet with `provided` scope
and shipping their implementation as a separate jar. Both source and binary compatibility matter
here, because a vendor jar built against one Comet release is loaded by another.

The SPI consists of:

- `CometS3CredentialProvider`, the interface a vendor implements.
- `CometS3Credentials`, the value a provider returns.
- `CometS3CredentialContext` and `CometS3AccessMode`, describing the request being served.

Additive changes are allowed in a minor release, for example a new accessor on
`CometS3CredentialContext`, because a vendor jar compiled against an earlier `1.x` continues to
load and run. Any change that would break such a jar, including adding an abstract method to
`CometS3CredentialProvider` without a default implementation, requires a major release.

`CometS3CredentialDispatcher` is the JNI entry point Comet uses to reach a provider. It is internal
despite living in the same package, and vendors must not call it.

See the [S3 Credential Providers](../user-guide/latest/s3-credential-providers.md) guide for the
full vendor contract.

### Everything Else Is Internal

Every other Scala, Java, and Rust type Comet ships is internal. This includes the rest of
`org.apache.comet.*`, everything Comet contributes to `org.apache.spark.*`, and all of the native
crates. These types exist to make the plugin work, not to be programmed against. They may be
renamed, changed, or removed in any release, including a patch release, with no deprecation cycle
and no upgrade guide entry.

User code must not import, extend, or call them. Where Comet documentation shows an internal class
in a code sample, treat it as a debugging aid for interactive use rather than as an interface with
a stability guarantee.

### Deprecation Cycle

Removing anything listed above as public, or changing it incompatibly, requires a deprecation cycle:
it must remain available, with a deprecation warning where one can be raised, for at least one minor
release, and may only be removed in a major release.

### Changing the Public API

`CometPublicApiSuite` pins the exact set of `@Public` types to the list above, so adding or removing
the annotation fails the build until the list is updated. That is deliberate: growing the public API
commits the project to supporting the addition indefinitely, which is a policy decision rather than
a routine code change. Agree the addition in an issue or on the mailing list first, then update the
annotation, this page, and the suite together in one pull request.

## Apache Spark Version Support

The currently supported Spark versions are listed on the
[Spark Version Compatibility](../user-guide/latest/compatibility/spark-versions.md) page. Comet
binaries are published per `(Spark minor × Scala binary version)` combination. Users must select
the binary that matches their Spark and Scala installation.

**Which Spark versions Comet supports is not governed by semantic versioning.** Adding support for
a new Spark minor is a Comet minor release, never a major one. Removing support for a Spark minor
is also a minor release, and is never by itself grounds for a major version bump.

The reasoning is that Comet's supported Spark matrix tracks the upstream Apache Spark project's
maintenance windows, which have nothing to do with Comet's own version numbers. Tying the two
together would force Comet major releases on a schedule set by another project, and would make the
major version number say something about Spark rather than about Comet's own compatibility.

Scala binary versions are treated the same way: adding or removing one is a minor release.

Users running a Spark version that a given Comet release no longer supports should stay on an
earlier Comet release until they can upgrade Spark. The deprecation notice described under
[Support Lifetime](#support-lifetime) is the signal to start planning that upgrade.

### New Version Adoption

Comet does not commit to a timeline for adopting a new Apache Spark minor release. The effort
required varies significantly from one release to the next, so Comet will not promise a
delivery date it cannot guarantee.

A Spark minor release becomes eligible for **supported** status only after upstream ships an
official GA release. Comet may publish **experimental** support for a pre-GA release (a
preview, release candidate, or snapshot) to gather feedback during development, but such
experimental support is explicitly not a commitment: it may lag upstream, break, or be
withdrawn at any time. Production users should target GA versions only.

The mechanical stages of a bring-up and the criteria a version must meet before being promoted
from experimental to supported are documented in the contributor guide's
[Adding Support for a New Spark Version](../contributor-guide/adding_a_new_spark_version.md)
page.

### Support Lifetime

Comet aligns its Spark support window with the upstream
[Apache Spark versioning policy](https://spark.apache.org/versioning-policy.html). A Spark minor
release is supported by Comet for as long as it is actively maintained by the upstream Apache
Spark project. Once upstream ends maintenance for a Spark minor, Comet removes it in two steps:

1. **Deprecation.** The next Comet minor release after upstream maintenance ends marks the Spark
   minor as deprecated in the release notes and on the
   [Spark Version Compatibility](../user-guide/latest/compatibility/spark-versions.md) page.
   Comet continues to build and publish binaries for the deprecated Spark minor during this
   release cycle.
2. **Removal.** The following Comet minor release removes the Spark minor and stops publishing
   binaries for it.

This gives users at least one Comet minor release of prior notice before a Spark minor is
dropped. For example, Spark 3.4 was deprecated in Comet 1.0.0 and removed in Comet
1.1.0.

### Patch Versions

Each Comet release supports the **latest patch version** of every Apache Spark minor release
that Comet targets at the time of release. When the upstream Spark project publishes a new patch
within a supported minor (for example, `3.5.8` → `3.5.9`), the next Comet release will pick it
up. Older Spark patches within the same minor are not separately supported.

## Release Cadence

Comet targets a minor release every four to six weeks. Patch releases are made on demand, only
when a critical bug or security fix needs to ship before the next minor release.

Only the most recent minor release receives patch releases. Comet does not currently backport
fixes to older minor releases; users are expected to upgrade forward.

## Native Library Coupling

Each Comet release ships a JVM jar and a native library that are built and tested together. The
two artifacts must come from the **same Comet release**. Mixing a JVM jar from one Comet release
with a native library from another is unsupported and may fail at runtime due to protobuf or FFI
incompatibilities.
