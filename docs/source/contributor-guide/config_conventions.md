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

# Configuration Conventions

Comet configuration keys live under `spark.comet.*` and are declared in
`spark/src/main/scala/org/apache/comet/CometConf.scala`. This page describes the naming
conventions those keys follow so that new configs stay consistent with the surface users
already know, and documents the process for renaming an existing key without breaking
deployments.

## Key Naming

A Comet config key is a dotted path of segments. Each segment describes a scope, from
broadest (the prefix) to narrowest (the specific setting).

- **Prefix.** All keys start with `spark.comet.` — never bare `comet.` and never a nested
  prefix like `spark.sql.comet.`.
- **Category segment.** The first segment after the prefix names a broad area:
  `exec` (execution and expressions), `scan` (source readers), `parquet` (Parquet-specific
  settings), `shuffle` (shuffle behavior), `explain` (plan explain/logging), `metrics`,
  `tracing`, `debug`, `testing`, `convert` (Spark → Comet source conversion), or
  `expression` / `operator` (per-expression / per-operator flags).
- **Segment casing.** Multi-word segments use `camelCase`, not dot- or kebab-separated.
  Prefer `spark.comet.foo.maxThreadNum` over `spark.comet.foo.max.thread.num` or
  `spark.comet.foo.max-thread-num`.
- **Boolean suffix.** Descriptor-form boolean flags that gate a subsystem or feature end
  in `.enabled` — for example `spark.comet.debug.enabled`, `spark.comet.metrics.enabled`,
  `spark.comet.parquet.rowFilterPushdown.enabled`. Action-form flags whose name is itself
  a verb (`force...`, `allow...`) can omit `.enabled` because the verb already encodes
  the action — for example `spark.comet.exec.forceShuffledHashJoin`.
- **Acronyms.** Treat acronyms consistently as `UDF`, `SHJ`, `SMJ`, `IO` — either all-caps
  or camelCase, but do not mix within one cluster (`pyarrowUdf` alongside `scalaUDF` would
  be a red flag; both spell it `UDF`).

## Symbol Naming (Scala)

Every config declares a Scala `val` in `CometConf.scala`. The symbol name is the
`UPPER_SNAKE_CASE` form of the key's meaningful segments, prefixed with `COMET_`.

Examples:

| Key                                             | Symbol                                      |
| ----------------------------------------------- | ------------------------------------------- |
| `spark.comet.enabled`                           | `COMET_ENABLED`                             |
| `spark.comet.exec.forceShuffledHashJoin`        | `COMET_FORCE_SHJ`                           |
| `spark.comet.parquet.rowFilterPushdown.enabled` | `COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED` |

The symbol name is what appears in code; the key is what appears in user configuration.
The two do not have to match segment-for-segment — brevity in the symbol is fine as long
as the key remains descriptive.

## Categories and Visibility

Every `ConfigEntry` must call `.category(...)`. The category is used to route the key into
the right table in the user guide's `configs.md`. Available categories are declared as
`CATEGORY_*` constants at the top of `CometConf.scala`. If a new config does not fit an
existing category, discuss adding a new one before landing the config.

A second, independent choice is whether to call `.internal()`, which keeps the key out of
`configs.md` altogether.

Neither choice is cosmetic. Together they decide whether Comet's
[versioning policy](../about/versioning_policy.md) covers the key. The policy's rule keys on these
two marks and nothing else: a key is exempt only if it is in the `testing` category or is marked
`internal()`, and every other `spark.comet.*` key is covered.

| Category  | `internal()` | Appears in `configs.md`          | Covered by the versioning policy |
| --------- | ------------ | -------------------------------- | -------------------------------- |
| any other | no           | yes, in its category's table     | **yes**                          |
| `testing` | no           | yes, under Development & Testing | no                               |
| any       | yes          | no                               | no                               |

Note that a key read by string rather than through a registered `ConfigEntry` — as the
per-expression `spark.comet.expression.<Name>.allowIncompatible` opt-ins are — has no way to carry
either mark, so it is covered. Reaching for a dynamic key is therefore not a way to avoid the
guarantee; if the key is a debugging aid, give it a real `ConfigEntry` in the `testing` category.

Being covered commits the project to the key's name, type, accepted values, default, and semantics
across minor releases. Being
[exempt](../about/versioning_policy.md#testing-and-internal-configurations-are-exempt) means the key
may be renamed, retyped, redefaulted, or removed in any release, including a patch release, with no
alias, no deprecation cycle, and no upgrade guide entry.

So reach for `testing` or `internal()` only when the key exists to let Comet's own suites, or
someone debugging Comet itself, reach a state the rest of the code is not built to support —
disabling native scans, running in on-heap mode, making a declined operator throw. Use `internal()`
in particular when Comet should not publish the key at all, so that nobody can adopt it in the first
place.

If a deployment would have a legitimate reason to set the key, it belongs in a non-`testing`
category and must not be `internal()`, and the guarantees come with it. Do not use either mechanism
as a way to ship a production knob without committing to it.

## Renaming an Existing Config

Configs under `spark.comet.*` are stable across minor releases: users may have set them in
production `spark-defaults.conf` files, Spark job submissions, or notebooks. Renaming a key
must not silently break those deployments.

Keys in the `testing` category, and keys marked `internal()`, are
[exempt from the versioning policy](../about/versioning_policy.md#testing-and-internal-configurations-are-exempt)
and may be renamed outright: skip the `withAlternative` call in step 1, and step 5 with it. The
rest of the checklist still applies, because a stale key string left behind in code or docs is a
bug either way.

Use the `withAlternative` builder on `ConfigBuilder` to keep the old key working as a
deprecated alias:

```scala
val COMET_FORCE_SHJ: ConfigEntry[Boolean] =
  conf(s"$COMET_EXEC_CONFIG_PREFIX.forceShuffledHashJoin")
    .withAlternative(s"$COMET_EXEC_CONFIG_PREFIX.replaceSortMergeJoin")
    .category(CATEGORY_EXEC)
    .doc("...")
    .booleanConf
    .createWithDefault(false)
```

Reading a value from the alternative logs a one-time deprecation warning per JVM per
alternative key, pointing users at the current key. The primary key always wins if both
are set.

`withAlternative` accepts multiple alternatives (checked in order), for keys that have
been renamed more than once.

The rename checklist for a single config:

1. Update the key string on the `conf(...)` call and add `.withAlternative(oldKey)`.
2. Rename the Scala `val` (for example `COMET_REPLACE_SMJ` → `COMET_FORCE_SHJ`).
3. Update every callsite of the Scala symbol — grep the whole repo.
4. Update every documented reference to the old key string — grep `docs/source/` for the
   old key and update to the new key. The auto-generated `configs.md` table will refresh
   on the next release-docs regeneration and does not need manual editing.
5. Add or update a test in `CometConfSuite` if the rename covers a new type of alias
   pattern (single-hop, multiple alternatives, etc.).

Removing a deprecated alias is a follow-up step that belongs to a later major release —
typically the next Comet major after the rename first ships. See the
[versioning policy](../about/versioning_policy.md) for the timing rules.

## Changing the Behavior of an Existing Config

A rename keeps behavior identical and only moves the key. A **behavior change** is different: the
same query, over the same data, with the same explicitly set configuration, starts producing a
different result or a different error. Changing a config's default value, or changing what its
existing values mean, is a behavior change.

Comet's [versioning policy](../about/versioning_policy.md) allows a behavior change to ship in a
minor release, but only when users have a documented way to opt back out. That escape hatch is a
boolean config under `spark.comet.legacy.*`, defaulting to `false`, whose only job is to restore
the previous behavior:

```scala
val COMET_LEGACY_FOO_BEHAVIOR: ConfigEntry[Boolean] =
  conf("spark.comet.legacy.fooBehavior")
    .category(CATEGORY_LEGACY)
    .doc("When true, restores the pre-1.1.0 behavior of <config>, which <describe old " +
      "behavior>. This config is deprecated and will be removed in a future major release.")
    .booleanConf
    .createWithDefault(false)
```

Naming follows the usual conventions: `spark.comet.legacy.` prefix, `camelCase` final segment, and
a `COMET_LEGACY_*` Scala symbol. No legacy config exists yet, so the first one to land must also add
the `CATEGORY_LEGACY` constant to `CometConf.scala` and a corresponding section to `configs.md`. The
doc string must state which release changed the behavior and what the old behavior was, so that
`configs.md` is self-explanatory without cross-referencing the upgrade guide.

The checklist for a behavior change:

1. Make the behavior change itself, gated on the new legacy config.
2. Declare the legacy config in `CometConf.scala` under `CATEGORY_LEGACY`, defaulting to `false`.
3. Add a section to the [upgrade guide](../user-guide/latest/migration-guide.md) under the
   upcoming release, describing the change and naming the config that reverts it.
4. Add a test covering both branches: the new behavior by default, and the old behavior with the
   legacy config set.

Two cases do **not** need a legacy config:

- **Correctness fixes.** When a `Compatible` expression or operator does not match Spark, that is a
  bug. Fixing it is a bug fix and may ship in any release, including a patch release. Add a legacy
  config only if the fix has an unusually wide blast radius, and say so in the PR description.
- **Changes to which expressions and operators run natively.** Falling back to Spark, or ceasing
  to, changes performance rather than results.

Nor does a change to a key in the `testing` category or a key marked `internal()`, both
[exempt from the versioning policy](../about/versioning_policy.md#testing-and-internal-configurations-are-exempt):
the default and the meaning may change in any release, with no legacy config and no upgrade guide
entry. Update the suites that set it in the same PR.

Removing a legacy config is a major-release change, handled the same way as removing a deprecated
alias.
