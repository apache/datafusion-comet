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
  settings), `shuffle` (shuffle behavior), `columnar` (columnar shuffle), `explain` (plan
  explain/logging), `metrics`, `tracing`, `testing`, `convert` (Spark → Comet source
  conversion), or `expression` / `operator` (per-expression / per-operator flags).
- **Segment casing.** Multi-word segments use `camelCase`, not dot- or kebab-separated.
  Prefer `spark.comet.foo.maxThreadNum` over `spark.comet.foo.max.thread.num` or
  `spark.comet.foo.max-thread-num`.
- **Boolean suffix.** Boolean flags end in `.enabled`. Prefer
  `spark.comet.foo.bar.enabled` over `spark.comet.foo.barEnabled` or a bare
  `spark.comet.foo.bar` that happens to be boolean.
- **Acronyms.** Treat acronyms consistently as `UDF`, `SHJ`, `SMJ`, `IO` — either all-caps
  or camelCase, but do not mix within one key (`pyarrowUdf` and `scalaUDF` in the same
  cluster is a red flag).

## Symbol Naming (Scala)

Every config declares a Scala `val` in `CometConf.scala`. The symbol name is the
`UPPER_SNAKE_CASE` form of the key's meaningful segments, prefixed with `COMET_`.

Examples:

| Key                                             | Symbol                      |
| ----------------------------------------------- | --------------------------- |
| `spark.comet.enabled`                           | `COMET_ENABLED`             |
| `spark.comet.exec.forceShuffleHashJoin.enabled` | `COMET_FORCE_SHJ`           |
| `spark.comet.parquet.rowFilterPushdown.enabled` | `COMET_ROW_FILTER_PUSHDOWN` |

The symbol name is what appears in code; the key is what appears in user configuration.
The two do not have to match segment-for-segment — brevity in the symbol is fine as long
as the key remains descriptive.

## Categories

Every `ConfigEntry` must call `.category(...)`. The category is used to route the key into
the right table in the user guide's `configs.md`. Available categories are declared as
`CATEGORY_*` constants at the top of `CometConf.scala`. If a new config does not fit an
existing category, discuss adding a new one before landing the config.

## Renaming an Existing Config

Configs under `spark.comet.*` are stable across minor releases: users may have set them in
production `spark-defaults.conf` files, Spark job submissions, or notebooks. Renaming a key
must not silently break those deployments.

Use the `withAlternative` builder on `ConfigBuilder` to keep the old key working as a
deprecated alias:

```scala
val COMET_FORCE_SHJ: ConfigEntry[Boolean] =
  conf(s"$COMET_EXEC_CONFIG_PREFIX.forceShuffleHashJoin.enabled")
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
