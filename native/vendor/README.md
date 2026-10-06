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

# DataFusion bitmap pruning backport

These two crates backport [DataFusion #25602](https://github.com/apache/datafusion/pull/25602)
to DataFusion 55.1.0. The native workspace patches both crates together, so hash joins and
Parquet pruning use the same bitmap type. All other DataFusion dependencies remain on the
existing locked release, as do Arrow, Parquet, and Iceberg.

The upstream implementation first landed at
`4bb9f8d3192f6c945f2bf933231380e60341af66`, whose dependency graph uses Arrow 60.
Comet's pinned Iceberg revision uses Arrow 59. This temporary backport makes the bitmap
available without also requiring an Iceberg migration or an unmerged Iceberg dependency.
It does not enable Comet's experimental join runtime filtering by default or add Delta scans.

## Provenance

The source archives are the published crates.io 55.1.0 packages, based on DataFusion release
commit `7d3835c71f30cbd3c3ae4041732267f1f453097a`. Their SHA-256 checksums match the
original entries in Comet's lockfile:

| Package                    | Archive SHA-256                                                    |
| -------------------------- | ------------------------------------------------------------------ |
| `datafusion-physical-plan` | `1265d58e5bce07d154e642a51ff43033b576a6ae40d50a29ac2b9f311004eb52` |
| `datafusion-pruning`       | `74b06b333405c05015836ed36cea02cc29d1e2ee8fb414a77532d8346fb56492` |

The crates retain their licenses, notices, normalized Cargo manifests, and original manifests.
Manifest license headers and the upstream Rust formatting configuration are included for
Comet's repository checks. Deprecation warnings are allowed in the physical-plan crate so
its pre-Rust-1.99 atomic API remains compatible with DataFusion 55.1.0's minimum Rust version.
Package-local lockfiles and Cargo cache markers are omitted; `native/Cargo.lock` controls the
build. [bitmap-backport.patch](bitmap-backport.patch) records every source and manifest change
relative to those archives.

## Adaptations to 55.1.0

- Preserve the four-argument `HashTableLookupExpr::new` API. The new
  `with_pruning_bitmap` builder attaches the optional summary without changing other callers.
- Use 55.1.0's existing hash-join build path. It has no prepared-build API; skip the summary
  whenever dynamic filters are not computed. Failure to reserve bitmap memory only disables
  this optional optimization.
- Retain the merged PR's conservative handling of `CASE` without `ELSE`, which must preserve
  implicit NULLs during Parquet full-match inference.
- Keep existing IN-list pruning unchanged. Port only the bitmap statistics rewrite and CASE
  support, adapting the recursive call to the 55.1.0 signature. Recognize lossless casts from
  unsigned integers to larger signed integers locally, since 55.1.0's cast helper predates
  that support.
- Include upstream bitmap, expression-sharing, and memory-limit tests, plus regressions for
  missing statistics and implicit NULLs. Comet's join test verifies that both supported native
  hash-join modes skip interior row groups that min/max filtering alone cannot exclude.

## Validation

From `native/`, run:

```sh
cargo test -p datafusion-pruning --lib
cargo test -p datafusion-physical-plan --lib key_range_bitmap
cargo test -p datafusion-physical-plan --lib pruning_bitmap
cargo test -p datafusion-comet --lib dynamic_filter
```

The Comet regression is named `bitmap_filter_prunes_parquet_row_groups_inside_build_bounds`.
It checks exact join results, reader attachment, row groups pruned, decoded rows, and bytes
read with filtering enabled and disabled. Page and decode-time row filtering are disabled
in this test to isolate row-group pruning.

## Removal

When Comet can adopt a DataFusion release containing #25602 together with compatible Arrow
and Iceberg dependencies, remove both vendored crates, their workspace-member entries, and
the `[patch.crates-io]` entries in `native/Cargo.toml`, then update the lockfile. Retain the
Comet regression test. Do not update just one patched crate: the producer and pruning reader
must agree on the hash-lookup expression and bitmap implementation.
