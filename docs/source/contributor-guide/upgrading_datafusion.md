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

# Upgrading DataFusion

Comet's native code depends on DataFusion, and through it on specific major versions of Arrow,
Parquet and object_store. A DataFusion major release usually changes APIs Comet calls, often moves
to a new Arrow major version, and sometimes changes behavior that Comet's tests depend on.

Comet doesn't wait for the release to find these changes. While the next DataFusion major
version is in development, Comet builds against DataFusion `main` on a long-lived upgrade branch,
so that breaks and regressions surface early, while there is still time to fix them upstream before
the release.

## The Upgrade Branch

The upgrade to DataFusion N lives on a branch named `datafusion-N` (for example, `datafusion-56`)
in `apache/datafusion-comet` itself, not on a fork, so any committer can push to it. One draft pull
request from that branch to `main` carries the upgrade, titled `deps: upgrade to DataFusion N.0.0`.
It stays a draft and is not merged until DataFusion N.0.0 is released.

An EPIC issue tracks the upgrade and every regression it finds, each as a sub-issue (for example,
[#6410](https://github.com/apache/datafusion-comet/issues/6410) for DataFusion 56). The pull
request links the EPIC.

Keep the branch as a small number of commits on top of current `main`, so the pull request shows
only the upgrade. Updating it means resetting onto `main`, so push with `--force-with-lease`, never
`--force`, and check whether someone else has pushed to the branch first.

## Pinning DataFusion Main

The DataFusion crates point at one DataFusion `main` revision with `git` and `rev`, not a version:

```toml
datafusion = { git = "https://github.com/apache/datafusion", rev = "<sha>", default-features = false, features = [...] }
```

Use the same revision everywhere DataFusion appears:

- the DataFusion crates in the `native/Cargo.toml` workspace, including `datafusion-spark`
- the DataFusion dev-dependencies in the member crates, such as `native/core/Cargo.toml`
- the contrib crates under `contrib/*/native/`, which have their own manifests

Move Arrow, Parquet and object_store to the versions that revision uses, then regenerate
`native/Cargo.lock` and `contrib/delta/native/Cargo.lock`. Run `cargo tree -d` in `native/` to
check that only one Arrow major version is in the build. Comet passes Arrow types directly to other
crates, so a second copy fails to compile or, worse, fails at the FFI boundary.

Other dependencies that share Arrow types with Comet, such as iceberg-rust, must move to the same
Arrow version. If their latest release or `main` is still on the old one, pin them to a revision
that has the upgrade, such as an open upstream pull request or a fork branch built from one. Leave
a comment in `Cargo.toml` that names the upstream pull request and says when to move back.

To move to a newer DataFusion revision, update every `rev` together, reset the branch onto `main`,
fix whatever no longer compiles, and record the new revision and its date in the pull request
description.

## Fixing the Build

Read DataFusion's upgrade guide for the release,
`docs/source/library-user-guide/upgrading/N.0.0.md` in the DataFusion repository at the pinned
revision, before fixing compile errors. It lists the API changes and the intended replacements.

Keep source changes to the ones the upgrade forces. A cleanup that the new version enables, such as
removing a workaround for a bug that is now fixed upstream, goes in a follow-up pull request after
the upgrade merges, and the upgrade pull request lists it.

When an API Comet calls is deprecated without a replacement, keep the call under
`#[allow(deprecated)]` with a comment, and file a DataFusion issue describing Comet's use case
before the API is removed.

The upgrade guide also lists behavior changes, such as different function return types or
different handling of statistics. Check each one against Comet. Comet registers its own versions of
many functions, so many changes never reach it. Record what was checked, and why each change does or
doesn't affect Comet, in the pull request description.

## Running the Tests

Work through the checklist in [Submitting a Pull Request](development.md#submitting-a-pull-request)
locally, including clippy. The contrib crates are outside the default build, so also run
`cargo check -p datafusion-comet --all-targets --features contrib-delta,contrib-lance` in `native/`.
Then run the Comet test suites. The pull request's default checks only run the Comet suites against the default Spark
profile, and a new DataFusion version can change results anywhere, so add these labels to the pull
request (see [Continuous Integration](ci.md)):

- `run-all-spark-profiles`: the Comet suites against the other Spark versions
- `run-spark-4.1-tests`: Spark's own SQL tests
- `run-iceberg-tests`: the Iceberg Spark tests
- `run-delta-tests`: the Delta contrib suites, which build against DataFusion too

The labels run again on every push, so add them after the branch builds cleanly, not before.

## Handling Regressions

A test that fails on the new DataFusion revision and passes on `main` is a regression. For each one:

1. Find the cause and the project that owns it. It may be DataFusion, or a change in Arrow, Parquet
   or another dependency that moved with it. Some upstream changes are deliberate, so check the
   upstream pull request before calling it a bug.
2. File a Comet issue as a sub-issue of the EPIC, describing the difference from Spark and the
   upstream cause.
3. Fix it in the project that owns it. A fix that merges to DataFusion `main` before the release
   branch is cut ships in N.0.0. If the fix belongs in Comet, make it on the upgrade branch.
4. If the fix won't land before the upgrade merges, ignore the test with a reason that links the
   Comet issue, and document the limitation on the affected feature's user guide page and in the
   [Comet Upgrade Guide](../user-guide/latest/migration-guide.md) section for the next release.

## Merging the Upgrade

Once DataFusion N.0.0 is released:

1. Replace every DataFusion `git` and `rev` with `version = "N.0.0"`, and regenerate the lockfiles.
2. Move any dependencies pinned to a fork or an unmerged pull request back to an upstream release
   or `main` revision, where one with the Arrow upgrade exists.
3. Rerun the tests and the labels above, and recheck the ignored tests against their issues.
4. Mark the pull request ready for review and merge it.
5. Delete the `datafusion-N` branch, and close the EPIC once its open sub-issues are moved to
   regular issues or fixed.
