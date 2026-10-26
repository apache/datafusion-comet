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

A DataFusion major release usually changes APIs Comet calls, often moves to new major versions of
the crates whose types cross DataFusion's API (today Arrow, Parquet and object_store), and sometimes
changes behavior that Comet's tests depend on. So while the next DataFusion major version is in
development, Comet builds against DataFusion `main` on a long-lived upgrade branch, which surfaces
breaks and regressions while there is still time to fix them upstream.

## The Upgrade Branch

The upgrade to DataFusion N lives on a branch named `datafusion-N` (for example, `datafusion-56`)
in `apache/datafusion-comet` itself, not on a fork, so any committer can push to it. One draft pull
request from that branch to `main` carries the upgrade, titled `deps: upgrade to DataFusion N.0.0`.
It stays a draft until DataFusion N.0.0 is released (see [Merging the Upgrade](#merging-the-upgrade)).

An EPIC issue titled `DataFusion N upgrade` tracks the upgrade, with each regression it finds as a
sub-issue. The pull request links the EPIC.

Keep the branch as a small number of commits on top of current `main`, so the pull request shows
only the upgrade. Updating it means resetting onto `main` and force pushing, as described in
[Submitting a Pull Request](development.md#submitting-a-pull-request).

## Pinning DataFusion Main

The DataFusion crates point at one DataFusion `main` revision with `git` and `rev`, not a version:

```toml
datafusion = { git = "https://github.com/apache/datafusion", rev = "<sha>", default-features = false, features = [...] }
```

Use the same revision in every manifest that names a DataFusion crate, including the dev-dependencies
and the contrib crates, which have their own manifests outside the workspace:

```shell
git grep -l 'datafusion' -- '*Cargo.toml'
```

Move every crate whose types cross DataFusion's API to the version that revision uses, then
regenerate every lockfile (`git ls-files '*Cargo.lock'`). Only one major version of each may be in
the build, because Comet passes their types directly between DataFusion and other crates. Check it
in `native/` and in each contrib directory with its own lockfile:

```shell
cargo tree -i arrow --workspace --all-features --depth 0
```

This fails with "multiple `arrow` packages" if two versions are in the build.

A dependency that is still on the old version, such as iceberg-rust, gets pinned to a revision that
has the upgrade, such as an open upstream pull request or a fork branch built from one. Leave a
comment in `Cargo.toml` that names the upstream pull request and says when to move back.

When moving to a newer DataFusion revision, make the change and fix the build locally, push it
once, and record the new revision and its date in the pull request description.

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
different handling of statistics. Check each one against Comet, and record in the pull request
description whether and why it affects Comet. Many don't, because Comet registers its own versions
of many functions.

## Running the Tests

Locally, work through the checklist in
[Submitting a Pull Request](development.md#submitting-a-pull-request), and build `native/` with
every `contrib-*` feature from `native/core/Cargo.toml` enabled, since the contrib crates are
outside the default build. Leave the Comet suites to CI.

The pull request's default checks don't cover the other Spark versions, Spark's own SQL tests, or
the Iceberg and Delta suites, and a new DataFusion version can change results in any of them. Once
the build is clean on a new DataFusion revision, add the labels from
[Continuous Integration](ci.md#opting-a-pull-request-into-a-suite-the-pr-tier-skips) that run them:
`run-all-spark-profiles`, `run-iceberg-tests`, `run-delta-tests`, and the Spark SQL tests label for
the default Spark version. A label stays on the pull request and reruns its suite on every push, so
remove the labels once their runs report.

## Handling Regressions

A test that fails on the new DataFusion revision and passes on `main` is a regression. For each one:

1. Find the cause and the project that owns it. It may be DataFusion, or a change in a dependency
   that moved with it. Some upstream changes are deliberate, so check the upstream pull request
   before calling it a bug.
2. File a Comet issue as a sub-issue of the EPIC, describing the difference from Spark and the
   upstream cause.
3. Fix it in the project that owns it. A fix that merges to DataFusion `main` before the release
   branch is cut ships in N.0.0. If the fix belongs in Comet, make it on the upgrade branch.
4. If the fix won't land before the upgrade merges, ignore the test with a reason that links the
   Comet issue. For a Comet SQL file test see
   [Handling test failures](sql-file-tests.md#handling-test-failures), and for one of Spark's own
   tests see [Running Spark SQL Tests](spark-sql-tests.md). Document the limitation on the affected
   feature's user guide page and in the
   [Comet Upgrade Guide](../user-guide/latest/migration-guide.md) section for the next release.

## Merging the Upgrade

Once DataFusion N.0.0 is released:

1. In one commit, replace every DataFusion `git` and `rev` with `version = "N.0.0"`, move any
   dependencies pinned to a fork or an unmerged pull request back to an upstream release or `main`
   revision where one with the upgrade exists, and regenerate the lockfiles.
2. Push it, add the labels above, and recheck the ignored tests against their issues.
3. Mark the pull request ready for review and merge it.
4. Delete the `datafusion-N` branch, and close the EPIC once its open sub-issues are moved to
   regular issues or fixed.
