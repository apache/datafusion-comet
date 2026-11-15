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

# Backporting to Release Branches

Comet releases are built from release branches (`branch-N.M`), which are cut from `main` when a
minor release is prepared. A fix lands on `main` first and is then backported to the release
branches that need it. This page covers which branches take backports, what qualifies, how to open
a backport, and how to check before a release that no fix has been missed.

## Which Branches Take Backports

Backports normally go to the branch of the most recent release. From the moment the next release
branch is cut until its `.0` release ships, the new branch takes them too, so that it doesn't ship
without the fixes that landed on `main` after the cut.

An older release line can still take a backport when the maintainers decide a fix is worth it, for
example a correctness or security fix for users who can't upgrade yet. There is no fixed cutoff.
Whichever branches a fix goes to, it also goes to every newer one; see
[Newer Branches Get the Fix Too](#newer-branches-get-the-fix-too).

A release branch that takes backports has a `backport-N.M` label, created when the branch is cut.
Committers add it to a pull request on `main` to mark it as a candidate for that branch, and add a
label for each branch that needs the fix. Anyone can suggest a backport in a comment on the pull
request.

## What to Backport

A patch release contains only bug fixes (see
[Patch Releases](../about/versioning_policy.md#patch-releases)). A pull request is a candidate when
it fixes a bug that the release branch has, such as wrong results, a crash or hang, a failure to
read or write data, a security problem, or a regression. Fixes for the branch's own build, CI and
flaky tests also qualify, because the branch has no other way to get them.

Judge a pull request by the issue it closes rather than by its title. Authors choose the prefix, so
`fix:` pull requests sometimes close issues labelled `enhancement`, and a `feat:` pull request can
fix a correctness bug.

A feature, a performance change or a refactor is an exception on a branch that has already shipped
a release, so its backport pull request has to explain why it is worth the risk. A new branch has
more room before its first release, and the release manager decides what else it takes.

## Newer Branches Get the Fix Too

A fix must never be in one release line and missing from a newer one. Users upgrade forward, so
anyone moving from the older line to the newer one would lose the fix and see a regression.

A fix that goes to a release branch therefore goes to every newer release branch as well:

- If it merged to `main` before a newer branch was cut, that branch already has it.
  `git merge-base --is-ancestor <commit> apache/branch-N.M` confirms this.
- If it merged after the cut, the newer branch needs its own backport. Open both backports
  together, and don't merge the older one until the newer one is ready to merge.
- Label the source pull request for every branch the fix goes to, so the whole set can be seen in
  one place.

For example, [#6025](https://github.com/apache/datafusion-comet/pull/6025) merged to `main` after
`branch-1.1` was cut. It went to `branch-1.1` in
[#6323](https://github.com/apache/datafusion-comet/pull/6323), and then to `branch-1.0` in
[#6325](https://github.com/apache/datafusion-comet/pull/6325).

## Changes That Start on a Release Branch

Some changes are made on a release branch directly. They include the release mechanics (the
version bump, change log and generated docs), fixes for code that has since changed on `main`, and
CI or build fixes found while backporting. Except for release mechanics, work out whether `main`
and the newer release branches need the same change. If they do, open it against `main` and
backport it from there like any other fix. If they don't, say why in the pull request. For example,
[#6285](https://github.com/apache/datafusion-comet/pull/6285) makes the Spark 3.4 and 3.5 jars on
`branch-1.0` run on Java 11, and its description explains that `main` dropped Java 11 in 1.1.0.

Open such a change as its own pull request, not as part of the backport of an unrelated fix. The
squash merge folds it into the fix's commit, where neither the
[check below](#checking-release-branches-before-a-release) nor anyone reading the branch's history
will notice that `main` lacks it. A CI fix that makes the Iceberg jobs test the Comet jar they build
went to `branch-1.0` this way, inside the backport of an unrelated fix
([#6277](https://github.com/apache/datafusion-comet/pull/6277)), instead of going to `main`.

## Opening a Backport

1. Cherry-pick the source pull request's commit from `main` with `git cherry-pick -x <commit>`. The
   `-x` records `(cherry picked from commit <commit>)` in the message, which the
   [check below](#checking-release-branches-before-a-release) relies on. You can prepare a backport
   from a pull request that has not merged yet, but pick its squash commit again once it merges, so
   that the message names the commit on `main`.
2. Backport one source pull request per pull request. When fixes depend on each other, put them in
   one pull request as separate commits, in the order they merged to `main`.
3. Title it `<type>: [branch-N.M] <source title> (#<source number>)`, for example
   `fix: [branch-1.1] decline native Iceberg writes with a custom location provider (#6216)`, so
   the change log names both pull requests.
4. Fill in the pull request template as usual. Link the source pull request and its issue, list the
   backports to other release branches or say why a branch doesn't need one, and describe every
   adaptation: anything that differs from the source commit, such as a conflict resolution, a
   dropped file, or a test moved to a suite that exists on the branch.

### Testing a Backport

A cherry-pick that applies cleanly can still fail on the branch, because the fix may rely on code
that landed on `main` after the cut. Build every target, including the tests (`./mvnw test-compile`,
and `cargo check --all-targets` in `native/`), then run the tests the fix added. Where practical,
also confirm that those tests fail on the branch without the fix, which shows that the branch had
the bug and that the tests catch it. [Release branches](ci.md#release-branches) describes what CI
runs on a pull request against a release branch.

Some differences between a release branch and `main` are easy to miss:

- On a release branch, `docs/source/user-guide/latest/configs.md` holds the generated tables
  rather than template markers (see
  [Generate Release Documentation](release_process.md#generate-release-documentation)). A backport
  that changes a configuration's description or default has to update its row there.
- Only `docs/source/user-guide/latest/` is published from a release branch, so changes to the
  contributor guide or `docs/source/conf.py` need no backport.
- CI builds with the latest stable Rust. When a new Rust release adds Clippy lints, `main` gets a
  commit that fixes them, and a release branch needs the same commit before its pull requests pass.

## Checking Release Branches Before a Release

Before tagging a release candidate, check that no fix on an older release branch is missing from a
newer one. For a new minor release, check the new branch against each older release branch that is
still taking backports. For a patch release, check its branch against every newer release branch,
so that a fix doesn't ship in the older line first.

This checks `branch-1.0` against `branch-1.1`:

```shell
old=branch-1.0 new=branch-1.1
git fetch apache
newcut=$(git merge-base apache/$new apache/main)
git rev-list --reverse "$(git merge-base apache/$old apache/main)..apache/$old" | while read -r c; do
  srcs=$(git log -1 --format=%B "$c" | sed -n 's/.*cherry picked from commit \([0-9a-f]\{40\}\).*/\1/p')
  [ -n "$srcs" ] || echo "check by hand: $(git log -1 --format='%h %s' "$c")"
  echo "$srcs" | while read -r s; do
    [ -n "$s" ] || continue
    git merge-base --is-ancestor "$s" "$newcut" && continue
    git log --format=%B "$newcut..apache/$new" | grep -q "cherry picked from commit $s" && continue
    echo "missing from $new: $(git log -1 --format='%h %s' "$s")"
  done
done
```

A source commit counts as present when it merged to `main` before `branch-1.1` was cut, or when
`branch-1.1` has its own backport of it. Each `missing from` line needs a backport before the
release. A `check by hand` line is a commit with no `-x` trailer: release mechanics, changes that
started on the release branch, and backports made without `-x`. Find its source pull request from
the title and check that one the same way.

The check reads only the commit messages. It cannot see a change added to a backport next to the
fix it names, and it counts a fix as present even if `main` reverted or rewrote it before the newer
branch was cut. To confirm that a fix survived, check that the tests it added still exist on the
newer branch.
