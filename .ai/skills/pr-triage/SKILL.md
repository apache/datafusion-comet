---
name: pr-triage
description: Triage open Comet pull requests. Applies exactly one type label (`bug`/`enhancement`) plus the supporting type labels (`performance`, `correctness`, `crash`, `test`, `build`, `documentation`) and the `area:*` labels for the subsystems each PR touches, deriving the area from the PR's changed files rather than its title alone. Asks a human before creating any new area label, and prints a report instead of commenting on PRs.
---

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

Run a pull request triage pass for the `apache/datafusion-comet` repository.

## Overview

Comet carries a large backlog of open pull requests. Unlabeled PRs are hard to
route to a reviewer with the right expertise. This skill labels them so that
`is:pr is:open label:area:shuffle` is a useful query.

For every open PR the skill:

1. Applies exactly one type label: `bug` or `enhancement`.
2. Applies the supporting type labels that fit: `performance`, `correctness`,
   `crash`, `test`, `build`, `documentation`.
3. Applies zero or more `area:*` labels for the subsystems the PR touches.
4. Prints a report of what it did.

This skill does **not** review the PR, comment on it, request changes, edit its
title or body, or close it. Labels only.

Priority labels (`priority:*`) are for issues, not PRs. Do not apply them here.

## Step 1: Read the Triage Guide

The area label table lives in the project's own guide:

```
docs/source/contributor-guide/bug_triage.md
```

Read it before classifying. If the guide and this skill disagree, the guide
wins. The guide is written for issues, but the bug-vs-enhancement definitions
and the area table apply to PRs unchanged.

Then list the labels that actually exist in the repo, because you may only
apply labels that already exist:

```bash
gh label list --repo apache/datafusion-comet --limit 200
```

## Step 2: Gather the Open PRs

```bash
gh pr list --repo apache/datafusion-comet --limit 300 \
  --json number,title,labels,isDraft \
  --jq '.[] | "\(.number)\t\(.labels|map(.name)|join(","))\t\(.title)"'
```

Triage drafts too. A draft PR still belongs to a subsystem, and the area label
is what makes it findable later.

If every open PR already carries a type label and at least one area label (or
is a dependency bump, see Step 4), stop and tell the user there is nothing to
triage.

## Step 3: Derive the Area From the Changed Files

Titles are unreliable for area. `perf: reuse zstd compression contexts` does
not say "shuffle", and `fix: support empty struct types` does not say which
subsystem broke. The changed file paths do say it. Fetch them for every PR
before classifying anything:

```bash
mkdir -p "$SCRATCH/files"
gh pr list --repo apache/datafusion-comet --limit 300 --json number --jq '.[].number' \
  > "$SCRATCH/nums.txt"
xargs -P 6 -I{} sh -c \
  'gh pr view {} --repo apache/datafusion-comet --json files \
     --jq "[.files[].path]|join(\" \")" > '"$SCRATCH"'/files/{}.txt 2>/dev/null' \
  < "$SCRATCH/nums.txt"
```

Use a scratch directory outside the repository. Collapse the paths to
directories before reading them, or a single wide PR will bury the rest.

Path-to-area mapping:

| Path                                                           | Area               |
| -------------------------------------------------------------- | ------------------ |
| `native/shuffle/`, `spark/.../comet/shuffle/`                  | `area:shuffle`     |
| `native/core/src/parquet/`, `spark/.../comet/parquet/`         | `area:scan`        |
| `contrib/delta*/`, `contrib/lance/`, object-store code         | `area:scan`        |
| `spark/.../comet/iceberg/`, `*Iceberg*Suite.scala`             | `area:Iceberg`     |
| native or Scala write paths, `WriteFilesExec`, writer metrics  | `area:writer`      |
| `native/spark-expr/`, `spark/.../comet/serde/`, SQL file tests | `area:expressions` |
| aggregate serde, `native/core/src/execution/agg*`              | `area:aggregation` |
| `native/core/src/execution/jni_api.rs`, FFI import/export, C2R | `area:ffi`         |
| memory pools, reservations, OOM handling                       | `area:memory`      |
| join operators, dynamic filter pushdown                        | `area:joins`       |
| `spark/.../comet/udf/`, PySpark UDF tests                      | `area:udf`         |
| `.github/workflows/`, `dev/`                                   | `area:ci`          |

A PR may carry several area labels; a PR touching Iceberg reads gets both
`area:Iceberg` and `area:scan`. A PR may also carry none: plan-rule, AQE,
EXPLAIN, and caching changes have no area label today, and that is fine. Do not
stretch an area label to cover something it does not describe.

`.github/` alone is not enough for `area:ci`. Most feature PRs touch a workflow
file to enable a test job. Apply `area:ci` when the CI configuration is the
point of the PR.

The pre-existing labels `spark 4.0`, `spark 4.1`, `spark 4.2`, `spark 3.x`,
`spark sql tests`, `array expressions`, `map expressions`, `json expressions`,
and `temporal expressions` are also area indicators. Apply them where they fit.

## Step 4: Classify the Type

Exactly one of `bug` or `enhancement` on every PR.

- `bug`: the PR repairs something that is broken on `main`. Wrong results,
  crashes, panics, regressions, a broken build, a failing test.
- `enhancement`: the PR adds functionality, performance, tests, docs, or
  refactors. Anything that was never expected to work yet.

The conventional-commit prefix is a starting point, not the answer:

- `fix:` is usually `bug`, but `fix: support <type>` for a type Comet never
  supported is an `enhancement` — nothing was broken, a fallback was taken.
- `perf:` is `enhancement` plus `performance`. A performance _regression fix_
  is `bug` plus `performance`.
- `feat:`, `refactor:`, `chore:` are `enhancement`.
- `test:` is `enhancement` plus `test`. `docs:` is `enhancement` plus
  `documentation`. `ci:` and `build:` are `enhancement` plus `build`.

Supporting labels, applied on top of the type label:

- `correctness` — the PR fixes or prevents silently wrong results. This is the
  label a release manager greps for; do not apply it to a PR that only fixes a
  thrown exception or a plan-shape difference.
- `crash` — the PR fixes a panic, segfault, or native abort.
- `performance` — the PR's purpose is throughput, latency, or allocation.
- `test` / `build` / `documentation` — the PR is predominantly that kind of
  change, or adds a substantial amount of it.

Dependency bumps opened by Dependabot already carry `dependencies`. Leave them
alone; do not add a type label to them.

## Step 5: Ask Before Creating a New Area Label

You will find clusters of PRs that no existing area covers. Do **not** create a
label on your own judgment, and do **not** force those PRs into an area that
does not fit.

Instead, collect the candidate areas with the PR numbers that would carry each
one, and ask the user which to create, giving the count per candidate so they
can judge whether a cluster is worth a label. Only create the ones they pick:

```bash
gh label create "area:<name>" --repo apache/datafusion-comet \
  --description "<one line>" --color C5DEF5
```

`C5DEF5` is the existing `area:*` color; keep new area labels consistent with
it. A cluster of fewer than three open PRs is usually not worth a new label.

When the user declines a candidate, leave those PRs without an area label
rather than mislabeling them.

## Step 6: Apply the Labels

One `gh` call per PR, adding every label at once:

```bash
gh pr edit <NUMBER> --repo apache/datafusion-comet \
  --add-label "bug,correctness,area:expressions,temporal expressions"
```

Notes:

- Pass the labels as a single comma-separated string with no spaces after the
  commas. Labels that contain spaces (`temporal expressions`) work inside that
  string as long as the whole string is quoted.
- `--add-label` is additive and idempotent, so re-running the pass is safe.
- If a PR already carries the _wrong_ type label, remove it in the same call
  with `--remove-label` so no PR ends up with both `bug` and `enhancement`.
- Only apply labels that exist in the repo. Skip a missing label and note it in
  the report rather than creating it outside Step 5.

With a backlog of a hundred or more PRs, drive the calls from a file of
`<number>|<labels>` lines through `xargs -P 6` calling a small helper script.
Do not build the command with `xargs -I{}` interpolation directly into a
`sh -c` string: long label lists overflow the argument buffer and `xargs`
fails with `command line cannot be assembled, too long`.

Record any `gh` failure with the PR number and the error text.

## Step 7: Verify

Re-list the PRs and confirm none are left without labels:

```bash
gh pr list --repo apache/datafusion-comet --limit 300 --json number,labels \
  --jq '.[] | select(.labels|length == 0) | .number'
```

Then print the label distribution, which is also the fastest way to spot a
misfire (a `correctness` count as large as the `bug` count means the label was
applied too freely):

```bash
gh pr list --repo apache/datafusion-comet --limit 300 --json labels \
  --jq '.[].labels[].name' | sort | uniq -c | sort -rn
```

## Step 8: Report to the User

Print a report in the terminal. Do not file a summary issue and do not comment
on any PR.

Include:

1. Number of PRs processed, and the bug / enhancement split.
2. Any new area labels created, and the count of PRs on each.
3. The area label distribution.
4. PRs left without an area label, and why (typically: plan rules, AQE, EXPLAIN,
   caching).
5. Any PR whose type label was corrected from the opposite one.
6. Any failures, with the PR number and the `gh` error.

Call out judgment calls the user may want to overrule: PRs titled `fix:` that
were labeled `enhancement`, and PRs where the area was ambiguous.
