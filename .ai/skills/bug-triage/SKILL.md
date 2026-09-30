---
name: bug-triage
description: Triage open Comet issues marked `requires-triage` per the project bug triage guide. Classifies each issue as a bug or an enhancement, checks whether each bug is a regression from the most recent release tag, applies the recommended type (`bug`/`enhancement`), priority, area, and `regression` labels, removes `requires-triage`, and files a dated summary issue listing what was done. A human reviews the summary issue and closes it when satisfied.
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

Run a bug triage pass for the `apache/datafusion-comet` repository.

## Overview

This skill triages every open issue carrying the `requires-triage` label. For
each one it:

1. Decides whether the issue is a bug or an enhancement (feature request).
2. Decides a priority (bugs only) and area labels using the project's triage
   guide.
3. Decides whether a bug is a regression from the most recent release, and
   applies the `regression` label if it is.
4. Applies those labels via `gh` (`bug` or `enhancement`, plus priority/area
   and possibly `regression`), ensuring `bug` and `enhancement` are never both
   present.
5. Removes the `requires-triage` label.
6. Records the decision (with rationale) in a single dated summary issue.

`requires-triage` is auto-applied to **every** new issue, not just bug reports,
so the first decision for each issue is always bug vs. enhancement.

A human reviewer reads the summary issue, sanity-checks the calls, and closes
it when satisfied. Any label correction is done by the reviewer directly on the
affected issue.

The triage criteria come from the project's own guide. Read it before doing any
classification work; do not rely on memory.

## Step 1: Read the Triage Guide

Read the canonical guide in this repository:

```
docs/source/contributor-guide/bug_triage.md
```

Use the priority decision tree, escalation triggers, area labels, and
prioritization principles from that guide. If the guide and this skill ever
disagree, the guide wins. Do not paraphrase the guide; quote the labels and
criteria verbatim when classifying.

## Step 2: Gather Issues That Need Triage

Fetch all open issues labeled `requires-triage`:

```bash
gh issue list \
  --repo apache/datafusion-comet \
  --label requires-triage \
  --state open \
  --limit 200 \
  --json number,title,author,createdAt,labels,body,url
```

If the list is empty, stop and tell the user there is nothing to triage. Do not
file an empty summary issue and do not modify any labels.

## Step 3: Classify Each Issue

For each issue, review the title and body and determine:

1. **Type label** (exactly one of `bug` / `enhancement`):
   - `bug`: something is broken. Wrong results, crashes, panics, regressions,
     test/CI failures, or any behavior that differs from what Comet should do.
   - `enhancement`: a request for new functionality or improvement that is not
     yet expected to work. New expression/operator support, a new config, a
     performance optimization request, a refactor, or a docs addition.
   - Do not rely on the title wording alone. An issue titled "bug: ..." may
     actually be a feature request, and an issue with no "bug" in the title may
     be a genuine defect. Classify from the actual content.
   - A bug and an enhancement are mutually exclusive: an issue must never carry
     both `bug` and `enhancement`. If the issue already has the wrong type label,
     remove it (see Step 6).
2. **Priority label** (exactly one, **bugs only**): apply the decision tree from
   the guide.
   - `priority:critical` for correctness issues (silent wrong results, data
     corruption) and security vulnerabilities
   - `priority:high` for crashes, panics, segfaults, NPEs on supported paths
   - `priority:medium` for functional bugs / performance regressions with
     workarounds
   - `priority:low` for test-only, CI flakes, tooling, cosmetic
3. **Area labels** (zero or more): from the area table in the guide
   (`area:writer`, `area:shuffle`, `area:aggregation`, `area:scan`,
   `area:expressions`, `area:ffi`, `area:ci`) plus the pre-existing area
   indicators (`spark 4`, `spark sql tests`). Area labels apply to both bugs
   and enhancements.
4. **Escalation note**: if the issue matches an escalation trigger from the
   guide (e.g., a `priority:high` crash that may also produce wrong results),
   note it in the summary.

## Step 4: Check Whether Each Bug Is a Regression

Run this step for every issue you classified as a `bug` in Step 3. Skip it for
enhancements: a missing feature cannot be a regression.

A bug is a **regression** when a workload that behaved correctly on the most
recent Comet release behaves incorrectly on `main` — wrong results, a new
failure, a crash, or a fallback-to-Spark path that has since become a wrong
native answer. It also covers a loss of safety: a query that failed with a
clear error on the release and now returns silently wrong data is a regression,
even though it never produced the right answer on either version. A defect that
already shipped in that release is **not** a regression, however recently it was
reported.

### Step 4a: Resolve the comparison point from the release tags

Never hard-code a version. Ask GitHub for the current latest release and
resolve its tag to a commit, so the comparison point moves forward on its own
as Comet ships:

```bash
LATEST_RELEASE=$(gh release view \
  --repo apache/datafusion-comet \
  --json tagName --jq .tagName)
git fetch --tags --quiet
LATEST_RELEASE_SHA=$(git rev-list -n 1 "$LATEST_RELEASE")
LATEST_RELEASE_DATE=$(git log -1 --format=%cI "$LATEST_RELEASE_SHA")
```

`gh release view` with no tag argument returns the release GitHub marks as
"Latest", which excludes pre-releases. Comet marked every `0.x` release as a
pre-release, so on a repository state where only pre-releases exist this
returns nothing; in that case fall back to the newest tag by commit date and
say which tag you used in the summary.

Compare against the **tag's commit date**, not the release's publication date.
The two differ — Comet's `1.0.0` tag was cut on 2026-08-04 and published on
2026-08-07 — and commits landing in that window are not in the release.

### Step 4b: Decide, cheapest evidence first

Stop at the first step that gives a definite answer.

1. **Issue creation date.** If the issue was opened before
   `LATEST_RELEASE_DATE`, the defect was reported before the tag was cut, so it
   shipped in that release. **Not a regression.** This is free and decisive for
   most of the backlog.
2. **Is the defective code present at the tag?** Read the implicated file at
   the tag and compare it to `main`:

   ```bash
   git show "$LATEST_RELEASE:native/spark-expr/src/datetime_funcs/unix_timestamp.rs"
   git grep -n "some_pattern" "$LATEST_RELEASE" -- path/to/dir
   git diff "$LATEST_RELEASE"..HEAD -- path/to/file
   ```

   If the defective logic is there verbatim, **not a regression**. If the file
   or the serde entry that reaches it does not exist at the tag, continue.

3. **Was the path reachable at the tag?** Being absent from the tag is not the
   same as being a regression. Split it:

   - The feature is new since the release (a new expression serde, a new
     operator, a new scan mode). A workload that ran on the release cannot
     reach it. **Not a regression** — it is a defect in new work.
   - The path existed and was correct, and a post-release change broke it.
     **Regression.**
   - The expression previously fell back to Spark (so it was correct) and a
     post-release change made it run natively with a wrong answer.
     **Regression** — the user-visible answer changed for the worse.

   A useful check for the reachability question is whether the Scala serde
   entry, shim, or native registration existed at the tag, not just the Rust
   kernel:

   ```bash
   git grep -n "classOf\[SomeExpression\]" "$LATEST_RELEASE" -- 'spark/src/main'
   git grep -n '"some_function"' "$LATEST_RELEASE" -- native/spark-expr/src/comet_scalar_funcs.rs
   ```

4. **Bisect or run the reproducer.** If steps 1–3 are inconclusive and the
   issue has a reproducer, check the tag out into a scratch worktree, build,
   and run it:

   ```bash
   git worktree add /tmp/comet-release-check "$LATEST_RELEASE"
   cd /tmp/comet-release-check/native && cargo build
   cd /tmp/comet-release-check && ./mvnw test -Dtest=none -Dsuites="<suite>"
   ```

   Remove the worktree when done (`git worktree remove /tmp/comet-release-check
--force`). This is the only way to settle a case where the defect depends on
   a dependency bump (a DataFusion or Arrow/Parquet major version) rather than
   on Comet's own code.

### Step 4c: Do not trust "pre-existing" in the issue body

Comet issues found during PR review very often say "this is pre-existing, not
caused by this PR". That claim is about the **pull request under review**, which
is a narrower and different claim than "this shipped in the last release". A
defect can be genuinely pre-existing relative to the PR that surfaced it and
still have landed after the release tag. Verify against the tag either way.

### Step 4d: Apply the label only on positive evidence

- Add `regression` only when step 4b gives you positive evidence that the
  release behaved correctly.
- If the evidence is inconclusive, **do not** apply `regression`. Record the
  issue under "Regression status unclear" in the summary and let the reviewer
  decide. Do not label the rest of the issue differently on this account —
  classification, priority, and area still apply.
- `regression` is orthogonal to priority: a regression keeps the priority its
  symptoms earn. Per the guide it is also an escalation trigger, so note in the
  summary when a regression sits below `priority:high`.

## Step 5: Skip Issues You Cannot Confidently Classify

If an issue is too ambiguous to classify with confidence (you cannot tell
whether it is a bug or an enhancement, or a bug lacks reproduction steps and the
priority is unclear):

- **Do not** apply a type label (`bug`/`enhancement`).
- **Do not** apply a priority label.
- **Do not** remove `requires-triage`.
- **Do not** comment on the issue or ask the reporter for more info from this
  skill (that is the human reviewer's call).
- Record it in the summary under a "Skipped — needs more info" section so the
  reviewer can follow up.

Guessing is worse than skipping.

An unclear _regression_ status on its own is not a reason to skip an issue.
Classify, prioritise, and label it as usual, and record it under "Regression
status unclear" per Step 4d.

## Step 6: Apply Labels

For each issue you classified in Step 3, apply the labels and remove
`requires-triage` in a single `gh` call.

For a bug, add the `bug` type label and a priority label:

```bash
gh issue edit <NUMBER> \
  --repo apache/datafusion-comet \
  --add-label "bug,priority:high,area:expressions" \
  --remove-label "requires-triage,enhancement"
```

For a bug you determined in Step 4 to be a regression, add `regression` too:

```bash
gh issue edit <NUMBER> \
  --repo apache/datafusion-comet \
  --add-label "bug,priority:critical,area:scan,regression" \
  --remove-label "requires-triage,enhancement"
```

For an enhancement, add the `enhancement` type label and no priority label:

```bash
gh issue edit <NUMBER> \
  --repo apache/datafusion-comet \
  --add-label "enhancement,area:expressions" \
  --remove-label "requires-triage,bug"
```

Notes:

- Always set exactly one type label. Add `bug` for bugs, `enhancement` for
  enhancements.
- Always remove the opposite type label so an issue never carries both `bug`
  and `enhancement`. `--remove-label` is a no-op if the label is not present,
  so it is safe to remove the opposite type unconditionally.
- Apply a priority label only to bugs. Do not add a priority label to
  enhancements.
- Apply `regression` only to bugs, and only on the positive evidence described
  in Step 4d. Never add it to an enhancement.
- Do not _remove_ an existing `regression` label. If you believe one is wrong,
  say so in the summary and leave the correction to the reviewer.
- Pass the labels as a single comma-separated string (no spaces around commas).
- Quote labels that contain spaces (e.g., `"spark 4"`).
- Only add labels that already exist in the repo. If a label from the guide is
  missing in the repo, skip it for that issue and record a note in the summary
  rather than creating new labels.
- Do not comment on the issue.

If `gh issue edit` fails for any issue, leave that issue's `requires-triage`
label intact and record the failure in the summary under a "Failed to label"
section.

## Step 7: File the Summary Issue

Compute today's date in `YYYY-MM-DD` form (use the system date, not memory):

```bash
TRIAGE_DATE=$(date -u +%Y-%m-%d)
```

Title: `Bug triage results: ${TRIAGE_DATE}`

Body: a markdown report with these sections, in this order:

1. **Header**
   - Date, total issues processed, count of bugs vs. enhancements, and counts
     per priority
   - The release tag the regression check compared against, and its commit date
     (e.g. "Regressions assessed against `1.0.0` (tagged 2026-08-04)")
   - Link to `docs/source/contributor-guide/bug_triage.md`
   - Note that labels have already been applied; the reviewer should spot-check
     and close this issue when satisfied
2. **Bugs** — one subsection per priority, ordered highest priority first
   (`priority:critical`, then `priority:high`, then `priority:medium`, then
   `priority:low`). Omit any subsection whose count is zero. Do **not** use a
   markdown table anywhere in this section; use nested bullet lists only.

   Within each subsection, one top-level bullet per issue:

   ```
   ### priority:critical

   - <issue title> ([#1234](https://github.com/apache/datafusion-comet/issues/1234))
     - Area labels: `area:expressions`, `area:scan`
     - Regression: no — the same code is present at `1.0.0`
     - Rationale: one sentence tying the call to the guide
   ```

   The issue number (not the title) is the link target. The title is plain
   text. If there are no area labels, write `Area labels: none`.

   The `Regression:` sub-bullet is required on every bug. Write `yes`, `no`, or
   `unclear`, followed by the one-line evidence that settled it — which step of
   Step 4b answered it, and against which tag.

3. **Enhancements** (omit section if empty) — one top-level bullet per issue in
   the same `<title> ([#N](url))` form, with an `Area labels:` sub-bullet and a
   one-sentence rationale for classifying it as an enhancement. Enhancements
   have no priority subsections and no `Regression:` sub-bullet.
4. **Regressions from `<tag>`** (omit section if empty) — every issue you
   labelled `regression`, collected in one place so a release manager can read
   them without scanning the priority sections. Bullet per issue in the same
   `<title> ([#N](url))` form, plus a sub-bullet naming the change that
   introduced it (a PR or commit, where you identified one) and a sub-bullet
   with its priority label.
5. **Regression status unclear** (omit if empty) — bullet per issue with the
   same `<title> ([#N](url))` form, plus a sub-bullet saying what you checked
   and what would settle it. These issues were still labelled and had
   `requires-triage` removed.
6. **Escalations to consider** (omit section if empty) — bullet per issue with
   the same `<title> ([#N](url))` form, plus a sub-bullet explaining the
   trigger from the guide.
7. **Skipped — needs more info** (omit if empty) — bullet per issue with the
   same `<title> ([#N](url))` form, plus a sub-bullet explaining what is
   missing.
8. **Failed to label** (omit if empty) — bullet per issue with the same
   `<title> ([#N](url))` form, plus a sub-bullet quoting the `gh` error.

File the issue with `gh`. Use a temp file for the body to keep quoting sane:

```bash
gh issue create \
  --repo apache/datafusion-comet \
  --title "Bug triage results: ${TRIAGE_DATE}" \
  --body-file /tmp/triage-summary-${TRIAGE_DATE}.md
```

Do not add labels to the summary issue itself.

After creating the issue, print its URL.

## Output to the User

Report back:

1. Number of `requires-triage` issues processed
2. Counts per priority that were applied
3. The release tag the regression check compared against, how many issues were
   labelled `regression`, and how many were left unclear
4. Number skipped (needs more info) and number failed
5. URL of the new summary issue

Do not paste the full per-issue listing back into the chat; it is in the
summary issue.

## What This Skill Must Not Do

- Do not invent priority or area labels that are not in the guide
- Do not create new labels in the repo
- Do not hard-code a release version in the regression check; always resolve it
  from the release tags as in Step 4a
- Do not apply `regression` on the basis of the issue's own "pre-existing"
  wording, a recent creation date, or a guess — only on the evidence in Step 4b
- Do not remove a `regression` label that is already on an issue
- Do not comment on the triaged issues
- Do not close any triaged issue
- Do not file the summary issue if there were zero `requires-triage` issues
- Do not re-label issues that were skipped or failed (leave `requires-triage`
  in place so they show up in the next pass)
- Do not include AI/Claude attribution in the summary issue
