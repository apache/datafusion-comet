---
name: bug-triage
description: Triage open Comet issues marked `requires-triage` per the project bug triage guide. Classifies each issue as a bug or an enhancement, applies the recommended type (`bug`/`enhancement`), priority, and area labels, removes `requires-triage`, and files a dated summary issue listing what was done. A human reviews the summary issue and closes it when satisfied.
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
3. Applies those labels via `gh` (`bug` or `enhancement`, plus priority/area),
   ensuring `bug` and `enhancement` are never both present.
4. Removes the `requires-triage` label.
5. Records the decision (with rationale) in a single dated summary issue.

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
     remove it (see Step 5).
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

## Step 4: Skip Issues You Cannot Confidently Classify

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

## Step 5: Apply Labels

For each issue you classified in Step 3, apply the labels and remove
`requires-triage` in a single `gh` call.

For a bug, add the `bug` type label and a priority label:

```bash
gh issue edit <NUMBER> \
  --repo apache/datafusion-comet \
  --add-label "bug,priority:high,area:expressions" \
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
- Pass the labels as a single comma-separated string (no spaces around commas).
- Quote labels that contain spaces (e.g., `"spark 4"`).
- Only add labels that already exist in the repo. If a label from the guide is
  missing in the repo, skip it for that issue and record a note in the summary
  rather than creating new labels.
- Do not comment on the issue.

If `gh issue edit` fails for any issue, leave that issue's `requires-triage`
label intact and record the failure in the summary under a "Failed to label"
section.

## Step 6: File the Summary Issue

Compute today's date in `YYYY-MM-DD` form (use the system date, not memory):

```bash
TRIAGE_DATE=$(date -u +%Y-%m-%d)
```

Title: `Bug triage results: ${TRIAGE_DATE}`

Body: a markdown report with these sections, in this order:

1. **Header**
   - Date, total issues processed, count of bugs vs. enhancements, and counts
     per priority
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
     - Rationale: one sentence tying the call to the guide
   ```

   The issue number (not the title) is the link target. The title is plain
   text. If there are no area labels, write `Area labels: none`.
3. **Enhancements** (omit section if empty) — one top-level bullet per issue in
   the same `<title> ([#N](url))` form, with an `Area labels:` sub-bullet and a
   one-sentence rationale for classifying it as an enhancement. Enhancements
   have no priority subsections.
4. **Escalations to consider** (omit section if empty) — bullet per issue with
   the same `<title> ([#N](url))` form, plus a sub-bullet explaining the
   trigger from the guide.
5. **Skipped — needs more info** (omit if empty) — bullet per issue with the
   same `<title> ([#N](url))` form, plus a sub-bullet explaining what is
   missing.
6. **Failed to label** (omit if empty) — bullet per issue with the same
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
3. Number skipped (needs more info) and number failed
4. URL of the new summary issue

Do not paste the full per-issue listing back into the chat; it is in the
summary issue.

## What This Skill Must Not Do

- Do not invent priority or area labels that are not in the guide
- Do not create new labels in the repo
- Do not comment on the triaged issues
- Do not close any triaged issue
- Do not file the summary issue if there were zero `requires-triage` issues
- Do not re-label issues that were skipped or failed (leave `requires-triage`
  in place so they show up in the next pass)
- Do not include AI/Claude attribution in the summary issue
