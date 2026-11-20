---
name: bug-triage
description: Triage open Comet issues marked `requires-triage` per the project bug triage guide. Classifies each issue as a bug or an enhancement, applies the recommended type (`bug`/`enhancement`), priority, and area labels, removes `requires-triage`, and files a dated summary issue that opens with all open critical bugs by area (with the change from last week) and then lists what was triaged. A human reviews the summary issue and closes it when satisfied.
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
5. Files a single dated summary issue. It opens with every open critical bug
   grouped by area, marks the ones new this week, and compares the counts with
   the previous report when that report has them. The issues triaged in this
   pass follow, kept short for a 30 minute review.

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
- Record it in the summary under "Skipped - needs more info" (in the "Needs a
  decision" section) so the reviewer can follow up.

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

## Step 6: Build the Open Critical Bugs Summary

The report opens with the state of **all** open critical bugs, not just this
week's. Build it after Step 5 so that critical bugs labeled in this pass are
included.

Fetch every open critical bug:

```bash
gh issue list \
  --repo apache/datafusion-comet \
  --label bug --label priority:critical \
  --state open \
  --limit 300 \
  --json number,title,labels,url,createdAt
```

**Primary area.** Give each issue exactly one area so the counts add up to the
total. Use the first label from this list that the issue carries:
`area:Iceberg`, `area:Delta`, `area:udf`, `area:writer`, `area:shuffle`,
`area:joins`, `area:aggregation`, `area:memory`, `area:ffi`, `area:scan`,
`area:expressions`, `area:ci`. An issue with none of them goes under
`No area label`. Only read the labels that are already on the issue here; this
step never adds one.

**New this week.** A critical bug is new if it was in this pass's
`requires-triage` queue and ended up `bug` + `priority:critical`. That includes
issues whose author already applied the labels.

**Age.** Every issue in the report shows how old it is, from its `createdAt`.
Age is the number of whole calendar days (UTC) from the day the issue was
created to the report date. Write it as `N days`, `1 day`, or `today` for an
issue created on the report date. Take `createdAt` from the Step 2 fetch for
issues in this pass and from the fetch above for the other critical bugs.

**Comparison with last week.** Find the previous report:

```bash
gh issue list \
  --repo apache/datafusion-comet \
  --search '"Bug triage results:" in:title' \
  --state all \
  --limit 20 \
  --json number,title,createdAt,body
```

Keep only titles that start with `Bug triage results: `, drop any created today,
and take the one with the latest `createdAt`. Then compare only if **both** hold:

1. It was created within the last 10 days.
2. Its `## Open critical bugs` section has area headings of the form
   `### <area> (<N> open...` and a first line starting with `<N> open`.

Reports from before this format do not have those headings. If either condition
fails, leave out `vs last week` entirely and say nothing about a trend. Do not
estimate the previous count from the old report's critical subsection, because
that lists only that week's new issues.

If both hold, take each area's `<N> open` from its heading and the total from
the first line, and use the difference from this week's count. An area missing
from the previous report had zero. Keep a heading for an area that dropped to
zero when the previous report had it, so the drop is visible. The previous
counts are what that report published; do not recompute them from today's
labels.

## Step 7: File the Summary Issue

Compute today's date in `YYYY-MM-DD` form (use the system date, not memory):

```bash
TRIAGE_DATE=$(date -u +%Y-%m-%d)
```

Title: `Bug triage results: ${TRIAGE_DATE}`

The report is read in a 30 minute meeting, so keep it short. Detail goes where a
decision is needed; everything else is a title and a link. Sections, in this
order:

1. **Header** — three lines at most: the date; issues processed with the split
   into bugs and enhancements; bugs by priority for this pass. Link to
   `docs/source/contributor-guide/bug_triage.md`. Say that labels are already
   applied and the reviewer should close the issue when satisfied.
2. **Open critical bugs** — the heading must be exactly `## Open critical bugs`,
   because next week's run looks for it. Use this layout:

   ```
   ## Open critical bugs

   25 open, 2 new this week, 1 fewer than last week.

   ### area:Iceberg (8 open, 1 new, +1 vs last week)

   - **NEW** <title> (2 days) ([#1234](url)) - one sentence on why it is critical
   - <title> (27 days) ([#1200](url))

   ### area:expressions (7 open, -1 vs last week)

   - <title> (12 days) ([#1180](url))
   ```

   - Do not add a table. The counts are in the first line and the headings.
   - Heading form: `### <area> (<N> open[, <N> new][, <+N|-N|0> vs last week])`.
     Leave out `new` when it is zero. The area is the primary area from Step 6,
     or `No area label`. Next week's run reads `<N> open` from these headings.
   - Order the sections by open count, largest first, and leave out areas with no
     open critical bugs unless the previous report had a heading for them.
   - Within a section, list the new issues first, marked `**NEW**` and followed
     by a one-sentence rationale. Older issues are a title, an age and a link
     only.
   - Leave out `vs last week` from every heading, and the "fewer/more than last
     week" clause in the first line, when Step 6 says there is nothing to
     compare. Never write "n/a" or "first week".
   - The first line says "more" or "fewer" and gives the number; if the total
     did not change, say "the same as last week".
   - An area that dropped to zero keeps its heading, for example
     `### area:writer (0 open, -2 vs last week)`, with no bullets.

3. **Needs a decision** — omit any part that is empty; omit the heading if all
   are empty. Each is a bullet per issue in `<title> (27 days) ([#N](url))` form
   with one sub-bullet.
   - **Escalations to consider**: the trigger from the guide.
   - **Skipped - needs more info**: what is missing.
   - **Failed to label**: the quoted `gh` error.
4. **Other new bugs** — `priority:high`, `priority:medium` and `priority:low`
   from this pass, in that order; critical bugs are already in section 2. Omit
   a priority with no issues. Do **not** use a markdown table here.

   - `priority:high`: `<title> (27 days) ([#N](url))` with a one-sentence
     rationale sub-bullet.
   - `priority:medium` and `priority:low`: title and link only, no sub-bullets,
     inside a collapsed block:

     ```
     <details>
     <summary>priority:medium (5)</summary>

     - <title> (3 days) ([#1234](url))

     </details>
     ```

5. **Enhancements** — omit if empty. The count in the summary line, then titles
   and links only inside a collapsed `<details>` block, with no area labels or
   rationale.

In every list the issue number is the link target and the title is plain text.
The age goes right after the title, in the form shown above, on every issue in
the report including the collapsed lists.

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
3. Open critical bugs: total, how many are new, and the change from last week
   (or that there was no comparable previous report)
4. Number skipped (needs more info) and number failed
5. URL of the new summary issue

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
- Do not add or change labels while building the open critical bugs summary;
  it only reads the labels already on the issues
- Do not show a comparison with last week unless the previous report has an
  `## Open critical bugs` section with per-area counts in its headings and is at
  most 10 days old
