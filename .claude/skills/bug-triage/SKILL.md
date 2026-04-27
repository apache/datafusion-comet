---
name: bug-triage
description: Triage open Comet issues marked `requires-triage` per the project bug triage guide and file a GitHub issue summarizing recommended priority, area labels, and `good first issue` candidates. A human reviews the summary issue and applies the labels.
---

Run a bug triage pass for the `apache/datafusion-comet` repository.

## Overview

This skill produces a triage report as a new GitHub issue. It does NOT apply
labels, close issues, or comment on the triaged issues. A human reviewer reads
the summary issue, applies the recommended labels, and closes the summary issue
when satisfied.

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
file an empty summary issue.

## Step 3: Classify Each Issue

For each issue, review the title and body and determine:

1. **Priority label** (exactly one): apply the decision tree from the guide.
   - `priority:critical` if it could produce silent wrong results
   - `priority:high` for crashes, panics, segfaults, NPEs on supported paths
   - `priority:medium` for functional bugs / perf regressions with workarounds
   - `priority:low` for test-only, CI flakes, tooling, cosmetic
2. **Area labels** (zero or more): from the area table in the guide
   (`area:writer`, `area:shuffle`, `area:aggregation`, `area:scan`,
   `area:expressions`, `area:ffi`, `area:ci`) plus the pre-existing area
   indicators (`native_datafusion`, `native_iceberg_compat`, `spark 4`,
   `spark sql tests`).
3. **`good first issue`**: only if the fix is likely straightforward AND
   well-scoped. When in doubt, leave it off.
4. **Escalation note**: if the issue matches an escalation trigger from the
   guide (e.g., a `priority:high` crash that may also produce wrong results),
   call it out.
5. **Needs more info**: if the report has no reproduction steps, mark it as
   needing a follow-up question to the reporter rather than guessing.

If you cannot confidently classify an issue from its title and body, say so
explicitly in the summary instead of guessing.

Do NOT edit, label, or comment on the triaged issues. Recommendations only.

## Step 4: File the Summary Issue

Compute today's date in `YYYY-MM-DD` form (use the system date, not memory):

```bash
TRIAGE_DATE=$(date -u +%Y-%m-%d)
```

Title: `Bug triage results: ${TRIAGE_DATE}`

Body: a markdown table or per-issue section listing, for each triaged issue:

- Issue number and title (linked)
- Recommended priority label
- Recommended area labels
- `good first issue`? (yes/no)
- Escalation / needs-more-info notes
- One-sentence rationale tying the recommendation to the guide

Include a short header that:

- States the date, the number of issues triaged, and the count per priority
- Reminds reviewers that nothing has been labeled yet, and asks them to apply
  the recommended labels (and remove `requires-triage`) before closing this
  summary issue
- Links to `docs/source/contributor-guide/bug_triage.md`

File the issue with `gh`:

```bash
gh issue create \
  --repo apache/datafusion-comet \
  --title "Bug triage results: ${TRIAGE_DATE}" \
  --body-file <(cat <<'EOF'
... summary markdown here ...
EOF
)
```

Do not add labels to the summary issue itself. The human reviewer decides
whether to label it (and will close it when done).

After creating the issue, print its URL so the reviewer can find it.

## Output to the User

Report back:

1. Number of `requires-triage` issues processed
2. Count per recommended priority
3. URL of the new summary issue

Do not paste the full triage table back into the chat; it is in the issue.

## What This Skill Must Not Do

- Do not apply labels to the triaged issues
- Do not remove `requires-triage` from any issue
- Do not comment on the triaged issues
- Do not close any issue
- Do not file the summary issue if there were zero `requires-triage` issues
- Do not invent priority or area labels that are not in the guide
- Do not include AI/Claude attribution in the summary issue
