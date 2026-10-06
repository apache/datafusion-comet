---
name: pr-triage
description: Triage open Comet pull requests for pull request hygiene. Checks each PR body against the repository's PR template, then looks for open PRs that overlap or compete with it, and posts a single comment on the PR noting anything the author needs to fix. Reports back a per-PR summary in the terminal.
argument-hint: "[pr-number ...]"
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

This skill triages open pull requests for hygiene problems that block review.
For each PR in scope it:

1. Checks the PR body against `.github/pull_request_template.md` — are the
   template's sections present, and is each one actually answered?
2. Checks the linked issue — does it exist, is it open, and is it already
   claimed by a merged or open PR?
3. Looks for other open PRs that overlap or compete with it, and reads the
   contested diff before asserting that they do.
4. Posts at most one comment per PR raising whatever it found, in the
   maintainer's own voice.
5. Reports a per-PR summary in the terminal.

This is **not** a code review. It does not judge whether the change is correct,
well-designed, or Spark-compatible — use the `review-comet-pr` skill for that.
It only looks at whether the PR is presented well enough to be reviewed, and
whether someone else is already doing the same work.

It also does not repeat checks that CI already runs. The conventional-commit
title format is enforced by `.github/workflows/pr_title_check.yml` and type/area
labels are applied by `.github/workflows/label_prs.yml`; never comment on
either.

## Step 1: Read the PR Template

Read the canonical template in this repository:

```
.github/pull_request_template.md
```

Derive the required section list from that file, not from memory. At the time of
writing it has four `##` headings:

1. `Which issue does this PR close?`
2. `Rationale for this change`
3. `What changes are included in this PR?`
4. `How are these changes tested?`

If the template and this skill ever disagree, the template wins. The
instructional HTML comments inside the template also carry the project's real
expectations (for example: "We generally require a GitHub issue to be filed for
all bug fixes and enhancements", and "If tests are not included in your PR,
please explain why"). Read them; they are the standard you are checking against.

## Step 2: Determine Scope

If the user passed one or more PR numbers as arguments, triage exactly those and
skip the filtering below (including the draft and age filters — an explicit
request overrides them).

Otherwise the default scope is every open PR that is:

- **not a draft** — a draft is still being written, so template gaps are not yet
  a finding. `isDraft` is not the whole test: contributors here also mark work in
  progress in the title (`[WIP]`, `[draft]`, `WIP:`). Treat those as drafts too.
- **not bot-authored** — skip `dependabot[bot]` and any other `*[bot]` author;
  dependency bumps have no template to fill in
- **opened within the last 4 weeks** — older PRs are usually blocked on
  something other than hygiene, and a late nag adds noise
- **not already triaged** — see Step 6

## Step 3: Gather the PR Corpus

Fetch every open PR once. You need the full set — not just the ones in scope —
because the overlap analysis in Step 5 scores each in-scope PR against all of
them:

```bash
gh pr list \
  --repo apache/datafusion-comet \
  --state open \
  --limit 400 \
  --json number,title,author,createdAt,isDraft,labels,body,url,files,closingIssuesReferences \
  > /tmp/comet-open-prs.json
```

Derive the in-scope set from this file using the Step 2 filters. If the in-scope
set is empty, say so and stop. Do not widen the window to find something to do.

## Step 4: Check Template Conformance

For each in-scope PR, work from the `body` field.

First normalize: strip HTML comment blocks (`<!-- ... -->`) before judging
whether a section has content. Authors routinely leave the template's
instructional comments in place, which is fine — a section that contains _only_
those comments is unfilled, a section with prose next to them is filled.

Then check:

**Missing sections.** A `##` heading from the template that does not appear at
all. If _no_ template heading appears, the author replaced the template
wholesale — report that as one finding, not four.

**Empty sections.** A heading present with no prose under it after normalizing.

**The issue link.** The section is answered if any of these hold:

- `closingIssuesReferences` is non-empty (this is GitHub's own parse of
  `Closes #N` / `Fixes #N`; trust it over regexing the body yourself), **or**
- the body explains the relationship in prose — `Part of #N`, `Related to #N`,
  `Extracted from #N`, `Depends on #N`, or an explicit statement that no issue
  is needed and why.

The literal placeholder `Closes #.` left from the template counts as unfilled.
Look for it only inside the issue section and only outside code spans and fenced
blocks — a whole-body substring search matches PRs that merely quote the
placeholder in prose, this skill's own PR among them.

Be slow to turn this one into a comment. On a typical pass, most PRs with no
`closingIssuesReferences` are fine: doc, test, CI, and chore PRs legitimately
have no issue behind them, and plenty of authors write the relationship in prose
that no regex will match. Read the section before deciding it is unanswered.

When an issue number is referenced, check it:

```bash
gh issue view <N> --repo apache/datafusion-comet --json number,title,state,stateReason,url
```

- Issue does not exist, or the number points at a PR rather than an issue →
  finding.
- Issue is **closed** → finding, and a strong duplicate signal. Find out what
  closed it; the work may already be on `main`.

**The testing section.** Apply judgment rather than a rule:

- Code changed, no tests anywhere in the diff, and the section does not explain
  why → finding. This is the one worth raising most often.
- Code changed, tests added, section thin → not a finding. Do not comment.
- No code changed (docs, comments, CI config) → not a finding.

The changed-file list alone does **not** tell you whether tests were added. Rust
tests live in inline `#[cfg(test)]` modules in the same `.rs` file as the code,
so a Rust PR that touches one source file and nothing else may still be fully
tested. Check the diff before claiming tests are missing:

```bash
gh pr diff <N> --repo apache/datafusion-comet | grep -E '^\+.*(#\[test\]|#\[cfg\(test\)\])'
```

On the JVM side tests are separate files, under `spark/src/test/`,
`common/src/test/`, or as `.sql` fixtures under
`spark/src/test/resources/sql-tests/`.

**Exemptions.** Do not raise a missing-issue finding on a PR where the project
plainly does not want an issue: a typo or doc-only fix, a comment-only change, a
revert, or release preparation. The template says issues are required for "bug
fixes and enhancements" — hold that line and no further.

## Step 5: Check for Overlapping and Competing PRs

Three signals, in increasing cost and increasing reliability. The third is the
one that decides.

**5a. File overlap, IDF-weighted.** Hub files carry no signal in this repo —
`planner.rs`, `QueryPlanSerde.scala`, `CometConf.scala` and the two
`pr_build_*.yml` files are touched by dozens of open PRs at once. Weight each
shared file by `log(N / prs_touching_it)` and drop any file touched by more than
six open PRs:

```bash
python3 - <<'PY'
import json, math
from collections import defaultdict

prs = [p for p in json.load(open("/tmp/comet-open-prs.json"))
       if not p["author"]["login"].endswith("[bot]")]
title = {p["number"]: p["title"] for p in prs}

by_file = defaultdict(set)
for p in prs:
    for f in p["files"]:
        by_file[f["path"]].add(p["number"])

N = len(prs)
weights = {f: math.log(N / len(o)) for f, o in by_file.items() if len(o) <= 6}

scores, shared = defaultdict(float), defaultdict(list)
for f, owners in by_file.items():
    if f not in weights:
        continue
    o = sorted(owners)
    for i, a in enumerate(o):
        for b in o[i + 1:]:
            scores[(a, b)] += weights[f]
            shared[(a, b)].append(f)

for (a, b), s in sorted(scores.items(), key=lambda kv: -kv[1])[:40]:
    print(f"{s:6.2f}  #{a} / #{b}")
    print(f"        {title[a][:78]}")
    print(f"        {title[b][:78]}")
    print(f"        shared: {', '.join(sorted(shared[(a, b)])[:6])}")
PY
```

Keep only the pairs where at least one member is in scope.

**5b. Shared closing issue.** Compare `closingIssuesReferences` across the
corpus. Two PRs formally claiming the same issue is a near-certain duplicate,
but it is rare — duplicates in this repo usually close _different_ issues while
solving the same problem. Do not rely on this signal alone.

**5c. Read the diff.** This is the step that matters. PR titles and descriptions
go stale on long-lived branches, and a comment written from a stale title is
wrong in the direction the author and every reviewer can see:

```bash
gh pr diff <N> --repo apache/datafusion-comet -- <contested-file>
```

Never assert that two PRs compete until you have read what both actually do to
the shared file.

**Shapes worth knowing.** The highest-yield overlap in this repo is _codegen
dispatcher vs. native implementation for the same expression_: one contributor
mixes in `CodegenDispatchFallback`, another implements the kernel natively or
wires up a `datafusion-spark` function. Same serde object, same `.sql` fixture,
mutually exclusive. The second is a large stale PR superseded by a
differently-implemented merge — detect it by checking whether files the PR
_adds_ already exist on `main` (`git log --diff-filter=A -- <path>`), not by the
mergeable flag.

**Pairs to skip, not comment on.** High file overlap is expected and harmless
for stacked PRs, sequential splits of one piece of work, and PRs that already
declare the dependency in the body (`Depends on #N`, `Extracted from #N`) or
already cross-reference each other in the comments. Check for the
cross-reference before you write one.

## Step 6: Decide Whether to Comment

Read the existing conversation first:

```bash
gh pr view <N> --repo apache/datafusion-comet \
  --json comments,reviews,isDraft,mergeable
```

Skip the PR entirely if:

- a comment on it contains the marker `<!-- comet-pr-triage -->` — it has been
  triaged before. Re-comment only if you found something genuinely new since,
  and say in the comment that you are following up.
- someone already raised the same point. Do not restate another reviewer's
  comment in your own words.
- you found nothing. **Most PRs should produce no comment.** A clean PR is the
  normal case and silence is the correct output for it.

Ignore GitHub Copilot's reviews and comments; do not treat them as prior
coverage and do not build on them.

Post at most **one** comment per PR covering everything you found. Never split
findings across multiple comments and never leave inline review comments — this
skill comments on the conversation, not on lines of code.

## Step 7: Draft the Comments

The comment is posted under a maintainer's account, so it must read like a
maintainer wrote it.

- Prose paragraphs, not bullet lists. No bold-label prefixes, no headings.
- Lead with the concern. No preamble, no "Thanks for the PR!", no praise
  sandwich, no restating the PR's own summary back at the author.
- Propose the fix concretely, as a question — "Could you add a `Closes #NNNN`
  line so this shows up in the release notes?" beats "The issue link section is
  incomplete."
- Two or three short paragraphs at most. If you found four things, raise the two
  that actually block review.
- Name specifics: the section, the file, the competing PR number.
- No AI or Claude attribution anywhere in the comment.

End the body with the marker on its own line so a later pass can tell the PR has
been triaged. It is invisible in rendered markdown:

```
<!-- comet-pr-triage -->
```

For an overlap finding, comment on the in-scope PR only — naming the other PR
creates a cross-reference on it automatically, so a second comment is noise.
State what each side actually does to the shared file (from Step 5c, not from
the titles), and ask the two authors how they want to resolve it rather than
declaring a winner.

A comment in the right register looks like this:

> Both this and #5874 change how `length` handles binary input in
> `strings.scala` — this one routes it through the codegen dispatcher and #5874
> implements it natively, and they touch the same `length.sql` fixture, so only
> one can land. @author1 @author2, could you two work out which approach you
> want to go with and close the other?
>
> `<!-- comet-pr-triage -->`

And one for a template gap:

> Could you fill in the rationale and testing sections? The diff changes
> overflow behavior in `numeric.rs` and I can't tell from the description
> whether the existing casts suite covers the new path or whether this needs a
> fixture of its own.
>
> `<!-- comet-pr-triage -->`

## Step 8: Confirm, Then Post

Print every drafted comment in full, grouped by PR, and ask the user to confirm
before posting the first one. These are public comments on an Apache project and
a wrong one is visible to everyone; one confirmation for the whole batch is
enough. Skip the checkpoint only if the user's invoking message already told you
to post without asking.

After confirmation:

```bash
gh pr comment <N> --repo apache/datafusion-comet --body-file /tmp/pr-triage-<N>.md
```

Use a body file rather than `--body` to keep quoting sane. If `gh pr comment`
fails, record the failure and move on; do not retry against a different PR.

## Output to the User

Report back:

1. Number of open PRs fetched, and how many were in scope after filtering
2. Per in-scope PR, one line: number, what you found, and whether you commented
3. The overlap pairs you confirmed by reading the diff, and the ones you
   dismissed as stacked or already cross-referenced
4. URLs of the comments you posted
5. Anything you skipped and why

Keep the per-PR lines to one line each. The detail belongs in the comments.

## What This Skill Must Not Do

- Do not review the code — no correctness, design, or Spark-compatibility
  feedback. That is `review-comet-pr` and `deep-review-comet-pr`.
- Do not comment on the PR title format or on missing labels; CI handles both.
- Do not add, remove, or change labels on any PR.
- Do not close, approve, request changes on, or merge any PR.
- Do not comment on draft PRs or bot PRs unless the user named them explicitly.
- Do not comment twice on the same PR in one pass, and do not re-comment on a PR
  already carrying the `<!-- comet-pr-triage -->` marker unless there is
  something new.
- Do not assert that two PRs compete without having read the contested diff.
- Do not nag about a missing issue link on a typo fix, doc-only change, revert,
  or release-prep PR.
- Do not include AI/Claude attribution in any posted comment.
