---
name: audit-comet-expression-batch
description: Dispatcher that picks the next un-audited Spark expression from `spark_expressions_support.md` and runs `audit-comet-expression-autonomous` against it in a fresh subagent. Pacing is one expression per invocation by default. Drive it manually or with `/loop` to march through the backlog.
argument-hint: [--count N]
---

Process up to `N` un-audited expressions (default `N=1`) by dispatching `audit-comet-expression-autonomous` as a subagent for each.

## Pre-flight

Verify:

1. `gh auth status` succeeds.
2. `git status --porcelain` in the parent repo is clean.
3. `docs/source/contributor-guide/spark_expressions_support.md` exists.

Abort if any pre-flight check fails.

Parse `--count N` from `$ARGUMENTS`. Default to `N=1` if absent or unparseable.

## Step 1: Build the candidate list

Read `docs/source/contributor-guide/spark_expressions_support.md`. For each line matching `^- \[x\] <name>$`, check whether the immediately-following lines (until the next `- [` or blank-then-`### `) contain any sub-bullet (`  - `). If no sub-bullet exists, the expression is a candidate.

Reference one-liner (the skill agent should implement equivalent logic and not depend on this exact awk script):

```bash
awk '
  /^### / { cat=$2; next }
  /^- \[x\] / {
    if (prev_was_candidate) print prev_name
    prev_name = $3
    prev_was_candidate = 1
    next
  }
  /^  - / { prev_was_candidate = 0; next }
  END { if (prev_was_candidate) print prev_name }
' docs/source/contributor-guide/spark_expressions_support.md
```

Build the candidate list in source order. Truncate to the first `N`.

If the list is empty, exit with the message "Nothing to audit. Every `[x]` expression in `spark_expressions_support.md` already has at least one audit sub-bullet."

## Step 2: For each candidate, dispatch the per-expression skill

For each expression in the truncated list:

1. **Branch-exists check.** Check the remote for an in-flight audit:

   ```bash
   gh api -X GET "repos/apache/datafusion-comet/branches/autonomous-audit/<expr-snake-case>" \
     >/dev/null 2>&1 && echo EXISTS || echo OK
   ```

   If `EXISTS`, append `<expression>: skipped (already in flight)` to the per-run summary and move to the next candidate. Do not count it against `N`.

2. **Dispatch subagent.** Invoke the `Agent` tool with:
   - `subagent_type: claude`
   - `isolation: worktree`
   - `description`: `audit <expression>`
   - `prompt`: the literal text below, with `<expression>` substituted

     ```
     Invoke the `audit-comet-expression-autonomous` skill on the expression `<expression>`. Follow the skill's instructions exactly. When the skill finishes, return its final-output block verbatim so I can capture the PR URL and any filed issue URLs.
     ```

3. **Capture output.** Parse the subagent's final message for the PR URL and any issue URLs. Append to the per-run summary.

## Step 3: Print the per-run summary

Print exactly:

```
Dispatched <K> audit(s):
  - <expression>: PR <url>  (issues: <url>, <url>, or "none")
  - <expression>: skipped (already in flight)
  ...

Next un-audited expression: <name>  (or "none, backlog clear")
```

The "Next un-audited expression" line is informational. It tells the user what `/loop` will pick on the next tick. Compute it by re-running Step 1's candidate-list logic post-dispatch.

## What this skill must not do

- Do not open PRs directly. PR opening is the per-expression skill's job.
- Do not modify any expression's source, tests, or docs directly. All edits flow through the subagent.
- Do not pick more than `N` expressions per invocation, even if there are more candidates remaining.
- Do not retry a subagent that failed. Surface the failure in the summary and let the human decide.

## Future work (do not implement yet)

- Re-auditing expressions whose audit sub-bullets predate a configurable freshness threshold.
- Parallel fan-out via `superpowers:dispatching-parallel-agents`.
- Auditing `[ ]` (unimplemented) expressions to flag drift in the support doc itself.
