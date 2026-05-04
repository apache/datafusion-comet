---
name: audit-spark-commit
description: Audit a single Apache Spark commit to determine whether it impacts DataFusion Comet. Reads the contributor guide for the rubric, fetches the commit, proposes a verdict, and updates dev/spark-commit-audit.md after the user reviews.
argument-hint: <commit-hash>
---

Audit Apache Spark commit `$ARGUMENTS` for impact on DataFusion Comet.

The full process and rubric live in
`docs/source/contributor-guide/spark_commit_audit.md`. Read that page first
so the rubric is in context. The steps below are a thin orchestration of
the per-commit workflow.

## Inputs

A single Spark commit hash, short or full. PR numbers are not accepted; the
caller must resolve them.

## Steps

### 1. Read the contributor guide

Read `docs/source/contributor-guide/spark_commit_audit.md` start to finish.
The "Rubric" section is the source of truth for the verdict. The "Comet
scope reference" table tells you which subsystems Comet currently
implements.

### 2. Fetch the Spark commit

Ensure `apache/spark` is cloned to a cache dir:

```bash
SPARK_DIR="/tmp/spark-audit-clone"
if [ ! -d "$SPARK_DIR" ]; then
  git clone https://github.com/apache/spark.git "$SPARK_DIR"
else
  git -C "$SPARK_DIR" fetch origin master
fi
```

Then read the commit:

```bash
git -C "$SPARK_DIR" show --stat $ARGUMENTS
```

For deeper investigation, read the changed files directly with `git -C "$SPARK_DIR" show $ARGUMENTS:<path>` or by checking out the commit in the cache dir.

If a SPARK JIRA or GitHub PR is referenced in the commit message, fetch
that for additional context using `gh` if available.

### 3. Confirm scope

Confirm the commit touches `sql/` and is not entirely under `sql/connect/`
or `sql/hive-thriftserver/`. If it is out of scope, propose `not-relevant`
with a one-line note explaining why and proceed to step 5.

### 4. Apply the rubric

Walk the "Relevant" trigger list and the "Not relevant" bucket list from
the contributor guide. Cross-reference the affected subsystem against the
"Comet scope reference" table.

Propose one of:

- `relevant`: the commit affects a subsystem Comet emulates.
- `not-relevant`: the commit does not affect Comet.
- `unclear`: the rubric cannot determine impact without more research.

Compose a one-sentence prose note that explains the verdict (e.g. "Adds a
new ANSI overflow check in `Add` expression that Comet currently does not
match"). Keep notes concise; the line is a single bullet.

### 5. Update the audit log

Locate the existing `[needs-triage]` line for this commit in
`dev/spark-commit-audit.md`. Match by short hash.

If the line is missing, abort and tell the user:

> The commit `$ARGUMENTS` is not in `dev/spark-commit-audit.md`. Re-run
> `python dev/regenerate-spark-audit.py` from the release virtualenv to
> pick up new commits, then invoke this skill again.

Do not append the line; the bootstrap script is the single source of
truth for membership in the queue.

If the line is present, propose the updated line to the user and wait for
approval or edits. The format is:

```
- `<short-hash>` <date> [<state>] <subject>. <prose-note>[. comet#<NNNN>]
```

On approval, replace the line in place using the Edit tool. Do not commit
and do not push.

### 6. Offer to handle the Comet tracking issue

If the verdict is `relevant`, ask the user:

> This commit is `relevant`. How would you like to handle the Comet
> tracking issue?
>
> - **(a)** Draft the issue body to a local markdown file under
>   `/tmp/comet-audit-issue-<hash>.md` for review.
> - **(b)** File a Comet GitHub issue immediately via `gh issue create`,
>   after I show you the title and body.
> - **(c)** Skip; I will handle it later.

For **(b)**, show the title and body and confirm before running `gh`. If
the user confirms, run `gh issue create --repo apache/datafusion-comet
--title "..." --body-file <file>` and report the resulting issue URL,
then offer to add `comet#NNNN` to the audit log line.

## What this skill does NOT do

- Resolve PR numbers to commits.
- Audit more than one commit per invocation.
- Append a new line if the commit is missing from the log (it tells the
  user to re-run the bootstrap script instead).
- Commit, push, or open Comet PRs.

## Tone and style

- Keep prose notes to one sentence.
- Use backticks around code references.
- Avoid em dashes; use periods or restructure.
