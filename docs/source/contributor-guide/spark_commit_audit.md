<!---
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

# Spark Commit Audit

This page describes how the Comet community audits Apache Spark `master`
commits since `branch-4.2` was cut, so that the project stays aware of
upstream changes and does not silently diverge from Spark behavior.

## Why we audit

Comet emulates Spark behavior across many subsystems: expressions, the
optimizer, Parquet read and write, shuffle, joins, aggregates, and more.
When Spark changes behavior upstream, Comet may need to follow. The audit
is the mechanism that makes "did anyone notice this commit?" answerable.

## Scope

In scope: commits on Apache Spark `master` since the `branch-4.2` cut that
touch the `sql/` subtree.

Out of scope:

- `sql/connect/` (Spark Connect)
- `sql/hive-thriftserver/`
- Commits backported only to release branches. If a change is relevant it
  lands on `master` first.
- Commits before the `branch-4.2` cut.

## Where the log lives

The audit log is `dev/spark-commit-audit.md`. Each line corresponds to one
Spark commit. The format is:

`` - `<8-char-hash>` <YYYY-MM-DD> [<state>] <subject>[. <prose-note>][. comet#<NNNN>] ``

Lines are kept oldest-first. New commits are appended by the bootstrap
script.

### States

| State | Meaning |
|---|---|
| `needs-triage` | Not yet audited. Initial state for every entry. |
| `relevant` | Audited; the commit affects Comet. A Comet issue link is recommended but not required. |
| `not-relevant` | Audited; the commit does not affect Comet. |
| `unclear` | Audited; the auditor could not decide. The prose note should explain why. |

## Rubric

### "Relevant" triggers

Mark a commit `relevant` if any of the following apply:

1. Adds, removes, or renames a Spark expression or function.
2. Changes evaluation behavior of an existing expression: null handling,
   ANSI mode, overflow, dictionary encoding, or casting.
3. Changes the optimizer or planner in a way that produces a different
   physical plan shape.
4. Changes Parquet reader or writer behavior, or pushdown semantics.
5. Changes shuffle or exchange behavior.
6. Changes operator behavior in joins, aggregates, window, or sort.
7. Changes type coercion, resolution, or analysis rules.
8. Changes ANSI mode defaults or semantics.
9. Adds a new SQL config that affects behavior Comet emulates.
10. Adds new Spark tests that exercise behavior Comet may not yet match.

### "Not relevant" buckets

Mark a commit `not-relevant` if it falls into one of these:

- Spark Connect-only changes (already filtered at enumeration time).
- Hive thriftserver-only changes (already filtered).
- Codegen-only refactors. Comet does not use Spark's whole-stage codegen.
- Pure refactors with no behavior change.
- Docs, comments, build, or CI changes.
- Test-only changes that exercise behavior Comet already matches.

### Comet scope reference

Use the table below as a quick check for whether the affected subsystem is
one Comet currently cares about. See the
[Compatibility Guide](https://datafusion.apache.org/comet/user-guide/compatibility.html)
for the authoritative list.

| Subsystem | Comet support |
|---|---|
| Expressions | Many supported, see compat guide |
| Parquet read | Supported |
| Parquet write | Partially supported |
| Native shuffle | Supported |
| JVM shuffle | Supported |
| Hash joins | Supported |
| Sort merge joins | Supported |
| Hash aggregate | Supported |
| Window | Partially supported |
| Sort | Supported |
| Scan pushdown | Partially supported |

## Workflow

1. Pull the latest `dev/spark-commit-audit.md` and grab a contiguous chunk
   of `[needs-triage]` lines, typically 10 to 20 commits.
2. For each commit, read the Spark PR or commit, apply the rubric, and set
   the state. Add an optional one-sentence prose note and, when relevant,
   a `comet#NNNN` link.
3. When the verdict is `relevant` and there is no existing tracking issue,
   filing one is recommended but not required.
4. Open a Comet PR titled `chore: audit Spark commits <short-hash>..<short-hash>`.

## Tools

Claude users can run the `audit-spark-commit` skill on each commit hash
to get a proposed verdict that follows this rubric. Contributors who do
not use Claude follow the manual process above.

## Bootstrapping and incremental updates

The audit log is generated and maintained by `dev/regenerate-spark-audit.py`.
Run it from the existing release virtualenv:

```sh
cd dev/release && source venv/bin/activate
export GITHUB_TOKEN=<your-token>
python ../regenerate-spark-audit.py
```

The script is idempotent: it preserves existing verdicts and prose notes by
short-hash and only appends new `[needs-triage]` lines for commits that have
appeared since the last run.

Useful flags:

- `--dry-run`: print the resulting block without writing the file.
- `--limit N`: only consider the most recent N in-scope commits (for
  testing).
