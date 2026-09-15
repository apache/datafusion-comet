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

# Bug Triage Guide

This guide describes how we prioritize and triage bugs in the Comet project. The goal is to ensure
that the most impactful bugs — especially correctness issues that produce wrong results — are
identified and addressed before less critical issues.

## Type Labels

Every new issue is auto-labeled with `requires-triage`, and this applies to **all** issues, not
just bug reports. The first triage decision for any issue is therefore whether it is a bug or an
enhancement.

| Label         | Description                                                                                                                                            |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `bug`         | Something is broken: wrong results, crashes, panics, regressions, test/CI failures                                                                     |
| `enhancement` | A request for new functionality or improvement: new expression/operator support, a new config, performance optimization, refactoring, or documentation |

Apply exactly one of these to every triaged issue. An issue must never carry both `bug` and
`enhancement`. Classify from the actual content of the issue, not from the title wording: an issue
titled "bug: ..." may really be a feature request, and a genuine defect may not mention "bug" in
its title at all.

Only bugs receive a priority label (see below). Enhancements are not assigned a priority label
through this process. Both bugs and enhancements may receive area labels.

## Priority Labels

Every bug should have exactly one priority label. When filing or triaging a bug, apply the
appropriate label from the table below.

| Label               | Color  | Description                                                                          | Examples                                                              |
| ------------------- | ------ | ------------------------------------------------------------------------------------ | --------------------------------------------------------------------- |
| `priority:critical` | Red    | Data corruption, silent wrong results, security vulnerabilities                      | Wrong aggregation results, FFI data corruption, incorrect cast output |
| `priority:high`     | Orange | Crashes, panics, segfaults, major functional breakage affecting production workloads | Native engine panic, JVM segfault, NPE on supported code path         |
| `priority:medium`   | Yellow | Functional bugs, performance regressions, broken features that have workarounds      | Missing expression support, writer feature gaps, excessive spilling   |
| `priority:low`      | Green  | Minor issues, test-only failures, tooling, CI flakes, cosmetic issues                | Flaky CI test, build script edge case, documentation generator bug    |

### How to Choose a Priority

Use this decision tree:

1. **Can this bug cause silent wrong results?** If yes → `priority:critical`. These are the most
   dangerous bugs because users may not notice the incorrect output.
2. **Does this bug crash the JVM or native engine?** If yes → `priority:high`. Crashes are
   disruptive but at least visible to the user.
3. **Does this bug break a feature or cause significant performance degradation?** If yes →
   `priority:medium`. The user can work around it (e.g., falling back to Spark) but it impacts
   the value of Comet.
4. **Everything else** → `priority:low`. Test failures, CI issues, tooling, and cosmetic problems.

### Escalation Triggers

A bug should be escalated to a higher priority if:

- A `priority:high` crash is discovered to also produce wrong results silently in some cases →
  escalate to `priority:critical`
- A `priority:medium` bug is reported by multiple users or affects a common workload → consider
  escalating to `priority:high`
- A `priority:low` CI flake is blocking PR merges consistently → escalate to `priority:medium`
- A bug turns out to be a `regression` from the most recent release → consider escalating one
  level, because users who upgrade are exposed to it without changing anything on their side

## Regression Label

| Label        | Description                                             |
| ------------ | ------------------------------------------------------- |
| `regression` | A bug that did not affect the most recent Comet release |

Apply `regression` to a bug when a workload that behaved correctly on the most recent release
behaves incorrectly on `main`. That covers wrong results, a new failure, a new crash, and the case
where an expression used to fall back to Spark (and was therefore correct) and now runs natively
with a wrong answer. It also covers a loss of safety: a query that failed with a clear error on the
last release and now returns silently wrong data is a regression, even though it never produced the
right answer on either version.

A defect that already shipped in the most recent release is **not** a regression, no matter how
recently it was reported. Neither is a defect in a feature added after that release: a workload
running on the release cannot reach code that did not exist yet.

`regression` is orthogonal to priority. A regression still gets the priority label its symptoms
earn, and it is an escalation trigger rather than a priority of its own. It applies only to bugs.

### Determining the Comparison Point

Always compare against the most recent release **tag**, resolved at triage time rather than
hard-coded, so the comparison point moves forward as Comet ships:

```bash
LATEST_RELEASE=$(gh release view --repo apache/datafusion-comet --json tagName --jq .tagName)
git fetch --tags
git log -1 --format=%cI "$LATEST_RELEASE"
```

Compare against the **tag's commit date**, not the release's publication date — commits that land
between the two are not in the release.

### Establishing Regression Status

Work through these in order and stop at the first definite answer:

1. **Issue creation date.** An issue opened before the tag was cut describes behavior that shipped
   in that release. Not a regression.
2. **Is the defective code present at the tag?** `git show "$LATEST_RELEASE:<path>"`,
   `git grep <pattern> "$LATEST_RELEASE"`, or `git diff "$LATEST_RELEASE"..HEAD -- <path>`. If the
   defective logic is there verbatim, not a regression.
3. **Was the path reachable at the tag?** Check that the Scala serde entry, shim, or native
   registration existed, not just the kernel. Code absent from the tag means new work, not a
   regression — unless a post-release change broke a path that used to be correct.
4. **Run the reproducer against the tag.** Build the tag in a scratch worktree and run it. This is
   the only way to settle cases that turn on a dependency bump (a DataFusion or Arrow/Parquet major
   version) rather than on Comet's own code.

Issues found during PR review often say "this is pre-existing, not caused by this PR". That is a
claim about the pull request under review, not about the last release; a defect can be pre-existing
relative to the PR that surfaced it and still have landed after the tag. Verify against the tag.

If the evidence is inconclusive, leave `regression` off and say so on the issue rather than
guessing.

## Area Labels

Area labels indicate which subsystem is affected. A bug may have multiple area labels. These
help contributors find bugs in their area of expertise.

| Label              | Description                               |
| ------------------ | ----------------------------------------- |
| `area:writer`      | Native writer (Parquet and other formats) |
| `area:shuffle`     | Shuffle (JVM and native)                  |
| `area:aggregation` | Hash aggregates, aggregate expressions    |
| `area:scan`        | Data source scan (Parquet, CSV, Iceberg)  |
| `area:expressions` | Expression evaluation                     |
| `area:ffi`         | Arrow FFI / JNI boundary                  |
| `area:ci`          | CI/CD, GitHub Actions, build tooling      |

The following pre-existing labels also serve as area indicators: `spark 4`, `spark sql tests`.

## Triage Process

Every new issue is automatically labeled with `requires-triage` when it is opened. This makes it
easy to find issues that have not yet been triaged by filtering on that label. Once an issue has
been triaged, remove the `requires-triage` label and apply the appropriate priority and area labels.

### For New Issues

When a new issue is filed:

1. **Decide bug or enhancement.** Apply `bug` or `enhancement` based on the content of the issue.
   The remaining steps about priority apply only to bugs.
2. **Reproduce or verify** the issue if possible. If the report lacks reproduction steps, ask
   the reporter for more details.
3. **Assess correctness impact first.** Ask: "Could this produce wrong results silently?" This
   is more important than whether it crashes.
4. **Apply a priority label** using the decision tree above (bugs only).
5. **Check whether the bug is a regression** from the most recent release tag and apply
   `regression` if it is (bugs only).
6. **Apply area labels** to indicate the affected subsystem(s).
7. **Apply `good first issue`** if the fix is likely straightforward and well-scoped.
8. **Remove the `requires-triage` label** to indicate triage is complete.

### For Existing Bugs

Periodically review open bugs to ensure priorities are still accurate:

- Has a `priority:medium` bug been open for a long time with user reports? Consider escalating.
- Has a `priority:high` bug been fixed by a related change? Close it.
- Are there clusters of related bugs that should be tracked under an EPIC?
- Does an open bug need its regression status re-checked against a newer release? A bug that was
  a regression from one release is still a regression once the next release ships with it
  unfixed, so `regression` stays until the bug is fixed.

### Prioritization Principles

1. **Correctness over crashes.** A bug that silently returns wrong results is worse than one that
   crashes, because crashes are at least visible.
2. **User-reported over test-only.** A bug hit by a real user on a real workload takes priority
   over one found only in test suites.
3. **Core path over experimental.** Bugs in widely-used expressions and operators take priority over
   bugs in experimental features.
4. **Production safety over feature completeness.** Fixing a data corruption bug is more important
   than adding support for a new expression.

## Common Bug Categories

### Correctness Bugs (`priority:critical`)

These are bugs where Comet produces different results than Spark without any error or warning.
Examples include:

- Incorrect cast behavior (e.g., negative zero to string)
- Aggregate functions ignoring configuration (e.g., `ignoreNulls`)
- Data corruption in FFI boundary (e.g., boolean arrays with non-zero offset)
- Type mismatches between partial and final aggregation stages

When fixing correctness bugs, always add a regression test that verifies the output matches Spark.

### Crash Bugs (`priority:high`)

These are bugs where the native engine panics, segfaults, or throws an unhandled exception.
Common patterns include:

- **All-scalar inputs:** Some expressions assume at least one columnar input and panic when all
  inputs are literals (e.g., when `ConstantFolding` is disabled)
- **Type mismatches:** Downcasting to the wrong Arrow array type
- **Memory safety:** FFI boundary issues, unaligned arrays, GlobalRef lifecycle

### Aggregate Planning Bugs

Several bugs relate to how Comet plans hash aggregates across stage boundaries. The key issue is
that Spark's AQE may materialize a Comet partial aggregate but then run the final aggregate in
Spark (or vice versa), and the intermediate formats may not be compatible. The closed
EPIC [#2892](https://github.com/apache/datafusion-comet/issues/2892) collects the historical
reports and is a good starting point when a new one comes in.

### Native Writer Bugs

The native Parquet writer has a cluster of known test failures tracked as individual issues, from
[#3417](https://github.com/apache/datafusion-comet/issues/3417) to
[#3430](https://github.com/apache/datafusion-comet/issues/3430). These are lower priority since the
native writer is still maturing, but they should be addressed before the writer is promoted to
production-ready status.

## How to Help with Triage

Triage is a valuable contribution that doesn't require writing code. You can help by:

- Reviewing new issues and suggesting a priority label
- Reproducing reported bugs and adding details
- Identifying duplicate issues
- Linking related issues together
- Testing whether old bugs have been fixed by recent changes
- Checking whether an open bug is a `regression` from the most recent release tag
