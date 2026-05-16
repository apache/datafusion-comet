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

The following pre-existing labels also serve as area indicators: `native_datafusion`,
`native_iceberg_compat`, `spark sql tests`.

## Spark Version Labels

If a bug only reproduces on (or is otherwise specific to) a particular Spark version, apply the
matching version label so the issue surfaces in version-specific queries. A bug may have multiple
version labels if it affects more than one supported version.

| Label       | Use when                                       |
| ----------- | ---------------------------------------------- |
| `spark 4.0` | Issue is specific to Spark 4.0                 |
| `spark 4.1` | Issue is specific to Spark 4.1                 |
| `spark 4.2` | Issue is specific to Spark 4.2                 |

Do not apply a Spark version label to bugs that reproduce on every supported version: they belong
to general triage, not version-specific tracking.

## Triage Process

Every new issue is automatically labeled with `requires-triage` when it is opened. This makes it
easy to find issues that have not yet been triaged by filtering on that label. Once an issue has
been triaged, remove the `requires-triage` label and apply the appropriate priority and area labels.

### For New Issues

When a new bug is filed:

1. **Reproduce or verify** the issue if possible. If the report lacks reproduction steps, ask
   the reporter for more details.
2. **Assess correctness impact first.** Ask: "Could this produce wrong results silently?" This
   is more important than whether it crashes.
3. **Apply a priority label** using the decision tree above.
4. **Apply area labels** to indicate the affected subsystem(s).
5. **Apply a Spark version label** (e.g., `spark 4.1`) if the bug is specific to one or more
   supported Spark versions.
6. **Apply `good first issue`** if the fix is likely straightforward and well-scoped.
7. **Remove the `requires-triage` label** to indicate triage is complete.

### For Existing Bugs

Periodically review open bugs to ensure priorities are still accurate:

- Has a `priority:medium` bug been open for a long time with user reports? Consider escalating.
- Has a `priority:high` bug been fixed by a related change? Close it.
- Are there clusters of related bugs that should be tracked under an EPIC?

### Prioritization Principles

1. **Correctness over crashes.** A bug that silently returns wrong results is worse than one that
   crashes, because crashes are at least visible.
2. **User-reported over test-only.** A bug hit by a real user on a real workload takes priority
   over one found only in test suites.
3. **Core path over experimental.** Bugs in the default scan mode (`native_comet`) or widely-used
   expressions take priority over bugs in experimental features like `native_datafusion` or
   `native_iceberg_compat`.
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
Spark (or vice versa), and the intermediate formats may not be compatible. See the
[EPIC #2892](https://github.com/apache/datafusion-comet/issues/2892) for the full picture.

### Native Writer Bugs

The native Parquet writer has a cluster of known test failures tracked as individual issues
(#3417–#3430). These are lower priority since the native writer is still maturing, but they
should be addressed before the writer is promoted to production-ready status.

## How to Help with Triage

Triage is a valuable contribution that doesn't require writing code. You can help by:

- Reviewing new issues and suggesting a priority label
- Reproducing reported bugs and adding details
- Identifying duplicate issues
- Linking related issues together
- Testing whether old bugs have been fixed by recent changes
