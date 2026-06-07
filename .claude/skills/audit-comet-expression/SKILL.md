---
name: audit-comet-expression
description: Audit an existing Comet expression for correctness and test coverage. Studies the Spark implementation across versions 3.4.3, 3.5.8, and 4.0.1, reviews the Comet and DataFusion implementations, identifies missing test coverage, and offers to implement additional tests.
argument-hint: <expression-name>
---

Audit the Comet implementation of the `$ARGUMENTS` expression for correctness and test coverage.

## Overview

This audit covers:

1. Spark implementation across versions 3.4.3, 3.5.8, and 4.0.1
2. Comet Scala serde implementation
3. Comet Rust / DataFusion implementation
4. Existing test coverage (Comet SQL Tests and Comet Scala Tests)
5. Gap analysis and test recommendations

---

## Step 1: Locate the Spark Implementations

Clone specific Spark version tags (use shallow clones to avoid polluting the workspace). Only clone a version if it is not already present.

```bash
set -eu -o pipefail
for tag in v3.4.3 v3.5.8 v4.0.1; do
  dir="/tmp/spark-${tag}"
  if [ ! -d "$dir" ]; then
    git clone --depth 1 --branch "$tag" https://github.com/apache/spark.git "$dir"
  fi
done
```

### Find the expression class in each Spark version

Search the Catalyst SQL expressions source:

```bash
for tag in v3.4.3 v3.5.8 v4.0.1; do
  dir="/tmp/spark-${tag}"
  echo "=== $tag ==="
  find "$dir/sql/catalyst/src/main/scala" -name "*.scala" | \
    xargs grep -l "case class $ARGUMENTS\b\|object $ARGUMENTS\b" 2>/dev/null
done
```

If the expression is not found in catalyst, also check core:

```bash
for tag in v3.4.3 v3.5.8 v4.0.1; do
  dir="/tmp/spark-${tag}"
  echo "=== $tag ==="
  find "$dir/sql" -name "*.scala" | \
    xargs grep -l "case class $ARGUMENTS\b\|object $ARGUMENTS\b" 2>/dev/null
done
```

### Read the Spark source for each version

For each Spark version, read the expression file and note:

- The `eval`, `nullSafeEval`, and `doGenCode` / `doGenCodeSafe` methods
- The `inputTypes` and `dataType` fields (accepted input types, return type)
- Null handling strategy (`nullable`, `nullSafeEval`)
- ANSI mode behavior (`ansiEnabled`, `failOnError`)
- Special cases, guards, `require` assertions, and runtime exceptions
- Any constants or configuration the expression reads

### Compare across Spark versions

Produce a concise diff summary of what changed between:

- 3.4.3 → 3.5.8
- 3.5.8 → 4.0.1

Pay attention to:

- New input types added or removed
- Behavior changes for edge cases (null, overflow, empty, boundary)
- New ANSI mode branches
- New parameters or configuration
- Breaking API changes that Comet must shim

---

## Step 2: Locate the Spark Tests

```bash
for tag in v3.4.3 v3.5.8 v4.0.1; do
  dir="/tmp/spark-${tag}"
  echo "=== $tag ==="
  find "$dir/sql" -name "*.scala" -path "*/test/*" | \
    xargs grep -l "$ARGUMENTS" 2>/dev/null
done
```

Read the relevant Spark test files and produce a list of:

- Input types covered
- Edge cases exercised (null, empty, overflow, negative, boundary values, special characters, etc.)
- ANSI mode tests
- Error cases

This list will be the reference for the coverage gap analysis in Step 5.

---

## Step 3: Locate the Comet Implementation

### Scala serde

```bash
# Find the serde object
grep -r "$ARGUMENTS" spark/src/main/scala/org/apache/comet/serde/ --include="*.scala" -l
grep -r "$ARGUMENTS" spark/src/main/scala/org/apache/comet/ --include="*.scala" -l
```

Read the serde implementation and check:

- Which Spark versions the serde handles
- Whether all input types Spark accepts are handled
- Whether `convert` validates expression shape (e.g. literal-only arguments) before serializing

Then audit the support-level and reason methods as described below.

#### Audit `getSupportLevel`, `getIncompatibleReasons`, `getUnsupportedReasons`, and `convert`

These four methods must stay aligned. Each has a distinct purpose, and the
most common bugs in Comet serdes are misalignments between them.

**Pick the right support level.**

- `Unsupported(Some(reason))`: Comet cannot run this case at all. The
  dispatcher in `QueryPlanSerde.exprToProtoInternal` falls back to Spark
  unconditionally. Use this when an input type, option, or expression shape
  is not implemented, or when running the native path would crash or error.
- `Incompatible(Some(reason))`: Comet can run this, but results may differ
  from Spark. The dispatcher only allows it when
  `spark.comet.expr.allowIncompatible=true` (or the per-expression
  equivalent). Use this for known result differences such as locale
  sensitivity, timezone handling, ordering ambiguity, or floating-point
  precision.
- `Compatible(None)`: full Spark compatibility for this combination of
  inputs and options.
- `Compatible(Some(note))`: fully compatible but with a docs-only caveat.
  Note that any `Some(...)` on `Compatible` triggers a runtime
  `logWarning`, so reserve it for genuinely useful caveats.

Decision rule: if the user would be surprised by a _wrong answer_, it is
`Incompatible`. If the user would be surprised by a _runtime error or
unsupported-type message_, it is `Unsupported`.

**Runtime vs docs split.**

- The `notes` field on `Incompatible(Some(...))` and `Unsupported(Some(...))`
  flows into EXPLAIN output via the dispatcher (see
  `QueryPlanSerde.exprToProtoInternal`, around the `case Incompatible` /
  `case Unsupported` branches). This is what users see when they ask why
  Comet fell back.
- `getIncompatibleReasons()` and `getUnsupportedReasons()` are read only by
  `GenerateDocs` when building
  `docs/source/user-guide/compatibility.md`. They are static (no `expr`
  argument) and should enumerate every distinct reason the expression
  could ever return at runtime.
- `convert` may also call `withInfo(expr, "reason")` and return `None` for
  cases that cannot be detected from the expression alone (for example,
  non-literal arguments, or child conversion failures). Those reasons
  belong in `getUnsupportedReasons()` too.

**Consistency rules.**

1. If `getSupportLevel` can return `Incompatible(...)` for any input,
   override `getIncompatibleReasons()` and include a reason for every
   incompatible branch.
2. If `getSupportLevel` can return `Unsupported(...)` for any input,
   override `getUnsupportedReasons()` and include a reason for every
   unsupported branch.
3. If `convert` has its own `withInfo(...); None` fallbacks (e.g. literal
   checks), enumerate those reasons in `getUnsupportedReasons()` too.
4. Extract each reason into a `private val` and reference it from both
   `getSupportLevel` and `get*Reasons()`. Do not duplicate the string
   inline. Canonical example:
   `spark/src/main/scala/org/apache/comet/serde/arrays.scala::CometArrayIntersect`.
   It declares `private val incompatReason` and
   `private val unsupportedCollationReason`, and each is referenced from
   `getSupportLevel` and the matching reasons method.
5. Prefer `Incompatible(Some(reason))` over `Incompatible(None)`. The
   `None` form drops the reason from EXPLAIN output, leaving users with a
   generic "not fully compatible" message and forcing them to read the
   docs to find out why.
6. Gate compatibility decisions in `getSupportLevel`, not inside `convert`.
   Putting the check inside `convert` (e.g. reading a config flag and
   calling `withInfo`) bypasses the dispatcher's `allowIncompatible`
   handling and the EXPLAIN message becomes inconsistent with what the
   doc generator produces.

**Wording guidelines for reason strings.**

Reasons appear verbatim in the Compatibility Guide (rendered as Markdown)
and in EXPLAIN output, so they are user-facing.

- Lead with the user-observable effect, then the cause if helpful.
  ✅ "Result array element order may differ from Spark when the right array
  is longer than the left."
  ❌ "DataFusion probes the longer side."
- Use sentence case and end with a period.
- Use backticks around config keys, type names, and SQL identifiers.
- Link to a tracking GitHub issue for known incompatibilities so users can
  follow progress: `(https://github.com/apache/datafusion-comet/issues/NNNN)`.
- Keep it concise. Single sentence is best.
- Do not write "Incompatible reason: ..." or "Unsupported because ...".
  The doc generator adds the framing.
- Phrase Incompatible reasons as _what differs from Spark_, not _what is
  missing_. Phrase Unsupported reasons as _what does not run_. If you find
  yourself writing an "Incompatible" reason that says "Comet only supports
  X" or "Y is not supported", the support level is probably wrong: it
  should be `Unsupported`.

**Common antipatterns to flag during the audit.**

- A `private val reason` constant declared near the top of the object, but
  `getSupportLevel` hardcodes a different string inline. The doc and the
  EXPLAIN message will drift.
  Real example: `CometHour` declares `incompatReason` then hardcodes a
  near-duplicate string in `getSupportLevel`.
- Reason text duplicated in two places without a shared constant.
  Real examples: `CometMinute`, `CometSecond`.
- `Incompatible(None)` paired with a populated `getIncompatibleReasons()`.
  The reason reaches the docs but not EXPLAIN output.
  Real example: `CometInitCap`.
- `getIncompatibleReasons()` overridden but `getSupportLevel` never returns
  `Incompatible(...)`. Either the reasons method is dead code, or
  `getSupportLevel` is missing a branch.
- A reason string phrased as "X is not supported" attached to an
  `Incompatible` branch (or vice versa). Re-read it and decide which
  support level it really belongs to.
- `convert` bails out via `withInfo` for a case that is fully knowable
  from the expression (e.g. an unsupported child data type). Move the
  check into `getSupportLevel` so the dispatcher handles it uniformly.

### Shims

```bash
find spark/src/main -name "CometExprShim.scala" | xargs grep -l "$ARGUMENTS" 2>/dev/null
```

If shims exist, read them and note any version-specific handling.

### Rust / DataFusion implementation

```bash
# Search for the function in native/spark-expr
grep -r "$ARGUMENTS" native/spark-expr/src/ --include="*.rs" -l
grep -r "$ARGUMENTS" native/core/src/ --include="*.rs" -l
```

If the expression delegates to DataFusion, find it there too. Set `$DATAFUSION_SRC` to a local DataFusion checkout, or fall back to searching the cargo registry:

```bash
if [ -n "${DATAFUSION_SRC:-}" ]; then
  grep -r "$ARGUMENTS" "$DATAFUSION_SRC" --include="*.rs" -l 2>/dev/null | head -10
else
  # Fall back to cargo registry (may include unrelated crates)
  grep -r "$ARGUMENTS" ~/.cargo/registry/src/*/datafusion* --include="*.rs" -l 2>/dev/null | head -10
fi
```

Read the Rust implementation and check:

- Null handling (does it propagate nulls correctly?)
- Overflow and underflow handling (returns `Err` vs panics)
- Type dispatch (does it handle all types that Spark supports?)
- ANSI / fail-on-error mode

---

## Step 4: Locate Existing Comet Tests

### Comet SQL Tests

```bash
# Find SQL test files for this expression
find spark/src/test/resources/sql-tests/expressions/ -name "*.sql" | \
  xargs grep -l "$ARGUMENTS" 2>/dev/null

# Also check if there's a dedicated file
find spark/src/test/resources/sql-tests/expressions/ -name "*$(echo $ARGUMENTS | tr '[:upper:]' '[:lower:]')*"
```

Read every SQL test file found and list:

- Table schemas and data values used
- Queries exercised
- Query modes used (`query`, `spark_answer_only`, `tolerance`, `ignore`, `expect_error`)
- Any ConfigMatrix directives

### Comet Scala Tests

```bash
grep -r "$ARGUMENTS" spark/src/test/scala/ --include="*.scala" -l
```

Read the relevant Comet Scala Tests and list:

- Input types covered
- Edge cases exercised
- Whether constant folding is disabled for literal tests

---

## Step 5: Gap Analysis

Compare the Spark test coverage (Step 2) against the Comet test coverage (Step 4). Produce a structured gap report:

### Coverage matrix

For each of the following dimensions, note whether it is covered in Comet tests or missing:

| Dimension                                                                                              | Spark tests it | Comet SQL Test | Comet Scala Test | Gap? |
| ------------------------------------------------------------------------------------------------------ | -------------- | -------------- | ---------------- | ---- |
| Column reference argument(s)                                                                           |                |                |                  |      |
| Literal argument(s)                                                                                    |                |                |                  |      |
| NULL input                                                                                             |                |                |                  |      |
| Empty string / empty array / empty map                                                                 |                |                |                  |      |
| Array/map with NULL elements                                                                           |                |                |                  |      |
| Zero, negative zero, negative values (numeric)                                                         |                |                |                  |      |
| Underflow, overflow                                                                                    |                |                |                  |      |
| Boundary values (INT_MIN, INT_MAX, Long.MinValue, minimum positive, etc.)                              |                |                |                  |      |
| NaN, Infinity, -Infinity, subnormal (float/double)                                                     |                |                |                  |      |
| Multibyte / special UTF-8 (composed vs decomposed, e.g. `é` U+00E9 vs `e` + U+0301, non-Latin scripts) |                |                |                  |      |
| ANSI mode (failOnError=true)                                                                           |                |                |                  |      |
| Non-ANSI mode (failOnError=false)                                                                      |                |                |                  |      |
| All supported input types                                                                              |                |                |                  |      |
| Parquet dictionary encoding (ConfigMatrix)                                                             |                |                |                  |      |
| Cross-version behavior differences                                                                     |                |                |                  |      |

### Implementation gaps

Also review the Comet implementation (Step 3) against the Spark behavior (Step 1):

- Are there input types that Spark supports but `getSupportLevel` returns `Unsupported` without comment?
- Are there behavioral differences that are NOT marked `Incompatible` but should be?
- Are there behavioral differences between Spark versions that the Comet implementation does not account for (missing shim)?
- Does the Rust implementation match the Spark behavior for all edge cases?

### Support-level consistency audit

Walk through this checklist against the serde. Each failed item is a
finding for Step 6.

1. **Support level matches behavior.** For each branch of
   `getSupportLevel`, decide whether the user-observable effect is a wrong
   answer (`Incompatible`) or a fallback / error (`Unsupported`). Flag any
   branch where the label does not match the behavior.
2. **Reasons cover every branch.** Every distinct reason that
   `getSupportLevel` can return as `Incompatible(Some(r))` must appear in
   `getIncompatibleReasons()`. Same for `Unsupported(Some(r))` and
   `getUnsupportedReasons()`. Missing reasons silently drop from the
   Compatibility Guide.
3. **Reasons are not dead code.** If `getIncompatibleReasons()` is
   overridden but `getSupportLevel` never returns `Incompatible(...)`,
   either the reason is stale or `getSupportLevel` is missing a branch.
   Same for `getUnsupportedReasons()`.
4. **Reason strings are shared via `private val`.** If the same reason
   appears as a string literal in two places, flag it: changes to one
   will not propagate to the other.
5. **Inline reason matches the constant.** If a `private val reason` is
   declared but `getSupportLevel` uses a different string literal, flag
   it as a drift bug.
6. **`Incompatible(None)` has no docs-only reason.** If
   `getSupportLevel` returns `Incompatible(None)` but
   `getIncompatibleReasons()` is non-empty, flag it: the EXPLAIN message
   will be generic while the docs show a specific reason. Switch to
   `Incompatible(Some(reason))`.
7. **`convert` fallbacks are documented.** If `convert` calls
   `withInfo(expr, "...")` and returns `None` for cases not covered by
   `getSupportLevel` (e.g. non-literal arguments, unsupported expression
   shapes), confirm the reason is also listed in
   `getUnsupportedReasons()`.
8. **Compatibility decisions live in `getSupportLevel`.** If `convert`
   reads a config flag and bails out, prefer moving the check into
   `getSupportLevel` so the dispatcher handles `allowIncompatible`
   uniformly. `CometCaseConversionBase` is an example of the in-`convert`
   pattern that this skill recommends against.
9. **Reason wording.** Each reason should describe the user-observable
   effect, use sentence case with a period, backtick config keys and
   types, and link a tracking issue when one exists. Flag reasons that
   read like internal implementation notes ("DataFusion probes the longer
   side") or that mismatch their support level (an "Incompatible" reason
   that says "X is not supported").

---

## Step 6: Recommendations

Summarize findings as a prioritized list.

### High priority: correctness divergences and high-risk coverage gaps

Cases where Comet produces a different observable result from Spark
(wrong value, missing exception, accepted-instead-of-rejected input,
etc.), **and** untested cases on a known-fragile axis where a
divergence is plausible: overflow / out-of-range inputs, NULL handling,
timezones and DST transitions, ANSI mode, leap years, version-specific
Spark semantics, and any edge case explicitly called out in Step 1.

Treat an untested high-risk case as a likely divergence until proven
otherwise. Each item in this bucket becomes a captured test in Step 7.
For an untested case, run it manually first to determine the current
behaviour, then commit either a regression test (passes) or a
`query ignore(<issue-url>)` test (fails).

### Medium priority: missing test coverage

Low-risk coverage gaps: additional input permutations on already-tested
code paths, redundant happy-path values, or cases where Comet and Spark
share a well-exercised implementation. The behaviour appears to match
and the axis is not on the high-risk list above.

### Low priority: cosmetic and consistency issues

Reason-string drift, missing `get*Reasons()` overrides, dead branches,
etc. These come from the Step 5 consistency audit.

---

## Step 7: Apply Findings (Tests and Fixes), Then Offer the Rest

High-priority findings (correctness divergences and high-risk coverage
gaps) and consistency issues from Step 5 / Step 6 must not be left as
prose. Apply them in the same PR as the audit. Only low-risk missing
coverage requires the user's go-ahead, because adding tests for cases
that already work on well-exercised paths is incremental polish.

### High-priority findings: capture as tests

Every high-priority finding becomes a regression test in the same PR as
the audit, so future readers can run it. This covers both confirmed
divergences and high-risk untested cases. For the latter, run the case
manually first to determine whether it currently passes or fails, then
follow the same workflow below: a passing case is committed as a plain
`query` regression test; a failing case is committed as
`query ignore(<issue-url>)` after filing the bug.

For each high-priority finding, do the following in this order:

1. **Search for an existing tracking issue.**

   ```bash
   gh issue list --search "<expression> <symptom> in:title,body" --state all --limit 5
   ```

   Match on both the expression name and a distinguishing keyword
   (ANSI, timezone, NTZ, overflow, etc.).

2. **If no issue exists, file one.** Use the `correctness` label plus
   the relevant area label (e.g. `temporal expressions`). Keep the
   title in the form "[Bug] <expression> <one-line symptom>". Include
   the Spark version, a minimal repro, and the divergent result.

3. **If the fix is trivial and you are confident, fix it inline.**
   Then add the test in the default `query` mode so it locks in the
   fix. "Trivial" means a few lines in one file with no native code
   changes, no support-level reshuffling, and no semantics decisions
   that the user should weigh in on.

4. **Otherwise, add the test in `query ignore(<issue-url>)` mode** with
   a one-line SQL comment above the query explaining the symptom. The
   test file lives next to the existing tests for the expression. Do
   not skip writing the test because the bug is unfixed: the captured
   reproduction is the whole point of this step.

   ```sql
   -- Comet returns NULL where Spark throws under spark.sql.ansi.enabled=true
   query ignore(https://github.com/apache/datafusion-comet/issues/NNNN)
   SELECT next_day(date('2024-01-01'), 'NOT_A_DAY')
   ```

   When the underlying bug is fixed, the `ignore(...)` is removed and
   the test starts running. This is also the contract documented in
   `docs/source/contributor-guide/sql-file-tests.md`.

### Support-level consistency: apply fixes automatically

Apply every finding from the Step 5 consistency audit in the same PR.
These are mechanical edits with no behavioural impact, so they do not
need user approval. The classes of fix are:

- Extract a duplicated or drifting reason string into a `private val`
  and reference it from both `getSupportLevel` and `get*Reasons()`.
- Switch `Incompatible(None)` to `Incompatible(Some(reason))` so the
  reason reaches EXPLAIN output, not only the docs.
- Add a missing `get*Reasons()` override that enumerates every
  reason the support level can return.
- Move a compatibility check out of `convert` and into
  `getSupportLevel` so the dispatcher handles `allowIncompatible`
  uniformly (the `withInfo` call in `convert` should disappear).
- Hoist a reason shared by multiple serdes (e.g. a recurring
  TimestampNTZ caveat) into a small `private object` companion in the
  same file, mirroring `UTCTimestampSerde`.

Each fix is one of these patterns. If a finding requires a semantics
decision (e.g. is a specific branch really `Unsupported`, or is it
`Incompatible`?), do not guess: leave it as a prose recommendation in
the PR description and call it out for the reviewer.

After every fix, build the affected module to make sure the edit
compiles. Do not run the full suite; targeted tests suffice if the
fix could plausibly affect behaviour.

### Low-risk missing test coverage: ask the user

This is the only Step 7 category that pauses for user input. It only
covers the Medium-priority bucket from Step 6: low-risk gaps on
well-exercised code paths. High-risk untested cases belong above, in
"High-priority findings: capture as tests". Adding tests for cases
that already work on well-exercised paths is incremental polish, not
part of the audit's required output.

> Spark exercises the following cases that have no Comet test. Would
> you like me to add them?
>
> - [list each missing test case]
>
> I can add them as Comet SQL Tests in `spark/src/test/resources/sql-tests/expressions/<category>/$ARGUMENTS.sql`
> (or as Comet Scala Tests in `CometExpressionSuite` for cases that require programmatic setup).

If the user says yes, implement the tests following the format described
in `docs/source/contributor-guide/sql-file-tests.md`. Prefer Comet SQL
Tests over Comet Scala Tests.

### Comet SQL Tests template

```sql
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_$ARGUMENTS(...) USING parquet

statement
INSERT INTO test_$ARGUMENTS VALUES
  (...),
  (NULL)

-- column argument
query
SELECT $ARGUMENTS(col) FROM test_$ARGUMENTS

-- literal arguments
query
SELECT $ARGUMENTS('value'), $ARGUMENTS(''), $ARGUMENTS(NULL)
```

### Verify the tests pass

After implementing tests, tell the user how to run them:

```bash
./mvnw test -DwildcardSuites="CometSqlFileTestSuite" -Dsuites="org.apache.comet.CometSqlFileTestSuite $ARGUMENTS" -Dtest=none
```

---

## Step 8: Record the Audit

After completing the audit (whether or not tests were added), record the findings on the
expression's category page under `docs/source/contributor-guide/expression-audits/`.
The page is named after the Spark function-registry category (e.g. `agg_funcs.md`,
`string_funcs.md`, `datetime_funcs.md`). If you are unsure of the category, match the one
used for the expression in `docs/source/user-guide/latest/expressions.md`.

- If the category page does not exist yet, create it with the ASF license header, a
  `# <category> Expression Audits` title, and the standard intro blockquote used by the
  other pages.
- Add (or update) a `## <function_name>` section, keeping sections alphabetically ordered.
- Under that heading, add one bullet per Spark version checked, each including:
  - Spark version (e.g. 3.4.3, 3.5.8, 4.0.1)
  - Today's date
  - A brief note for any version-specific finding (behavioral difference, known
    incompatibility); omit the note if nothing notable.

---

## Output Format

Present the audit as:

1. **Expression Summary** - Brief description of what `$ARGUMENTS` does, its input/output types, and null behavior
2. **Spark Version Differences** - Summary of any behavioral or API differences across Spark 3.4.3, 3.5.8, and 4.0.1
3. **Comet Implementation Notes** - Summary of how Comet implements this expression and any concerns
4. **Coverage Gap Analysis** - The gap table from Step 5, plus implementation gaps
5. **Recommendations** - Prioritized list from Step 6
6. **Offer to add tests** - The prompt from Step 7

## Tone and Style

- Write in clear, concise prose
- Use backticks around code references (function names, file paths, class names, types, config keys)
- Avoid robotic or formulaic language
- Be constructive and acknowledge what is already well-covered before raising gaps
- Avoid em dashes and semicolons; use separate sentences instead
