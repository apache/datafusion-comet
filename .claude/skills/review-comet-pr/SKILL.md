---
name: review-comet-pr
description: Review a DataFusion Comet pull request for Spark compatibility and implementation correctness. Provides guidance to a reviewer rather than posting comments directly.
argument-hint: <pr-number>
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

Review Comet PR #$ARGUMENTS

## Before You Start

### Gather PR Metadata

Fetch the PR details to understand the scope:

```bash
gh pr view $ARGUMENTS --repo apache/datafusion-comet --json title,body,author,isDraft,state,files
```

### Review Existing Comments First

Before forming your review:

1. **Read all existing review comments** on the PR
2. **Check the conversation tab** for any discussion
3. **Avoid duplicating feedback** that others have already provided
4. **Build on existing discussions** rather than starting new threads on the same topic
5. **If you have no additional concerns beyond what's already discussed, say so**
6. **Ignore Copilot reviews** - do not reference or build upon comments from GitHub Copilot

```bash
# View existing comments on a PR
gh pr view $ARGUMENTS --repo apache/datafusion-comet --comments
```

---

## Review Workflow

### 1. Gather Context

Read the changed files and understand the area of the codebase being modified:

```bash
# View the diff
gh pr diff $ARGUMENTS --repo apache/datafusion-comet
```

For expression PRs, check how similar expressions are implemented in the codebase. Look at the serde files in `spark/src/main/scala/org/apache/comet/serde/` and Rust implementations in `native/spark-expr/src/`.

### 2. Read Spark Source (Expression PRs)

**For any PR that adds or modifies an expression, you must read the Spark source code to understand the canonical behavior.** This is the authoritative reference for what Comet must match.

1. **Clone or update the Spark repo:**

   ```bash
   # Clone if not already present (use /tmp to avoid polluting the workspace)
   if [ ! -d /tmp/spark ]; then
     git clone --depth 1 https://github.com/apache/spark.git /tmp/spark
   fi
   ```

2. **Find the expression implementation in Spark:**

   ```bash
   # Search for the expression class (e.g., for "Conv", "Hex", "Substring")
   find /tmp/spark/sql/catalyst/src/main/scala -name "*.scala" | xargs grep -l "case class <ExpressionName>"
   ```

3. **Read the Spark implementation carefully.** Pay attention to:
   - The `eval` and `doGenEval`/`nullSafeEval` methods. These define the exact behavior.
   - The `inputTypes` and `dataType` fields. These define which types Spark accepts and what it returns.
   - Null handling. Does it use `nullable = true`? Does `nullSafeEval` handle nulls implicitly?
   - Special cases, guards, and `require` assertions.
   - ANSI mode branches (look for `SQLConf.get.ansiEnabled` or `failOnError`).

4. **Read the Spark tests for the expression:**

   ```bash
   # Find test files
   find /tmp/spark/sql -name "*.scala" -path "*/test/*" | xargs grep -l "<ExpressionName>"
   ```

5. **Compare the Spark behavior against the Comet implementation in the PR.** Identify:
   - Edge cases tested in Spark but not in the PR
   - Data types supported in Spark but not handled in the PR
   - Behavioral differences that should be marked `Incompatible`

6. **Suggest additional tests** for any edge cases or type combinations covered in Spark's tests that are missing from the PR's tests.

### 3. Spark Compatibility Check

**This is the most critical aspect of Comet reviews.** Comet must produce identical results to Spark.

For expression PRs, verify against the Spark source you read in step 2:

1. **Check edge cases**
   - Null handling
   - Overflow behavior
   - Empty input behavior
   - Type-specific behavior

2. **Verify all data types are handled**
   - Does Spark support this type? (Check `inputTypes` in Spark source)
   - Does the PR handle all Spark-supported types?

3. **Check for ANSI mode differences**
   - Spark behavior may differ between legacy and ANSI modes
   - PR should handle both or mark as `Incompatible`

### 4. Check Against Implementation Guidelines

**Always verify PRs follow the implementation guidelines.**

#### Scala Serde (`spark/src/main/scala/org/apache/comet/serde/`)

- [ ] Expression class correctly identified
- [ ] All child expressions converted via `exprToProtoInternal`
- [ ] Return type correctly serialized
- [ ] `getSupportLevel` reflects true compatibility:
  - `Compatible()` - matches Spark exactly
  - `Incompatible(Some("reason"))` - differs in documented ways
  - `Unsupported(Some("reason"))` - cannot be implemented
- [ ] Serde in appropriate file (`datetime.scala`, `strings.scala`, `arithmetic.scala`, etc.)

#### Registration (`QueryPlanSerde.scala`)

- [ ] Added to correct map (temporal, string, arithmetic, etc.)
- [ ] No duplicate registrations
- [ ] Import statement added

#### Rust Implementation (if applicable)

Location: `native/spark-expr/src/`

- [ ] Matches DataFusion and Arrow conventions
- [ ] Null handling is correct
- [ ] No panics. Use `Result` types.
- [ ] Efficient array operations (avoid row-by-row)

#### Tests - Prefer Comet SQL Tests

**Expression tests should use Comet SQL Tests (`CometSqlFileTestSuite`) where possible.** This framework automatically runs each query through both Spark and Comet and compares results. No Scala code is needed. Only fall back to Comet Scala Tests in `CometExpressionSuite` when Comet SQL Tests cannot express the test. Examples include complex `DataFrame` setup, programmatic data generation, or non-expression tests.

**Comet SQL Test location:** `spark/src/test/resources/sql-tests/expressions/<category>/`

Categories include: `aggregate/`, `array/`, `string/`, `math/`, `struct/`, `map/`, `datetime/`, `hash/`, etc.

**Comet SQL Test structure:**

```sql
-- Create test data
statement
CREATE TABLE test_crc32(col string, a int, b float) USING parquet

statement
INSERT INTO test_crc32 VALUES ('Spark', 10, 1.5), (NULL, NULL, NULL), ('', 0, 0.0)

-- Default mode: verifies native Comet execution + result matches Spark
query
SELECT crc32(col) FROM test_crc32

-- spark_answer_only: compares results without requiring native execution
query spark_answer_only
SELECT crc32(cast(a as string)) FROM test_crc32

-- tolerance: allows numeric variance for floating-point results
query tolerance=0.0001
SELECT cos(v) FROM test_trig

-- expect_fallback: asserts fallback to Spark occurs
query expect_fallback(unsupported expression)
SELECT unsupported_func(v) FROM test_table

-- expect_error: verifies both engines throw matching exceptions
query expect_error(ARITHMETIC_OVERFLOW)
SELECT 2147483647 + 1

-- ignore: skip queries with known bugs (include GitHub issue link)
query ignore(https://github.com/apache/datafusion-comet/issues/NNNN)
SELECT known_buggy_expr(v) FROM test_table
```

**Running Comet SQL Tests:**

```bash
# All Comet SQL Tests
./mvnw test -Dsuites="org.apache.comet.CometSqlFileTestSuite" -Dtest=none

# Specific test file (substring match)
./mvnw test -Dsuites="org.apache.comet.CometSqlFileTestSuite crc32" -Dtest=none
```

**CRITICAL: Verify all test requirements (regardless of framework):**

- [ ] Basic functionality tested (column data, not just literals)
- [ ] Null handling tested (`SELECT expression(NULL)`)
- [ ] Edge cases tested (empty input, overflow, boundary values)
- [ ] Both literal values and column references tested (they use different code paths)
- [ ] For timestamp/datetime expressions, timezone handling is tested (e.g., UTC, non-UTC session timezone, timestamps with and without timezone)
- [ ] One expression per SQL file for easier debugging
- [ ] If using Comet Scala Tests instead, literal tests MUST disable constant folding:
  ```scala
  withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
      "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
    checkSparkAnswerAndOperator("SELECT func(literal)")
  }
  ```

### 5. Performance Review (Expression PRs)

**For PRs that add new expressions, performance is not optional.** The whole point of Comet is to be faster than Spark. If a new expression is not faster, it may not be worth adding.

1. **Check that the PR includes microbenchmark results.** The PR description should contain benchmark numbers comparing Comet vs Spark for the new expression. If benchmark results are missing, flag this as a required addition.

2. **Look for a microbenchmark implementation.** Expression benchmarks live in `spark/src/test/scala/org/apache/spark/sql/benchmark/`. Check whether the PR adds a benchmark for the new expression.

3. **Review the benchmark results if provided:**
   - Is Comet actually faster than Spark for this expression?
   - Are the benchmarks representative? They should test with realistic data sizes, not just trivial inputs.
   - Are different data types benchmarked if the expression supports multiple types?

4. **Review the Rust implementation for performance concerns:**
   - Unnecessary allocations or copies
   - Row-by-row processing where batch/array operations are possible
   - Redundant type conversions
   - Inefficient string handling (e.g., repeated UTF-8 validation)
   - Missing use of Arrow compute kernels where they exist

5. **If benchmark results show Comet is slower than Spark**, flag this clearly. The PR should explain why the regression is acceptable or include a plan to optimize.

### 6. Check CI Test Failures

**Always check the CI status and summarize any test failures in your review.**

```bash
# View CI check status
gh pr checks $ARGUMENTS --repo apache/datafusion-comet

# View failed check details
gh pr checks $ARGUMENTS --repo apache/datafusion-comet --failed
```

### 7. Documentation Check

Some user-facing docs are auto-generated from the serde. Others are hand-edited. Treat them differently.

**Generated by `GenerateDocs`** — do NOT ask the contributor to edit these by hand. CI regenerates and publishes them on every merge to `main`:

- Compatibility guide pages under `docs/source/user-guide/latest/compatibility/expressions/` (`math.md`, `datetime.md`, `array.md`, `string.md`, `aggregate.md`, `struct.md`, `map.md`, `misc.md`, `cast.md`)
- Configuration reference at `docs/source/user-guide/latest/configs.md`

For these, check the _source_ instead. Does the new or modified `CometExpressionSerde` provide accurate `getIncompatibleReasons()` and `getUnsupportedReasons()` strings? Each returned string is rendered as a bullet on the corresponding compat page. Common gaps to flag:

- Expression marked `Incompatible(Some("..."))` in `getSupportLevel` but `getIncompatibleReasons()` is empty, so the compat page shows it as supported with no caveats.
- `Unsupported(Some("..."))` for specific data types or argument shapes but no `getUnsupportedReasons()` to surface the limitation to users.
- Reason strings drifting from the `notes` argument passed to `Compatible` / `Incompatible` / `Unsupported`. They do not have to match exactly, but consistency helps users.
- Reason strings that are too terse to be useful in user-facing docs (a single word, no context, no link to a tracking issue when behavior is known to differ).

See `docs/source/contributor-guide/adding_a_new_expression.md` (sections "Documenting Incompatible and Unsupported Reasons") for the contract these methods follow.

**Hand-edited** — PR should update if relevant:

- `docs/source/user-guide/latest/expressions.md` — the supported-expressions list. New expressions belong here.
- Other `latest/compatibility/` pages such as `floating-point.md`, `operators.md`, `regex.md`, `scans.md`.
- Top-level user-guide pages such as `iceberg.md`, `installation.md`, `tuning.md` when the PR changes user-visible behavior.

If the PR adds a new expression but does not update `expressions.md`, flag that. If it touches incompatibility behavior, flag that the serde reasons should reflect the change.

### 8. Recurring Review Checks

These checks encode the most frequent findings senior Comet reviewers
make on real PRs. They are split into checks that apply to almost any
non-trivial diff and checks that only apply when the PR touches native
performance code (Rust perf, cast, arithmetic, overflow, buffer
sizing, criterion benchmarks). Skip a section that does not match the
diff. A checklist item that does not match the diff is not a finding.
Do not manufacture one to fill the list.

#### 8a. General checks (any non-trivial PR)

1. **Diff-branch test coverage.** For every added branch (new `if`,
   new `match` arm, new eval-mode arm), locate the test that would
   fail if _that specific branch_ regressed. If the pre-existing
   tests still pass without touching the new branch, that branch is
   untested. This is the single most common missing item. ANSI, Try,
   and Legacy arms are independent code paths — a Legacy test does
   not cover ANSI.

2. **Error assertion depth.** For ANSI / try-cast / overflow paths, do
   not accept `assert!(result.is_err())`. Require matching on the
   exact `SparkError` variant and asserting every field (`value`,
   `precision`, `scale`, `from_type`, `to_type`). Also check
   sign-symmetry: if the overflow-error path uses a substring like
   `"too large"` in `find(...)`, negative overflow may fall through to
   an `.unwrap_or(0)` and report the wrong value.

3. **Comment / code alignment.** Read every added or modified comment
   and confirm it names the correct method (`.any(...)` vs
   `.contains(...)`), states the correct Spark behavior (e.g. does
   Spark actually return NULL for `2020-02-30` or throw?), references
   config keys that still exist after any rename in the same PR, and
   does not overclaim invariants ("output identical to before" when
   single-partition output shape actually changed). A comment that
   drifts from the code is a review finding.

4. **Sibling call sites.** For a fix applied to one helper, grep for
   structurally identical siblings before approving. Common families:
   `date_trunc` / `timestamp_trunc` / `timestamp_trunc_ntz`, scalar
   and array variants of the same helper, `to_uppercase` /
   `eq_ignore_ascii_case` in temporal code. Do this only when the fix
   is a pattern likely to recur, not on every one-line change. If
   siblings exist, fold them in or require a follow-up issue linked
   from the PR body. Do not accept "we can do the others later"
   without an issue number.

5. **Load-bearing invariants have a `debug_assert!` or a comment.**
   When correctness depends on an unchecked cast (`x as i32`), a
   specific IPC/format version (`MetadataVersion::V5`), a
   finalization step earlier in the loop (`append_value("")` on the
   null branch), or a caller-side precondition, either
   `debug_assert!` the invariant at the enforcement point or leave a
   one-line comment naming it. A future edit that silently violates
   the invariant should not slip past review.

#### 8b. Native performance PR checks

Only apply this subsection when the PR adds a fast path, tunes a
buffer size, adds or edits a criterion benchmark, or claims a
speed-up. On non-perf PRs (serde changes, planner rules, docs,
Scala-only changes), skip it. A finding here should tie to something
the diff actually introduced.

1. **Fast path AND fallback each hit.** When a PR adds an i64 fast
   path with an i128 fallback (or ASCII fast path with Unicode
   fallback, dictionary fast path with general fallback), confirm at
   least one test input lands on each arm. A test suite whose inputs
   all fit in the fast path would still pass if the fallback returned
   wrong answers.

2. **Upstream API first.** Before accepting a hand-rolled helper
   (dictionary detection, integer-to-string formatting, ASCII case
   folding, schema flattening), grep arrow-rs and std for an existing
   equivalent. Recent hits: `Schema::flattened_fields()`, `write!` on
   a `String` for integer formatting, arrow-rs
   `AVERAGE_STRING_LENGTH`. Also flag Rust monomorphization
   asymmetries: paired helpers should both take `impl Fn`, not one
   `&dyn Fn` and one `impl Fn`, and callers should not pass `&f` to
   an `impl Fn` parameter.

3. **Capacity hints and worst-case reasoning.** Any
   `with_capacity(rows * K)` or arithmetic like `4 * n.div_ceil(3) + n`
   needs three checks: (a) `K` matches arrow-rs's own defaults where
   they exist (e.g. `AVERAGE_STRING_LENGTH = 16` for string buffers),
   (b) the value is the true worst case for the encoding used, and
   (c) any comment justifying the formula matches the actual
   arithmetic (`+ num_rows` is not needed on top of `div_ceil`).
   Over-provisioning by 3x on the common path is a real cost.

4. **Benchmark integrity.** For any new Criterion benchmark, confirm:
   (a) the `group/function` name is unique across `benches/` — two
   benches with the same name silently clobber each other's
   `target/criterion/...` baseline directory; (b) RNGs are seeded
   (`StdRng::seed_from_u64(...)`), never `rand::random`, so numbers
   are reproducible; (c) every benchmark mentioned in the PR
   description actually exists as committed code — hallucinated bench
   names in perf PR descriptions are a recurring pattern; (d) the
   default (non-fast-path) case is measured before and after so the
   claim is evidence-based, not just the case the fast path targets.

5. **Panic-safety of new helpers.** Table lookups with arithmetic
   fallbacks (e.g. `POW10_I128.get(exp).copied().unwrap_or_else(||
   10_i128.pow(exp))`) must be total across all inputs the caller can
   actually reach. Trace every call site and confirm the argument is
   bounded before the call. `10_i128.pow(40)` panics in debug and
   wraps in release. `n.checked_mul(k).unwrap()` at the top of a hot
   loop is not exception-safe.

### 9. Common Comet Review Issues

1. **Incomplete type support**: Spark expression supports types not handled in PR
2. **Missing edge cases**: Null, overflow, empty string, negative values
3. **Wrong return type**: Return type must match Spark exactly
4. **Tests in wrong framework**: Expression tests should use Comet SQL Tests (`CometSqlFileTestSuite`) rather than adding to Comet Scala Tests like `CometExpressionSuite`. Suggest migration if the PR adds Comet Scala Tests for expressions that could use Comet SQL Tests instead.
5. **Stale native code**: PR might need `./mvnw install -pl common -DskipTests`
6. **Missing `getSupportLevel`**: Edge cases should be marked as `Incompatible`
7. **Scalar function name collides with a DataFusion built-in**: If the PR registers a Spark function whose name is also defined by `datafusion-functions` (e.g. `levenshtein`, `concat`, `coalesce`, `sha2`, `regexp_replace`), check that the serde sets the return type explicitly via `scalarFunctionExprToProtoWithReturnType` rather than `scalarFunctionExprToProto` or the bare `CometScalarFunction(name)` shortcut. Without an explicit return type, the native planner consults DataFusion's UDF registry first for type resolution, and any arity or input-type difference between the Spark and DataFusion versions will fail native execution with `Error from DataFusion: Function 'X' expects N arguments but received M`. The Comet UDF is only swapped in _after_ DF's signature validation passes. See the "When to set the return type explicitly" section in `docs/source/contributor-guide/adding_a_new_expression.md`.

---

## Review Bar

Hold a high bar. Several rounds of review and revision before merge are normal and expected. Prefer iterating on the PR over merging it and trusting a follow-up, because follow-ups that are not filed as issues do not get done. Treat "we can fix that later" as "that will not get fixed."

Every finding must be actionable. Before writing a comment, decide whether it is worth addressing before merge. If it is, raise it and expect a response. If it is not, cut it. There is no third tier.

- Never label feedback as "not a blocker", "nit", "minor", "optional", "low priority", or "feel free to ignore". Either raise it as something to address or drop it. That label tells the author to skip it and leaves nobody accountable for it.
- If a finding is real but genuinely belongs in separate work, say that plainly and ask for a tracking issue, then reference the issue link in the review. Do not leave it as a floating remark.
- Bikeshedding is worse than silence. A preference with no correctness, performance, compatibility, or maintainability argument behind it does not go in the review.
- This bar is about what gets raised, not about how it is worded. Keep the tone below. A question you expect an answer to still counts as something the author needs to address.

---

## Output Format

Present your review as guidance for the reviewer. Structure your output as:

1. **PR Summary** - Brief description of what the PR does
2. **CI Status** - Summary of CI check results
3. **Findings** - Your analysis organized by area (Spark compatibility, implementation, tests, etc.)
4. **Suggested Review Comments** - Specific comments the reviewer could leave on the PR, with file and line references where applicable. Everything here is something you expect the author to address. Anything that did not clear that bar should not appear.

## Review Tone and Style

Write reviews that sound human and conversational. Avoid:

- Robotic or formulaic language
- Em dashes. Use separate sentences instead.
- Semicolons. Use separate sentences instead.

Instead:

- Write in flowing paragraphs using simple grammar
- Keep sentences short and separate rather than joining them with punctuation
- Be kind and constructive, even when raising concerns
- Use backticks around any code references (function names, file paths, class names, types, config keys, etc.)
- **Suggest** adding tests rather than stating tests are missing (e.g., "It might be worth adding a test for X" not "Tests are missing for X")
- **Ask questions** about edge cases rather than asserting they aren't handled (e.g., "Does this handle the case where X is null?" not "This doesn't handle null")
- Frame concerns as questions or suggestions when possible
- Acknowledge what the PR does well before raising concerns

## Do Not Post Comments

**IMPORTANT: Never post comments or reviews on the PR directly.** This skill is for providing guidance to a human reviewer. Present all findings and suggested comments to the user. The user will decide what to post.
