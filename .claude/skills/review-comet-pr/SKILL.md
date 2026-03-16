---
name: review-comet-pr
description: Review a DataFusion Comet pull request for Spark compatibility and implementation correctness. Provides guidance to a reviewer rather than posting comments directly.
argument-hint: <pr-number>
---

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

#### Tests - Prefer SQL File-Based Framework

**Expression tests should use the SQL file-based framework (`CometSqlFileTestSuite`) where possible.** This framework automatically runs each query through both Spark and Comet and compares results. No Scala code is needed. Only fall back to Scala tests in `CometExpressionSuite` when the SQL framework cannot express the test. Examples include complex `DataFrame` setup, programmatic data generation, or non-expression tests.

**SQL file test location:** `spark/src/test/resources/sql-tests/expressions/<category>/`

Categories include: `aggregate/`, `array/`, `string/`, `math/`, `struct/`, `map/`, `datetime/`, `hash/`, etc.

**SQL file structure:**

```sql
-- ConfigMatrix: parquet.enable.dictionary=false,true

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

**Running SQL file tests:**

```bash
# All SQL file tests
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
- [ ] If using Scala tests instead, literal tests MUST disable constant folding:
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

Check whether the PR requires updates to user-facing documentation in `docs/`:

- **Compatibility guide** (`docs/source/user-guide/compatibility.md`): New expressions or operators should be listed. Incompatible behaviors should be documented.
- **Configuration guide** (`docs/source/user-guide/configs.md`): New config options should be documented.
- **Expressions list** (`docs/source/user-guide/expressions.md`): New expressions should be added.

If the PR adds a new expression or operator but does not update the relevant docs, flag this as something that needs to be addressed.

### 8. Common Comet Review Issues

1. **Incomplete type support**: Spark expression supports types not handled in PR
2. **Missing edge cases**: Null, overflow, empty string, negative values
3. **Wrong return type**: Return type must match Spark exactly
4. **Tests in wrong framework**: Expression tests should use the SQL file-based framework (`CometSqlFileTestSuite`) rather than adding to Scala test suites like `CometExpressionSuite`. Suggest migration if the PR adds Scala tests for expressions that could use SQL files instead.
5. **Stale native code**: PR might need `./mvnw install -pl common -DskipTests`
6. **Missing `getSupportLevel`**: Edge cases should be marked as `Incompatible`

---

## Output Format

Present your review as guidance for the reviewer. Structure your output as:

1. **PR Summary** - Brief description of what the PR does
2. **CI Status** - Summary of CI check results
3. **Findings** - Your analysis organized by area (Spark compatibility, implementation, tests, etc.)
4. **Suggested Review Comments** - Specific comments the reviewer could leave on the PR, with file and line references where applicable

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

## No AI Disclosure

Do not add any AI disclosure or "generated with AI" footer to review text.
