---
name: review-comet-expression-pr
description: Use when reviewing a DataFusion Comet pull request that adds or changes a Spark expression, its Scala serde, its protobuf message, or its native Rust implementation. Load alongside review-comet-pr, which covers the parts of the review that apply to every PR.
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

Expression-specific review for Comet PR #$ARGUMENTS.

**REQUIRED BACKGROUND:** Use `review-comet-pr` for PR metadata, existing comments, CI, the review
bar, and the output format. This skill only covers what is specific to expressions.

## Read the Contributor Guide First

| Doc                                                        | What you need from it                                                                     |
| ---------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `docs/source/contributor-guide/adding_a_new_expression.md` | The serde contract, support levels, when to set the return type explicitly, shimming      |
| `docs/source/contributor-guide/sql-file-tests.md`          | The test framework expression PRs are expected to use, and every directive it supports    |
| `docs/source/contributor-guide/optimizing_expressions.md`  | The benchmark workflow and the no-regression rule, for PRs that change an existing kernel |

Hold the diff against these. If the PR does something a guide says to do differently, either the PR
is wrong or the guide is out of date, and you need to say which.

## 1. Read the Spark Source

**For any PR that adds or modifies an expression, you must read the Spark source to understand the
canonical behavior.** This is the authoritative reference for what Comet must match. Do not review
an expression PR from the diff alone.

Clone Spark into `/tmp/spark` if it is not already there, using a shallow clone, then find the
expression class under `sql/catalyst/src/main/scala` by searching for its `case class` declaration.

Read the Spark implementation carefully. Pay attention to:

- `eval` and `doGenCode` / `nullSafeEval`. These define the exact behavior.
- `inputTypes` and `dataType`. These define which types Spark accepts and what it returns.
- Null handling. Does it use `nullable = true`? Does `nullSafeEval` handle nulls implicitly?
- Special cases, guards, and `require` assertions.
- ANSI mode branches, usually `SQLConf.get.ansiEnabled` or `failOnError`.
- Behavior differences across the Spark versions Comet supports. If the expression changed between
  3.5 and 4.0, the serde needs a shim, not a single implementation.

Then read the Spark tests for the expression, under the test source trees in `sql/`.

Compare Spark's behavior against the PR and identify edge cases Spark tests that the PR does not,
types Spark supports that the PR does not handle, and behavioral differences that should be marked
`Incompatible`.

## 2. Scala Serde

Location: `spark/src/main/scala/org/apache/comet/serde/`

- [ ] Expression class correctly identified
- [ ] All child expressions converted via `exprToProtoInternal`
- [ ] Return type correctly serialized
- [ ] `getSupportLevel` reflects true compatibility rather than the happy path
- [ ] Serde lives in the appropriate file (`datetime.scala`, `strings.scala`, `arithmetic.scala`, and so on)
- [ ] ANSI and `fail_on_error` handling matches the constraints in `adding_a_new_expression.md`

### Registration in `QueryPlanSerde.scala`

- [ ] Added to the correct map (temporal, string, arithmetic, and so on)
- [ ] No duplicate registrations
- [ ] Import statement added

### Return type collisions with DataFusion built-ins

If the PR registers a Spark function whose name is also defined by `datafusion-functions`, such as
`levenshtein`, `concat`, `coalesce`, `sha2`, or `regexp_replace`, check that the serde sets the
return type explicitly via `scalarFunctionExprToProtoWithReturnType` rather than
`scalarFunctionExprToProto` or the bare `CometScalarFunction(name)` shortcut.

Without an explicit return type the native planner consults DataFusion's UDF registry first for
type resolution, and any arity or input-type difference between the Spark and DataFusion versions
fails native execution with `Error from DataFusion: Function 'X' expects N arguments but received
M`. The Comet UDF is only swapped in after DataFusion's signature validation passes. See "When to
set the return type explicitly" in `adding_a_new_expression.md`.

## 3. Rust Implementation

Location: `native/spark-expr/src/`, registered in `comet_scalar_funcs.rs`.

- [ ] Matches DataFusion and Arrow conventions
- [ ] Null handling is correct, including nulls inside nested types
- [ ] No panics. Use `Result`.
- [ ] Batch operations rather than row-by-row where a kernel exists
- [ ] Invalid UTF-8 going into a native `StringType` goes through `decode_utf8_spark_lossy`

Before accepting a hand-written kernel, ask whether the function already exists upstream in
DataFusion or the `datafusion-spark` crate. Comet prefers wiring an upstream function over carrying
its own copy.

## 4. Tests

**Expression tests should use Comet SQL Tests (`CometSqlFileTestSuite`) where possible.** The
framework runs each query through both Spark and Comet and compares results, with no Scala code.
Fall back to Comet Scala Tests in `CometExpressionSuite` only when the SQL framework cannot express
the test, for example complex `DataFrame` setup or programmatic data generation.

Test location: `spark/src/test/resources/sql-tests/expressions/<category>/`, with categories
`aggregate/`, `array/`, `string/`, `math/`, `struct/`, `map/`, `datetime/`, `hash/`, and others.

```sql
statement
CREATE TABLE test_crc32(col string, a int, b float) USING parquet

statement
INSERT INTO test_crc32 VALUES ('Spark', 10, 1.5), (NULL, NULL, NULL), ('', 0, 0.0)

-- default mode: verifies native Comet execution and that the result matches Spark
query
SELECT crc32(col) FROM test_crc32

-- compares results without requiring native execution
query spark_answer_only
SELECT crc32(cast(a as string)) FROM test_crc32

-- allows numeric variance for floating-point results
query tolerance=0.0001
SELECT cos(v) FROM test_trig

-- asserts fallback to Spark occurs
query expect_fallback(unsupported expression)
SELECT unsupported_func(v) FROM test_table

-- verifies both engines throw matching exceptions
query expect_error(ARITHMETIC_OVERFLOW)
SELECT 2147483647 + 1

-- skip a query with a known bug, with the issue link
query ignore(https://github.com/apache/datafusion-comet/issues/NNNN)
SELECT known_buggy_expr(v) FROM test_table
```

Run the whole suite with `-Dsuites="org.apache.comet.CometSqlFileTestSuite" -Dtest=none`, or a
single file by appending a substring of its name to the suite argument.

**Verify all test requirements, whichever framework is used:**

- [ ] Basic functionality tested against column data, not only literals
- [ ] Null handling tested
- [ ] Edge cases tested: empty input, overflow, boundary values, negative values
- [ ] Both literal and column arguments tested, in every combination for multi-argument
      expressions. They take different code paths.
- [ ] Timezone handling tested for timestamp and datetime expressions, including a non-UTC session
      timezone and timestamps with and without timezone
- [ ] SQL syntax gated with `MinSparkVersion` when it only parses on newer Spark
- [ ] `expect_error` patterns substring-match what both Spark and Comet actually throw
- [ ] One expression per SQL file
- [ ] Comet Scala literal tests disable constant folding:

```scala
withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
    "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
  checkSparkAnswerAndOperator("SELECT func(literal)")
}
```

## 5. Performance

**For PRs that add a new native expression, performance is not optional.** The point of a native
implementation is to be faster than Spark's codegen. If it is not faster, it may not be worth
carrying.

1. Does the PR description contain benchmark numbers comparing Comet against Spark? If not, ask for
   them.
2. Does the PR add a benchmark under `spark/src/test/scala/org/apache/spark/sql/benchmark/`?
3. Are the benchmarks representative, with realistic data sizes and the data shapes that break
   optimizations, such as nulls, dictionaries, and wide strings?
4. Does the Rust implementation allocate or copy more than it needs to, process row by row where a
   kernel exists, redo type conversions, or revalidate UTF-8 repeatedly?
5. If a benchmark shows Comet slower than Spark, flag it. The PR needs to explain why that is
   acceptable or include a plan to fix it.

For PRs optimizing an existing kernel, `optimizing_expressions.md` defines the no-regression rule
and the requirement to record a performance audit. Hold the PR to both.

## 6. Documentation

### Generated, review the source instead

The compatibility pages under `docs/source/user-guide/latest/compatibility/expressions/` and
`docs/source/user-guide/latest/configs.md` are produced by `GenerateDocs` and regenerated by CI on
merge. Never ask the contributor to hand-edit them. Check what feeds them:

- Expression marked `Incompatible(Some("..."))` but `getIncompatibleReasons()` is empty, so the
  compat page shows it as supported with no caveats.
- `Unsupported(Some("..."))` for specific types or argument shapes with no
  `getUnsupportedReasons()` to surface the limitation.
- Reason strings that drift from the `notes` passed to `Compatible` / `Incompatible` /
  `Unsupported`. They need not match exactly, but consistency helps.
- Reason strings too terse to be useful in user-facing docs: a single word, no context, no link to
  a tracking issue when behavior is known to differ.

### Hand-edited, the PR should update these

- `docs/source/user-guide/latest/expressions.md`, the supported-expressions list. A new expression
  belongs here.
- Other `latest/compatibility/` pages such as `floating-point.md`, `regex.md`, `operators.md`.

### Does the PR make the contributor guide stale?

Check `adding_a_new_expression.md` against the diff. It goes stale when a PR:

- Changes the `CometExpressionSerde` interface, the support-level API, or the
  `getIncompatibleReasons` / `getUnsupportedReasons` contract
- Adds or renames an expression map in `QueryPlanSerde.scala`, since the guide names them
- Changes how the return type is resolved, which would invalidate the "when to set the return type
  explicitly" section
- Changes the shimming pattern or adds a Spark version to the set the guide lists
- Changes how scalar functions are registered in `comet_scalar_funcs.rs`

Check `sql-file-tests.md` when the PR adds, renames, or changes a test directive. The directive
reference in that doc is the only place they are documented.

## Common Expression Review Findings

1. **Incomplete type support**, Spark supports types the PR does not handle
2. **Missing edge cases**, null, overflow, empty string, negative values
3. **Wrong return type**, it must match Spark exactly
4. **Tests in the wrong framework**, Scala tests where a SQL file test would do
5. **Missing `getSupportLevel`**, divergences left undeclared rather than marked `Incompatible`
6. **Version-specific Spark behavior implemented once**, with no shim
7. **Name collides with a DataFusion built-in** and no explicit return type
