---
name: review-datafusion-pr
description: Review an Apache DataFusion pull request for correctness, Spark compatibility of `datafusion-spark` expressions, and for breaking API changes that may affect DataFusion Comet. Provides guidance to a reviewer rather than posting comments directly.
argument-hint: <pr-number>
---

Review DataFusion PR #$ARGUMENTS

## Before You Start

### Gather PR Metadata

Fetch the PR details to understand the scope:

```bash
gh pr view $ARGUMENTS --repo apache/datafusion --json title,body,author,isDraft,state,files
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
gh pr view $ARGUMENTS --repo apache/datafusion --comments
```

### Classify the PR

Look at the changed files and decide which review tracks apply. More than one may apply.

- **`datafusion/spark/` expression change** — adds or modifies a Spark-compatible function. Apply the Spark expression track.
- **`datafusion/sqllogictest/test_files/spark/` test change** — modifies `.slt` tests for Spark functions. Apply the Spark expression track.
- **Core DataFusion change** — modifies `datafusion/core`, `datafusion/physical-expr`, `datafusion/physical-plan`, `datafusion/expr`, `datafusion/common`, `datafusion/datasource`, or `datafusion/physical-expr-adapter`. Apply the Comet API impact track. These are the crates Comet depends on directly.
- **Other** — protocol-level changes, release infrastructure, docs. Comet impact is usually low, but still skim for breaking changes.

The Comet native workspace declares its DataFusion dependencies in `native/Cargo.toml`. The crates Comet currently pulls in are `datafusion`, `datafusion-datasource`, `datafusion-physical-expr-adapter`, and `datafusion-spark`. Changes to public items in those crates are the ones most likely to affect Comet.

---

## Review Workflow

### 1. Gather Context

```bash
# View the full diff
gh pr diff $ARGUMENTS --repo apache/datafusion
```

For expression PRs, check how similar expressions are implemented in `datafusion/spark/src/function/`. The subdirectories mirror expression categories (`math/`, `string/`, `datetime/`, `array/`, `hash/`, etc.). Each function typically has a Rust module and a matching `.slt` file under `datafusion/sqllogictest/test_files/spark/<category>/<name>.slt`.

### 2. Spark Expression Track

For PRs that add or modify a `datafusion-spark` function, correctness means **matching Spark behavior**, not DataFusion's own semantics. SLT files use DataFusion syntax and run against DataFusion only, so they cannot verify Spark equivalence on their own. The reviewer must cross-check behavior against the real Spark implementation.

#### 2.1 Read the Spark source code

1. **Clone or update the Spark repo:**

   ```bash
   if [ ! -d /tmp/spark ]; then
     git clone --depth 1 https://github.com/apache/spark.git /tmp/spark
   fi
   ```

2. **Find the expression class:**

   ```bash
   find /tmp/spark/sql/catalyst/src/main/scala -name "*.scala" \
     | xargs grep -l "case class <ExpressionName>\b"
   ```

3. **Read the Spark implementation carefully.** Pay attention to:
   - `eval`, `nullSafeEval`, and `doGenCode` (these define the canonical behavior)
   - `inputTypes` and `dataType` (accepted input types and return type)
   - Null handling (`nullable`, `nullSafeEval`)
   - ANSI mode branches (look for `SQLConf.get.ansiEnabled` or `failOnError`)
   - Special cases, guards, `require` assertions, runtime exceptions

4. **Read the Spark tests for the expression:**

   ```bash
   find /tmp/spark/sql -name "*.scala" -path "*/test/*" \
     | xargs grep -l "<ExpressionName>"
   ```

5. **Compare the Spark behavior against the DataFusion implementation in the PR.** Identify:
   - Edge cases tested in Spark but not covered in the `.slt` file
   - Data types supported in Spark but not handled in the PR
   - Behavioral differences that the PR does not document

#### 2.2 Review the Rust implementation

Location: `datafusion/spark/src/function/<category>/<name>.rs`

Check:

- Null handling propagates correctly
- Overflow and underflow return `Err` when Spark would throw under ANSI mode, return the Spark-defined value otherwise
- Type dispatch covers all types that Spark supports for this function
- ANSI vs non-ANSI branches match Spark behavior. The SLT framework exposes `datafusion.execution.enable_ansi_mode` for this.
- Uses Arrow compute kernels where available. Row-by-row loops are a red flag.
- No panics. Return `DataFusionError` variants instead.
- UDF registration is wired up in the function module's `mod.rs` and in the category's `mod.rs`.

#### 2.3 Review the `.slt` test file

Location: `datafusion/sqllogictest/test_files/spark/<category>/<name>.slt`

**The `.slt` test format, in brief:**

```sql
# Scalar input
query I
SELECT abs(-1::INT);
----
1

# Array input
query I
SELECT abs(a) FROM (VALUES (-1::INT), (NULL)) AS t(a);
----
1
NULL

# ANSI mode error
statement ok
set datafusion.execution.enable_ansi_mode = true;

query error DataFusion error: Arrow error: Compute error: Int32 overflow on abs\(\-2147483648\)
select abs((-2147483648)::INT);

statement ok
set datafusion.execution.enable_ansi_mode = false;
```

Verify the test file against the `datafusion-spark` testing guide in `datafusion/sqllogictest/test_files/spark/README.md`:

- [ ] Both scalar and array inputs are exercised (the README requires this)
- [ ] All accepted Spark input types are tested with explicit casts (`0::INT`, `0::BIGINT`, etc.) — DataFusion and Spark do not infer types the same way
- [ ] Null input is tested
- [ ] Edge cases: empty string, boundary values (e.g., `INT_MIN`), `NaN`, `Infinity`, `-0.0`, negative values for numeric functions
- [ ] ANSI mode behavior is wrapped in `set datafusion.execution.enable_ansi_mode = true/false` pairs where Spark differs between modes
- [ ] Test only contains `SELECT` statements for the function under test, with no unrelated setup
- [ ] Header comments cite the upstream source if ported (the existing files show the pattern)

#### 2.4 Manually verify Spark equivalence

**This is the step SLT cannot do for you.** SLT only proves that DataFusion returns a stable result, not that the result matches Spark. For each non-trivial test case in the `.slt` file (or for each case the PR added or changed), run the equivalent query in Spark and compare.

**Recommended tools:**

- `spark-sql` shell from an Apache Spark binary distribution
- `spark-shell` with `spark.sql("...").show(false)` for richer output
- PySpark for quick one-liners

**Translation notes when porting a `.slt` query to Spark:**

- `value::TYPE` → `CAST(value AS TYPE)` (Spark does not accept the `::` cast operator)
- `datafusion.execution.enable_ansi_mode = true` → `SET spark.sql.ansi.enabled = true;`
- `INT`, `BIGINT`, `TINYINT`, `SMALLINT`, `FLOAT`, `DOUBLE`, `DECIMAL(p,s)`, `STRING`, `DATE`, `TIMESTAMP` map directly. `UTF8` / `UTF8VIEW` are DataFusion internal variants that both map to Spark `STRING`.
- Literal `NULL` casts may need to be written as `CAST(NULL AS INT)` in Spark for the type to be pinned.
- Array and map literal syntax differs. Use `array(...)` and `map(...)` in Spark.

**What to compare:**

- The concrete result value
- The result type (Spark `printSchema()` or `DESCRIBE` is authoritative)
- Null vs exception behavior. If Spark returns `NULL` and DataFusion raises, that is a mismatch. If Spark throws under ANSI and DataFusion returns `NULL`, that is also a mismatch.
- Precision and scale for decimal results

If the behavior diverges from Spark, flag it. The PR should either fix the divergence or call it out explicitly in a comment, in the function docstring, or as a follow-up issue. `datafusion-spark` is a best-effort compatibility layer, not a strict re-implementation, but undocumented divergences are bugs.

#### 2.5 Running the `.slt` tests locally

```bash
cd /path/to/datafusion
cargo test --test sqllogictests -- spark
```

To regenerate an `.slt` file after intentional result changes, use the `--complete` mode (see the `sqllogictest` crate's docs at `datafusion/sqllogictest/README.md`).

### 3. Comet API Impact Track

Comet pins to a specific DataFusion version in `native/Cargo.toml`. Any PR that changes public items in the crates Comet depends on may require Comet-side work when Comet upgrades. Flagging these early helps the DataFusion maintainers understand downstream impact and helps Comet plan its upgrade.

**Crates Comet depends on (as of `native/Cargo.toml`):**

- `datafusion` (with `unicode_expressions`, `crypto_expressions`, `nested_expressions`, `parquet` features)
- `datafusion-datasource`
- `datafusion-physical-expr-adapter`
- `datafusion-spark` (with `core` feature)

**What to look for in the diff:**

- [ ] Removed or renamed public functions, methods, structs, traits, enums, or modules
- [ ] Changed trait method signatures (new methods without default implementations, changed parameter or return types)
- [ ] Changed public struct fields or added `#[non_exhaustive]`
- [ ] Changed generic bounds on public types
- [ ] Behavior changes to public APIs that Comet relies on (e.g., `ExecutionPlan`, `PhysicalExpr`, `SessionState`, `SchemaAdapter`, `ParquetExec`, `SortExec`, `AggregateExec`, `HashJoinExec`, `SortMergeJoinExec`, `ScalarUDF` / `ScalarUDFImpl`)
- [ ] Changes to physical-expression evaluation semantics (null handling, schema inference, type coercion)
- [ ] Changes to the Parquet reader path (statistics, predicate pushdown, schema evolution)
- [ ] Configuration key renames or removals that Comet forwards or documents
- [ ] Feature flag changes on crates Comet depends on

**How to check whether Comet uses a symbol that is changing:**

```bash
cd /path/to/datafusion-comet
grep -rn "<SymbolName>" native/ --include="*.rs"
```

Also check `native/core/src/execution/planner.rs`, since many Comet plan node conversions import DataFusion types directly.

When you find an affected symbol, include a short note in the review describing how Comet uses it and whether a shim or upgrade PR will be needed. Link to the relevant Comet file and line. You do not need to propose the full fix, but naming the impacted code makes the downstream cost concrete for the maintainers.

### 4. Check CI Test Failures

```bash
gh pr checks $ARGUMENTS --repo apache/datafusion
gh pr checks $ARGUMENTS --repo apache/datafusion --failed
```

Summarize any failed checks in your review. DataFusion CI runs `cargo test`, `cargo clippy`, `cargo fmt`, sqllogictests, and extended benchmark smoke tests on selected PRs.

### 5. Documentation Check

- **User guide** (`docs/source/user-guide/`): New public APIs or SQL features should be documented.
- **Spark function list**: The `datafusion-spark` crate maintains implementation tracking in its README and in the epic issue referenced from the README. New functions should update those.
- **Changelog / release notes**: DataFusion maintains release notes automatically, but API breaks should ideally be called out in the PR description.

### 6. Common Review Issues

1. **SLT passes but Spark differs** — the most important failure mode. Always cross-check in Spark.
2. **Missing array-input tests** — scalar-only tests let dispatch bugs through.
3. **Missing type casts in `.slt`** — without explicit casts, DataFusion may pick a different type than Spark would.
4. **Missing ANSI mode coverage** for functions that behave differently under ANSI.
5. **Public API changes without a changelog note or a mention in the PR body** — increases downstream surprise for Comet and other consumers.
6. **Row-by-row Rust implementations** where Arrow compute kernels exist.
7. **Panics in function code** — use `DataFusionError` instead.

---

## Output Format

Present your review as guidance for the reviewer. Structure your output as:

1. **PR Summary** — brief description of what the PR does
2. **Track(s) Applied** — Spark expression, Comet API impact, or both
3. **CI Status** — summary of CI check results
4. **Findings** — analysis organized by area (Spark compatibility, Rust implementation, `.slt` coverage, Comet impact, docs)
5. **Spark Cross-Check Results** — for expression PRs, a list of queries you ran in Spark and whether results matched
6. **Suggested Review Comments** — specific comments the reviewer could leave on the PR, with file and line references where applicable

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
