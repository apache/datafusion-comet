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
4. Existing test coverage (SQL file tests and Scala tests)
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
- Whether `getSupportLevel` is implemented and accurate
- Whether all input types are handled
- Whether any types are explicitly marked `Unsupported`

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

### SQL file tests

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

### Scala tests

```bash
grep -r "$ARGUMENTS" spark/src/test/scala/ --include="*.scala" -l
```

Read the relevant Scala test files and list:

- Input types covered
- Edge cases exercised
- Whether constant folding is disabled for literal tests

---

## Step 5: Gap Analysis

Compare the Spark test coverage (Step 2) against the Comet test coverage (Step 4). Produce a structured gap report:

### Coverage matrix

For each of the following dimensions, note whether it is covered in Comet tests or missing:

| Dimension                                                                                              | Spark tests it | Comet SQL test | Comet Scala test | Gap? |
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

---

## Step 6: Recommendations

Summarize findings as a prioritized list.

### High priority

Issues where Comet may silently produce wrong results compared to Spark.

### Medium priority

Missing test coverage for edge cases that could expose bugs.

### Low priority

Minor gaps, cosmetic improvements, or nice-to-have tests.

---

## Step 7: Offer to Implement Missing Tests

After presenting the gap analysis, ask the user:

> I found the following missing test cases. Would you like me to implement them?
>
> - [list each missing test case]
>
> I can add them as SQL file tests in `spark/src/test/resources/sql-tests/expressions/<category>/$ARGUMENTS.sql`
> (or as Scala tests in `CometExpressionSuite` for cases that require programmatic setup).

If the user says yes, implement the missing tests following the SQL file test format described in
`docs/source/contributor-guide/sql-file-tests.md`. Prefer SQL file tests over Scala tests.

### SQL file test template

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

## Step 8: Update the Expression Support Doc

After completing the audit (whether or not tests were added), add sub-bullets under the expression's
entry in `docs/source/contributor-guide/spark_expressions_support.md`.

Add one sub-bullet per Spark version checked, each including:

- Spark version (e.g. 3.4.3, 3.5.8, 4.0.1)
- Today's date
- A brief note for any version-specific finding (behavioral difference, known incompatibility); omit if nothing notable

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
