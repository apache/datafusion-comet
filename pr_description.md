## Summary

- Add a SQL-file-based test framework for expression testing with `CometSqlFileTestSuite` and `SqlFileTestParser`
- Mark queries that expose native engine bugs with `ignore` directives linked to GitHub issues
- Mark expected fallback cases with `expect_fallback` directives

## How the SQL file test framework works

### Overview

`CometSqlFileTestSuite` automatically discovers `.sql` test files under `spark/src/test/resources/sql-tests/expressions/` and registers each one as a ScalaTest test case. This allows expression tests to be written as plain SQL files rather than Scala code.

### SQL file format

Each `.sql` file contains a sequence of **statements** and **queries**, separated by blank lines:

```sql
-- Config: spark.comet.regexp.allowIncompatible=true
-- ConfigMatrix: parquet.enable.dictionary=false,true
-- MinSparkVersion: 3.5

statement
CREATE TABLE test(s string) USING parquet

statement
INSERT INTO test VALUES ('hello'), (NULL)

query
SELECT upper(s) FROM test

-- literal arguments
query tolerance=1e-6
SELECT ln(1.0), ln(2.718281828459045)
```

**File-level directives** (in comment headers):
- `-- Config: key=value` — sets a Spark SQL config for all queries in the file
- `-- ConfigMatrix: key=val1,val2,...` — runs the entire file once per value (generates combinatorial test cases)
- `-- MinSparkVersion: X.Y` — skips the test on older Spark versions

**Record types:**
- `statement` — executes DDL/DML (CREATE TABLE, INSERT, etc.) without checking results
- `query` — executes a SELECT and compares Comet results against Spark, verifying both correctness and native execution coverage

**Query assertion modes:**
- `query` — (**default**) checks that results match Spark AND that all operators ran natively in Comet
- `query spark_answer_only` — checks results match Spark but does not verify native execution
- `query tolerance=1e-6` — checks results match within a floating-point tolerance
- `query expect_fallback(reason)` — checks results match Spark AND verifies that Comet fell back to Spark with a reason string containing the given text
- `query ignore(reason)` — skips the query entirely (used for known bugs, with a link to the GitHub issue)

### Test runner (`CometSqlFileTestSuite`)

The test runner:
1. Discovers all `.sql` files recursively under `sql-tests/`
2. Parses each file using `SqlFileTestParser`
3. Generates test case combinations from `ConfigMatrix` (e.g., `parquet.enable.dictionary=false,true` produces two test cases per file)
4. For each test case, creates tables, runs statements, and validates queries using the specified assertion mode
5. Disables Spark's `ConstantFolding` optimizer rule so that literal-only expressions are evaluated by Comet's native engine rather than being folded away at plan time
6. Cleans up tables after each test

### Test file organization

Test files are organized into category subdirectories:

```
sql-tests/expressions/
├── array/           (array_contains, flatten, size, ...)
├── bitwise/         (bitwise operations)
├── cast/            (type casting)
├── conditional/     (if, case_when, coalesce, predicates)
├── datetime/        (hour, date_add, trunc, ...)
├── decimal/         (decimal arithmetic)
├── hash/            (md5, sha1, sha2, hash, xxhash64)
├── map/             (get_map_value, map_from_arrays)
├── math/            (abs, sin, log, round, ...)
├── misc/            (width_bucket, scalar_subquery, ...)
├── string/          (substring, concat, like, ...)
└── struct/          (create_named_struct, json_to_structs, ...)
```

## Native engine issues discovered

These issues were found by testing literal argument combinations with constant folding disabled:

| Issue | Title |
|-------|-------|
| [#3336](https://github.com/apache/datafusion-comet/issues/3336) | Native engine panics on all-scalar inputs for hour, minute, second, unix_timestamp |
| [#3337](https://github.com/apache/datafusion-comet/issues/3337) | Native engine panics on all-scalar inputs for Substring and StringSpace |
| [#3338](https://github.com/apache/datafusion-comet/issues/3338) | Native engine panics with 'index out of bounds' on literal array expressions |
| [#3339](https://github.com/apache/datafusion-comet/issues/3339) | Native engine crashes on concat_ws with literal NULL separator |
| [#3340](https://github.com/apache/datafusion-comet/issues/3340) | Native engine crashes on literal sha2() with 'Unsupported argument types' |
| [#3341](https://github.com/apache/datafusion-comet/issues/3341) | Native engine panics on scalar bit_count() |
| [#3342](https://github.com/apache/datafusion-comet/issues/3342) | Native engine crashes on literal DateTrunc and TimestampTrunc |
| [#3343](https://github.com/apache/datafusion-comet/issues/3343) | Native engine crashes on all-literal RLIKE expression |
| [#3344](https://github.com/apache/datafusion-comet/issues/3344) | Native replace() returns wrong result for empty-string search |
| [#3345](https://github.com/apache/datafusion-comet/issues/3345) | array_contains returns wrong result for literal array with NULL cast |
| [#3346](https://github.com/apache/datafusion-comet/issues/3346) | array_contains returns null instead of false for empty array with literal value |

## Pre-existing issues also linked from ignored tests

| Issue | Title |
|-------|-------|
| [#3326](https://github.com/apache/datafusion-comet/issues/3326) | space() with negative input causes native crash |
| [#3327](https://github.com/apache/datafusion-comet/issues/3327) | map_from_arrays() with NULL inputs causes native crash |
| [#3331](https://github.com/apache/datafusion-comet/issues/3331) | width_bucket fails: Int32 downcast to Int64Array |
| [#3332](https://github.com/apache/datafusion-comet/issues/3332) | GetArrayItem returns incorrect results with dynamic index |

