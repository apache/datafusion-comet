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

# SQL File Tests

`CometSqlFileTestSuite` is a test suite that automatically discovers `.sql` test files and
runs each query through both Spark and Comet, comparing results. This provides a lightweight
way to add expression and operator test coverage without writing Scala test code.

## Running the tests

Run all SQL file tests:

```shell
./mvnw test -Dsuites="org.apache.comet.CometSqlFileTestSuite" -Dtest=none
```

Run a single test file by adding the file name (without `.sql` extension) after the suite name:

```shell
./mvnw test -Dsuites="org.apache.comet.CometSqlFileTestSuite create_named_struct" -Dtest=none
```

This uses ScalaTest's substring matching, so the argument must match part of the test name.
Test names follow the pattern `sql-file: expressions/<category>/<file>.sql [<config>]`.

## Test file location

SQL test files live under:

```
spark/src/test/resources/sql-tests/expressions/
```

Files are organized into category subdirectories:

```
expressions/
  aggregate/     -- avg, sum, count, min_max, ...
  array/         -- array_contains, array_append, get_array_item, ...
  bitwise/
  cast/
  conditional/   -- case_when, coalesce, if_expr, ...
  datetime/      -- date_add, date_diff, unix_timestamp, ...
  decimal/
  hash/
  map/           -- get_map_value, map_keys, map_values, ...
  math/          -- abs, ceil, floor, round, sqrt, ...
  misc/          -- width_bucket, scalar_subquery, ...
  string/        -- concat, like, substring, lower, upper, ...
  struct/        -- create_named_struct, get_struct_field, ...
```

The test suite recursively discovers all `.sql` files in these directories. Each file becomes
one or more ScalaTest test cases.

## File format

A test file consists of SQL comments, directives, statements, and queries separated by blank
lines. Here is a minimal example:

```sql
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_abs(v double) USING parquet

statement
INSERT INTO test_abs VALUES (1.5), (-2.5), (0.0), (NULL)

query
SELECT abs(v) FROM test_abs
```

### Directives

Directives are SQL comments at the top of the file that configure how the test runs.

#### `Config`

Sets a Spark SQL config for all queries in the file.

```sql
-- Config: spark.sql.ansi.enabled=true
```

#### `ConfigMatrix`

Runs the entire file once per combination of values. Multiple `ConfigMatrix` lines produce a
cross product of all combinations.

```sql
-- ConfigMatrix: parquet.enable.dictionary=false,true
```

This generates two test cases:

```
sql-file: expressions/cast/cast.sql [parquet.enable.dictionary=false]
sql-file: expressions/cast/cast.sql [parquet.enable.dictionary=true]
```

#### `MinSparkVersion`

Skips the file when running on a Spark version older than the specified version.

```sql
-- MinSparkVersion: 3.5
```

### Statements

A `statement` block executes DDL or DML and does not check results. Use this for `CREATE TABLE`
and `INSERT` commands. Table names are automatically extracted for cleanup after the test.

```sql
statement
CREATE TABLE my_table(x int, y double) USING parquet

statement
INSERT INTO my_table VALUES (1, 2.0), (3, 4.0), (NULL, NULL)
```

### Queries

A `query` block executes a SELECT and compares results between Spark and Comet. The query
mode controls how results are validated.

#### `query` (default mode)

Checks that the query runs natively on Comet (not falling back to Spark) and that results
match Spark exactly.

```sql
query
SELECT abs(v) FROM test_abs
```

#### `query spark_answer_only`

Only checks that Comet results match Spark. Does not assert that the query runs natively.
Use this for expressions that Comet may not fully support yet but should still produce
correct results.

```sql
query spark_answer_only
SELECT some_expression(v) FROM test_table
```

#### `query tolerance=<value>`

Checks results with a numeric tolerance. Useful for floating-point functions where small
differences are acceptable.

```sql
query tolerance=0.0001
SELECT cos(v) FROM test_trig
```

#### `query expect_fallback(<reason>)`

Asserts that the query falls back to Spark and verifies the fallback reason contains the
given string.

```sql
query expect_fallback(unsupported expression)
SELECT unsupported_func(v) FROM test_table
```

#### `query ignore(<reason>)`

Skips the query entirely. Use this for queries that hit known bugs. The reason should be a
link to the tracking GitHub issue.

```sql
-- Comet bug: space(-1) causes native crash
query ignore(https://github.com/apache/datafusion-comet/issues/3326)
SELECT space(n) FROM test_space WHERE n < 0
```

#### `query expect_error(<pattern>)`

Asserts that both Spark and Comet throw an exception containing the given pattern. Use this
for ANSI mode tests where invalid operations should throw errors.

```sql
-- Config: spark.sql.ansi.enabled=true

-- integer overflow should throw in ANSI mode
query expect_error(ARITHMETIC_OVERFLOW)
SELECT 2147483647 + 1

-- division by zero should throw in ANSI mode
query expect_error(DIVIDE_BY_ZERO)
SELECT 1 / 0

-- array out of bounds should throw in ANSI mode
query expect_error(INVALID_ARRAY_INDEX)
SELECT array(1, 2, 3)[10]
```

## Adding a new test

1. Create a `.sql` file under the appropriate subdirectory in
   `spark/src/test/resources/sql-tests/expressions/`. Create a new subdirectory if no
   existing category fits.

2. Add the Apache license header as a SQL comment.

3. Add a `ConfigMatrix` directive if the test should run with multiple Parquet configurations.
   Most expression tests use:

   ```sql
   -- ConfigMatrix: parquet.enable.dictionary=false,true
   ```

4. Create tables and insert test data using `statement` blocks. Include edge cases such as
   `NULL`, boundary values, and negative numbers.

5. Add `query` blocks for each expression or behavior to test. Use the default `query` mode
   when you expect Comet to run the expression natively. Use `query spark_answer_only` when
   native execution is not yet expected.

6. Run the tests to verify:

   ```shell
   ./mvnw test -Dsuites="org.apache.comet.CometSqlFileTestSuite" -Dtest=none
   ```

### Tips for writing thorough tests

#### Cover all combinations of literal and column arguments

Comet often uses different code paths for literal values versus column references. Tests
should exercise both. For a function with multiple arguments, test every useful combination.

For a single-argument function, test both a column reference and a literal:

```sql
-- column argument (reads from Parquet, goes through columnar evaluation)
query
SELECT ascii(s) FROM test_ascii

-- literal arguments
query
SELECT ascii('A'), ascii(''), ascii(NULL)
```

For a multi-argument function like `concat_ws(sep, str1, str2, ...)`, test with the separator
as a column versus a literal, and similarly for the other arguments:

```sql
-- all columns
query
SELECT concat_ws(sep, a, b) FROM test_table

-- literal separator, column values
query
SELECT concat_ws(',', a, b) FROM test_table

-- all literals
query
SELECT concat_ws(',', 'hello', 'world')
```

**Note on constant folding:** Normally Spark constant-folds all-literal expressions during
planning, so Comet would never see them. However, `CometSqlFileTestSuite` automatically
disables constant folding (by excluding `ConstantFolding` from the optimizer rules), so
all-literal queries are evaluated by Comet's native engine. This means you can use the
default `query` mode for all-literal cases and they will be tested natively just like
column-based queries.

#### Cover edge cases

Include edge-case values in your test data. The exact cases depend on the function, but
common ones include:

- **NULL values** -- every test should include NULLs
- **Empty strings** -- for string functions
- **Zero, negative, and very large numbers** -- for numeric functions
- **Boundary values** -- `INT_MIN`, `INT_MAX`, `NaN`, `Infinity`, `-Infinity` for numeric
  types
- **Special characters and multibyte UTF-8** -- for string functions (e.g. `'é'`, `'中文'`,
  `'\t'`)
- **Empty arrays/maps** -- for collection functions
- **Single-element and multi-element collections** -- for aggregate and collection functions

#### One file per expression

Keep each `.sql` file focused on a single expression or a small group of closely related
expressions. This makes failures easy to locate and keeps files readable.

#### Use comments to label sections

Add SQL comments before query blocks to describe what aspect of the expression is being
tested. This helps reviewers and future maintainers understand the intent:

```sql
-- literal separator with NULL values
query
SELECT concat_ws(',', NULL, 'b', 'c')

-- empty separator
query
SELECT concat_ws('', a, b, c) FROM test_table
```

### Using agentic coding tools

Writing thorough SQL test files is a task well suited to agentic coding tools such as
Claude Code. You can point the tool at an existing test file as an example, describe the
expression you want to test, and ask it to generate a complete `.sql` file covering all
argument combinations and edge cases. This is significantly faster than writing the
combinatorial test cases by hand and helps ensure nothing is missed.

For example:

```
Read the test file spark/src/test/resources/sql-tests/expressions/string/ascii.sql
and the documentation in docs/source/contributor-guide/sql-file-tests.md.
Then write a similar test file for the `reverse` function, covering column arguments,
literal arguments, NULLs, empty strings, and multibyte characters.
```

## Handling test failures

When a query fails due to a known Comet bug:

1. File a GitHub issue describing the problem.
2. Change the query mode to `ignore(...)` with a link to the issue.
3. Optionally add a SQL comment above the query explaining the problem.

```sql
-- GetArrayItem returns incorrect results with dynamic index
query ignore(https://github.com/apache/datafusion-comet/issues/3332)
SELECT arr[idx] FROM test_get_array_item
```

When the bug is fixed, remove the `ignore(...)` and restore the original query mode.

## Architecture

The test infrastructure consists of two Scala files:

- **`SqlFileTestParser`** (`spark/src/test/scala/org/apache/comet/SqlFileTestParser.scala`) --
  Parses `.sql` files into a `SqlTestFile` data structure containing directives, statements,
  and queries.

- **`CometSqlFileTestSuite`** (`spark/src/test/scala/org/apache/comet/CometSqlFileTestSuite.scala`) --
  Discovers test files at suite initialization time, generates ScalaTest test cases for each
  file and config combination, and executes them using `CometTestBase` assertion methods.

Tables created in test files are automatically cleaned up after each test.
