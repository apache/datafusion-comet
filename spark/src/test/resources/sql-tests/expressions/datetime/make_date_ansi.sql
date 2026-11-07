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

-- ANSI mode: Spark's MakeDate wraps the java.time.DateTimeException raised by LocalDate.of in
-- ansiDateTimeArgumentOutOfRange (4.x) / ansiDateTimeError (3.x) when
-- spark.sql.ansi.enabled=true. The DATETIME_FIELD_OUT_OF_BOUNDS subclass and message parameters
-- vary by Spark version. Comet's native SparkMakeDate throws the same java.time-style message
-- under ANSI instead of returning NULL. The expect_error patterns below are substrings of that
-- message and match across Spark versions.
-- Config: spark.sql.ansi.enabled=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false
-- Config: spark.sql.subexpressionElimination.enabled=true
-- Config: spark.sql.datetime.java8API.enabled=true
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.sql.optimizer.inSetConversionThreshold=10

-- Sentinel: a valid date must still execute natively under ANSI. This guards against the
-- expect_error queries passing vacuously if make_date were to fall back to Spark.
-- Materialize these expression-test results before exposing a row iterator, so native
-- evaluation is safe even when a later consumer reads only a prefix.

query
SELECT /*+ REPARTITION(1) */ make_date(2024, 2, 28)

query
SELECT /*+ REPARTITION(1) */ make_date(300000, 6, 15), make_date(300000, 2, 29)

-- February 30 is not a valid date.
query expect_error(Invalid date)
SELECT /*+ REPARTITION(1) */ make_date(2024, 2, 30)

-- Month 13 is out of range.
query expect_error(MonthOfYear)
SELECT /*+ REPARTITION(1) */ make_date(2024, 13, 1)

-- Day 0 is out of range.
query expect_error(DayOfMonth)
SELECT /*+ REPARTITION(1) */ make_date(2024, 6, 0)

statement
CREATE TABLE make_date_arguments(y INT, m INT, inner_m INT) USING parquet

statement
INSERT INTO make_date_arguments VALUES
  (NULL, 1, 13), (2024, NULL, 13), (2024, 1, 1)

-- A nonempty IN evaluates its probe. Keep this two-item list as In rather than InSet.
query expect_native(make_date, in)
SELECT /*+ REPARTITION(1) */ make_date(y, m, 1) IN (DATE '2024-01-01', DATE '2024-02-01')
FROM make_date_arguments

statement
CREATE TABLE make_date_membership(y INT, s STRING) USING parquet

statement
INSERT INTO make_date_membership VALUES
  (2024, 'bad'), (2025, '2025-01-01'), (NULL, 'bad')

-- A match or NULL probe skips the throwing cast on that row, even when another row needs it.
query expect_fallback(make_date requires Spark evaluation under In with computed candidates)
SELECT /*+ REPARTITION(1) */ make_date(y, 1, 1) IN (DATE '2024-01-01', CAST(s AS DATE))
FROM make_date_membership

-- Structs with floating-point fields use Comet's nested membership implementation.
query expect_fallback(make_date requires Spark evaluation under In with computed candidates)
SELECT /*+ REPARTITION(1) */ (make_date(y, 1, 1), 0D) IN ((DATE '2024-01-01', 0D), (CAST(s AS DATE), 0D))
FROM make_date_membership WHERE y IS NOT NULL

query expect_fallback(make_date requires Spark evaluation under In with computed candidates)
SELECT /*+ REPARTITION(1) */ (make_date(y, 1, 1), 0D) NOT IN ((DATE '2024-01-01', 0D), (CAST(s AS DATE), 0D))
FROM make_date_membership WHERE y IS NOT NULL

-- A candidate required by an unmatched probe must still fail.
query expect_error(CAST_INVALID_INPUT)
SELECT /*+ REPARTITION(1) */ make_date(y, 1, 1) IN (DATE '2023-01-01', CAST(s AS DATE))
FROM make_date_membership WHERE y = 2024

-- Safe structs retain the native path through the analyzer's nullability-widening casts.
query expect_native(make_date, in)
SELECT /*+ REPARTITION(1) */ (make_date(y, 1, 1), 0D) IN ((DATE '2024-01-01', 0D), (DATE '2025-01-01', 0D))
FROM make_date_membership

-- Year and month NULLs skip the invalid day expression on just those rows.
query expect_native(make_date)
SELECT /*+ REPARTITION(1) */ make_date(y, m, dayofmonth(make_date(2024, inner_m, 1))) FROM make_date_arguments

-- A NULL year also skips a computed month.
query expect_native(make_date)
SELECT /*+ REPARTITION(1) */ make_date(y, month(make_date(2024, inner_m, 1)), 1)
FROM make_date_arguments WHERE m IS NOT NULL

-- A later NULL must not hide an error in the required first argument.
query expect_error(MonthOfYear)
SELECT /*+ REPARTITION(1) */ make_date(year(make_date(2024, inner_m, 1)), 1, y)
FROM make_date_arguments WHERE y IS NULL

-- These date functions always evaluate their first operand, even when it returns NULL.
query expect_native(make_date, date_add)
SELECT /*+ REPARTITION(1) */ date_add(make_date(y, m, 1), 1) FROM make_date_arguments

query expect_native(make_date, date_sub)
SELECT /*+ REPARTITION(1) */ date_sub(make_date(y, m, 1), 1) FROM make_date_arguments

query expect_native(make_date, date_format)
SELECT /*+ REPARTITION(1) */ date_format(make_date(y, m, 1), 'yyyy-MM-dd') FROM make_date_arguments

query expect_native(make_date, datediff)
SELECT /*+ REPARTITION(1) */ datediff(make_date(y, m, 1), DATE '2024-01-01') FROM make_date_arguments

-- A NULL first operand still skips the invalid date in the second operand.
query expect_fallback(make_date requires Spark evaluation under DateDiff)
SELECT /*+ REPARTITION(1) */ datediff(make_date(y, m, 1), make_date(2024, inner_m, 1)) FROM make_date_arguments

-- Container constructors evaluate every child and do not need a parent fallback.
query expect_native(make_date)
SELECT /*+ REPARTITION(1) */ array(make_date(y, m, 1), DATE '2024-01-01'),
       named_struct('date', make_date(y, m, 1)) FROM make_date_arguments

-- Greatest and least evaluate every operand, including nullable dates in either position.
query expect_native(make_date, greatest, least)
SELECT /*+ REPARTITION(1) */ greatest(make_date(y, m, 1), DATE '2024-02-01'),
       least(DATE '2024-02-01', make_date(y, m, 1)) FROM make_date_arguments

-- Hashes evaluate every operand, even after a NULL, with the date in either position.
query expect_native(make_date, hash, xxhash64)
SELECT /*+ REPARTITION(1) */ hash(make_date(y, m, 1), y), xxhash64(y, make_date(y, m, 1))
FROM make_date_arguments

-- Arithmetic keeps required date operands native when skipped siblings are harmless.
query expect_native(make_date)
SELECT /*+ REPARTITION(1) */ unix_date(make_date(y, m, 1)) + 1,
       unix_date(make_date(y, m, 1)) - y,
       2 * unix_date(make_date(y, m, 1)) FROM make_date_arguments

-- A NULL date skips a throwing right operand, so the arithmetic must still fall back.
query expect_fallback(make_date requires Spark evaluation under Add)
SELECT /*+ REPARTITION(1) */ unix_date(make_date(y, 1, 1)) + CAST(s AS INT)
FROM make_date_membership WHERE y IS NULL

statement
CREATE TABLE make_date_parents(lhs INT, guard BOOLEAN, m INT) USING parquet

statement
INSERT INTO make_date_parents VALUES (NULL, true, 13), (1, false, 1)

-- Division evaluates its right operand first and skips the date when the divisor is NULL.
query expect_fallback(make_date requires Spark evaluation under Divide)
SELECT /*+ REPARTITION(1) */ unix_date(make_date(2024, m, 1)) / lhs FROM make_date_parents

-- Native arithmetic/Boolean parents are still eager, so keep these in Spark.
query expect_fallback(make_date requires Spark evaluation under)
SELECT /*+ REPARTITION(1) */ lhs + unix_date(make_date(2024, m, 1)) FROM make_date_parents

-- An eager inner parent must not remove an outer parent's short-circuit guard.
query expect_fallback(make_date requires Spark evaluation under Add)
SELECT /*+ REPARTITION(1) */ lhs + unix_date(greatest(make_date(2024, m, 1), DATE '2024-01-01'))
FROM make_date_parents

query expect_fallback(make_date requires Spark evaluation under Add)
SELECT /*+ REPARTITION(1) */ lhs + hash(make_date(2024, m, 1), lhs) FROM make_date_parents

query expect_fallback(make_date requires Spark evaluation under Add)
SELECT /*+ REPARTITION(1) */ lhs + (unix_date(make_date(2024, m, 1)) + 1) FROM make_date_parents

query expect_fallback(make_date requires Spark evaluation under)
SELECT /*+ REPARTITION(1) */ guard OR unix_date(make_date(2024, m, 1)) > 0 FROM make_date_parents

-- Existing conditional expressions safely pass only selected rows to make_date.
query expect_native(make_date)
SELECT /*+ REPARTITION(1) */ IF(guard, DATE '2024-01-01', make_date(2024, m, 1)) FROM make_date_parents

query expect_native(make_date)
SELECT /*+ REPARTITION(1) */ CASE WHEN guard THEN DATE '2024-01-01' ELSE make_date(2024, m, 1) END
FROM make_date_parents

query expect_native(make_date)
SELECT /*+ REPARTITION(1) */ COALESCE(CASE WHEN guard THEN DATE '2024-01-01' END, make_date(2024, m, 1))
FROM make_date_parents

statement
CREATE TABLE make_date_shared(y INT, d INT) USING parquet

statement
INSERT INTO make_date_shared VALUES (NULL, 0), (2024, 0)

-- Spark computes the shared day expression outside the constructor's NULL checks.
query expect_fallback(make_date requires Spark evaluation when a computed argument)
SELECT /*+ REPARTITION(1) */ make_date(y, 1, d + 1), d + 1 FROM make_date_shared

statement
SET spark.sql.subexpressionElimination.enabled=false

query expect_native(make_date)
SELECT /*+ REPARTITION(1) */ make_date(y, 1, d + 1), d + 1 FROM make_date_shared

-- The Date32 endpoints remain valid, and NULL arguments skip overflow validation.
statement
CREATE TABLE make_date_bounds(y INT, m INT, d INT) USING parquet

statement
INSERT INTO make_date_bounds VALUES
  (5881580, 7, 11), (-5877641, 6, 23),
  (NULL, 13, 32), (6000000, NULL, 1), (6000000, 1, NULL)

query expect_native(make_date)
SELECT /*+ REPARTITION(1) */ unix_date(make_date(y, m, d)) FROM make_date_bounds

-- Valid calendar dates just outside Date32 must overflow, regardless of ANSI mode.
statement
CREATE TABLE make_date_overflow(y INT, m INT, d INT) USING parquet

statement
INSERT INTO make_date_overflow VALUES (5881580, 7, 12), (-5877641, 6, 22)

query expect_error_class(ARITHMETIC_OVERFLOW)
SELECT /*+ REPARTITION(1) */ make_date(y, m, d) FROM make_date_overflow WHERE y > 0

query expect_error_class(ARITHMETIC_OVERFLOW)
SELECT /*+ REPARTITION(1) */ make_date(y, m, d) FROM make_date_overflow WHERE y < 0

-- An empty input must not evaluate overflowing scalar arguments.
query expect_native(make_date)
SELECT /*+ REPARTITION(1) */ make_date(6000000, 1, 1) FROM make_date_overflow WHERE y = 0

-- Invalid calendar fields must report their own error, not Date32 overflow.
query expect_error(MonthOfYear)
SELECT /*+ REPARTITION(1) */ make_date(6000000, 13, 32)

query expect_error(Invalid date)
SELECT /*+ REPARTITION(1) */ make_date(6000000, 2, 30)

query expect_error(Year)
SELECT /*+ REPARTITION(1) */ make_date(1000000000, 13, 32)
