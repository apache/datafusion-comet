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
query
SELECT make_date(2024, 2, 28)

query
SELECT make_date(300000, 6, 15), make_date(300000, 2, 29)

-- February 30 is not a valid date.
query expect_error(Invalid date)
SELECT make_date(2024, 2, 30)

-- Month 13 is out of range.
query expect_error(MonthOfYear)
SELECT make_date(2024, 13, 1)

-- Day 0 is out of range.
query expect_error(DayOfMonth)
SELECT make_date(2024, 6, 0)

statement
CREATE TABLE make_date_arguments(y INT, m INT, inner_m INT) USING parquet

statement
INSERT INTO make_date_arguments VALUES
  (NULL, 1, 13), (2024, NULL, 13), (2024, 1, 1)

-- A nonempty IN evaluates its probe. Keep this two-item list as In rather than InSet.
query expect_native(make_date, in)
SELECT make_date(y, m, 1) IN (DATE '2024-01-01', DATE '2024-02-01')
FROM make_date_arguments

statement
CREATE TABLE make_date_membership(y INT, s STRING) USING parquet

statement
INSERT INTO make_date_membership VALUES
  (2024, 'bad'), (2025, '2025-01-01'), (NULL, 'bad')

-- A match or NULL probe skips the throwing cast on that row, even when another row needs it.
query expect_fallback(make_date requires Spark evaluation under In with computed candidates)
SELECT make_date(y, 1, 1) IN (DATE '2024-01-01', CAST(s AS DATE))
FROM make_date_membership

-- Structs with floating-point fields use Comet's nested membership implementation.
query expect_fallback(make_date requires Spark evaluation under In with computed candidates)
SELECT (make_date(y, 1, 1), 0D) IN ((DATE '2024-01-01', 0D), (CAST(s AS DATE), 0D))
FROM make_date_membership WHERE y IS NOT NULL

query expect_fallback(make_date requires Spark evaluation under In with computed candidates)
SELECT (make_date(y, 1, 1), 0D) NOT IN ((DATE '2024-01-01', 0D), (CAST(s AS DATE), 0D))
FROM make_date_membership WHERE y IS NOT NULL

-- A candidate required by an unmatched probe must still fail.
query expect_error(CAST_INVALID_INPUT)
SELECT make_date(y, 1, 1) IN (DATE '2023-01-01', CAST(s AS DATE))
FROM make_date_membership WHERE y = 2024

-- Safe structs retain the native path through the analyzer's nullability-widening casts.
query expect_native(make_date, in)
SELECT (make_date(y, 1, 1), 0D) IN ((DATE '2024-01-01', 0D), (DATE '2025-01-01', 0D))
FROM make_date_membership

-- Year and month NULLs skip the invalid day expression on just those rows.
query expect_native(make_date)
SELECT make_date(y, m, dayofmonth(make_date(2024, inner_m, 1))) FROM make_date_arguments

-- A NULL year also skips a computed month.
query expect_native(make_date)
SELECT make_date(y, month(make_date(2024, inner_m, 1)), 1)
FROM make_date_arguments WHERE m IS NOT NULL

-- A later NULL must not hide an error in the required first argument.
query expect_error(MonthOfYear)
SELECT make_date(year(make_date(2024, inner_m, 1)), 1, y)
FROM make_date_arguments WHERE y IS NULL

-- These date functions always evaluate their first operand, even when it returns NULL.
query expect_native(make_date, date_add)
SELECT date_add(make_date(y, m, 1), 1) FROM make_date_arguments

query expect_native(make_date, date_sub)
SELECT date_sub(make_date(y, m, 1), 1) FROM make_date_arguments

query expect_native(make_date, date_format)
SELECT date_format(make_date(y, m, 1), 'yyyy-MM-dd') FROM make_date_arguments

query expect_native(make_date, datediff)
SELECT datediff(make_date(y, m, 1), DATE '2024-01-01') FROM make_date_arguments

-- A NULL first operand still skips the invalid date in the second operand.
query expect_fallback(make_date requires Spark evaluation under DateDiff)
SELECT datediff(make_date(y, m, 1), make_date(2024, inner_m, 1)) FROM make_date_arguments

-- Container constructors evaluate every child and do not need a parent fallback.
query expect_native(make_date)
SELECT array(make_date(y, m, 1), DATE '2024-01-01'),
       named_struct('date', make_date(y, m, 1)) FROM make_date_arguments

-- Greatest and least evaluate every operand, including nullable dates in either position.
query expect_native(make_date, greatest, least)
SELECT greatest(make_date(y, m, 1), DATE '2024-02-01'),
       least(DATE '2024-02-01', make_date(y, m, 1)) FROM make_date_arguments

statement
CREATE TABLE make_date_parents(lhs INT, guard BOOLEAN, m INT) USING parquet

statement
INSERT INTO make_date_parents VALUES (NULL, true, 13), (1, false, 1)

-- Native arithmetic/Boolean parents are still eager, so keep these in Spark.
query expect_fallback(make_date requires Spark evaluation under)
SELECT lhs + unix_date(make_date(2024, m, 1)) FROM make_date_parents

-- An eager inner parent must not remove an outer parent's short-circuit guard.
query expect_fallback(make_date requires Spark evaluation under Add)
SELECT lhs + unix_date(greatest(make_date(2024, m, 1), DATE '2024-01-01'))
FROM make_date_parents

query expect_fallback(make_date requires Spark evaluation under)
SELECT guard OR unix_date(make_date(2024, m, 1)) > 0 FROM make_date_parents

-- Existing conditional expressions safely pass only selected rows to make_date.
query expect_native(make_date)
SELECT IF(guard, DATE '2024-01-01', make_date(2024, m, 1)) FROM make_date_parents

query expect_native(make_date)
SELECT CASE WHEN guard THEN DATE '2024-01-01' ELSE make_date(2024, m, 1) END
FROM make_date_parents

query expect_native(make_date)
SELECT COALESCE(CASE WHEN guard THEN DATE '2024-01-01' END, make_date(2024, m, 1))
FROM make_date_parents

statement
CREATE TABLE make_date_shared(y INT, d INT) USING parquet

statement
INSERT INTO make_date_shared VALUES (NULL, 0), (2024, 0)

-- Spark computes the shared day expression outside the constructor's NULL checks.
query expect_fallback(make_date requires Spark evaluation when a computed argument)
SELECT make_date(y, 1, d + 1), d + 1 FROM make_date_shared

statement
SET spark.sql.subexpressionElimination.enabled=false

query expect_native(make_date)
SELECT make_date(y, 1, d + 1), d + 1 FROM make_date_shared
