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

-- ConfigMatrix: spark.sql.ansi.enabled=false,true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false
-- Config: spark.sql.subexpressionElimination.enabled=true
-- Config: spark.sql.datetime.java8API.enabled=true
-- Config: spark.sql.session.timeZone=UTC

statement
CREATE TABLE make_date_overflow(y INT, m INT, d INT) USING parquet

statement
INSERT INTO make_date_overflow VALUES (6000000, 1, 1)

-- Java accepts the calendar date; its epoch day does not fit Date32, even without ANSI.
query expect_error_class(ARITHMETIC_OVERFLOW)
SELECT make_date(y, m, d) FROM make_date_overflow

statement
CREATE TABLE make_date_boundaries(y INT, m INT, d INT) USING parquet

statement
INSERT INTO make_date_boundaries VALUES
  (5881580, 7, 11), (-5877641, 6, 23),
  (NULL, 13, 32), (6000000, NULL, 1), (6000000, 1, NULL)

query expect_native(make_date)
SELECT unix_date(make_date(y, m, d)) FROM make_date_boundaries

-- Container constructors evaluate every child and do not need a parent fallback.
query expect_native(make_date)
SELECT array(make_date(y, m, d), DATE '2024-01-01'),
       named_struct('date', make_date(y, m, d)) FROM make_date_boundaries

statement
CREATE TABLE make_date_arguments(y INT, m INT, inner_y INT) USING parquet

statement
INSERT INTO make_date_arguments VALUES
  (NULL, 1, 6000000), (2024, NULL, 6000000), (2024, 1, 2024)

-- These date functions always evaluate their first operand, even when it returns NULL.
query expect_native(make_date, date_add)
SELECT date_add(make_date(y, m, 1), 1) FROM make_date_arguments

query expect_native(make_date, date_sub)
SELECT date_sub(make_date(y, m, 1), 1) FROM make_date_arguments

query expect_native(make_date, date_format)
SELECT date_format(make_date(y, m, 1), 'yyyy-MM-dd') FROM make_date_arguments

query expect_native(make_date, datediff)
SELECT datediff(make_date(y, m, 1), DATE '2024-01-01') FROM make_date_arguments

-- A NULL first operand still skips the overflowing date in the second operand.
query expect_fallback(make_date requires Spark evaluation under DateDiff)
SELECT datediff(make_date(y, m, 1), make_date(inner_y, 1, 1)) FROM make_date_arguments

-- Year and month NULLs skip the overflowing day expression on just those rows.
query expect_native(make_date)
SELECT make_date(y, m, dayofmonth(make_date(inner_y, 1, 1))) FROM make_date_arguments

-- A NULL year also skips a computed month, before it can overflow.
query expect_native(make_date)
SELECT make_date(y, month(make_date(inner_y, 1, 1)), 1)
FROM make_date_arguments WHERE m IS NOT NULL

-- A later NULL must not hide an error in the required first argument.
query expect_error_class(ARITHMETIC_OVERFLOW)
SELECT make_date(year(make_date(inner_y, 1, 1)), 1, y)
FROM make_date_arguments WHERE inner_y = 6000000

statement
CREATE TABLE make_date_parents(lhs INT, guard BOOLEAN, y INT, d INT) USING parquet

statement
INSERT INTO make_date_parents VALUES
  (NULL, true, 6000000, 1), (1, false, 2024, 1)

-- Native arithmetic/Boolean parents are still eager, so keep these in Spark.
query expect_fallback(make_date requires Spark evaluation under)
SELECT lhs + unix_date(make_date(y, 1, d)) FROM make_date_parents

query expect_fallback(make_date requires Spark evaluation under)
SELECT guard OR unix_date(make_date(y, 1, d)) > 0 FROM make_date_parents

-- Existing conditional expressions safely pass only selected rows to make_date.
query expect_native(make_date)
SELECT IF(guard, DATE '2024-01-01', make_date(y, 1, d)) FROM make_date_parents

query expect_native(make_date)
SELECT CASE WHEN guard THEN DATE '2024-01-01' ELSE make_date(y, 1, d) END
FROM make_date_parents

query expect_native(make_date)
SELECT COALESCE(CASE WHEN guard THEN DATE '2024-01-01' END, make_date(y, 1, d))
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
