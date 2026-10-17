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

-- Spark's projection evaluates a subexpression that several of its expressions share for every
-- row, before the expressions that use it. A DOUBLE-keyed map lookup runs through the JVM codegen
-- dispatcher, which compiles each lookup alone, so Spark's own `nullSafeCodeGen` there would skip
-- the shared key on a row whose map is NULL. Under ANSI mode Spark raises on that row where the
-- dispatched lookup would return NULL, so a lookup whose later argument holds a shared
-- subexpression falls back to Spark instead.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=true

statement
CREATE TABLE test_map_lookup_shared(
  m1 map<double, int>, m2 map<double, int>, x double, y double, k int)
USING parquet

statement
INSERT INTO test_map_lookup_shared VALUES
  (CAST(NULL AS MAP<DOUBLE, INT>), CAST(NULL AS MAP<DOUBLE, INT>), 0.0D, CAST(NULL AS DOUBLE),
   1),
  (map(0.5D, 5), map(0.5D, 6), 2.0D, 4.0D, 2)

-- The shared `1.0D / x` divides by zero on the row whose maps are NULL.
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(m1, 1.0D / x), element_at(m2, 1.0D / x) FROM test_map_lookup_shared

query expect_error(DIVIDE_BY_ZERO)
SELECT m1[1.0D / x], m2[1.0D / x] FROM test_map_lookup_shared

-- The keys differ, but both hold the shared `1.0D / x` below the top of the key.
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(m1, 1.0D / x + 1.0D), element_at(m2, 1.0D / x + 2.0D)
FROM test_map_lookup_shared

-- A divide evaluates its divisor first and skips its dividend when the divisor is NULL, so on the
-- row whose `y` is NULL each lookup skips the shared `1.0D / x`, although it sits in the map
-- argument.
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(if((1.0D / x) / y > 0.0D, m1, m2), 0.5D),
  element_at(if((1.0D / x) / (y + 1.0D) > 0.0D, m2, m1), 0.5D)
FROM test_map_lookup_shared

-- An aggregate's update evaluates a subexpression its functions share for every row too.
query expect_error(DIVIDE_BY_ZERO)
SELECT max(element_at(m1, 1.0D / x)), max(element_at(m2, 1.0D / x)) FROM test_map_lookup_shared

-- On rows where the divide cannot fail, the shared key still sends the lookups back to Spark.
statement
CREATE TABLE test_map_lookup_shared_nonzero(
  m1 map<double, int>, m2 map<double, int>, x double, y double)
USING parquet

statement
INSERT INTO test_map_lookup_shared_nonzero VALUES
  (CAST(NULL AS MAP<DOUBLE, INT>), CAST(NULL AS MAP<DOUBLE, INT>), 4.0D, CAST(NULL AS DOUBLE)),
  (map(0.5D, 5), map(0.5D, 6), 2.0D, 4.0D)

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT element_at(m1, 1.0D / x), element_at(m2, 1.0D / x) FROM test_map_lookup_shared_nonzero

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT element_at(if((1.0D / x) / y > 0.0D, m1, m2), 0.5D),
  element_at(if((1.0D / x) / (y + 1.0D) > 0.0D, m2, m1), 0.5D)
FROM test_map_lookup_shared_nonzero

query expect_dispatch(element_at)
SELECT element_at(if((1.0D / x) / y > 0.0D, m1, m2), 0.5D) FROM test_map_lookup_shared_nonzero

-- `pmod` skips its dividend the same way.
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(if(pmod(1.0D / x, y) > 0.0D, m1, m2), 0.5D),
  element_at(if(pmod(1.0D / x, y + 1.0D) > 0.0D, m2, m1), 0.5D)
FROM test_map_lookup_shared

-- A lookup that repeats `1.0D / x` in its key stays dispatched: the dispatcher eliminates the
-- repeated subexpression as Spark does, so it evaluates the divide for every row and raises on
-- the row whose map is NULL.
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(m1, 1.0D / x + 1.0D / x) FROM test_map_lookup_shared

query expect_dispatch(element_at)
SELECT element_at(m1, 1.0D / x + 1.0D / x) FROM test_map_lookup_shared_nonzero

-- Subexpression elimination shares only what an `if` evaluates on every row, its predicate and
-- what both branches hold, so the `1.0D / x` in its else branch alone is not shared with the
-- lookup, and Spark skips that divide on the row whose `x` is zero.
query expect_dispatch(element_at)
SELECT element_at(m1, 1.0D / x), if(x = 0.0D, 0.0D, 1.0D / x) FROM test_map_lookup_shared

-- A single lookup shares nothing, so Spark skips the divide on the NULL map too.
query expect_dispatch(element_at)
SELECT element_at(m1, 1.0D / x) FROM test_map_lookup_shared

query expect_dispatch(getmapvalue)
SELECT m1[1.0D / x] FROM test_map_lookup_shared

-- Subexpression elimination never shares a column.
query expect_dispatch(element_at)
SELECT element_at(m1, x), element_at(m2, x) FROM test_map_lookup_shared

-- The shared `coalesce(m1, m2)` is the first argument, which each lookup evaluates anyway, and
-- the keys share nothing.
query expect_dispatch(element_at)
SELECT element_at(coalesce(m1, m2), x * 0.25D), element_at(coalesce(m1, m2), x * 0.5D)
FROM test_map_lookup_shared

-- A grouped aggregate whose final result looks up a computed key. The planner checks the final
-- aggregate before converting it, outside any operator, and that check must not decline the
-- lookup, or the native partial `avg` would be reverted to Spark.
query expect_dispatch(element_at)
SELECT k, element_at(map(CAST(k AS DOUBLE), 7), avg(x) + 1.0D)
FROM test_map_lookup_shared GROUP BY k

-- `locate` evaluates its start first and skips its substring when the start is NULL, so on the
-- row whose `n` is NULL each lookup skips the shared `1.0D / x`, although it sits in the
-- substring, the first argument of `locate`.
statement
CREATE TABLE test_map_lookup_shared_locate(
  m1 map<double, int>, m2 map<double, int>, x double, s string, n int)
USING parquet

statement
INSERT INTO test_map_lookup_shared_locate VALUES
  (map(0.5D, 5), map(0.5D, 6), 0.0D, 'abc', CAST(NULL AS INT)),
  (map(0.5D, 5), map(0.5D, 6), 2.0D, 'x0.5', 1)

query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(if(locate(CAST(1.0D / x AS STRING), s, n) > 0, m1, m2), 0.5D),
  element_at(if(locate(CAST(1.0D / x + 1.0D AS STRING), s, n) > 0, m2, m1), 0.5D)
FROM test_map_lookup_shared_locate

statement
CREATE TABLE test_map_lookup_shared_locate_nonzero(
  m1 map<double, int>, m2 map<double, int>, x double, s string, n int)
USING parquet

statement
INSERT INTO test_map_lookup_shared_locate_nonzero VALUES
  (map(0.5D, 5), map(0.5D, 6), 4.0D, 'abc', CAST(NULL AS INT)),
  (map(0.5D, 5), map(0.5D, 6), 2.0D, 'x0.5', 1)

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT element_at(if(locate(CAST(1.0D / x AS STRING), s, n) > 0, m1, m2), 0.5D),
  element_at(if(locate(CAST(1.0D / x + 1.0D AS STRING), s, n) > 0, m2, m1), 0.5D)
FROM test_map_lookup_shared_locate_nonzero

-- With subexpression elimination off a projection shares nothing, so its lookups stay
-- dispatched, but a whole-stage codegen aggregate still evaluates the subexpressions its
-- functions share for every row, so there the shared key still sends the lookups back to Spark.
statement
SET spark.sql.subexpressionElimination.enabled=false

query expect_error(DIVIDE_BY_ZERO)
SELECT max(element_at(m1, 1.0D / x)), max(element_at(m2, 1.0D / x)) FROM test_map_lookup_shared

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT max(element_at(m1, 1.0D / x)), max(element_at(m2, 1.0D / x))
FROM test_map_lookup_shared_nonzero

query expect_dispatch(element_at)
SELECT element_at(m1, 1.0D / x), element_at(m2, 1.0D / x) FROM test_map_lookup_shared

query expect_dispatch(getmapvalue)
SELECT m1[1.0D / x], m2[1.0D / x] FROM test_map_lookup_shared

-- The dispatcher eliminates a subexpression the lookup repeats only while the setting is on, so
-- with it off the repeated `1.0D / x` counts as shared in an aggregate, which raises on the row
-- whose map is NULL. A projection shares nothing, so there the same lookup stays dispatched.
query expect_error(DIVIDE_BY_ZERO)
SELECT max(element_at(m1, 1.0D / x + 1.0D / x)) FROM test_map_lookup_shared

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT max(element_at(m1, 1.0D / x + 1.0D / x)) FROM test_map_lookup_shared_nonzero

query expect_dispatch(element_at)
SELECT element_at(m1, 1.0D / x + 1.0D / x) FROM test_map_lookup_shared

-- A single lookup still shares nothing.
query expect_dispatch(element_at)
SELECT element_at(m1, 1.0D / x) FROM test_map_lookup_shared

-- Without ANSI mode the divide returns NULL, but the decline does not depend on ANSI mode, so the
-- lookups still fall back.
statement
SET spark.sql.subexpressionElimination.enabled=true

statement
SET spark.sql.ansi.enabled=false

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT element_at(m1, 1.0D / x), element_at(m2, 1.0D / x) FROM test_map_lookup_shared
