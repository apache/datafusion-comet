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

-- Spark's projection evaluates shared subexpressions on every row, but the dispatcher compiles
-- each DOUBLE-keyed lookup alone and skips its key on a NULL map, so such a lookup falls back.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=true

-- On the last row both `1.0D / x` and `1.0D / (x + 1.0D)` find a key.
statement
CREATE TABLE test_map_lookup_shared(
  m1 map<double, int>, m2 map<double, int>, x double, y double, k int)
USING parquet

statement
INSERT INTO test_map_lookup_shared VALUES
  (CAST(NULL AS MAP<DOUBLE, INT>), CAST(NULL AS MAP<DOUBLE, INT>), 0.0D, CAST(NULL AS DOUBLE),
   1),
  (map(0.5D, 5), map(0.5D, 6), 2.0D, 4.0D, 2),
  (map(1.0D, 7, 0.5D, 8), map(1.0D, 9, 0.5D, 10), 1.0D, 3.0D, 3)

-- The shared `1.0D / x` divides by zero on the row whose maps are NULL.
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(m1, 1.0D / x), element_at(m2, 1.0D / x) FROM test_map_lookup_shared

query expect_error(DIVIDE_BY_ZERO)
SELECT m1[1.0D / x], m2[1.0D / x] FROM test_map_lookup_shared

query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(m1, 1.0D / x + 1.0D), element_at(m2, 1.0D / x + 2.0D)
FROM test_map_lookup_shared

-- A divide skips its dividend when the divisor `y` is NULL, though it sits in the map argument
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(if((1.0D / x) / y > 0.0D, m1, m2), 0.5D),
  element_at(if((1.0D / x) / (y + 1.0D) > 0.0D, m2, m1), 0.5D)
FROM test_map_lookup_shared

-- An aggregate's update evaluates a subexpression its functions share for every row too.
query expect_error(DIVIDE_BY_ZERO)
SELECT max(element_at(m1, 1.0D / x)), max(element_at(m2, 1.0D / x)) FROM test_map_lookup_shared

-- `x + 1.0D` is never zero, yet the shared key still declines the dispatcher
query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT element_at(m1, 1.0D / (x + 1.0D)), element_at(m2, 1.0D / (x + 1.0D))
FROM test_map_lookup_shared

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT element_at(if((1.0D / (x + 1.0D)) / y > 0.0D, m1, m2), 0.5D),
  element_at(if((1.0D / (x + 1.0D)) / (y + 1.0D) > 0.0D, m2, m1), 0.5D)
FROM test_map_lookup_shared

query expect_dispatch(element_at)
SELECT element_at(if((1.0D / (x + 1.0D)) / y > 0.0D, m1, m2), 0.5D) FROM test_map_lookup_shared

-- `pmod` skips its dividend the same way.
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(if(pmod(1.0D / x, y) > 0.0D, m1, m2), 0.5D),
  element_at(if(pmod(1.0D / x, y + 1.0D) > 0.0D, m2, m1), 0.5D)
FROM test_map_lookup_shared

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT element_at(if(pmod(1.0D / (x + 1.0D), y) > 0.0D, m1, m2), 0.5D),
  element_at(if(pmod(1.0D / (x + 1.0D), y + 1.0D) > 0.0D, m2, m1), 0.5D)
FROM test_map_lookup_shared

-- A shared divisor is evaluated first, so it is never skipped: both raise here, and the
-- never-zero forms stay dispatched
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(if(y / (1.0D / x) > 0.0D, m1, m2), 0.5D),
  element_at(if((y + 1.0D) / (1.0D / x) > 0.0D, m2, m1), 0.5D)
FROM test_map_lookup_shared

query expect_dispatch(element_at)
SELECT element_at(if(y / (1.0D / (x + 1.0D)) > 0.0D, m1, m2), 0.5D),
  element_at(if((y + 1.0D) / (1.0D / (x + 1.0D)) > 0.0D, m2, m1), 0.5D)
FROM test_map_lookup_shared

query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(if(pmod(y, 1.0D / x) > 0.0D, m1, m2), 0.5D),
  element_at(if(pmod(y + 1.0D, 1.0D / x) > 0.0D, m2, m1), 0.5D)
FROM test_map_lookup_shared

query expect_dispatch(element_at)
SELECT element_at(if(pmod(y, 1.0D / (x + 1.0D)) > 0.0D, m1, m2), 0.5D),
  element_at(if(pmod(y + 1.0D, 1.0D / (x + 1.0D)) > 0.0D, m2, m1), 0.5D)
FROM test_map_lookup_shared

-- The dispatcher eliminates a subexpression the lookup itself repeats, as Spark does
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(m1, 1.0D / x + 1.0D / x) FROM test_map_lookup_shared

query expect_dispatch(element_at)
SELECT element_at(m1, 1.0D / (x + 1.0D) + 1.0D / (x + 1.0D)) FROM test_map_lookup_shared

-- An `if` shares only what it always evaluates, so its else-branch `1.0D / x` is not shared
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

-- The shared `coalesce(m1, m2)` is the first argument, which a lookup never skips
query expect_dispatch(element_at)
SELECT element_at(coalesce(m1, m2), x * 0.25D), element_at(coalesce(m1, m2), x * 0.5D)
FROM test_map_lookup_shared

-- The planner checks the final aggregate outside any operator; a decline there would send the
-- native partial `avg` back to Spark
query expect_dispatch(element_at)
SELECT k, element_at(map(CAST(k AS DOUBLE), 7), avg(x) + 1.0D)
FROM test_map_lookup_shared GROUP BY k

-- `locate` evaluates its start first, so a NULL `n` skips the shared divide in the substring
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

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT element_at(if(locate(CAST(1.0D / (x + 1.0D) AS STRING), s, n) > 0, m1, m2), 0.5D),
  element_at(if(locate(CAST(1.0D / (x + 1.0D) + 1.0D AS STRING), s, n) > 0, m2, m1), 0.5D)
FROM test_map_lookup_shared_locate

-- `array_join` evaluates a nullable replacement first, so a NULL `r` skips the shared array.
-- Nullable delimiters dodge SPARK-57200: before 4.2 a join whose array and delimiter cannot be
-- NULL but whose replacement can returns NULL on every row.
statement
CREATE TABLE test_map_lookup_shared_array_join(
  m1 map<double, int>, m2 map<double, int>, x double, d1 string, d2 string, r string)
USING parquet

statement
INSERT INTO test_map_lookup_shared_array_join VALUES
  (map(0.5D, 5), map(0.5D, 6), 0.0D, ',', ';', CAST(NULL AS STRING)),
  (map(0.5D, 5), map(0.5D, 6), 2.0D, ',', ';', 'z')

query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(if(array_join(array(CAST(1.0D / x AS STRING)), d1, r) IS NULL, m1, m2), 0.5D),
  element_at(if(array_join(array(CAST(1.0D / x AS STRING)), d2, r) IS NULL, m2, m1), 0.5D)
FROM test_map_lookup_shared_array_join

-- A NULL literal replacement can be NULL too, so it still declines.
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(if(array_join(array(CAST(1.0D / x AS STRING)), d1, CAST(NULL AS STRING)) IS NULL,
    m1, m2), 0.5D),
  element_at(if(array_join(array(CAST(1.0D / x AS STRING)), d2, CAST(NULL AS STRING)) IS NULL,
    m2, m1), 0.5D)
FROM test_map_lookup_shared_array_join

query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(if(array_join(array('a', 'b'), CAST(1.0D / x AS STRING), r) IS NULL, m1, m2),
    0.5D),
  element_at(if(array_join(array('a', 'c'), CAST(1.0D / x AS STRING), r) IS NULL, m2, m1), 0.5D)
FROM test_map_lookup_shared_array_join

-- Without a replacement the array is evaluated first, so Spark and the dispatcher both raise
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(if(array_join(array(CAST(1.0D / x AS STRING)), d1) IS NULL, m1, m2), 0.5D),
  element_at(if(array_join(array(CAST(1.0D / x AS STRING)), d2) IS NULL, m2, m1), 0.5D)
FROM test_map_lookup_shared_array_join

-- So does a divide in the replacement, or in the array when the replacement cannot be NULL
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(if(array_join(array('a'), d1, CAST(1.0D / x AS STRING)) IS NULL, m1, m2), 0.5D),
  element_at(if(array_join(array('b'), d2, CAST(1.0D / x AS STRING)) IS NULL, m2, m1), 0.5D)
FROM test_map_lookup_shared_array_join

query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(if(array_join(array(CAST(1.0D / x AS STRING)), d1, 'z') IS NULL, m1, m2), 0.5D),
  element_at(if(array_join(array(CAST(1.0D / x AS STRING)), d2, 'z') IS NULL, m2, m1), 0.5D)
FROM test_map_lookup_shared_array_join

-- A single lookup shares nothing, so Spark skips the divide on the NULL replacement too.
query expect_dispatch(element_at)
SELECT element_at(if(array_join(array(CAST(1.0D / x AS STRING)), d1, r) IS NULL, m1, m2), 0.5D)
FROM test_map_lookup_shared_array_join

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT element_at(if(array_join(array(CAST(1.0D / (x + 1.0D) AS STRING)), d1, r) IS NULL,
    m1, m2), 0.5D),
  element_at(if(array_join(array(CAST(1.0D / (x + 1.0D) AS STRING)), d2, r) IS NULL,
    m2, m1), 0.5D)
FROM test_map_lookup_shared_array_join

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT element_at(if(array_join(array('a', 'b'), CAST(1.0D / (x + 1.0D) AS STRING), r) IS NULL,
    m1, m2), 0.5D),
  element_at(if(array_join(array('a', 'c'), CAST(1.0D / (x + 1.0D) AS STRING), r) IS NULL,
    m2, m1), 0.5D)
FROM test_map_lookup_shared_array_join

query expect_dispatch(element_at)
SELECT element_at(if(array_join(array(CAST(1.0D / (x + 1.0D) AS STRING)), d1) IS NULL,
    m1, m2), 0.5D),
  element_at(if(array_join(array(CAST(1.0D / (x + 1.0D) AS STRING)), d2) IS NULL,
    m2, m1), 0.5D)
FROM test_map_lookup_shared_array_join

query expect_dispatch(element_at)
SELECT element_at(if(array_join(array('a'), d1, CAST(1.0D / (x + 1.0D) AS STRING)) IS NULL,
    m1, m2), 0.5D),
  element_at(if(array_join(array('b'), d2, CAST(1.0D / (x + 1.0D) AS STRING)) IS NULL,
    m2, m1), 0.5D)
FROM test_map_lookup_shared_array_join

query expect_dispatch(element_at)
SELECT element_at(if(array_join(array(CAST(1.0D / (x + 1.0D) AS STRING)), d1, 'z') IS NULL,
    m1, m2), 0.5D),
  element_at(if(array_join(array(CAST(1.0D / (x + 1.0D) AS STRING)), d2, 'z') IS NULL,
    m2, m1), 0.5D)
FROM test_map_lookup_shared_array_join

-- With elimination off only a whole-stage codegen aggregate shares subexpressions
statement
SET spark.sql.subexpressionElimination.enabled=false

query expect_error(DIVIDE_BY_ZERO)
SELECT max(element_at(m1, 1.0D / x)), max(element_at(m2, 1.0D / x)) FROM test_map_lookup_shared

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT max(element_at(m1, 1.0D / (x + 1.0D))), max(element_at(m2, 1.0D / (x + 1.0D)))
FROM test_map_lookup_shared

query expect_dispatch(element_at)
SELECT element_at(m1, 1.0D / x), element_at(m2, 1.0D / x) FROM test_map_lookup_shared

query expect_dispatch(getmapvalue)
SELECT m1[1.0D / x], m2[1.0D / x] FROM test_map_lookup_shared

-- With the setting off the dispatcher keeps a repeat, so an aggregate counts it as shared
query expect_error(DIVIDE_BY_ZERO)
SELECT max(element_at(m1, 1.0D / x + 1.0D / x)) FROM test_map_lookup_shared

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT max(element_at(m1, 1.0D / (x + 1.0D) + 1.0D / (x + 1.0D))) FROM test_map_lookup_shared

query expect_dispatch(element_at)
SELECT element_at(m1, 1.0D / x + 1.0D / x) FROM test_map_lookup_shared

query expect_dispatch(element_at)
SELECT element_at(m1, 1.0D / x) FROM test_map_lookup_shared

-- The decline does not depend on ANSI mode
statement
SET spark.sql.subexpressionElimination.enabled=true

statement
SET spark.sql.ansi.enabled=false

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT element_at(m1, 1.0D / x), element_at(m2, 1.0D / x) FROM test_map_lookup_shared
