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

-- ============================================================
-- Setup: shared tables
-- ============================================================

statement
CREATE TABLE test_first_last(i int, grp string) USING parquet

statement
INSERT INTO test_first_last VALUES (1, 'a'), (2, 'a'), (3, 'a'), (NULL, 'b'), (4, 'b')

statement
CREATE TABLE test_ignore_nulls(id int, val int, grp string) USING parquet

statement
INSERT INTO test_ignore_nulls VALUES
  (1, NULL, 'a'),
  (2, 10,   'a'),
  (3, 20,   'a'),
  (4, NULL, 'b'),
  (5, 30,   'b'),
  (6, NULL, 'b')

statement
CREATE TABLE test_all_nulls(val int, grp string) USING parquet

statement
INSERT INTO test_all_nulls VALUES (NULL, 'a'), (NULL, 'a'), (NULL, 'b'), (1, 'b')

statement
CREATE TABLE test_empty(val int) USING parquet

statement
CREATE TABLE test_single_row(val int) USING parquet

statement
INSERT INTO test_single_row VALUES (42)

statement
CREATE TABLE test_single_null(val int) USING parquet

statement
INSERT INTO test_single_null VALUES (NULL)

statement
CREATE TABLE test_types(
  i_val int,
  l_val bigint,
  d_val double,
  s_val string,
  b_val boolean,
  grp string
) USING parquet

statement
INSERT INTO test_types VALUES
  (NULL, NULL,  NULL,  NULL,  NULL,  'a'),
  (1,    100,   1.5,   'foo', true,  'a'),
  (2,    200,   2.5,   'bar', false, 'a'),
  (NULL, NULL,  NULL,  NULL,  NULL,  'b'),
  (NULL, NULL,  NULL,  NULL,  NULL,  'b')

statement
CREATE TABLE test_null_positions(id int, val int, grp string) USING parquet

statement
INSERT INTO test_null_positions VALUES
  (1, NULL, 'start_null'),
  (2, 10,   'start_null'),
  (3, 20,   'start_null'),
  (4, 10,   'mid_null'),
  (5, NULL, 'mid_null'),
  (6, 20,   'mid_null'),
  (7, 10,   'end_null'),
  (8, 20,   'end_null'),
  (9, NULL, 'end_null')

statement
CREATE TABLE test_expr_nulls(val int, grp string) USING parquet

statement
INSERT INTO test_expr_nulls VALUES (1, 'a'), (2, 'a'), (3, 'a'), (4, 'b'), (5, 'b'), (6, 'b')

statement
CREATE TABLE test_decimal(val DECIMAL(10,2), grp string) USING parquet

statement
INSERT INTO test_decimal VALUES (NULL, 'a'), (1.23, 'a'), (4.56, 'a'), (NULL, 'b'), (NULL, 'b')

statement
CREATE TABLE test_date(val DATE, grp string) USING parquet

statement
INSERT INTO test_date VALUES (NULL, 'a'), (DATE '2024-01-01', 'a'), (DATE '2024-12-31', 'a'), (NULL, 'b'), (NULL, 'b')

statement
CREATE TABLE test_large(val int, grp string) USING parquet

statement
INSERT INTO test_large
SELECT
  CASE WHEN id % 10 = 0 THEN id ELSE NULL END as val,
  CASE WHEN id < 500 THEN 'a' ELSE 'b' END as grp
FROM (SELECT explode(sequence(1, 1000)) as id)

-- ############################################################
-- FIRST
-- ############################################################

-- ============================================================
-- first: basic (default behavior includes nulls)
-- ============================================================

query
SELECT first(i) FROM test_first_last

query
SELECT first(i, true) FROM test_first_last

query
SELECT grp, first(i) FROM test_first_last GROUP BY grp ORDER BY grp

-- ============================================================
-- first IGNORE NULLS: basic
-- ============================================================

-- without grouping
query
SELECT first(val) IGNORE NULLS FROM test_ignore_nulls

-- with grouping
query
SELECT grp, first(val) IGNORE NULLS FROM test_ignore_nulls GROUP BY grp ORDER BY grp

-- contrast: default behavior (RESPECT NULLS) - null can appear as first
query
SELECT grp, first(val) FROM test_ignore_nulls GROUP BY grp ORDER BY grp

-- ============================================================
-- first IGNORE NULLS: all values null in a group
-- ============================================================

-- group 'a' has all nulls -> should return null even with IGNORE NULLS
query
SELECT grp, first(val) IGNORE NULLS FROM test_all_nulls GROUP BY grp ORDER BY grp

-- ============================================================
-- first IGNORE NULLS: empty table
-- ============================================================

query
SELECT first(val) IGNORE NULLS FROM test_empty

-- ============================================================
-- first IGNORE NULLS: single row
-- ============================================================

query
SELECT first(val) IGNORE NULLS FROM test_single_row

query
SELECT first(val) IGNORE NULLS FROM test_single_null

-- ============================================================
-- first IGNORE NULLS: multiple data types
-- ============================================================

query expect_fallback(SortAggregate is not supported)
SELECT grp,
  first(i_val) IGNORE NULLS,
  first(l_val) IGNORE NULLS,
  first(d_val) IGNORE NULLS,
  first(s_val) IGNORE NULLS,
  first(b_val) IGNORE NULLS
FROM test_types GROUP BY grp ORDER BY grp

-- ============================================================
-- first IGNORE NULLS: nulls at beginning, middle, end
-- ============================================================

query
SELECT grp, first(val) IGNORE NULLS FROM test_null_positions GROUP BY grp ORDER BY grp

-- ============================================================
-- first IGNORE NULLS: boolean parameter form
-- ============================================================

-- first(val, true) is equivalent to first(val) IGNORE NULLS
query
SELECT grp, first(val, true) FROM test_null_positions GROUP BY grp ORDER BY grp

-- first(val, false) is equivalent to first(val) (default, respect nulls)
query
SELECT grp, first(val, false) FROM test_null_positions GROUP BY grp ORDER BY grp

-- ============================================================
-- first IGNORE NULLS: with expressions producing nulls
-- ============================================================

-- IF expression produces nulls for even values
query
SELECT grp,
  first(IF(val % 2 = 0, NULL, val)) IGNORE NULLS
FROM test_expr_nulls GROUP BY grp ORDER BY grp

-- ============================================================
-- first IGNORE NULLS: mixed with other aggregations
-- ============================================================

query
SELECT grp,
  first(val) IGNORE NULLS,
  first(val),
  count(val),
  sum(val)
FROM test_ignore_nulls GROUP BY grp ORDER BY grp

-- ============================================================
-- first IGNORE NULLS: with HAVING clause
-- ============================================================

query
SELECT grp, first(val) IGNORE NULLS as f
FROM test_ignore_nulls
GROUP BY grp
HAVING first(val) IGNORE NULLS IS NOT NULL
ORDER BY grp

-- ============================================================
-- first IGNORE NULLS: decimal type
-- ============================================================

query
SELECT grp, first(val) IGNORE NULLS FROM test_decimal GROUP BY grp ORDER BY grp

-- ============================================================
-- first IGNORE NULLS: date type
-- ============================================================

query
SELECT grp, first(val) IGNORE NULLS FROM test_date GROUP BY grp ORDER BY grp

-- ============================================================
-- first IGNORE NULLS: large group (multi-batch)
-- ============================================================

query
SELECT grp, first(val) IGNORE NULLS FROM test_large GROUP BY grp ORDER BY grp

-- ############################################################
-- LAST
-- ############################################################

-- ============================================================
-- last: basic (default behavior includes nulls)
-- ============================================================

query
SELECT last(i) FROM test_first_last

query
SELECT last(i, true) FROM test_first_last

query
SELECT grp, last(i) FROM test_first_last GROUP BY grp ORDER BY grp

-- ============================================================
-- last IGNORE NULLS: basic
-- ============================================================

-- without grouping
query
SELECT last(val) IGNORE NULLS FROM test_ignore_nulls

-- with grouping
query
SELECT grp, last(val) IGNORE NULLS FROM test_ignore_nulls GROUP BY grp ORDER BY grp

-- contrast: default behavior (RESPECT NULLS) - null can appear as last
query
SELECT grp, last(val) FROM test_ignore_nulls GROUP BY grp ORDER BY grp

-- ============================================================
-- last IGNORE NULLS: all values null in a group
-- ============================================================

-- group 'a' has all nulls -> should return null even with IGNORE NULLS
query
SELECT grp, last(val) IGNORE NULLS FROM test_all_nulls GROUP BY grp ORDER BY grp

-- ============================================================
-- last IGNORE NULLS: empty table
-- ============================================================

query
SELECT last(val) IGNORE NULLS FROM test_empty

-- ============================================================
-- last IGNORE NULLS: single row
-- ============================================================

query
SELECT last(val) IGNORE NULLS FROM test_single_row

query
SELECT last(val) IGNORE NULLS FROM test_single_null

-- ============================================================
-- last IGNORE NULLS: multiple data types
-- ============================================================

query expect_fallback(SortAggregate is not supported)
SELECT grp,
  last(i_val) IGNORE NULLS,
  last(l_val) IGNORE NULLS,
  last(d_val) IGNORE NULLS,
  last(s_val) IGNORE NULLS,
  last(b_val) IGNORE NULLS
FROM test_types GROUP BY grp ORDER BY grp

-- ============================================================
-- last IGNORE NULLS: nulls at beginning, middle, end
-- ============================================================

query
SELECT grp, last(val) IGNORE NULLS FROM test_null_positions GROUP BY grp ORDER BY grp

-- ============================================================
-- last IGNORE NULLS: boolean parameter form
-- ============================================================

-- last(val, true) is equivalent to last(val) IGNORE NULLS
query
SELECT grp, last(val, true) FROM test_null_positions GROUP BY grp ORDER BY grp

-- last(val, false) is equivalent to last(val) (default, respect nulls)
query
SELECT grp, last(val, false) FROM test_null_positions GROUP BY grp ORDER BY grp

-- ============================================================
-- last IGNORE NULLS: with expressions producing nulls
-- ============================================================

-- IF expression produces nulls for even values
query
SELECT grp,
  last(IF(val % 2 = 0, NULL, val)) IGNORE NULLS
FROM test_expr_nulls GROUP BY grp ORDER BY grp

-- ============================================================
-- last IGNORE NULLS: mixed with other aggregations
-- ============================================================

query
SELECT grp,
  last(val) IGNORE NULLS,
  last(val),
  count(val),
  sum(val)
FROM test_ignore_nulls GROUP BY grp ORDER BY grp

-- ============================================================
-- last IGNORE NULLS: with HAVING clause
-- ============================================================

query
SELECT grp, last(val) IGNORE NULLS as l
FROM test_ignore_nulls
GROUP BY grp
HAVING last(val) IGNORE NULLS IS NOT NULL
ORDER BY grp

-- ============================================================
-- last IGNORE NULLS: decimal type
-- ============================================================

query
SELECT grp, last(val) IGNORE NULLS FROM test_decimal GROUP BY grp ORDER BY grp

-- ============================================================
-- last IGNORE NULLS: date type
-- ============================================================

query
SELECT grp, last(val) IGNORE NULLS FROM test_date GROUP BY grp ORDER BY grp

-- ============================================================
-- last IGNORE NULLS: large group (multi-batch)
-- ============================================================

query
SELECT grp, last(val) IGNORE NULLS FROM test_large GROUP BY grp ORDER BY grp

-- ############################################################
-- FIRST + LAST combined
-- ############################################################

-- ============================================================
-- first and last together in same query
-- ============================================================

query
SELECT first(i), last(i) FROM test_first_last

query
SELECT first(i, true), last(i, true) FROM test_first_last

query
SELECT grp, first(i), last(i) FROM test_first_last GROUP BY grp ORDER BY grp

query
SELECT grp,
  first(val) IGNORE NULLS,
  last(val) IGNORE NULLS
FROM test_ignore_nulls GROUP BY grp ORDER BY grp

query
SELECT grp,
  first(val) IGNORE NULLS,
  last(val) IGNORE NULLS,
  first(val),
  last(val),
  count(val),
  sum(val)
FROM test_ignore_nulls GROUP BY grp ORDER BY grp
