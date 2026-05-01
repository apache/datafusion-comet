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

-- Tests exercising the Spark PartialMerge aggregate mode, produced when
-- RewriteDistinctAggregates rewrites a mix of distinct and non-distinct
-- aggregates (or multiple distinct aggregates) into an Expand-based multi-stage
-- plan. In that plan the non-distinct aggregates flow through a middle
-- PartialMerge stage that must merge intermediate state buffers and re-emit
-- state (rather than evaluating a final value).

-- ConfigMatrix: parquet.enable.dictionary=false,true

-- ============================================================
-- Setup: shared tables
-- ============================================================

statement
CREATE TABLE pm_basic(i int, l bigint, d double, s string, grp string) USING parquet

statement
INSERT INTO pm_basic VALUES
  (1, 10,  1.5, 'a', 'g1'),
  (2, 20,  2.5, 'b', 'g1'),
  (1, 10,  1.5, 'a', 'g1'),
  (3, 30,  3.5, 'c', 'g2'),
  (3, 30,  3.5, 'c', 'g2'),
  (4, 40,  4.5, 'd', 'g2'),
  (5, 50,  5.5, 'e', 'g3'),
  (5, 50,  5.5, 'e', 'g3'),
  (5, 50,  5.5, 'e', 'g3')

statement
CREATE TABLE pm_nulls(i int, l bigint, d double, s string, grp string) USING parquet

statement
INSERT INTO pm_nulls VALUES
  (NULL, NULL, NULL, NULL, 'g1'),
  (1,    10,   1.5,  'a',  'g1'),
  (1,    10,   1.5,  'a',  'g1'),
  (2,    20,   2.5,  'b',  'g1'),
  (NULL, NULL, NULL, NULL, 'g2'),
  (NULL, NULL, NULL, NULL, 'g2'),
  (3,    30,   3.5,  'c',  'g2')

statement
CREATE TABLE pm_empty(i int, grp string) USING parquet

statement
CREATE TABLE pm_single(i int, grp string) USING parquet

statement
INSERT INTO pm_single VALUES (42, 'g1')

statement
CREATE TABLE pm_single_null(i int, grp string) USING parquet

statement
INSERT INTO pm_single_null VALUES (NULL, 'g1')

statement
CREATE TABLE pm_all_nulls(i int, grp string) USING parquet

statement
INSERT INTO pm_all_nulls VALUES (NULL, 'g1'), (NULL, 'g1'), (NULL, 'g2'), (NULL, 'g2')

statement
CREATE TABLE pm_decimal(v DECIMAL(10,2), w DECIMAL(20,4), grp string) USING parquet

statement
INSERT INTO pm_decimal VALUES
  (1.23,  10.0001, 'g1'),
  (1.23,  10.0001, 'g1'),
  (4.56,  20.0002, 'g1'),
  (NULL,  NULL,    'g2'),
  (7.89,  30.0003, 'g2'),
  (7.89,  30.0003, 'g2')

statement
CREATE TABLE pm_large(k int, v int, grp string) USING parquet

statement
INSERT INTO pm_large
SELECT
  CAST(id % 50 AS int) AS k,
  CAST(id AS int) AS v,
  CASE WHEN id < 500 THEN 'g1' ELSE 'g2' END AS grp
FROM (SELECT explode(sequence(1, 1000)) AS id)

statement
CREATE TABLE pm_boundary(i int, l bigint) USING parquet

statement
INSERT INTO pm_boundary VALUES
  (2147483647, 9223372036854775807),
  (-2147483648, -9223372036854775808),
  (0, 0),
  (1, 1),
  (1, 1)

-- ############################################################
-- BASIC partial merge patterns
-- ############################################################

-- ============================================================
-- basic: count(distinct) + non-distinct aggregates
-- ============================================================

query
SELECT count(DISTINCT i), sum(i) FROM pm_basic

query
SELECT count(DISTINCT i), count(i) FROM pm_basic

query
SELECT count(DISTINCT i), min(i), max(i) FROM pm_basic

query tolerance=1e-6
SELECT count(DISTINCT d), sum(d), avg(d) FROM pm_basic

-- ============================================================
-- basic: sum(distinct) + non-distinct aggregates
-- ============================================================

query
SELECT sum(DISTINCT i), sum(i) FROM pm_basic

query
SELECT sum(DISTINCT l), count(l) FROM pm_basic

-- ============================================================
-- basic: count(distinct) with GROUP BY
-- ============================================================

query
SELECT grp, count(DISTINCT i), sum(i) FROM pm_basic GROUP BY grp ORDER BY grp

query
SELECT grp, count(DISTINCT i) FROM pm_basic GROUP BY grp ORDER BY grp

-- ============================================================
-- basic: sum(distinct) with GROUP BY
-- ============================================================

query
SELECT grp, sum(DISTINCT i), sum(i), count(i) FROM pm_basic GROUP BY grp ORDER BY grp

-- ############################################################
-- ADVANCED partial merge patterns
-- ############################################################

-- ============================================================
-- advanced: multiple distinct aggregates on different columns
-- ============================================================

query
SELECT count(DISTINCT i), count(DISTINCT l), count(DISTINCT s) FROM pm_basic

query
SELECT count(DISTINCT i), count(DISTINCT l), sum(i) FROM pm_basic

query tolerance=1e-6
SELECT count(DISTINCT i), sum(DISTINCT l), avg(d) FROM pm_basic

-- ============================================================
-- advanced: non-count distinct + non-distinct (Expand pattern)
-- Exercises the middle PartialMerge stage for a float-producing aggregate
-- ============================================================

query tolerance=1e-6
SELECT avg(DISTINCT i), sum(i) FROM pm_basic

query tolerance=1e-6
SELECT avg(DISTINCT d), sum(d), count(d) FROM pm_basic

query
SELECT sum(DISTINCT i), max(i), min(i), count(i) FROM pm_basic

-- ============================================================
-- advanced: multi-column distinct
-- ============================================================

query
SELECT count(DISTINCT i, l), sum(i) FROM pm_basic

query
SELECT grp, count(DISTINCT i, l), count(*) FROM pm_basic GROUP BY grp ORDER BY grp

-- ============================================================
-- advanced: multiple distinct + multiple non-distinct + GROUP BY
-- ============================================================

query tolerance=1e-6
SELECT grp,
       count(DISTINCT i),
       sum(DISTINCT l),
       sum(i),
       avg(d),
       count(*)
FROM pm_basic GROUP BY grp ORDER BY grp

-- ============================================================
-- advanced: decimal partial merge (sum/avg produce decimal state)
-- ============================================================

query
SELECT count(DISTINCT v), sum(v) FROM pm_decimal

query
SELECT grp, count(DISTINCT v), sum(v), sum(w) FROM pm_decimal GROUP BY grp ORDER BY grp

query tolerance=1e-6
SELECT grp, count(DISTINCT v), avg(v) FROM pm_decimal GROUP BY grp ORDER BY grp

-- ============================================================
-- advanced: HAVING against a partial-merged aggregate
-- ============================================================

query
SELECT grp, count(DISTINCT i) AS c, sum(i) AS s
FROM pm_basic
GROUP BY grp
HAVING sum(i) > 5
ORDER BY grp

-- ============================================================
-- advanced: subquery feeds distinct + non-distinct aggregates
-- ============================================================

query
SELECT count(DISTINCT x), sum(x)
FROM (SELECT i + 1 AS x FROM pm_basic) t

-- ============================================================
-- advanced: large, multi-batch input
-- ============================================================

query
SELECT count(DISTINCT k), sum(v) FROM pm_large

query
SELECT grp, count(DISTINCT k), sum(v), count(*) FROM pm_large GROUP BY grp ORDER BY grp

-- ############################################################
-- EDGE cases
-- ############################################################

-- ============================================================
-- edge: NULLs interact with distinct + non-distinct
-- count(DISTINCT) ignores NULLs; sum/count do too
-- ============================================================

query
SELECT count(DISTINCT i), sum(i), count(i), count(*) FROM pm_nulls

query
SELECT grp, count(DISTINCT i), sum(i), count(i), count(*)
FROM pm_nulls GROUP BY grp ORDER BY grp

-- ============================================================
-- edge: empty table
-- ============================================================

query
SELECT count(DISTINCT i), sum(i), count(i) FROM pm_empty

query
SELECT grp, count(DISTINCT i), sum(i) FROM pm_empty GROUP BY grp ORDER BY grp

-- ============================================================
-- edge: single non-null row
-- ============================================================

query
SELECT count(DISTINCT i), sum(i), count(i) FROM pm_single

-- ============================================================
-- edge: single NULL row
-- ============================================================

query
SELECT count(DISTINCT i), sum(i), count(i) FROM pm_single_null

-- ============================================================
-- edge: all NULLs (per group and overall)
-- ============================================================

query
SELECT count(DISTINCT i), sum(i), count(i) FROM pm_all_nulls

query
SELECT grp, count(DISTINCT i), sum(i), count(i)
FROM pm_all_nulls GROUP BY grp ORDER BY grp

-- ============================================================
-- edge: int/long boundary values with duplicates
-- Partial merge must merge state without overflow when duplicates collapse
-- ============================================================

query
SELECT count(DISTINCT i), count(DISTINCT l) FROM pm_boundary

query
SELECT count(DISTINCT i), sum(DISTINCT i) FROM pm_boundary

-- ============================================================
-- edge: DISTINCT inside expression + non-distinct alongside
-- ============================================================

query
SELECT count(DISTINCT i + 1), sum(i) FROM pm_basic

query
SELECT grp, count(DISTINCT IF(i % 2 = 0, NULL, i)), sum(i)
FROM pm_basic GROUP BY grp ORDER BY grp

-- ############################################################
-- FALLBACK cases
-- ############################################################

-- FIRST/LAST aggregates in PartialMerge mode are order-dependent and
-- DataFusion's hash table may process rows in a different order than Spark's.
-- See https://github.com/apache/datafusion-comet/issues/4131

-- ============================================================
-- fallback: first + distinct count triggers PartialMerge on first
-- ============================================================

query expect_fallback(PartialMerge not supported for aggregates: first)
SELECT first(i), count(DISTINCT i) FROM pm_basic

query expect_fallback(PartialMerge not supported for aggregates: first)
SELECT grp, first(i), count(DISTINCT i) FROM pm_basic GROUP BY grp ORDER BY grp

-- ============================================================
-- fallback: last + distinct count triggers PartialMerge on last
-- ============================================================

query expect_fallback(PartialMerge not supported for aggregates: last)
SELECT last(i), count(DISTINCT i) FROM pm_basic

query expect_fallback(PartialMerge not supported for aggregates: last)
SELECT grp, last(i), count(DISTINCT i) FROM pm_basic GROUP BY grp ORDER BY grp

-- ============================================================
-- fallback: first and last together with distinct
-- ============================================================

query expect_fallback(PartialMerge not supported for aggregates: first, last)
SELECT first(i), last(i), count(DISTINCT i) FROM pm_basic

query expect_fallback(PartialMerge not supported for aggregates: first, last)
SELECT grp, first(i), last(i), count(DISTINCT i), sum(i)
FROM pm_basic GROUP BY grp ORDER BY grp

-- ============================================================
-- fallback: first/last IGNORE NULLS with distinct
-- ============================================================

query expect_fallback(PartialMerge not supported for aggregates: first)
SELECT grp, first(i) IGNORE NULLS, count(DISTINCT i)
FROM pm_nulls GROUP BY grp ORDER BY grp

query expect_fallback(PartialMerge not supported for aggregates: last)
SELECT grp, last(i) IGNORE NULLS, count(DISTINCT i)
FROM pm_nulls GROUP BY grp ORDER BY grp
