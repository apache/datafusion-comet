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

-- Expressions with float input, over the edge values (one column) or every ordered pair of
-- them (two columns). Each follows one of Spark's float equality rules, which the contributor
-- guide lists (contributor-guide/floating_point.md). The sweep tracks #6385.
--
-- The e and p CTEs negate the flagged stored values to make the sign-bit NaN at query time
-- (see comparison.sql).
--
-- hash and xxhash64 of a float, an array and a struct are in expressions/hash/hash.sql.
-- collect_set is in aggregates_collect_set_4_2.sql.
--
-- ConstantFolding is on so that the IN query reaches Comet with a folded literal, as Spark
-- plans it.

-- ConstantFolding: enabled

statement
CREATE TABLE fs_edge_double(id INT, c DOUBLE, neg BOOLEAN) USING parquet

statement
INSERT INTO fs_edge_double VALUES
  (1, 0.0D, false),
  (2, double('-0.0'), false),
  (3, double('NaN'), false),
  (4, double('NaN'), true),
  (5, 1.0D, false),
  (6, -1.0D, false),
  (7, double('Infinity'), false),
  (8, double('-Infinity'), false),
  (9, NULL, false)

statement
CREATE TABLE fs_pair_double USING parquet AS
SELECT a.id * 10 + b.id AS id, a.c AS l, b.c AS r, a.neg AS lneg, b.neg AS rneg
FROM fs_edge_double a CROSS JOIN fs_edge_double b

statement
CREATE TABLE fs_edge_float(id INT, c FLOAT, neg BOOLEAN) USING parquet

statement
INSERT INTO fs_edge_float VALUES
  (1, float(0.0), false),
  (2, float('-0.0'), false),
  (3, float('NaN'), false),
  (4, float('NaN'), true),
  (5, float(1.0), false),
  (6, float(-1.0), false),
  (7, float('Infinity'), false),
  (8, float('-Infinity'), false),
  (9, NULL, false)

statement
CREATE TABLE fs_pair_float USING parquet AS
SELECT a.id * 10 + b.id AS id, a.c AS l, b.c AS r, a.neg AS lneg, b.neg AS rneg
FROM fs_edge_float a CROSS JOIN fs_edge_float b

-- abs, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, abs(c) FROM e

-- abs, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, abs(c) FROM e

-- negative, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, -c FROM e

-- negative, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, -c FROM e

-- signum, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, signum(c) FROM e

-- signum, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, signum(c) FROM e

-- ceil, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, ceil(c) FROM e

-- ceil, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, ceil(c) FROM e

-- floor, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, floor(c) FROM e

-- floor, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, floor(c) FROM e

-- round, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, round(c, 1), bround(c, 1) FROM e

-- round, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, round(c, 1), bround(c, 1) FROM e

-- sqrt, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, sqrt(c) FROM e

-- sqrt, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, sqrt(c) FROM e

-- isnan, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, isnan(c) FROM e

-- isnan, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, isnan(c) FROM e

-- cast to string, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, CAST(c AS STRING) FROM e

-- cast to string, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, CAST(c AS STRING) FROM e

-- cast to integral, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, try_cast(c AS INT), try_cast(c AS BIGINT) FROM e

-- cast to integral, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, try_cast(c AS INT), try_cast(c AS BIGINT) FROM e

-- cast to decimal, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, CAST(c AS DECIMAL(10, 2)) FROM e

-- cast to decimal, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, CAST(c AS DECIMAL(10, 2)) FROM e

-- cast to boolean, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, CAST(c AS BOOLEAN) FROM e

-- cast to boolean, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, CAST(c AS BOOLEAN) FROM e

-- cast between float types, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, CAST(c AS DOUBLE), CAST(c AS FLOAT) FROM e

-- cast between float types, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, CAST(c AS DOUBLE), CAST(c AS FLOAT) FROM e

-- cast array to string, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, CAST(array(c) AS STRING) FROM e

-- cast array to string, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, CAST(array(c) AS STRING) FROM e

-- greatest, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, greatest(l, r) FROM p

-- greatest, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, greatest(l, r) FROM p

-- least, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, least(l, r) FROM p

-- least, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, least(l, r) FROM p

-- nanvl, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, nanvl(l, r) FROM p

-- nanvl, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, nanvl(l, r) FROM p

-- IN with column list, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, l IN (r, CAST('2.0' AS DOUBLE)) FROM p

-- IN with column list, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, l IN (r, CAST('2.0' AS FLOAT)) FROM p

-- CASE value WHEN, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, CASE l WHEN r THEN 1 ELSE 0 END FROM p

-- CASE value WHEN, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, CASE l WHEN r THEN 1 ELSE 0 END FROM p

-- array_contains, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array_contains(array(l, 2), r) FROM p

-- array_contains, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array_contains(array(l, 2), r) FROM p

-- array_position, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array_position(array(2, l), r) FROM p

-- array_position, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array_position(array(2, l), r) FROM p

-- array_remove, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array_remove(array(l, 2), r) FROM p

-- array_remove, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array_remove(array(l, 2), r) FROM p

-- arrays_overlap, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, arrays_overlap(array(l), array(r)) FROM p

-- arrays_overlap, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, arrays_overlap(array(l), array(r)) FROM p

-- array_distinct, DOUBLE
-- Whether float elements run natively depends on the Spark release (array_distinct.sql), so only
-- the answer is checked.
query spark_answer_only
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array_distinct(array(l, r)) FROM p

-- array_distinct, FLOAT
query spark_answer_only
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array_distinct(array(l, r)) FROM p

-- array_union, DOUBLE
-- Whether float elements run natively depends on the Spark release (array_union.sql), so only the
-- answer is checked.
query spark_answer_only
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array_union(array(l), array(r)) FROM p

-- array_union, FLOAT
query spark_answer_only
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array_union(array(l), array(r)) FROM p

-- array_intersect, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array_intersect(array(l), array(r)) FROM p

-- array_intersect, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array_intersect(array(l), array(r)) FROM p

-- array_except, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array_except(array(l), array(r)) FROM p

-- array_except, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array_except(array(l), array(r)) FROM p

-- sort_array, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, sort_array(array(l, r, 2)) FROM p

-- sort_array, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, sort_array(array(l, r, 2)) FROM p

-- sort_array descending, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, sort_array(array(l, r, 2), false) FROM p

-- sort_array descending, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, sort_array(array(l, r, 2), false) FROM p

-- array_sort, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array_sort(array(l, r, 2)) FROM p

-- array_sort, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array_sort(array(l, r, 2)) FROM p

-- array_max, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array_max(array(l, r)) FROM p

-- array_max, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array_max(array(l, r)) FROM p

-- array_min, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array_min(array(l, r)) FROM p

-- array_min, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array_min(array(l, r)) FROM p

-- map lookup, DOUBLE
-- A map with a float key falls back to Spark (map_builders_float_key_gate.sql), so only the answer
-- is checked.
query spark_answer_only
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, map(l, 1)[r], element_at(map(l, 1), r) FROM p WHERE l IS NOT NULL

-- map lookup, FLOAT
query spark_answer_only
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, map(l, 1)[r], element_at(map(l, 1), r) FROM p WHERE l IS NOT NULL

-- map_contains_key, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, map_contains_key(map(l, 1), r) FROM p WHERE l IS NOT NULL

-- map_contains_key, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, map_contains_key(map(l, 1), r) FROM p WHERE l IS NOT NULL
