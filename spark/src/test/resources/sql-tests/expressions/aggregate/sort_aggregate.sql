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

-- Disabling ObjectHashAggregate forces Spark to plan SortAggregateExec for the
-- TypedImperativeAggregate function collect_set, which is the operator this file exercises.
-- (The plan-shape assertion that a SortAggregateExec is actually produced lives in
-- CometAggregateSuite; here the default `query` mode asserts the whole plan, including the
-- pre-aggregate sorts and the Comet shuffle, runs natively and matches Spark.)
-- collect_set returns elements in nondeterministic order, so every result is wrapped in
-- sort_array for a stable comparison against Spark.
-- Config: spark.sql.execution.useObjectHashAggregateExec=false
-- Config: spark.comet.exec.strictFloatingPoint=true

-- ============================================================
-- Setup: tables
-- ============================================================

statement
CREATE TABLE sa_int(i int, g string) USING parquet

statement
INSERT INTO sa_int VALUES
  (1, 'a'), (2, 'a'), (1, 'a'), (3, 'a'),
  (4, 'b'), (4, 'b'), (NULL, 'b'), (5, 'b'),
  (NULL, 'c'), (NULL, 'c')

statement
CREATE TABLE sa_multikey(a int, k1 string, k2 int) USING parquet

statement
INSERT INTO sa_multikey VALUES
  (1, 'x', 10), (2, 'x', 10), (1, 'x', 10),
  (3, 'x', 20), (4, 'y', 10), (NULL, 'y', 10)

statement
CREATE TABLE sa_nulls(v int, g string) USING parquet

statement
INSERT INTO sa_nulls VALUES (NULL, 'a'), (NULL, 'a'), (NULL, 'b'), (1, 'b')

statement
CREATE TABLE sa_empty(v int, g string) USING parquet

statement
CREATE TABLE sa_single(v int) USING parquet

statement
INSERT INTO sa_single VALUES (42)

-- ============================================================
-- Global aggregate (no GROUP BY): single SortAggregate group
-- ============================================================

query
SELECT sort_array(collect_set(i)) FROM sa_int

-- ============================================================
-- Single grouping key
-- ============================================================

query
SELECT g, sort_array(collect_set(i)) FROM sa_int GROUP BY g ORDER BY g

-- ============================================================
-- Multiple grouping keys (sort ordering spans both keys)
-- ============================================================

query
SELECT k1, k2, sort_array(collect_set(a))
FROM sa_multikey GROUP BY k1, k2 ORDER BY k1, k2

-- ============================================================
-- Grouping key derived from an expression
-- ============================================================

query
SELECT upper(g) AS gg, sort_array(collect_set(i))
FROM sa_int GROUP BY upper(g) ORDER BY gg

-- ============================================================
-- All-NULL group returns an empty array
-- ============================================================

query
SELECT g, sort_array(collect_set(v)) FROM sa_nulls GROUP BY g ORDER BY g

-- ============================================================
-- Empty table
-- ============================================================

query
SELECT sort_array(collect_set(v)) FROM sa_empty

query
SELECT g, sort_array(collect_set(v)) FROM sa_empty GROUP BY g ORDER BY g

-- ============================================================
-- Single row
-- ============================================================

query
SELECT sort_array(collect_set(v)) FROM sa_single

-- ============================================================
-- collect_set mixed with hashable aggregates: collect_set forces the
-- whole aggregate onto SortAggregate, so count/sum/min/max ride along
-- ============================================================

query
SELECT g, sort_array(collect_set(i)), count(*), count(i), sum(i), min(i), max(i)
FROM sa_int GROUP BY g ORDER BY g

-- ============================================================
-- Multiple collect_set in one query
-- ============================================================

query
SELECT k1, sort_array(collect_set(a)), sort_array(collect_set(k2))
FROM sa_multikey GROUP BY k1 ORDER BY k1

-- ============================================================
-- DISTINCT (distinct-aggregate planner path)
-- ============================================================

query
SELECT g, sort_array(collect_set(DISTINCT i)) FROM sa_int GROUP BY g ORDER BY g

-- ============================================================
-- HAVING over a collect_set-derived predicate
-- ============================================================

query
SELECT g, sort_array(collect_set(i))
FROM sa_int GROUP BY g HAVING size(collect_set(i)) > 1 ORDER BY g

-- ============================================================
-- Representative data types through the SortAggregate path
-- ============================================================

statement
CREATE TABLE sa_string(v string, g string) USING parquet

statement
INSERT INTO sa_string VALUES
  ('hello', 'a'), ('world', 'a'), ('hello', 'a'), (NULL, 'a'),
  ('', 'b'), ('x', 'b'), ('', 'b'), (NULL, 'b')

query
SELECT g, sort_array(collect_set(v)) FROM sa_string GROUP BY g ORDER BY g

statement
CREATE TABLE sa_bigint(v bigint, g string) USING parquet

statement
INSERT INTO sa_bigint VALUES
  (1000000000000, 'a'), (2000000000000, 'a'), (1000000000000, 'a'), (NULL, 'a'),
  (3000000000000, 'b'), (NULL, 'b')

query
SELECT g, sort_array(collect_set(v)) FROM sa_bigint GROUP BY g ORDER BY g

statement
CREATE TABLE sa_decimal(v decimal(10,2), g string) USING parquet

statement
INSERT INTO sa_decimal VALUES
  (1.50, 'a'), (2.50, 'a'), (1.50, 'a'), (NULL, 'a'),
  (0.00, 'b'), (99999999.99, 'b'), (NULL, 'b')

query
SELECT g, sort_array(collect_set(v)) FROM sa_decimal GROUP BY g ORDER BY g

statement
CREATE TABLE sa_date(v date, g string) USING parquet

statement
INSERT INTO sa_date VALUES
  (DATE '2024-01-01', 'a'), (DATE '2024-06-15', 'a'), (DATE '2024-01-01', 'a'), (NULL, 'a'),
  (DATE '1970-01-01', 'b'), (NULL, 'b')

query
SELECT g, sort_array(collect_set(v)) FROM sa_date GROUP BY g ORDER BY g

-- ============================================================
-- Unsupported aggregate input under the SortAggregate path:
-- with strictFloatingPoint=true, collect_set on floating-point types is
-- incompatible, so the SortAggregateExec must fall back to Spark cleanly
-- rather than crash.
-- ============================================================

statement
CREATE TABLE sa_double(v double, g string) USING parquet

statement
INSERT INTO sa_double VALUES
  (1.1, 'a'), (2.2, 'a'), (1.1, 'a'), (NULL, 'a'),
  (CAST('NaN' AS DOUBLE), 'b'), (CAST('NaN' AS DOUBLE), 'b'), (1.0, 'b')

query expect_fallback(not fully compatible with Spark)
SELECT g, sort_array(collect_set(v)) FROM sa_double GROUP BY g ORDER BY g
