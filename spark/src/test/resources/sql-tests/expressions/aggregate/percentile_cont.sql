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

-- Tests for percentile_cont aggregate function
-- Uses similar test data as Spark's percentiles.sql

statement
CREATE TABLE test_percentile(k int, v int) USING parquet

statement
INSERT INTO test_percentile VALUES (0, 0), (0, 10), (0, 20), (0, 30), (0, 40), (1, 10), (1, 20), (2, 10), (2, 20), (2, 25), (2, 30), (3, 60), (4, NULL)

-- Basic percentile_cont (25th percentile) - should match Spark result: 10.0
query
SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY v) FROM test_percentile

-- percentile_cont with DESC ordering - should match Spark result: 30.0
query
SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC) FROM test_percentile

-- percentile_cont with GROUP BY - should match Spark results
query
SELECT k, percentile_cont(0.25) WITHIN GROUP (ORDER BY v) FROM test_percentile GROUP BY k ORDER BY k

-- percentile_cont with GROUP BY and DESC - should match Spark results
query
SELECT k, percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC) FROM test_percentile GROUP BY k ORDER BY k

-- median (50th percentile)
query
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM test_percentile

-- Multiple percentile_cont in same query
query
SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY v), percentile_cont(0.75) WITHIN GROUP (ORDER BY v) FROM test_percentile

-- ============================================================
-- Tests for negative values (sort order correctness is critical)
-- ============================================================

statement
CREATE TABLE test_negative(k int, v int) USING parquet

statement
INSERT INTO test_negative VALUES (0, -50), (0, -20), (0, 0), (0, 10), (0, 30), (1, -100), (1, -50), (1, 50), (1, 100)

-- Negative values with ASC ordering
query
SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY v) FROM test_negative

-- Negative values with DESC ordering
query
SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC) FROM test_negative

-- Negative values with GROUP BY
query
SELECT k, percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM test_negative GROUP BY k ORDER BY k

-- Negative values median
query
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM test_negative

-- ============================================================
-- Tests for boundary percentiles (0.0 and 1.0)
-- ============================================================

-- 0th percentile (minimum value)
query
SELECT percentile_cont(0.0) WITHIN GROUP (ORDER BY v) FROM test_percentile

-- 100th percentile (maximum value)
query
SELECT percentile_cont(1.0) WITHIN GROUP (ORDER BY v) FROM test_percentile

-- Boundary percentiles with negative values
query
SELECT percentile_cont(0.0) WITHIN GROUP (ORDER BY v), percentile_cont(1.0) WITHIN GROUP (ORDER BY v) FROM test_negative

-- Boundary percentiles with GROUP BY
query
SELECT k, percentile_cont(0.0) WITHIN GROUP (ORDER BY v), percentile_cont(1.0) WITHIN GROUP (ORDER BY v) FROM test_negative GROUP BY k ORDER BY k

-- ============================================================
-- Tests for all-null groups and single-value groups
-- ============================================================

statement
CREATE TABLE test_edge_cases(k int, v int) USING parquet

statement
INSERT INTO test_edge_cases VALUES (0, NULL), (0, NULL), (1, 42), (2, 10), (2, 10), (2, 10)

-- All-null group should return NULL
query
SELECT k, percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM test_edge_cases GROUP BY k ORDER BY k

-- Single value group
query
SELECT k, percentile_cont(0.25) WITHIN GROUP (ORDER BY v) FROM test_edge_cases WHERE k = 1 GROUP BY k

-- All same values in group
query
SELECT k, percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM test_edge_cases WHERE k = 2 GROUP BY k

-- Empty result (no rows match)
query
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM test_edge_cases WHERE k = 999

-- ============================================================
-- Tests for DOUBLE column type
-- ============================================================

statement
CREATE TABLE test_double(k int, v double) USING parquet

statement
INSERT INTO test_double VALUES (0, -1.5), (0, 0.0), (0, 1.5), (0, 3.0), (1, -100.25), (1, 0.5), (1, 100.75), (2, NULL)

-- Double values basic
query
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM test_double

-- Double values with GROUP BY
query
SELECT k, percentile_cont(0.25) WITHIN GROUP (ORDER BY v) FROM test_double GROUP BY k ORDER BY k

-- Double boundary percentiles
query
SELECT percentile_cont(0.0) WITHIN GROUP (ORDER BY v), percentile_cont(1.0) WITHIN GROUP (ORDER BY v) FROM test_double WHERE k = 0

-- Double with DESC ordering
query
SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC) FROM test_double WHERE k = 1

-- ============================================================
-- Tests for FLOAT column type
-- ============================================================

statement
CREATE TABLE test_float(k int, v float) USING parquet

statement
INSERT INTO test_float VALUES (0, -2.5), (0, -0.5), (0, 0.5), (0, 2.5), (1, -50.0), (1, 0.0), (1, 50.0)

-- Float values basic
query
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM test_float

-- Float values with GROUP BY
query
SELECT k, percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM test_float GROUP BY k ORDER BY k

-- Float boundary percentiles
query
SELECT percentile_cont(0.0) WITHIN GROUP (ORDER BY v), percentile_cont(1.0) WITHIN GROUP (ORDER BY v) FROM test_float

-- Float with negative values and DESC
query
SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY v DESC) FROM test_float WHERE k = 0
