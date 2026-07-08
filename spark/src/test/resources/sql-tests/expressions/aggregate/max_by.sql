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

-- max_by(x, y) returns the value of x associated with the maximum value of y.
--
-- The value (x) must be a fixed-length type: Spark only uses HashAggregate (the aggregate
-- operator Comet accelerates) when the aggregation buffer is mutable, so variable-length
-- value types such as string force SortAggregate and fall back to Spark.
--
-- Ordering values are kept unique within each group so results are deterministic (max_by is
-- non-deterministic when several rows tie on the maximum ordering).

-- ============================================================
-- Setup: tables
-- ============================================================

statement
CREATE TABLE mb_src(v int, ord int, grp string) USING parquet

statement
INSERT INTO mb_src VALUES
  (10, 10, 'g1'), (20, 50, 'g1'), (30, 20, 'g1'),
  (40, 40, 'g2'), (50, 5,  'g2'), (60, 30, 'g2'),
  (70, 99, 'g3')

-- ordering NULLs are ignored; a group of all-NULL orderings yields NULL
statement
CREATE TABLE mb_nulls(v int, ord int, grp string) USING parquet

statement
INSERT INTO mb_nulls VALUES
  (1, 10,   'g1'), (2, NULL, 'g1'), (3, 5, 'g1'),
  (4, NULL, 'g2'), (5, NULL, 'g2'),
  (6, 7,    'g3'), (NULL, 100, 'g3')

statement
CREATE TABLE mb_empty(v int, ord int) USING parquet

-- ============================================================
-- Global aggregate (no GROUP BY)
-- ============================================================

query
SELECT max_by(v, ord) FROM mb_src

-- ============================================================
-- GROUP BY
-- ============================================================

query
SELECT grp, max_by(v, ord) FROM mb_src GROUP BY grp ORDER BY grp

-- ============================================================
-- NULL handling: NULL orderings ignored; the value paired with the
-- maximum ordering may itself be NULL; all-NULL orderings yield NULL
-- ============================================================

query
SELECT grp, max_by(v, ord) FROM mb_nulls GROUP BY grp ORDER BY grp

-- ============================================================
-- Empty table yields NULL
-- ============================================================

query
SELECT max_by(v, ord) FROM mb_empty

-- ============================================================
-- Literal arguments (evaluated natively; constant folding is disabled)
-- ============================================================

query
SELECT max_by(5, 10), max_by(CAST(NULL AS INT), 20)

-- ============================================================
-- Mixed with other aggregates
-- ============================================================

query
SELECT grp, max_by(v, ord), count(*), max(ord)
FROM mb_src GROUP BY grp ORDER BY grp

-- ============================================================
-- BigInt value
-- ============================================================

statement
CREATE TABLE mb_long(val bigint, ord int, grp string) USING parquet

statement
INSERT INTO mb_long VALUES
  (1000000000000, 1, 'a'), (2000000000000, 3, 'a'), (3000000000000, 2, 'a'),
  (4000000000000, 5, 'b'), (5000000000000, 4, 'b')

query
SELECT grp, max_by(val, ord) FROM mb_long GROUP BY grp ORDER BY grp

-- ============================================================
-- Double value and double ordering (NaN is the maximum in Spark)
-- ============================================================

statement
CREATE TABLE mb_dbl(v double, ord double, grp string) USING parquet

statement
INSERT INTO mb_dbl VALUES
  (1.1, 1.5, 'g1'), (2.2, 2.5, 'g1'), (3.3, 0.5, 'g1'),
  (4.4, 1.0, 'g2'), (5.5, CAST('NaN' AS DOUBLE), 'g2'), (6.6, 100.0, 'g2'),
  (7.7, CAST('Infinity' AS DOUBLE), 'g3'), (8.8, 3.0, 'g3')

query
SELECT grp, max_by(v, ord) FROM mb_dbl GROUP BY grp ORDER BY grp

-- ============================================================
-- Decimal value and decimal ordering
-- ============================================================

statement
CREATE TABLE mb_dec(v decimal(10,2), ord decimal(10,2), grp string) USING parquet

statement
INSERT INTO mb_dec VALUES
  (10.01, 1.50, 'g1'), (20.02, 9.99, 'g1'), (30.03, 5.00, 'g1'),
  (40.04, 2.00, 'g2'), (50.05, 8.25, 'g2')

query
SELECT grp, max_by(v, ord) FROM mb_dec GROUP BY grp ORDER BY grp

-- ============================================================
-- Date / timestamp value and ordering
-- ============================================================

statement
CREATE TABLE mb_dt(d date, ts timestamp, ord int, grp string) USING parquet

statement
INSERT INTO mb_dt VALUES
  (DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00', 1, 'g1'),
  (DATE '2024-06-15', TIMESTAMP '2024-06-15 12:30:00', 3, 'g1'),
  (DATE '2023-12-31', TIMESTAMP '2023-12-31 23:59:59', 2, 'g1'),
  (DATE '2024-03-01', TIMESTAMP '2024-03-01 08:00:00', 1, 'g2')

query
SELECT grp, max_by(d, ord) FROM mb_dt GROUP BY grp ORDER BY grp

query
SELECT grp, max_by(ts, ord) FROM mb_dt GROUP BY grp ORDER BY grp

-- ============================================================
-- Date / timestamp as the ordering column
-- ============================================================

query
SELECT grp, max_by(ord, d) FROM mb_dt GROUP BY grp ORDER BY grp

query
SELECT grp, max_by(ord, ts) FROM mb_dt GROUP BY grp ORDER BY grp

-- ============================================================
-- Negative and boundary ordering values
-- ============================================================

statement
CREATE TABLE mb_bound(v int, ord bigint, grp string) USING parquet

statement
INSERT INTO mb_bound VALUES
  (1, -100,                 'g1'), (2, -5,                  'g1'), (3, -50, 'g1'),
  (4, -9223372036854775808, 'g2'), (5, 9223372036854775807, 'g2'), (6, 0, 'g2'),
  (7, -2147483648,          'g3'), (8, 2147483647,          'g3')

query
SELECT grp, max_by(v, ord) FROM mb_bound GROUP BY grp ORDER BY grp

-- ============================================================
-- Double ordering with -Infinity and Infinity
-- ============================================================

statement
CREATE TABLE mb_inf(v int, ord double, grp string) USING parquet

statement
INSERT INTO mb_inf VALUES
  (1, CAST('-Infinity' AS DOUBLE), 'g1'), (2, -1.0, 'g1'), (3, CAST('Infinity' AS DOUBLE), 'g1'),
  (4, CAST('-Infinity' AS DOUBLE), 'g2'), (5, -2.0, 'g2')

query
SELECT grp, max_by(v, ord) FROM mb_inf GROUP BY grp ORDER BY grp

-- ============================================================
-- Multiple max_by in one query
-- ============================================================

query
SELECT grp, max_by(v, ord), max_by(ord, v) FROM mb_src GROUP BY grp ORDER BY grp

-- ============================================================
-- Value and ordering are the same column
-- ============================================================

query
SELECT grp, max_by(ord, ord) FROM mb_src GROUP BY grp ORDER BY grp

-- ============================================================
-- Boolean value
-- ============================================================

statement
CREATE TABLE mb_bool(v boolean, ord int, grp string) USING parquet

statement
INSERT INTO mb_bool VALUES
  (true, 1, 'a'), (false, 3, 'a'), (true, 2, 'a'),
  (false, 5, 'b'), (true, 4, 'b')

query
SELECT grp, max_by(v, ord) FROM mb_bool GROUP BY grp ORDER BY grp
