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
-- Setup
-- ============================================================

statement
CREATE TABLE k_dbl(v double, grp string) USING parquet

statement
INSERT INTO k_dbl VALUES
  (-10.0, 'g1'), (-20.0, 'g1'), (100.0, 'g1'), (1000.0, 'g1'),
  (1.0,   'g2'), (10.0,  'g2'), (100.0, 'g2'), (10.0,  'g2'), (1.0, 'g2'),
  (42.0,  'g3'),
  (NULL,  'g4'), (NULL,  'g4'),
  (7.0,   'g5'), (7.0,   'g5'), (7.0,   'g5')

statement
CREATE TABLE k_int(v int, grp string) USING parquet

statement
INSERT INTO k_int VALUES
  (1, 'g1'), (10, 'g1'), (100, 'g1'), (10, 'g1'), (1, 'g1'),
  (NULL, 'g2'), (5, 'g2')

statement
CREATE TABLE k_dec(v decimal(10,2), grp string) USING parquet

statement
INSERT INTO k_dec VALUES
  (1.50, 'g1'), (2.50, 'g1'), (3.50, 'g1'), (4.50, 'g1')

statement
CREATE TABLE k_empty(v double) USING parquet

statement
CREATE TABLE k_spark_ex1(v double) USING parquet

statement
INSERT INTO k_spark_ex1 VALUES (-10.0), (-20.0), (100.0), (1000.0)

statement
CREATE TABLE k_spark_ex2(v double) USING parquet

statement
INSERT INTO k_spark_ex2 VALUES (1.0), (10.0), (100.0), (10.0), (1.0)

statement
CREATE TABLE k_single(v double) USING parquet

statement
INSERT INTO k_single VALUES (42.0)

statement
CREATE TABLE k_const(v double) USING parquet

statement
INSERT INTO k_const VALUES (7.0), (7.0), (7.0)

statement
CREATE TABLE k_lit(x int) USING parquet

statement
INSERT INTO k_lit VALUES (1)

-- ============================================================
-- Spark's own example: matches -0.7014368047529627.
-- ============================================================

query
SELECT kurtosis(v) FROM k_spark_ex1

-- Spark's second example: matches 0.19432323191699075.
query
SELECT kurtosis(v) FROM k_spark_ex2

-- ============================================================
-- GROUP BY over doubles: covers a "normal" group (g1), a heavier
-- group (g2), a single-value group (g3, m2=0 => NULL by default),
-- an all-NULL group (g4 => NULL), and constants (g5, m2=0).
-- ============================================================

query
SELECT grp, kurtosis(v) FROM k_dbl GROUP BY grp ORDER BY grp

-- ============================================================
-- Global aggregate (no GROUP BY).
-- ============================================================

query
SELECT kurtosis(v) FROM k_dbl

-- Empty table returns NULL.
query
SELECT kurtosis(v) FROM k_empty

-- ============================================================
-- Integer input: promoted to Double by Spark's ImplicitCastInputTypes.
-- ============================================================

query
SELECT grp, kurtosis(v) FROM k_int GROUP BY grp ORDER BY grp

-- ============================================================
-- Decimal input.
-- ============================================================

query
SELECT grp, kurtosis(v) FROM k_dec GROUP BY grp ORDER BY grp

-- ============================================================
-- Literal argument (constant folded; still exercises planning).
-- ============================================================

query
SELECT kurtosis(1.0) FROM k_lit

query
SELECT kurtosis(NULL) FROM k_lit

-- ============================================================
-- Divide-by-zero cases under default (nullOnDivideByZero=true):
-- single-value and all-equal groups both yield NULL. See
-- kurtosis_legacy.sql for the `legacyStatisticalAggregate=true`
-- variant that returns NaN instead.
-- ============================================================

query
SELECT kurtosis(v) FROM k_single

query
SELECT kurtosis(v) FROM k_const

-- ============================================================
-- FILTER (WHERE ...) — Partial only carries the filter.
-- ============================================================

query
SELECT grp, kurtosis(v) FILTER (WHERE v > 0) FROM k_dbl GROUP BY grp ORDER BY grp

-- ============================================================
-- Skewness is Spark's sibling in CentralMomentAgg; we don't
-- implement it here, so it should fall back. (This documents the
-- boundary; if we add skewness later, the expect_fallback
-- becomes a plain query.)
-- ============================================================

query expect_fallback(unsupported Spark aggregate function: skewness)
SELECT skewness(v) FROM k_dbl

-- ============================================================
-- Additional coverage requested by the audit.
-- ============================================================

statement
CREATE TABLE k_float(v float, grp string) USING parquet

statement
INSERT INTO k_float VALUES
  (CAST(1.0 AS FLOAT), 'g1'), (CAST(10.0 AS FLOAT), 'g1'),
  (CAST(100.0 AS FLOAT), 'g1'), (CAST(10.0 AS FLOAT), 'g1'),
  (CAST(1.0 AS FLOAT), 'g1')

statement
CREATE TABLE k_long(v bigint, grp string) USING parquet

statement
INSERT INTO k_long VALUES
  (1, 'g1'), (10, 'g1'), (100, 'g1'), (10, 'g1'), (1, 'g1')

statement
CREATE TABLE k_wnd(k int, part string, v double) USING parquet

statement
INSERT INTO k_wnd VALUES
  (1, 'p1', 1.0), (2, 'p1', 1.0), (3, 'p1', 2.0), (4, 'p1', 2.0),
  (5, 'p1', 3.0), (6, 'p1', 3.0), (7, 'p1', 3.0),
  (8, 'p2', 1.0), (9, 'p2', 2.0), (10, 'p2', 5.0)

-- Float input: promoted to Double by Spark's ImplicitCastInputTypes.
query
SELECT grp, kurtosis(v) FROM k_float GROUP BY grp ORDER BY grp

-- BigInt input.
query
SELECT grp, kurtosis(v) FROM k_long GROUP BY grp ORDER BY grp

-- Mixed with other CentralMomentAgg siblings in one query.
query
SELECT kurtosis(v), avg(v), stddev(v), count(*) FROM k_dbl WHERE v IS NOT NULL

-- Window use: matches Spark's `DataFrameWindowFunctionsSuite`
-- "skewness and kurtosis functions in window" test. Comet's window
-- path doesn't wire `kurtosis` as a window aggregate today, so this
-- falls back to Spark.
query expect_fallback(is not supported for window function)
SELECT k,
       kurtosis(v) OVER (PARTITION BY part ORDER BY k
                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM k_wnd ORDER BY k

-- ============================================================
-- Numerical stress: NaN/Infinity/-Infinity in a group. Spark
-- propagates these through arithmetic, so the aggregate is
-- expected to produce NaN or NULL rather than a finite value.
-- Use spark_answer_only because our Welford recurrence produces
-- byte-identical NaN payloads with a different bit pattern from
-- Spark's DeclarativeAggregate compilation; result-value equality
-- via Spark's own comparison is what the harness needs to see.
-- ============================================================

statement
CREATE TABLE k_nan(v double) USING parquet

statement
INSERT INTO k_nan VALUES
  (1.0), (2.0), (CAST('NaN' AS DOUBLE)), (3.0)

statement
CREATE TABLE k_inf(v double) USING parquet

statement
INSERT INTO k_inf VALUES
  (1.0), (2.0), (CAST('Infinity' AS DOUBLE)), (3.0)

statement
CREATE TABLE k_neg_inf(v double) USING parquet

statement
INSERT INTO k_neg_inf VALUES
  (1.0), (2.0), (CAST('-Infinity' AS DOUBLE)), (3.0)

query spark_answer_only
SELECT kurtosis(v) FROM k_nan

query spark_answer_only
SELECT kurtosis(v) FROM k_inf

query spark_answer_only
SELECT kurtosis(v) FROM k_neg_inf

-- ============================================================
-- Large-magnitude inputs: Welford is numerically stable but the
-- final `n * m4 / (m2 * m2) - 3.0` can still differ from Spark's
-- codegen at high magnitudes. Use spark_answer_only.
-- ============================================================

statement
CREATE TABLE k_big(v double) USING parquet

statement
INSERT INTO k_big VALUES
  (1.0e15), (2.0e15), (3.0e15), (4.0e15), (5.0e15)

query spark_answer_only
SELECT kurtosis(v) FROM k_big
