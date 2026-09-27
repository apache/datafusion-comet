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

-- Comet's `mode` is opt-in via allowIncompatible because Spark breaks ties non-deterministically.
-- Every compared query below has a single value with the strictly-highest frequency per group so
-- that Comet's smallest-value tie-break agrees with Spark's arbitrary choice.
-- Config: spark.comet.expression.Mode.allowIncompatible=true

-- ============================================================
-- Setup: tables
-- ============================================================

statement
CREATE TABLE mode_int(v int, grp string) USING parquet

statement
INSERT INTO mode_int VALUES
  (10, 'a'), (10, 'a'), (7, 'a'), (NULL, 'a'),
  (5, 'b'), (5, 'b'), (5, 'b'), (9, 'b'), (NULL, 'b'),
  (NULL, 'c'), (NULL, 'c')

statement
CREATE TABLE mode_all_null(v int) USING parquet

statement
INSERT INTO mode_all_null VALUES (NULL), (NULL)

-- ============================================================
-- Global aggregate (no GROUP BY): unique mode
-- ============================================================

query
SELECT mode(v) FROM mode_int

-- ============================================================
-- GROUP BY: unique mode per group; NULLs ignored
-- ============================================================

query
SELECT grp, mode(v) FROM mode_int GROUP BY grp ORDER BY grp

-- ============================================================
-- All-NULL input returns NULL
-- ============================================================

query
SELECT mode(v) FROM mode_all_null

-- ============================================================
-- Mixed with other aggregates
-- ============================================================

query
SELECT grp, mode(v), count(*), sum(v) FROM mode_int GROUP BY grp ORDER BY grp

-- ============================================================
-- HAVING clause
-- ============================================================

query
SELECT grp, mode(v) FROM mode_int GROUP BY grp HAVING count(v) > 3 ORDER BY grp

-- ============================================================
-- Boolean
-- ============================================================

statement
CREATE TABLE mode_bool(v boolean, grp string) USING parquet

statement
INSERT INTO mode_bool VALUES
  (true, 'a'), (true, 'a'), (false, 'a'), (NULL, 'a'),
  (false, 'b'), (false, 'b'), (true, 'b')

query
SELECT grp, mode(v) FROM mode_bool GROUP BY grp ORDER BY grp

-- ============================================================
-- Byte / Short / Long
-- ============================================================

statement
CREATE TABLE mode_nums(b tinyint, s smallint, l bigint, grp string) USING parquet

statement
INSERT INTO mode_nums VALUES
  (1, 100, 1000000000000, 'a'), (1, 100, 1000000000000, 'a'), (2, 200, 2000000000000, 'a'),
  (3, 300, 3000000000000, 'b'), (3, 300, 3000000000000, 'b'), (4, 400, 4000000000000, 'b')

query
SELECT grp, mode(b), mode(s), mode(l) FROM mode_nums GROUP BY grp ORDER BY grp

-- ============================================================
-- Float / Double
--
-- Whether `-0.0` and `0.0` share a frequency-map key is version-dependent: Spark 3.4-4.1 key on
-- `java.lang.Double.equals` and keep them apart, while Spark 4.2.0+ folds `-0.0` into `0.0`
-- (SPARK-57329). Each group below still has a unique winner under both behaviours.
--
-- Negative zero must be written as `-0.0D`, not `CAST(-0.0 AS DOUBLE)`: an unsuffixed `-0.0` is a
-- DecimalType literal, and Decimal has no signed zero, so the cast yields `+0.0` and the column
-- would silently contain no negative zeros at all.
-- ============================================================

statement
CREATE TABLE mode_double(v double, grp string) USING parquet

statement
INSERT INTO mode_double VALUES
  (1.5, 'a'), (1.5, 'a'), (2.5, 'a'), (NULL, 'a'),
  (0.0D, 'b'), (-0.0D, 'b'), (-0.0D, 'b'), (7.0, 'b')

query
SELECT grp, mode(v) FROM mode_double GROUP BY grp ORDER BY grp

-- ============================================================
-- Signed zeros: which value wins depends on whether -0.0 and 0.0 share a key
--
-- The counts are -0.0:2, 0.0:2, 5.0:3 (the SPARK-57329 reproducer). Keeping the zeros apart makes
-- 5.0 the unique winner; folding them together makes the zero win with 4. The two candidate
-- answers differ in magnitude, so this catches the divergence even where a `-0.0` vs `0.0`
-- difference would not be visible. Neither behaviour produces a tie, so there is no dependence on
-- Spark's non-deterministic tie-break.
-- ============================================================

statement
CREATE TABLE mode_signed_zero(v double, grp string) USING parquet

statement
INSERT INTO mode_signed_zero VALUES
  (-0.0D, 'a'), (-0.0D, 'a'),
  (0.0D, 'a'), (0.0D, 'a'),
  (5.0D, 'a'), (5.0D, 'a'), (5.0D, 'a')

query
SELECT grp, mode(v) FROM mode_signed_zero GROUP BY grp ORDER BY grp

-- ============================================================
-- NaN collapses to a single key on every supported version
--
-- `doubleToLongBits` maps every NaN bit pattern to one value, so the two NaNs outvote the single
-- 1.0 and the mode is NaN.
-- ============================================================

statement
CREATE TABLE mode_nan(v double, grp string) USING parquet

statement
INSERT INTO mode_nan VALUES
  (CAST('NaN' AS DOUBLE), 'a'), (CAST('NaN' AS DOUBLE), 'a'), (1.0D, 'a')

query
SELECT grp, mode(v) FROM mode_nan GROUP BY grp ORDER BY grp

-- ============================================================
-- Decimal
-- ============================================================

statement
CREATE TABLE mode_decimal(v decimal(10,2), grp string) USING parquet

statement
INSERT INTO mode_decimal VALUES
  (1.50, 'a'), (1.50, 'a'), (2.50, 'a'), (NULL, 'a'),
  (99999999.99, 'b'), (99999999.99, 'b'), (0.00, 'b')

query
SELECT grp, mode(v) FROM mode_decimal GROUP BY grp ORDER BY grp

-- ============================================================
-- String
-- ============================================================

statement
CREATE TABLE mode_string(v string, grp string) USING parquet

statement
INSERT INTO mode_string VALUES
  ('hello', 'a'), ('hello', 'a'), ('world', 'a'), (NULL, 'a'),
  ('', 'b'), ('', 'b'), ('x', 'b')

query
SELECT grp, mode(v) FROM mode_string GROUP BY grp ORDER BY grp

-- ============================================================
-- Date / Timestamp
-- ============================================================

statement
CREATE TABLE mode_temporal(d date, t timestamp, grp string) USING parquet

statement
INSERT INTO mode_temporal VALUES
  (DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00', 'a'),
  (DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00', 'a'),
  (DATE '2024-06-15', TIMESTAMP '2024-06-15 12:30:00', 'a'),
  (DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00', 'b'),
  (DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00', 'b'),
  (DATE '2000-12-31', TIMESTAMP '2000-12-31 23:59:59', 'b')

query
SELECT grp, mode(d), mode(t) FROM mode_temporal GROUP BY grp ORDER BY grp

-- ============================================================
-- TimestampNTZ (declared supported by isSupportedType, so exercise it directly)
-- ============================================================

statement
CREATE TABLE mode_ntz(t timestamp_ntz, grp string) USING parquet

statement
INSERT INTO mode_ntz VALUES
  (TIMESTAMP_NTZ '2024-01-01 00:00:00', 'a'),
  (TIMESTAMP_NTZ '2024-01-01 00:00:00', 'a'),
  (TIMESTAMP_NTZ '2024-06-15 12:30:00', 'a'),
  (NULL, 'a'),
  (TIMESTAMP_NTZ '1970-01-01 00:00:00', 'b'),
  (TIMESTAMP_NTZ '1970-01-01 00:00:00', 'b'),
  (TIMESTAMP_NTZ '2000-12-31 23:59:59', 'b')

query
SELECT grp, mode(t) FROM mode_ntz GROUP BY grp ORDER BY grp

-- ============================================================
-- Unsupported input type falls back to Spark
--
-- A single row keeps the result deterministic: Spark's mode on BinaryType compares Array[Byte]
-- keys by reference, so any binary multiset with repeats is a full tie and returns an arbitrary
-- value. One row avoids that while still exercising the unsupported-type fallback.
-- ============================================================

statement
CREATE TABLE mode_binary(v binary) USING parquet

statement
INSERT INTO mode_binary VALUES (X'CAFE')

query expect_fallback(does not support input type)
SELECT mode(v) FROM mode_binary
