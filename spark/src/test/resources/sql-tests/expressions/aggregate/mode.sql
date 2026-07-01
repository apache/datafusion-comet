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
-- Float / Double: -0.0 and 0.0 normalize to one key
-- ============================================================

statement
CREATE TABLE mode_double(v double, grp string) USING parquet

statement
INSERT INTO mode_double VALUES
  (1.5, 'a'), (1.5, 'a'), (2.5, 'a'), (NULL, 'a'),
  (CAST(0.0 AS DOUBLE), 'b'), (CAST(-0.0 AS DOUBLE), 'b'), (CAST(-0.0 AS DOUBLE), 'b'), (7.0, 'b')

query
SELECT grp, mode(v) FROM mode_double GROUP BY grp ORDER BY grp

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
