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

-- Config: spark.comet.exec.strictFloatingPoint=true
-- ConfigMatrix: parquet.enable.dictionary=false,true

-- ============================================================
-- Setup: tables
-- ============================================================

statement
CREATE TABLE cs_src_int(i int, grp string) USING parquet

statement
INSERT INTO cs_src_int VALUES
  (1, 'a'), (2, 'a'), (1, 'a'), (3, 'a'),
  (4, 'b'), (4, 'b'), (NULL, 'b'), (5, 'b'),
  (NULL, 'c'), (NULL, 'c')

statement
CREATE TABLE cs_src_nulls(val int, grp string) USING parquet

statement
INSERT INTO cs_src_nulls VALUES
  (NULL, 'a'), (NULL, 'a'), (NULL, 'b'), (1, 'b')

statement
CREATE TABLE cs_src_empty(val int) USING parquet

statement
CREATE TABLE cs_src_single(val int) USING parquet

statement
INSERT INTO cs_src_single VALUES (42)

statement
CREATE TABLE cs_src_dupes(val int, grp string) USING parquet

statement
INSERT INTO cs_src_dupes VALUES (7, 'a'), (7, 'a'), (7, 'a'), (8, 'b'), (9, 'b')

-- ============================================================
-- Basic: integer dedup (global aggregate, no GROUP BY)
-- ============================================================

query
SELECT sort_array(collect_set(i)) FROM cs_src_int

-- ============================================================
-- GROUP BY: integer dedup per group
-- ============================================================

query
SELECT grp, sort_array(collect_set(i)) FROM cs_src_int GROUP BY grp ORDER BY grp

-- ============================================================
-- NULLs: all NULLs in a group returns empty array
-- ============================================================

query
SELECT grp, sort_array(collect_set(val)) FROM cs_src_nulls GROUP BY grp ORDER BY grp

-- ============================================================
-- Empty table: returns empty array
-- ============================================================

query
SELECT sort_array(collect_set(val)) FROM cs_src_empty

-- ============================================================
-- Single value
-- ============================================================

query
SELECT sort_array(collect_set(val)) FROM cs_src_single

-- ============================================================
-- All duplicates in a group
-- ============================================================

query
SELECT grp, sort_array(collect_set(val)) FROM cs_src_dupes GROUP BY grp ORDER BY grp

-- ============================================================
-- Boolean (with NULLs)
-- ============================================================

statement
CREATE TABLE cs_src_bool(v boolean, grp string) USING parquet

statement
INSERT INTO cs_src_bool VALUES
  (true,  'a'), (false, 'a'), (true,  'a'), (NULL, 'a'),
  (NULL,  'b'), (true,  'b')

query
SELECT grp, sort_array(collect_set(v)) FROM cs_src_bool GROUP BY grp ORDER BY grp

-- ============================================================
-- Byte / Short (with NULLs)
-- ============================================================

statement
CREATE TABLE cs_src_small(b tinyint, s smallint, grp string) USING parquet

statement
INSERT INTO cs_src_small VALUES
  (1, 100, 'a'), (2, 200, 'a'), (1, 100, 'a'), (NULL, NULL, 'a'),
  (3, 300, 'b'), (NULL, 300, 'b')

query
SELECT grp, sort_array(collect_set(b)) FROM cs_src_small GROUP BY grp ORDER BY grp

query
SELECT grp, sort_array(collect_set(s)) FROM cs_src_small GROUP BY grp ORDER BY grp

-- ============================================================
-- Int / BigInt (with NULLs)
-- ============================================================

statement
CREATE TABLE cs_src_intbig(i int, bi bigint, grp string) USING parquet

statement
INSERT INTO cs_src_intbig VALUES
  (10, 1000000000000, 'a'), (20, 2000000000000, 'a'),
  (10, 1000000000000, 'a'), (NULL, NULL, 'a'),
  (30, 3000000000000, 'b'), (30, NULL, 'b')

query
SELECT grp, sort_array(collect_set(i)) FROM cs_src_intbig GROUP BY grp ORDER BY grp

query
SELECT grp, sort_array(collect_set(bi)) FROM cs_src_intbig GROUP BY grp ORDER BY grp

-- ============================================================
-- Float (with NULLs, NaN, Inf, -Inf, +0, -0)
-- Comet deduplicates NaN while Spark does not; with
-- strictFloatingPoint=true collect_set falls back to Spark.
-- ============================================================

statement
CREATE TABLE cs_src_float(v float, grp string) USING parquet

statement
INSERT INTO cs_src_float VALUES
  (1.5,                'a'), (2.5,                'a'), (1.5, 'a'), (NULL, 'a'),
  (CAST('NaN' AS FLOAT), 'b'), (CAST('NaN' AS FLOAT), 'b'), (1.0, 'b'),
  (CAST('Infinity' AS FLOAT), 'c'), (CAST('-Infinity' AS FLOAT), 'c'), (CAST('Infinity' AS FLOAT), 'c'),
  (CAST(0.0 AS FLOAT), 'd'), (CAST(-0.0 AS FLOAT), 'd'), (1.0, 'd'), (NULL, 'd')

query expect_fallback(not fully compatible with Spark)
SELECT grp, sort_array(collect_set(v)) FROM cs_src_float GROUP BY grp ORDER BY grp

-- ============================================================
-- Double (with NULLs, NaN, Inf, -Inf, +0, -0)
-- ============================================================

statement
CREATE TABLE cs_src_double(v double, grp string) USING parquet

statement
INSERT INTO cs_src_double VALUES
  (1.1,   'a'), (2.2,   'a'), (1.1, 'a'), (NULL, 'a'),
  (CAST('NaN' AS DOUBLE), 'b'), (CAST('NaN' AS DOUBLE), 'b'), (1.0, 'b'),
  (CAST('Infinity' AS DOUBLE), 'c'), (CAST('-Infinity' AS DOUBLE), 'c'), (CAST('Infinity' AS DOUBLE), 'c'),
  (0.0,  'd'), (-0.0,  'd'), (1.0, 'd'), (NULL, 'd')

query expect_fallback(not fully compatible with Spark)
SELECT grp, sort_array(collect_set(v)) FROM cs_src_double GROUP BY grp ORDER BY grp

-- ============================================================
-- String (with NULLs and empty string)
-- ============================================================

statement
CREATE TABLE cs_src_string(v string, grp string) USING parquet

statement
INSERT INTO cs_src_string VALUES
  ('hello', 'a'), ('world', 'a'), ('hello', 'a'), (NULL, 'a'),
  ('', 'b'), ('x', 'b'), ('', 'b'), (NULL, 'b')

query
SELECT grp, sort_array(collect_set(v)) FROM cs_src_string GROUP BY grp ORDER BY grp

-- ============================================================
-- Binary (with NULLs)
-- ============================================================

statement
CREATE TABLE cs_src_binary(v binary, grp string) USING parquet

statement
INSERT INTO cs_src_binary VALUES
  (X'CAFE', 'a'), (X'BABE', 'a'), (X'CAFE', 'a'), (NULL, 'a'),
  (X'',     'b'), (X'FF',   'b'), (NULL, 'b')

query
SELECT grp, sort_array(collect_set(v)) FROM cs_src_binary GROUP BY grp ORDER BY grp

-- ============================================================
-- Decimal (with NULLs)
-- ============================================================

statement
CREATE TABLE cs_src_decimal(v decimal(10,2), grp string) USING parquet

statement
INSERT INTO cs_src_decimal VALUES
  (1.50, 'a'), (2.50, 'a'), (1.50, 'a'), (NULL, 'a'),
  (0.00, 'b'), (99999999.99, 'b'), (NULL, 'b')

query
SELECT grp, sort_array(collect_set(v)) FROM cs_src_decimal GROUP BY grp ORDER BY grp

-- ============================================================
-- Date (with NULLs)
-- ============================================================

statement
CREATE TABLE cs_src_date(v date, grp string) USING parquet

statement
INSERT INTO cs_src_date VALUES
  (DATE '2024-01-01', 'a'), (DATE '2024-06-15', 'a'), (DATE '2024-01-01', 'a'), (NULL, 'a'),
  (DATE '1970-01-01', 'b'), (NULL, 'b')

query
SELECT grp, sort_array(collect_set(v)) FROM cs_src_date GROUP BY grp ORDER BY grp

-- ============================================================
-- Timestamp (with NULLs)
-- ============================================================

statement
CREATE TABLE cs_src_ts(v timestamp, grp string) USING parquet

statement
INSERT INTO cs_src_ts VALUES
  (TIMESTAMP '2024-01-01 00:00:00', 'a'), (TIMESTAMP '2024-06-15 12:30:00', 'a'),
  (TIMESTAMP '2024-01-01 00:00:00', 'a'), (NULL, 'a'),
  (TIMESTAMP '1970-01-01 00:00:00', 'b'), (NULL, 'b')

query
SELECT grp, sort_array(collect_set(v)) FROM cs_src_ts GROUP BY grp ORDER BY grp

-- ============================================================
-- Mixed with other aggregates
-- ============================================================

query
SELECT grp, sort_array(collect_set(i)), count(*), sum(i)
FROM cs_src_int GROUP BY grp ORDER BY grp

-- ============================================================
-- Multiple collect_set in the same query
-- ============================================================

statement
CREATE TABLE cs_src_multi(a int, b string, grp string) USING parquet

statement
INSERT INTO cs_src_multi VALUES
  (1, 'x', 'g1'), (2, 'y', 'g1'), (1, 'x', 'g1'),
  (3, 'z', 'g2'), (NULL, NULL, 'g2')

query
SELECT grp, sort_array(collect_set(a)), sort_array(collect_set(b))
FROM cs_src_multi GROUP BY grp ORDER BY grp

-- ============================================================
-- DISTINCT: semantically redundant but exercises a different
-- planner path (distinct aggregate handling)
-- ============================================================

query
SELECT grp, sort_array(collect_set(DISTINCT i)) FROM cs_src_int GROUP BY grp ORDER BY grp

-- ============================================================
-- HAVING clause with collect_set
-- ============================================================

query
SELECT grp, sort_array(collect_set(i))
FROM cs_src_int GROUP BY grp HAVING size(collect_set(i)) > 1 ORDER BY grp
