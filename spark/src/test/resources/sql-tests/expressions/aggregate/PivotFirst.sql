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

-- PivotFirst is Spark's internal aggregate for the second phase of the optimized pivot plan,
-- which the analyzer rule ResolvePivot builds whenever every aggregate output type is in
-- PivotFirst.supportsDataType (Boolean, Byte, Short, Int, Long, Float, Double, Decimal).
-- These SQL tests exercise that path via SQL PIVOT clauses, one per supported value type and
-- per supported pivot column type, plus a handful of edge cases (unmatched pivot values,
-- all-null groups, null pivot column). Every native query asserts that PivotFirst itself ran
-- natively, so a plan without it fails rather than passing on Spark's other pivot plan. The
-- pivot column types and pivot lists whose matching Comet does not reproduce come last and
-- must fall back.

-- ============================================================
-- Setup: sales-like table used for most cases
-- ============================================================

statement
CREATE TABLE pf_sales (year int, course string, earnings int) USING parquet

statement
INSERT INTO pf_sales VALUES
  (2012, 'dotNET', 15000),
  (2012, 'Java',   20000),
  (2013, 'dotNET', 48000),
  (2013, 'Java',   30000)

-- ============================================================
-- Int values: canonical optimized-pivot query.
-- ============================================================

query expect_native(pivotfirst)
SELECT * FROM pf_sales
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java'))
  ORDER BY year

-- ============================================================
-- Long values: sum of a bigint aggregates to bigint.
-- ============================================================

statement
CREATE TABLE pf_sales_bi (year int, course string, earnings bigint) USING parquet

statement
INSERT INTO pf_sales_bi VALUES
  (2012, 'dotNET', 15000000000),
  (2012, 'Java',   20000000000),
  (2013, 'dotNET', 48000000000),
  (2013, 'Java',   30000000000)

query expect_native(pivotfirst)
SELECT * FROM pf_sales_bi
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java'))
  ORDER BY year

-- ============================================================
-- Double values: avg(earnings) is double.
-- ============================================================

query expect_native(pivotfirst)
SELECT * FROM pf_sales
  PIVOT (avg(earnings)
    FOR course IN ('dotNET', 'Java'))
  ORDER BY year

-- ============================================================
-- Float values: the column is already float, and min keeps the
-- type where sum would widen it to double.
-- ============================================================

statement
CREATE TABLE pf_sales_f (year int, course string, earnings float) USING parquet

statement
INSERT INTO pf_sales_f VALUES
  (2012, 'dotNET', CAST(15000 AS FLOAT)),
  (2012, 'Java',   CAST(20000 AS FLOAT)),
  (2013, 'dotNET', CAST(48000 AS FLOAT)),
  (2013, 'Java',   CAST(30000 AS FLOAT))

query expect_native(pivotfirst)
SELECT * FROM pf_sales_f
  PIVOT (min(earnings)
    FOR course IN ('dotNET', 'Java'))
  ORDER BY year

-- ============================================================
-- Short and Byte value types (sum promotes them to bigint, so use min
-- to keep the aggregate output type equal to the input type).
-- ============================================================

statement
CREATE TABLE pf_sales_small (
  year int, course string, s smallint, b tinyint) USING parquet

statement
INSERT INTO pf_sales_small VALUES
  (2012, 'dotNET', 150, 10),
  (2012, 'Java',   200, 20),
  (2013, 'dotNET', 480, 30),
  (2013, 'Java',   300, 40)

query expect_native(pivotfirst)
SELECT * FROM pf_sales_small
  PIVOT (min(s)
    FOR course IN ('dotNET', 'Java'))
  ORDER BY year

query expect_native(pivotfirst)
SELECT * FROM pf_sales_small
  PIVOT (min(b)
    FOR course IN ('dotNET', 'Java'))
  ORDER BY year

-- ============================================================
-- Boolean values: bool_or's output type is boolean.
-- ============================================================

statement
CREATE TABLE pf_flags (year int, course string, flag boolean) USING parquet

statement
INSERT INTO pf_flags VALUES
  (2012, 'dotNET', true),
  (2012, 'Java',   false),
  (2013, 'dotNET', false),
  (2013, 'Java',   true)

query expect_native(pivotfirst)
SELECT * FROM pf_flags
  PIVOT (bool_or(flag)
    FOR course IN ('dotNET', 'Java'))
  ORDER BY year

-- ============================================================
-- Decimal values.
-- ============================================================

statement
CREATE TABLE pf_sales_dec (year int, course string, earnings decimal(10,2)) USING parquet

statement
INSERT INTO pf_sales_dec VALUES
  (2012, 'dotNET', 15000.00),
  (2012, 'Java',   20000.00),
  (2013, 'dotNET', 48000.00),
  (2013, 'Java',   30000.00)

query expect_native(pivotfirst)
SELECT * FROM pf_sales_dec
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java'))
  ORDER BY year

-- ============================================================
-- Multiple aggregates in a single pivot.
-- ============================================================

query expect_native(pivotfirst)
SELECT * FROM pf_sales
  PIVOT (sum(earnings) AS s, avg(earnings) AS a
    FOR course IN ('dotNET', 'Java'))
  ORDER BY year

-- ============================================================
-- Pivot values that do not appear in the data: the corresponding
-- output slots must be NULL.
-- ============================================================

query expect_native(pivotfirst)
SELECT * FROM pf_sales
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java', 'MissingA', 'MissingB'))
  ORDER BY year

-- ============================================================
-- Rows whose pivot column is not in the pivot list must be ignored:
-- 'Python' below should not contribute to any output slot.
-- ============================================================

statement
CREATE TABLE pf_sales_extra (year int, course string, earnings int) USING parquet

statement
INSERT INTO pf_sales_extra VALUES
  (2012, 'dotNET', 15000),
  (2012, 'Java',   20000),
  (2012, 'Python', 12345),
  (2013, 'dotNET', 48000),
  (2013, 'Java',   30000),
  (2013, 'Python', 67890)

query expect_native(pivotfirst)
SELECT * FROM pf_sales_extra
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java'))
  ORDER BY year

-- ============================================================
-- Group with no matching rows: every output slot is NULL for that
-- pivot combination. Ensures NULL initialization matches Spark.
-- ============================================================

statement
CREATE TABLE pf_sales_partial (year int, course string, earnings int) USING parquet

statement
INSERT INTO pf_sales_partial VALUES
  (2012, 'dotNET', 15000),
  (2012, 'Java',   20000),
  (2013, 'Python', 42000)

query expect_native(pivotfirst)
SELECT * FROM pf_sales_partial
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java'))
  ORDER BY year

-- ============================================================
-- NULL value in an aggregated column: the null must not clobber a
-- previously written slot (Spark PivotFirst.update ignores nulls).
-- ============================================================

statement
CREATE TABLE pf_sales_nullable (year int, course string, earnings int) USING parquet

statement
INSERT INTO pf_sales_nullable VALUES
  (2012, 'dotNET', 15000),
  (2012, 'dotNET', NULL),
  (2012, 'Java',   NULL),
  (2013, 'dotNET', 48000),
  (2013, 'Java',   30000)

query expect_native(pivotfirst)
SELECT * FROM pf_sales_nullable
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java'))
  ORDER BY year

-- ============================================================
-- NULL in the pivot COLUMN. Spark tracks this with a dedicated
-- test ("pivot with null should not throw NPE") because early
-- versions of PivotFirst NPE'd on null keys with a TreeMap. A null
-- row matches no slot unless the pivot list holds NULL too.
-- ============================================================

statement
CREATE TABLE pf_sales_nullpivot (year int, course string, earnings int) USING parquet

statement
INSERT INTO pf_sales_nullpivot VALUES
  (2012, 'dotNET', 15000),
  (2012, NULL,     9999),
  (2013, 'Java',   30000),
  (2013, NULL,     42)

query expect_native(pivotfirst)
SELECT * FROM pf_sales_nullpivot
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java'))
  ORDER BY year

-- ============================================================
-- Non-string pivot column: integer. Exercises the pivot value
-- serialization path with a numeric literal.
-- ============================================================

statement
CREATE TABLE pf_sales_intpivot (region string, year int, earnings int) USING parquet

statement
INSERT INTO pf_sales_intpivot VALUES
  ('NA', 2012, 15000),
  ('NA', 2013, 48000),
  ('EU', 2012, 20000),
  ('EU', 2013, 30000)

query expect_native(pivotfirst)
SELECT * FROM pf_sales_intpivot
  PIVOT (sum(earnings)
    FOR year IN (2012, 2013))
  ORDER BY region

-- ============================================================
-- Non-string pivot column: date. Exercises the pivot value
-- serialization path with a DateType literal.
-- ============================================================

statement
CREATE TABLE pf_sales_datepivot (region string, d date, earnings int) USING parquet

statement
INSERT INTO pf_sales_datepivot VALUES
  ('NA', DATE '2024-01-01', 15000),
  ('NA', DATE '2024-06-15', 48000),
  ('EU', DATE '2024-01-01', 20000),
  ('EU', DATE '2024-06-15', 30000)

query expect_native(pivotfirst)
SELECT * FROM pf_sales_datepivot
  PIVOT (sum(earnings)
    FOR d IN (DATE '2024-01-01', DATE '2024-06-15'))
  ORDER BY region

-- ============================================================
-- Non-string pivot column: double, including the two values whose
-- key matching differs between Spark and a plain hash of the native
-- scalar. Spark keys the pivot list on the boxed Catalyst value, so
-- -0.0 and 0.0 are the same key while NaN matches nothing at all.
--
-- The zero rows are written as -0.0D / 0.0D rather than
-- CAST(-0.0 AS DOUBLE), because the cast constant-folds to +0.0 and
-- would make the case vacuous.
-- ============================================================

statement
CREATE TABLE pf_sales_dblpivot (region string, k double, earnings int) USING parquet

statement
INSERT INTO pf_sales_dblpivot VALUES
  ('NA', 0.0D,             15000),
  ('NA', -0.0D,            48000),
  ('NA', double('NaN'),    11000),
  ('NA', 1.5D,             20000),
  ('EU', 0.0D,             30000),
  ('EU', double('NaN'),    12000),
  ('EU', 1.5D,             40000)

-- -0.0 in the pivot list has to collect both signs of zero (63000 / 30000),
-- and the NaN column has to stay NULL for every group.
query expect_native(pivotfirst)
SELECT * FROM pf_sales_dblpivot
  PIVOT (sum(earnings)
    FOR k IN (-0.0D, double('NaN'), 1.5D))
  ORDER BY region

-- The mirror: a 0.0 entry in the pivot list must also catch the -0.0 rows.
query expect_native(pivotfirst)
SELECT * FROM pf_sales_dblpivot
  PIVOT (sum(earnings)
    FOR k IN (0.0D, 1.5D))
  ORDER BY region

-- ============================================================
-- Non-string pivot column: float. Same two rules.
-- ============================================================

statement
CREATE TABLE pf_sales_fltpivot (region string, k float, earnings int) USING parquet

statement
INSERT INTO pf_sales_fltpivot VALUES
  ('NA', 0.0F,          15000),
  ('NA', -0.0F,         48000),
  ('NA', float('NaN'),  11000),
  ('EU', -0.0F,         30000)

query expect_native(pivotfirst)
SELECT * FROM pf_sales_fltpivot
  PIVOT (sum(earnings)
    FOR k IN (-0.0F, float('NaN')))
  ORDER BY region

-- ============================================================
-- NULL in the pivot LIST collects the rows whose pivot column is
-- NULL, as Spark's HashMap lookup of a null key does.
-- ============================================================

query expect_native(pivotfirst)
SELECT * FROM pf_sales_nullpivot
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', NULL))
  ORDER BY year

-- ============================================================
-- Two NaN entries are two distinct keys for Spark (NaN equals
-- nothing), so the list has no duplicates and stays native, and
-- both columns stay NULL.
-- ============================================================

query expect_native(pivotfirst)
SELECT * FROM pf_sales_dblpivot
  PIVOT (sum(earnings)
    FOR k IN (double('NaN'), 1.5D, double('NaN')))
  ORDER BY region

-- ============================================================
-- The other pivot column types that run natively: boolean,
-- tinyint, smallint, bigint, decimal, timestamp and timestamp_ntz.
-- ============================================================

statement
CREATE TABLE pf_sales_types (
  region string, flag boolean, b tinyint, s smallint, l bigint, d decimal(10,2),
  ts timestamp, ntz timestamp_ntz, earnings int) USING parquet

statement
INSERT INTO pf_sales_types VALUES
  ('NA', true,  1, 100, 10000000000, 1.50,
   TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP_NTZ '2024-01-01 00:00:00', 15000),
  ('NA', false, 2, 200, 20000000000, 2.25,
   TIMESTAMP '2024-06-15 12:30:00', TIMESTAMP_NTZ '2024-06-15 12:30:00', 48000),
  ('EU', true,  2, 200, 20000000000, 2.25,
   TIMESTAMP '2024-06-15 12:30:00', TIMESTAMP_NTZ '2024-06-15 12:30:00', 20000),
  ('EU', NULL,  3, 300, 30000000000, 3.00,
   TIMESTAMP '2025-01-01 00:00:00', TIMESTAMP_NTZ '2025-01-01 00:00:00', 30000)

query expect_native(pivotfirst)
SELECT region, `true`, `false` FROM (
  SELECT region, flag, earnings FROM pf_sales_types)
  PIVOT (sum(earnings) FOR flag IN (true, false))
  ORDER BY region

query expect_native(pivotfirst)
SELECT * FROM (SELECT region, b, earnings FROM pf_sales_types)
  PIVOT (sum(earnings) FOR b IN (CAST(1 AS TINYINT), CAST(2 AS TINYINT)))
  ORDER BY region

query expect_native(pivotfirst)
SELECT * FROM (SELECT region, s, earnings FROM pf_sales_types)
  PIVOT (sum(earnings) FOR s IN (CAST(100 AS SMALLINT), CAST(300 AS SMALLINT)))
  ORDER BY region

query expect_native(pivotfirst)
SELECT * FROM (SELECT region, l, earnings FROM pf_sales_types)
  PIVOT (sum(earnings) FOR l IN (10000000000, 20000000000))
  ORDER BY region

query expect_native(pivotfirst)
SELECT * FROM (SELECT region, d, earnings FROM pf_sales_types)
  PIVOT (sum(earnings) FOR d IN (2.25, 3.00))
  ORDER BY region

query expect_native(pivotfirst)
SELECT * FROM (SELECT region, ts, earnings FROM pf_sales_types)
  PIVOT (sum(earnings)
    FOR ts IN (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-06-15 12:30:00'))
  ORDER BY region

query expect_native(pivotfirst)
SELECT * FROM (SELECT region, ntz, earnings FROM pf_sales_types)
  PIVOT (sum(earnings)
    FOR ntz IN (TIMESTAMP_NTZ '2024-06-15 12:30:00', TIMESTAMP_NTZ '2025-01-01 00:00:00'))
  ORDER BY region

-- ============================================================
-- No grouping column: the pivoted relation has only the pivot and
-- value columns, so PivotFirst runs ungrouped.
-- ============================================================

query expect_native(pivotfirst)
SELECT * FROM (SELECT course, earnings FROM pf_sales)
  PIVOT (sum(earnings) FOR course IN ('dotNET', 'Java'))

-- Over no rows the result is still one row, of nulls. A literal
-- WHERE false lets the optimizer replace the scan with an empty
-- local relation, which runs in Spark. These fixtures run without
-- constant folding, so 1 = 0 keeps the scan and the filter native.
query expect_native(pivotfirst)
SELECT * FROM (SELECT course, earnings FROM pf_sales WHERE 1 = 0)
  PIVOT (sum(earnings) FOR course IN ('dotNET', 'Java'))

-- ============================================================
-- More groups than one batch holds, each with several partial
-- states. Repartitioning the input by the pivot column means the
-- first aggregation needs no shuffle, so each partition's partial
-- PivotFirst writes the slots of its own pivot values for nearly
-- every group, and the final aggregation merges those states.
-- Without the hint, AQE coalesces the first aggregation's shuffle
-- into one partition, and each group has a single partial state.
-- Each (g, k) pair has at most one row, so the result is
-- deterministic.
-- ============================================================

statement
CREATE TABLE pf_many_groups (g int, k string, v int) USING parquet

statement
INSERT INTO pf_many_groups
SELECT g, k, v FROM (
  SELECT CAST(id % 20000 AS INT) AS g,
    CASE id DIV 20000 WHEN 0 THEN 'a' WHEN 1 THEN 'b' ELSE 'c' END AS k,
    CASE WHEN id % 11 = 0 THEN NULL ELSE CAST(id AS INT) END AS v
  FROM range(60000))
WHERE NOT (k = 'b' AND g % 7 = 0)

query expect_native(pivotfirst)
SELECT * FROM (SELECT /*+ REPARTITION(4, k) */ g, k, v FROM pf_many_groups)
  PIVOT (sum(v) FOR k IN ('a', 'b', 'c', 'd'))

-- ============================================================
-- Pivot column types that fall back. Spark compares binary pivot
-- values by reference before 4.2, so no row matches there, and by
-- content from 4.2. It matches arrays and structs through their
-- interpreted ordering. A pivot on two columns pivots on a struct.
-- ============================================================

statement
CREATE TABLE pf_sales_complex (
  region string, k binary, a array<int>, course string, year int, earnings int)
  USING parquet

statement
INSERT INTO pf_sales_complex VALUES
  ('NA', X'61', array(1, 1), 'dotNET', 2012, 15000),
  ('NA', X'62', array(2, 2), 'Java',   2013, 48000),
  ('EU', X'61', array(1, 1), 'dotNET', 2013, 20000)

query expect_fallback(Unsupported pivot column data type: BinaryType)
SELECT * FROM (SELECT region, k, earnings FROM pf_sales_complex)
  PIVOT (sum(earnings) FOR k IN (X'61', X'62'))
  ORDER BY region

query expect_fallback(Unsupported pivot column data type: ArrayType)
SELECT * FROM (SELECT region, a, earnings FROM pf_sales_complex)
  PIVOT (sum(earnings) FOR a IN (array(1, 1), array(2, 2)))
  ORDER BY region

query expect_fallback(Unsupported pivot column data type: StructType)
SELECT * FROM (SELECT region, course, year, earnings FROM pf_sales_complex)
  PIVOT (sum(earnings) FOR (course, year) IN (('dotNET', 2012), ('Java', 2013)))
  ORDER BY region

-- ============================================================
-- Duplicate pivot values fall back. Spark keeps one buffer slot per
-- distinct value, so its partial aggregate emits fewer columns than
-- the list has entries, and the projection reads the missing ones
-- past the end of the array: NULL with ANSI off, an error with it
-- on. 0.0 and -0.0 are duplicates under Spark's key equality. Only
-- rows matching the first entry are used, because Spark writes a
-- repeated value's row past the end of its buffer.
-- ============================================================

statement
SET spark.sql.ansi.enabled=false

query expect_fallback(Pivot values list contains duplicates)
SELECT * FROM pf_sales_intpivot
  PIVOT (sum(earnings) FOR year IN (2012, 2014, 2014))
  ORDER BY region

statement
CREATE TABLE pf_sales_dblnozero (region string, k double, earnings int) USING parquet

statement
INSERT INTO pf_sales_dblnozero VALUES
  ('NA', 1.5D, 15000),
  ('NA', 2.5D, 48000),
  ('EU', 1.5D, 20000)

query expect_fallback(Pivot values list contains duplicates)
SELECT * FROM pf_sales_dblnozero
  PIVOT (sum(earnings) FOR k IN (1.5D, 0.0D, -0.0D))
  ORDER BY region
