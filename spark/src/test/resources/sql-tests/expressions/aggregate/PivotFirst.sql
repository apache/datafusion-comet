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

-- PivotFirst is Spark's internal aggregate emitted by the two-phase optimized pivot plan
-- (PivotTransformer). It only runs when every aggregate output type is in
-- PivotFirst.supportsDataType (Boolean, Byte, Short, Int, Long, Float, Double, Decimal).
-- These SQL tests exercise that path via SQL PIVOT clauses, one per supported value type
-- plus a handful of edge cases (unmatched pivot values, all-null groups, multiple
-- aggregates). They intentionally use `spark_answer_only` because the pivot plan's final
-- projection is a Spark expression (ExtractValue on the aggregate output) so the whole
-- query does not always run natively even when PivotFirst itself does.

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
-- Int values: canonical optimized-pivot query. Extra pivot values
-- (1..10) push the aggregate over the threshold that turns on the
-- PivotFirst plan.
-- ============================================================

query spark_answer_only
SELECT * FROM pf_sales
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'))
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

query spark_answer_only
SELECT * FROM pf_sales_bi
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'))
  ORDER BY year

-- ============================================================
-- Double values: avg(earnings) is double.
-- ============================================================

query spark_answer_only
SELECT * FROM pf_sales
  PIVOT (avg(earnings)
    FOR course IN ('dotNET', 'Java', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'))
  ORDER BY year

-- ============================================================
-- Float values: cast the sum result to float via a wrapping min.
-- ============================================================

statement
CREATE TABLE pf_sales_f (year int, course string, earnings float) USING parquet

statement
INSERT INTO pf_sales_f VALUES
  (2012, 'dotNET', CAST(15000 AS FLOAT)),
  (2012, 'Java',   CAST(20000 AS FLOAT)),
  (2013, 'dotNET', CAST(48000 AS FLOAT)),
  (2013, 'Java',   CAST(30000 AS FLOAT))

query spark_answer_only
SELECT * FROM pf_sales_f
  PIVOT (min(earnings)
    FOR course IN ('dotNET', 'Java', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'))
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

query spark_answer_only
SELECT * FROM pf_sales_small
  PIVOT (min(s)
    FOR course IN ('dotNET', 'Java', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'))
  ORDER BY year

query spark_answer_only
SELECT * FROM pf_sales_small
  PIVOT (min(b)
    FOR course IN ('dotNET', 'Java', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'))
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

query spark_answer_only
SELECT * FROM pf_flags
  PIVOT (bool_or(flag)
    FOR course IN ('dotNET', 'Java', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'))
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

query spark_answer_only
SELECT * FROM pf_sales_dec
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'))
  ORDER BY year

-- ============================================================
-- Multiple aggregates in a single pivot.
-- ============================================================

query spark_answer_only
SELECT * FROM pf_sales
  PIVOT (sum(earnings) AS s, avg(earnings) AS a
    FOR course IN ('dotNET', 'Java', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'))
  ORDER BY year

-- ============================================================
-- Pivot values that do not appear in the data: the corresponding
-- output slots must be NULL.
-- ============================================================

query spark_answer_only
SELECT * FROM pf_sales
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java', 'MissingA', 'MissingB',
                   '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'))
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

query spark_answer_only
SELECT * FROM pf_sales_extra
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'))
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

query spark_answer_only
SELECT * FROM pf_sales_partial
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'))
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

query spark_answer_only
SELECT * FROM pf_sales_nullable
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'))
  ORDER BY year

-- ============================================================
-- NULL in the pivot COLUMN. Spark tracks this with a dedicated
-- test ("pivot with null should not throw NPE") because early
-- versions of PivotFirst NPE'd on null keys with a TreeMap.
-- Comet uses a HashMap<ScalarValue, usize> where ScalarValue::Null
-- hashes safely, so this must produce the same rows as Spark.
-- ============================================================

statement
CREATE TABLE pf_sales_nullpivot (year int, course string, earnings int) USING parquet

statement
INSERT INTO pf_sales_nullpivot VALUES
  (2012, 'dotNET', 15000),
  (2012, NULL,     9999),
  (2013, 'Java',   30000),
  (2013, NULL,     42)

query spark_answer_only
SELECT * FROM pf_sales_nullpivot
  PIVOT (sum(earnings)
    FOR course IN ('dotNET', 'Java', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'))
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

query spark_answer_only
SELECT * FROM pf_sales_intpivot
  PIVOT (sum(earnings)
    FOR year IN (2012, 2013, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
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

query spark_answer_only
SELECT * FROM pf_sales_datepivot
  PIVOT (sum(earnings)
    FOR d IN (DATE '2024-01-01', DATE '2024-06-15',
              DATE '1970-01-01', DATE '1970-01-02', DATE '1970-01-03',
              DATE '1970-01-04', DATE '1970-01-05', DATE '1970-01-06',
              DATE '1970-01-07', DATE '1970-01-08', DATE '1970-01-09',
              DATE '1970-01-10'))
  ORDER BY region
