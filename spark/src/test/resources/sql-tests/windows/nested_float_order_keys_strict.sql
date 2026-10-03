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

-- Strict floating-point mode and sort or window keys that nest floats in arrays and structs.
-- nested_float_order_keys.sql checks that Comet normalizes those floats to match Spark. Strict
-- mode still declines a key whose type can hold a null element or field, because Spark orders
-- such a null below every value whatever the key's null order, the native sort places it by the
-- null order (#6476), and a RANGE window frame orders it above every value (#6477). A key whose
-- type cannot hold a null stays native.
--
-- The test harness admits incompatible sort orders by default, so turn that off to check the
-- shipped policy.
-- Config: spark.comet.exec.strictFloatingPoint=true
-- Config: spark.comet.expression.SortOrder.allowIncompatible=false

statement
CREATE TABLE nested_float_strict(id INT, g INT, d DOUBLE, f FLOAT, s BOOLEAN) USING parquet

statement
INSERT INTO nested_float_strict VALUES
  (1, 1, 0.0D, float('0.0'), true),
  (2, 1, 0.0D, float('0.0'), false),
  (3, 1, 1.0D, float('1.0'), false),
  (4, 2, double('NaN'), float('NaN'), false),
  (5, 2, double('NaN'), float('NaN'), true),
  (6, 2, double('Infinity'), float('Infinity'), false),
  (7, 2, -1.0D, float('-1.0'), false),
  (8, 1, NULL, NULL, false),
  (9, 2, double('-Infinity'), float('-Infinity'), false)

-- Keys over the nullable columns can hold a null element or field, whether or not any row does,
-- so they fall back, and Spark's answers come back. Row 8 holds one. Native, ORDER BY array(d)
-- NULLS LAST puts row 8 last, where Spark puts it first, and the running sum spans the whole
-- partition on row 8 and every row after it.
query expect_fallback(can hold a null element or field)
SELECT id FROM nested_float_strict ORDER BY array(d) NULLS LAST, id

query expect_fallback(can hold a null element or field)
SELECT id, SUM(id) OVER (ORDER BY array(d)) AS running FROM nested_float_strict

query expect_fallback(can hold a null element or field)
SELECT id FROM nested_float_strict ORDER BY named_struct('x', f) DESC NULLS FIRST, id

query expect_fallback(can hold a null element or field)
SELECT id FROM nested_float_strict ORDER BY array(IF(s, -d, d)), id LIMIT 4

query expect_fallback(can hold a null element or field)
SELECT id,
  RANK() OVER (PARTITION BY g ORDER BY named_struct('x', IF(s, -d, d))) AS r,
  DENSE_RANK() OVER (ORDER BY array(IF(s, -f, f)) DESC) AS dr
FROM nested_float_strict

query expect_fallback(can hold a null element or field)
SELECT id FROM (
  SELECT id, RANK() OVER (ORDER BY array(IF(s, -d, d))) AS r FROM nested_float_strict
) WHERE r <= 3

-- coalesce with a literal is never null, so an array or struct of it cannot hold a null, and
-- these keys stay native. Row 8 becomes a third zero. Each ORDER BY ends with a unique
-- tiebreaker, so peers come out in its order, and the running sums need no null row to leave out.
query
SELECT id FROM nested_float_strict ORDER BY array(coalesce(IF(s, -d, d), 0.0D)), id DESC

query
SELECT id FROM nested_float_strict ORDER BY named_struct('x', coalesce(IF(s, -f, f), 0.0F)) DESC, id

query
SELECT id FROM nested_float_strict ORDER BY array(coalesce(IF(s, -d, d), 0.0D)) DESC, id LIMIT 4

query
SELECT id,
  RANK() OVER (ORDER BY array(coalesce(IF(s, -d, d), 0.0D))) AS r,
  SUM(id) OVER (ORDER BY array(coalesce(IF(s, -d, d), 0.0D))) AS running,
  DENSE_RANK() OVER (PARTITION BY g ORDER BY named_struct('x', coalesce(IF(s, -f, f), 0.0F)) DESC) AS dr
FROM nested_float_strict

-- A rank limit keeps every peer of the last rank it admits
query
SELECT id FROM (
  SELECT id, RANK() OVER (ORDER BY array(coalesce(IF(s, -d, d), 0.0D))) AS r
  FROM nested_float_strict
) WHERE r <= 3

query
SELECT id FROM (
  SELECT id,
    DENSE_RANK() OVER (PARTITION BY g ORDER BY named_struct('x', coalesce(IF(s, -f, f), 0.0F)) DESC) AS r
  FROM nested_float_strict
) WHERE r <= 1

-- DataFusion cannot compare an array of structs or arrays, or a struct holding an array, to find
-- the CURRENT ROW bound of a RANGE frame (apache/datafusion#24937), so a running aggregate over
-- such a key falls back even when the key cannot hold a null. window_functions.sql checks the
-- same without floats.
query expect_fallback(RANGE frame on array<struct<x:double>> ORDER BY is not supported)
SELECT id, COUNT(id) OVER (ORDER BY array(named_struct('x', coalesce(d, 0.0D))), id) AS running
FROM nested_float_strict

query expect_fallback(RANGE frame on struct<a:array<float>> ORDER BY is not supported)
SELECT id,
  SUM(id) OVER (PARTITION BY g
                ORDER BY named_struct('a', array(coalesce(IF(s, -f, f), 0.0F))) DESC) AS by_group
FROM nested_float_strict

-- Ranks and ROWS frames over the same keys stay native
query
SELECT id,
  RANK() OVER (PARTITION BY g ORDER BY array(named_struct('x', coalesce(IF(s, -d, d), 0.0D)))) AS r,
  SUM(id) OVER (PARTITION BY g ORDER BY array(named_struct('x', coalesce(IF(s, -d, d), 0.0D))), id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
FROM nested_float_strict
