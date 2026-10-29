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

-- `filter` with a lambda whose body Comet evaluates natively runs through DataFusion's
-- `array_filter`. Each query below must return what Spark returns, including for NULL arrays,
-- empty arrays, NULL elements and lambdas that capture a column.

-- ConfigMatrix: spark.comet.exec.nativeLambda.enabled=true,false

statement
CREATE TABLE test_filter_lambda(
  id int,
  strs array<string>,
  ints array<int>,
  longs array<bigint>,
  dbls array<double>,
  flts array<float>,
  decs array<decimal(10, 2)>,
  dates array<date>,
  tss array<timestamp>,
  bins array<binary>,
  bools array<boolean>,
  ntzs array<timestamp_ntz>,
  x int,
  threshold int,
  needle string
) USING parquet

statement
INSERT INTO test_filter_lambda VALUES
  (1, array('a', NULL, '', 'b', ' ', NULL, 'abc'), array(1, NULL, 0, 5, -3), array(1L, NULL, 0L, 9000000000L),
   array(1.5, NULL, CAST('NaN' AS DOUBLE), -0.0, 0.0, CAST('Infinity' AS DOUBLE)),
   array(CAST(1.5 AS FLOAT), NULL, CAST('NaN' AS FLOAT), CAST(2.5 AS FLOAT)),
   array(CAST(1.25 AS DECIMAL(10, 2)), NULL, CAST(-3.50 AS DECIMAL(10, 2)), CAST(0 AS DECIMAL(10, 2))),
   array(DATE '2020-01-01', NULL, DATE '2024-02-29'),
   array(TIMESTAMP '2020-01-01 00:00:00', NULL, TIMESTAMP '2024-02-29 12:34:56'),
   array(X'01', NULL, X'', X'FF'),
   array(true, NULL, false),
   array(TIMESTAMP_NTZ '2020-01-01 00:00:00', NULL, TIMESTAMP_NTZ '2024-02-29 12:34:56'),
   3, 2, 'a'),
  (2, array(), array(), array(), array(), array(), array(), array(), array(), array(), array(), array(), 0, 0, NULL),
  (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (4, array(NULL, NULL), array(NULL, NULL), array(NULL, NULL), array(NULL, NULL), array(NULL, NULL),
   array(NULL, NULL), array(NULL, NULL), array(NULL, NULL), array(NULL, NULL), array(NULL, NULL), array(NULL, NULL), 1, NULL, ''),
  (5, array('x', 'y'), array(7, 8, 9), array(7L, 8L), array(7.0, 8.0), array(CAST(7 AS FLOAT)),
   array(CAST(7 AS DECIMAL(10, 2))), array(DATE '2000-01-01'), array(TIMESTAMP '2000-01-01 00:00:00'),
   array(X'AA'), array(true), array(TIMESTAMP_NTZ '2000-01-01 00:00:00'), 8, 7, 'y')

-- The customer shape: drop NULL and empty-string elements.
query
SELECT id, filter(strs, x -> nvl(x, '') != '') FROM test_filter_lambda

query
SELECT id, filter(strs, x -> x IS NOT NULL AND x != '') FROM test_filter_lambda

query
SELECT id, filter(strs, x -> coalesce(x, '') <> '' AND length(x) > 1) FROM test_filter_lambda

-- On a numeric array `nvl(x, '')` makes the lambda cast each element to a string.
query
SELECT id, filter(ints, x -> nvl(x, '') != '') FROM test_filter_lambda

query
SELECT id, filter(ints, x -> x > 0) FROM test_filter_lambda

query
SELECT id, filter(ints, x -> x % 2 = 1 OR x IS NULL) FROM test_filter_lambda

query
SELECT id, filter(longs, x -> x > 0) FROM test_filter_lambda

query
SELECT id, filter(dbls, x -> x > 1.0) FROM test_filter_lambda

query
SELECT id, filter(dbls, x -> isnan(x) OR x = 0.0) FROM test_filter_lambda

query
SELECT id, filter(flts, x -> x > 2) FROM test_filter_lambda

query
SELECT id, filter(decs, x -> x > 0) FROM test_filter_lambda

query
SELECT id, filter(dates, x -> x > DATE '2021-01-01') FROM test_filter_lambda

query
SELECT id, filter(tss, x -> x > TIMESTAMP '2021-01-01 00:00:00') FROM test_filter_lambda

query
SELECT id, filter(ntzs, x -> x > TIMESTAMP_NTZ '2021-01-01 00:00:00') FROM test_filter_lambda

query
SELECT id, filter(bins, x -> length(x) > 0) FROM test_filter_lambda

query
SELECT id, filter(bools, x -> x) FROM test_filter_lambda

query
SELECT id, filter(bools, x -> NOT x) FROM test_filter_lambda

-- Constant results: every element kept, or none.
query
SELECT id, filter(ints, x -> true) FROM test_filter_lambda

query
SELECT id, filter(ints, x -> false) FROM test_filter_lambda

query
SELECT id, filter(ints, x -> CAST(NULL AS BOOLEAN)) FROM test_filter_lambda

-- Captured columns, one value per row.
query
SELECT id, filter(ints, e -> e > threshold) FROM test_filter_lambda

query
SELECT id, filter(strs, e -> e = needle) FROM test_filter_lambda

query
SELECT id, filter(ints, e -> e + x > threshold + 1) FROM test_filter_lambda

-- The lambda parameter shadows a column of the same name.
query
SELECT id, filter(ints, x -> x > 0) FROM test_filter_lambda WHERE x IS NOT NULL

-- A captured column that the lambda body does not mention, and a parameter the body ignores.
query
SELECT id, filter(ints, e -> threshold > 1) FROM test_filter_lambda

-- Filters over a filter, and over a computed array.
query
SELECT id, filter(filter(ints, a -> a IS NOT NULL), b -> b > 0) FROM test_filter_lambda

query
SELECT id, filter(array(x, threshold, 5), v -> v > 1) FROM test_filter_lambda

query
SELECT id, size(filter(ints, v -> v > 0)) FROM test_filter_lambda

-- A body that cannot run natively stays on the codegen dispatcher and must still match Spark.
query
SELECT id, filter(strs, v -> v RLIKE '^a') FROM test_filter_lambda

-- A nested lambda runs through the dispatcher.
query
SELECT id, filter(ints, v -> exists(ints, w -> w = v + 1)) FROM test_filter_lambda

-- A two-parameter lambda runs through the dispatcher.
query
SELECT id, filter(ints, (v, i) -> v > i) FROM test_filter_lambda

-- Literal arrays, with and without a captured column. No input column feeds the array.
query
SELECT id, filter(array(1, NULL, 3, 5), v -> v > 1) FROM test_filter_lambda

query
SELECT id, filter(array('a', NULL, '', 'b'), v -> nvl(v, '') != '') FROM test_filter_lambda

query
SELECT id, filter(array(1, 2, 3, 4), v -> v > threshold) FROM test_filter_lambda

-- Batches of one shape: every array NULL, every array empty, and no rows at all.
query
SELECT id, filter(ints, v -> v > 0) FROM test_filter_lambda WHERE id = 3

query
SELECT id, filter(strs, v -> nvl(v, '') != '') FROM test_filter_lambda WHERE id = 2

query
SELECT id, filter(ints, v -> v > 0) FROM test_filter_lambda WHERE id < 0
