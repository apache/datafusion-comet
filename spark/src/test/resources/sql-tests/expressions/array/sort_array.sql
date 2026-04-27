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

statement
CREATE TABLE test_sort_array_int(arr array<int>) USING parquet

statement
INSERT INTO test_sort_array_int VALUES
  (array(3, 1, 4, 1, 5)),
  (array(3, NULL, 1, NULL, 2)),
  (array()),
  (NULL)

query
SELECT sort_array(arr) FROM test_sort_array_int

query
SELECT sort_array(arr, true) FROM test_sort_array_int

query
SELECT sort_array(arr, false) FROM test_sort_array_int

statement
CREATE TABLE test_sort_array_string(arr array<string>) USING parquet

statement
INSERT INTO test_sort_array_string VALUES
  (array('d', 'c', 'b', 'a')),
  (array('b', NULL, 'a')),
  (array()),
  (NULL)

query
SELECT sort_array(arr) FROM test_sort_array_string

query
SELECT sort_array(arr, true) FROM test_sort_array_string

query
SELECT sort_array(arr, false) FROM test_sort_array_string

statement
CREATE TABLE test_sort_array_double(arr array<double>) USING parquet

statement
INSERT INTO test_sort_array_double VALUES
  (array(
    CAST('Infinity' AS DOUBLE),
    CAST('-Infinity' AS DOUBLE),
    CAST('NaN' AS DOUBLE),
    3.0,
    1.0,
    NULL,
    -0.0,
    0.0)),
  (array(
    CAST('NaN' AS DOUBLE),
    CAST('NaN' AS DOUBLE),
    CAST('Infinity' AS DOUBLE),
    CAST('-Infinity' AS DOUBLE),
    -5.0,
    2.0)),
  (array()),
  (NULL)

query
SELECT sort_array(arr) FROM test_sort_array_double

query
SELECT sort_array(arr, true) FROM test_sort_array_double

query
SELECT sort_array(arr, false) FROM test_sort_array_double

statement
CREATE TABLE test_sort_array_float(arr array<float>) USING parquet

statement
INSERT INTO test_sort_array_float VALUES
  (array(
    CAST('Infinity' AS FLOAT),
    CAST('-Infinity' AS FLOAT),
    CAST('NaN' AS FLOAT),
    CAST(3.0 AS FLOAT),
    CAST(1.0 AS FLOAT),
    CAST(NULL AS FLOAT),
    CAST(-0.0 AS FLOAT),
    CAST(0.0 AS FLOAT))),
  (array(
    CAST('NaN' AS FLOAT),
    CAST('Infinity' AS FLOAT),
    CAST('-Infinity' AS FLOAT),
    CAST(-5.0 AS FLOAT),
    CAST(2.0 AS FLOAT))),
  (array()),
  (NULL)

query
SELECT sort_array(arr) FROM test_sort_array_float

query
SELECT sort_array(arr, true) FROM test_sort_array_float

query
SELECT sort_array(arr, false) FROM test_sort_array_float

statement
CREATE TABLE test_sort_array_decimal(arr array<decimal(12, 3)>) USING parquet

statement
INSERT INTO test_sort_array_decimal VALUES
  (CAST(array(CAST(100 AS DECIMAL(10, 0)), CAST(10 AS DECIMAL(10, 0))) AS array<decimal(12, 3)>)),
  (CAST(array(
    CAST(1 AS DECIMAL(10, 0)),
    CAST(1.0 AS DECIMAL(10, 1)),
    CAST(1.00 AS DECIMAL(10, 2)),
    CAST(1.000 AS DECIMAL(10, 3))) AS array<decimal(12, 3)>)),
  (array()),
  (NULL)

query
SELECT sort_array(arr) FROM test_sort_array_decimal

query
SELECT sort_array(arr, true) FROM test_sort_array_decimal

query
SELECT sort_array(arr, false) FROM test_sort_array_decimal

statement
CREATE TABLE test_sort_array_boolean(arr array<boolean>) USING parquet

statement
INSERT INTO test_sort_array_boolean VALUES
  (array(true, false, true, false)),
  (array(true, false, true, NULL, false)),
  (array()),
  (NULL)

query
SELECT sort_array(arr) FROM test_sort_array_boolean

query
SELECT sort_array(arr, true) FROM test_sort_array_boolean

query
SELECT sort_array(arr, false) FROM test_sort_array_boolean

statement
CREATE TABLE test_sort_array_date(arr array<date>) USING parquet

statement
INSERT INTO test_sort_array_date VALUES
  (array(DATE '2026-01-03', DATE '2026-01-01', DATE '2026-01-02')),
  (array(DATE '2026-01-02', NULL, DATE '2026-01-01')),
  (array()),
  (NULL)

query
SELECT sort_array(arr) FROM test_sort_array_date

query
SELECT sort_array(arr, true) FROM test_sort_array_date

query
SELECT sort_array(arr, false) FROM test_sort_array_date

statement
CREATE TABLE test_sort_array_timestamp(arr array<timestamp>) USING parquet

statement
INSERT INTO test_sort_array_timestamp VALUES
  (array(
    TIMESTAMP '2026-01-03 01:00:00',
    TIMESTAMP '2026-01-01 02:00:00',
    TIMESTAMP '2026-01-02 03:00:00')),
  (array(
    TIMESTAMP '2026-01-02 00:00:00',
    NULL,
    TIMESTAMP '2026-01-01 00:00:00')),
  (array()),
  (NULL)

query
SELECT sort_array(arr) FROM test_sort_array_timestamp

query
SELECT sort_array(arr, true) FROM test_sort_array_timestamp

query
SELECT sort_array(arr, false) FROM test_sort_array_timestamp

statement
CREATE TABLE test_sort_array_binary(arr array<binary>) USING parquet

statement
INSERT INTO test_sort_array_binary VALUES
  (array(unhex('FF'), unhex('00'), unhex('0A'))),
  (array(unhex('0B'), NULL, unhex('01'))),
  (array()),
  (NULL)

query
SELECT
  hex(element_at(sorted_arr, 1)),
  hex(element_at(sorted_arr, 2)),
  hex(element_at(sorted_arr, 3))
FROM (
  SELECT sort_array(arr) AS sorted_arr
  FROM test_sort_array_binary
)

query
SELECT
  hex(element_at(sorted_arr, 1)),
  hex(element_at(sorted_arr, 2)),
  hex(element_at(sorted_arr, 3))
FROM (
  SELECT sort_array(arr, true) AS sorted_arr
  FROM test_sort_array_binary
)

query
SELECT
  hex(element_at(sorted_arr, 1)),
  hex(element_at(sorted_arr, 2)),
  hex(element_at(sorted_arr, 3))
FROM (
  SELECT sort_array(arr, false) AS sorted_arr
  FROM test_sort_array_binary
)

statement
CREATE TABLE test_sort_array_struct(arr array<struct<a:int,b:string>>) USING parquet

statement
INSERT INTO test_sort_array_struct VALUES
  (array(
    named_struct('a', 2, 'b', 'b'),
    named_struct('a', 1, 'b', 'c'),
    named_struct('a', 1, 'b', 'a'))),
  (array(
    named_struct('a', 2, 'b', NULL),
    named_struct('a', 1, 'b', 'z'),
    named_struct('a', 1, 'b', NULL))),
  (array()),
  (NULL)

query
SELECT sort_array(arr) FROM test_sort_array_struct

query
SELECT sort_array(arr, false) FROM test_sort_array_struct

statement
CREATE TABLE test_sort_array_nested(arr array<array<int>>) USING parquet

statement
INSERT INTO test_sort_array_nested VALUES
  (array(array(2, 3), array(1), array(2, 1))),
  (array(array(1, NULL), array(1), NULL)),
  (array()),
  (NULL)

query
SELECT sort_array(arr) FROM test_sort_array_nested

query
SELECT sort_array(arr, false) FROM test_sort_array_nested

statement
CREATE TABLE test_sort_array_nested_struct(arr array<array<struct<a:int>>>) USING parquet

statement
INSERT INTO test_sort_array_nested_struct VALUES
  (array(
    array(named_struct('a', 2)),
    array(named_struct('a', 1)))),
  (array()),
  (NULL)

query expect_fallback(Sort on array element type ArrayType(StructType(StructField(a,IntegerType)
SELECT sort_array(arr) FROM test_sort_array_nested_struct

query expect_fallback(Sort on array element type ArrayType(StructType(StructField(a,IntegerType)
SELECT sort_array(arr, false) FROM test_sort_array_nested_struct

-- literal arguments
-- IgnoreFromSparkVersion: 4.1 https://github.com/apache/datafusion-comet/issues/4098
query
SELECT
  sort_array(array(3, 1, 4, 1, 5)),
  sort_array(array(3, 1, 4, 1, 5), true),
  sort_array(array(3, NULL, 1, NULL, 2)),
  sort_array(array(3, NULL, 1, NULL, 2), false),
  sort_array(
    array(
      CAST('Infinity' AS DOUBLE),
      CAST('-Infinity' AS DOUBLE),
      CAST('NaN' AS DOUBLE),
      1.0,
      NULL,
      -0.0,
      0.0)),
  sort_array(
    array(
      CAST('Infinity' AS DOUBLE),
      CAST('-Infinity' AS DOUBLE),
      CAST('NaN' AS DOUBLE),
      1.0,
      NULL,
      -0.0,
      0.0),
    false),
  sort_array(
    array(
      CAST('Infinity' AS FLOAT),
      CAST('-Infinity' AS FLOAT),
      CAST('NaN' AS FLOAT),
      CAST(1.0 AS FLOAT),
      CAST(NULL AS FLOAT),
      CAST(-0.0 AS FLOAT),
      CAST(0.0 AS FLOAT))),
  sort_array(
    array(
      CAST('Infinity' AS FLOAT),
      CAST('-Infinity' AS FLOAT),
      CAST('NaN' AS FLOAT),
      CAST(1.0 AS FLOAT),
      CAST(NULL AS FLOAT),
      CAST(-0.0 AS FLOAT),
      CAST(0.0 AS FLOAT)),
    false),
  sort_array(
    CAST(array(
      CAST(100 AS DECIMAL(10, 0)),
      CAST(10 AS DECIMAL(10, 0)),
      CAST(1 AS DECIMAL(10, 0)),
      CAST(1.00 AS DECIMAL(10, 2))) AS array<decimal(12, 3)>)),
  sort_array(
    CAST(array(
      CAST(100 AS DECIMAL(10, 0)),
      CAST(10 AS DECIMAL(10, 0)),
      CAST(1 AS DECIMAL(10, 0)),
      CAST(1.00 AS DECIMAL(10, 2))) AS array<decimal(12, 3)>),
    false),
  sort_array(array(true, false, true, false)),
  sort_array(array(true, false, true, NULL, false)),
  sort_array(array(true, false, true, NULL, false), false),
  sort_array(array(DATE '2026-01-03', DATE '2026-01-01', DATE '2026-01-02')),
  sort_array(array(DATE '2026-01-02', NULL, DATE '2026-01-01'), false),
  sort_array(
    array(
      TIMESTAMP '2026-01-03 01:00:00',
      TIMESTAMP '2026-01-01 02:00:00',
      TIMESTAMP '2026-01-02 03:00:00')),
  sort_array(
    array(
      TIMESTAMP '2026-01-02 00:00:00',
      NULL,
      TIMESTAMP '2026-01-01 00:00:00'),
    false),
  hex(element_at(sort_array(array(unhex('FF'), unhex('00'), unhex('0A'))), 1)),
  hex(element_at(sort_array(array(unhex('FF'), unhex('00'), unhex('0A'))), 2)),
  hex(element_at(sort_array(array(unhex('FF'), unhex('00'), unhex('0A'))), 3)),
  hex(element_at(sort_array(array(unhex('0B'), NULL, unhex('01')), false), 1)),
  hex(element_at(sort_array(array(unhex('0B'), NULL, unhex('01')), false), 2)),
  hex(element_at(sort_array(array(unhex('0B'), NULL, unhex('01')), false), 3)),
  sort_array(
    array(
      named_struct('a', 2, 'b', 'b'),
      named_struct('a', 1, 'b', 'c'),
      named_struct('a', 1, 'b', 'a'))),
  sort_array(array(array(2, 3), array(1), array(2, 1))),
  sort_array(array(array(1, NULL), array(1), NULL)),
  sort_array(array(NULL, NULL)),
  sort_array(cast(NULL as array<int>))

query expect_fallback(Sort on array element type ArrayType(StructType(StructField(a,IntegerType)
SELECT sort_array(
  array(
    array(named_struct('a', 2)),
    array(named_struct('a', 1))))

query expect_error(BOOLEAN)
SELECT sort_array(array(3, 1, 4, 1, 5), 1)
