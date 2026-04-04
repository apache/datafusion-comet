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
CREATE TABLE test_sort_array_float(arr array<double>) USING parquet

statement
INSERT INTO test_sort_array_float VALUES
  (array(CAST('NaN' AS DOUBLE), 3.0, 1.0, NULL, -0.0, 0.0)),
  (array(CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE), -5.0, 2.0)),
  (array()),
  (NULL)

query
SELECT sort_array(arr) FROM test_sort_array_float

query
SELECT sort_array(arr, true) FROM test_sort_array_float

query
SELECT sort_array(arr, false) FROM test_sort_array_float

statement
CREATE TABLE test_sort_array_decimal(arr array<decimal(10, 0)>) USING parquet

statement
INSERT INTO test_sort_array_decimal VALUES
  (array(CAST(100 AS DECIMAL(10, 0)), CAST(10 AS DECIMAL(10, 0)))),
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
query
SELECT
  sort_array(array(3, 1, 4, 1, 5)),
  sort_array(array(3, 1, 4, 1, 5), true),
  sort_array(array(3, NULL, 1, NULL, 2)),
  sort_array(array(3, NULL, 1, NULL, 2), false),
  sort_array(array(CAST('NaN' AS DOUBLE), 1.0, NULL, -0.0, 0.0)),
  sort_array(array(CAST('NaN' AS DOUBLE), 1.0, NULL, -0.0, 0.0), false),
  sort_array(array(CAST(100 AS DECIMAL(10, 0)), CAST(10 AS DECIMAL(10, 0)))),
  sort_array(
    array(CAST(100 AS DECIMAL(10, 0)), CAST(10 AS DECIMAL(10, 0))),
    false),
  sort_array(array(true, false, true, false)),
  sort_array(array(true, false, true, NULL, false)),
  sort_array(array(true, false, true, NULL, false), false),
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
