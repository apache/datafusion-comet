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

-- ===== INT arrays =====

statement
CREATE TABLE test_array_distinct_int(arr array<int>) USING parquet

statement
INSERT INTO test_array_distinct_int VALUES
  (array(1, 2, 2, 3, 3)),
  (array()),
  (NULL),
  (array(NULL, 1, NULL, 2)),
  (array(1)),
  (array(NULL, NULL, NULL)),
  (array(-2147483648, 2147483647, -2147483648, 0)),
  (array(0, -1, -1, 0, 1))

-- column argument
query
SELECT array_distinct(arr) FROM test_array_distinct_int

-- literal arguments
query
SELECT array_distinct(array(1, 2, 2, 3, 3))

-- all NULLs
query
SELECT array_distinct(array(CAST(NULL AS INT), CAST(NULL AS INT)))

-- NULL input
query
SELECT array_distinct(CAST(NULL AS array<int>))

-- boundary values
query
SELECT array_distinct(array(-2147483648, 2147483647, -2147483648, 2147483647, 0))

-- ===== LONG arrays =====

statement
CREATE TABLE test_array_distinct_long(arr array<bigint>) USING parquet

statement
INSERT INTO test_array_distinct_long VALUES
  (array(1, 2, 2, 3, 3)),
  (NULL),
  (array(NULL, 1, NULL, 2)),
  (array(-9223372036854775808, 9223372036854775807, -9223372036854775808))

query
SELECT array_distinct(arr) FROM test_array_distinct_long

-- boundary values
query
SELECT array_distinct(array(CAST(-9223372036854775808 AS BIGINT), CAST(9223372036854775807 AS BIGINT), CAST(-9223372036854775808 AS BIGINT)))

-- ===== STRING arrays =====

statement
CREATE TABLE test_array_distinct_string(arr array<string>) USING parquet

statement
INSERT INTO test_array_distinct_string VALUES
  (array('b', 'a', 'a', 'c', 'b')),
  (array('')),
  (NULL),
  (array(NULL, 'a', NULL, 'a')),
  (array('', '', NULL, '')),
  (array('hello', 'world', 'hello'))

query
SELECT array_distinct(arr) FROM test_array_distinct_string

-- empty string and NULL distinction
query
SELECT array_distinct(array('', NULL, '', NULL, 'a'))

-- ===== BOOLEAN arrays =====

statement
CREATE TABLE test_array_distinct_bool(arr array<boolean>) USING parquet

statement
INSERT INTO test_array_distinct_bool VALUES
  (array(true, false, false, true)),
  (array(true, true)),
  (NULL),
  (array(NULL, true, NULL, false))

query
SELECT array_distinct(arr) FROM test_array_distinct_bool

-- ===== DOUBLE arrays =====

statement
CREATE TABLE test_array_distinct_double(arr array<double>) USING parquet

statement
INSERT INTO test_array_distinct_double VALUES
  (array(1.123, 0.1234, 1.121, 1.123, 0.1234)),
  (NULL),
  (array(NULL, 1.0, NULL, 2.0)),
  (array(CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE))),
  (array(CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE), 1.0, 1.0)),
  (array(CAST('NaN' AS DOUBLE), NULL, CAST('NaN' AS DOUBLE), NULL, 1.0)),
  (array(CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE), 0.0))

query
SELECT array_distinct(arr) FROM test_array_distinct_double

-- NaN deduplication
query
SELECT array_distinct(array(CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE), 1.0, 1.0))

-- NaN with NULL
query
SELECT array_distinct(array(CAST('NaN' AS DOUBLE), NULL, CAST('NaN' AS DOUBLE), NULL, 1.0))

-- Infinity
query
SELECT array_distinct(array(CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE), 0.0))

-- negative zero (literal). Spark's NormalizeFloatingNumbers rewrites -0.0 to 0.0 at
-- analysis time, so both Spark and Comet collapse it and agree here.
query ignore(https://issues.apache.org/jira/browse/SPARK-54918)
SELECT array_distinct(array(0.0, double('-0.0'), 1.0))

-- negative zero (column-sourced). NormalizeFloatingNumbers does not touch parquet
-- columns, so Spark keeps -0.0 distinct from 0.0 while Comet (DataFusion) collapses
-- them: array_distinct([0.0, -0.0, 1.0]) is [0.0, -0.0, 1.0] in Spark but [0.0, 1.0]
-- in Comet. Skip until Spark normalizes these zeros (Spark 4.2+, SPARK-54918).
statement
CREATE TABLE test_array_distinct_dbl_negzero(arr array<double>) USING parquet

statement
INSERT INTO test_array_distinct_dbl_negzero VALUES
  (array(0.0, double('-0.0'), 1.0))

query ignore(https://issues.apache.org/jira/browse/SPARK-54918)
SELECT array_distinct(arr) FROM test_array_distinct_dbl_negzero

-- ===== FLOAT arrays =====

statement
CREATE TABLE test_array_distinct_float(arr array<float>) USING parquet

statement
INSERT INTO test_array_distinct_float VALUES
  (array(CAST(1.123 AS FLOAT), CAST(0.1234 AS FLOAT), CAST(1.121 AS FLOAT), CAST(1.123 AS FLOAT))),
  (NULL),
  (array(CAST(NULL AS FLOAT), CAST(1.0 AS FLOAT), CAST(NULL AS FLOAT))),
  (array(CAST('NaN' AS FLOAT), CAST('NaN' AS FLOAT))),
  (array(CAST('NaN' AS FLOAT), CAST('NaN' AS FLOAT), CAST(1.0 AS FLOAT))),
  (array(CAST('NaN' AS FLOAT), NULL, CAST('NaN' AS FLOAT), NULL, CAST(1.0 AS FLOAT))),
  (array(CAST('Infinity' AS FLOAT), CAST('-Infinity' AS FLOAT), CAST('Infinity' AS FLOAT), CAST(0.0 AS FLOAT)))

query
SELECT array_distinct(arr) FROM test_array_distinct_float

-- Float NaN deduplication
query
SELECT array_distinct(array(CAST('NaN' AS FLOAT), CAST('NaN' AS FLOAT), CAST(1.0 AS FLOAT)))

-- negative zero (column-sourced). Same divergence as the double case above, skipped
-- until Spark normalizes these zeros (Spark 4.2+, SPARK-54918).
statement
CREATE TABLE test_array_distinct_flt_negzero(arr array<float>) USING parquet

statement
INSERT INTO test_array_distinct_flt_negzero VALUES
  (array(CAST(0.0 AS FLOAT), float('-0.0'), CAST(1.0 AS FLOAT)))

query ignore(https://issues.apache.org/jira/browse/SPARK-54918)
SELECT array_distinct(arr) FROM test_array_distinct_flt_negzero

-- ===== DECIMAL arrays =====

statement
CREATE TABLE test_array_distinct_decimal(arr array<decimal(10,2)>) USING parquet

statement
INSERT INTO test_array_distinct_decimal VALUES
  (array(1.10, 2.20, 1.10, 3.30)),
  (NULL),
  (array(NULL, 1.10, NULL, 1.10))

query
SELECT array_distinct(arr) FROM test_array_distinct_decimal

-- ===== Nested array (array of arrays) =====

query
SELECT array_distinct(array(array(1, 2), array(3, 4), array(1, 2), array(3, 4)))

query
SELECT array_distinct(array(array(1, 2), CAST(NULL AS array<int>), array(1, 2), CAST(NULL AS array<int>)))
