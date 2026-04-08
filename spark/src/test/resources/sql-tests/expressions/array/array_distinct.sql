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
query spark_answer_only
SELECT array_distinct(arr) FROM test_array_distinct_int

-- literal arguments
query spark_answer_only
SELECT array_distinct(array(1, 2, 2, 3, 3))

-- all NULLs
query spark_answer_only
SELECT array_distinct(array(CAST(NULL AS INT), CAST(NULL AS INT)))

-- NULL input
query spark_answer_only
SELECT array_distinct(CAST(NULL AS array<int>))

-- boundary values
query spark_answer_only
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

query spark_answer_only
SELECT array_distinct(arr) FROM test_array_distinct_long

-- boundary values
query spark_answer_only
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

query spark_answer_only
SELECT array_distinct(arr) FROM test_array_distinct_string

-- empty string and NULL distinction
query spark_answer_only
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

query spark_answer_only
SELECT array_distinct(arr) FROM test_array_distinct_bool

-- ===== DOUBLE arrays =====

statement
CREATE TABLE test_array_distinct_double(arr array<double>) USING parquet

statement
INSERT INTO test_array_distinct_double VALUES
  (array(1.123, 0.1234, 1.121, 1.123, 0.1234)),
  (NULL),
  (array(NULL, 1.0, NULL, 2.0))

query spark_answer_only
SELECT array_distinct(arr) FROM test_array_distinct_double

-- NaN deduplication
query spark_answer_only
SELECT array_distinct(array(CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE), 1.0, 1.0))

-- NaN with NULL
query spark_answer_only
SELECT array_distinct(array(CAST('NaN' AS DOUBLE), NULL, CAST('NaN' AS DOUBLE), NULL, 1.0))

-- Infinity
query spark_answer_only
SELECT array_distinct(array(CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE), 0.0))

-- negative zero
query spark_answer_only
SELECT array_distinct(array(0.0, -0.0, 1.0))

-- ===== FLOAT arrays =====

statement
CREATE TABLE test_array_distinct_float(arr array<float>) USING parquet

statement
INSERT INTO test_array_distinct_float VALUES
  (array(CAST(1.123 AS FLOAT), CAST(0.1234 AS FLOAT), CAST(1.121 AS FLOAT), CAST(1.123 AS FLOAT))),
  (NULL),
  (array(CAST(NULL AS FLOAT), CAST(1.0 AS FLOAT), CAST(NULL AS FLOAT)))

query spark_answer_only
SELECT array_distinct(arr) FROM test_array_distinct_float

-- Float NaN deduplication
query spark_answer_only
SELECT array_distinct(array(CAST('NaN' AS FLOAT), CAST('NaN' AS FLOAT), CAST(1.0 AS FLOAT)))

-- ===== DECIMAL arrays =====

statement
CREATE TABLE test_array_distinct_decimal(arr array<decimal(10,2)>) USING parquet

statement
INSERT INTO test_array_distinct_decimal VALUES
  (array(1.10, 2.20, 1.10, 3.30)),
  (NULL),
  (array(NULL, 1.10, NULL, 1.10))

query spark_answer_only
SELECT array_distinct(arr) FROM test_array_distinct_decimal

-- ===== Nested array (array of arrays) =====

query spark_answer_only
SELECT array_distinct(array(array(1, 2), array(3, 4), array(1, 2), array(3, 4)))

query spark_answer_only
SELECT array_distinct(array(array(1, 2), CAST(NULL AS array<int>), array(1, 2), CAST(NULL AS array<int>)))
