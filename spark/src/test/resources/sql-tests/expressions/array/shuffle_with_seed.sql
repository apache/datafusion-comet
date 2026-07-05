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

-- The two argument shuffle(array, seed) form only exists in Spark 4.0+. With a fixed seed the
-- permutation is deterministic, so Comet must reproduce Spark's output exactly. Comet drives the
-- same Commons Math3 MersenneTwister and inside-out Fisher-Yates as
-- org.apache.spark.sql.catalyst.util.RandomIndicesGenerator, combining the seed with the partition
-- index, so these queries assert bit-for-bit equality with Spark in the default query mode.

-- MinSparkVersion: 4.0

-- ===== INT arrays =====

statement
CREATE TABLE test_shuffle_seed_int(arr array<int>) USING parquet

statement
INSERT INTO test_shuffle_seed_int VALUES
  (array(1, 2, 3, 4, 5)),
  (array()),
  (NULL),
  (array(NULL, 1, NULL, 2)),
  (array(1)),
  (array(-2147483648, 2147483647, 0)),
  (array(10, 20, 30, 40, 50, 60, 70, 80, 90, 100))

-- column argument, fixed seed
query
SELECT shuffle(arr, 42) FROM test_shuffle_seed_int

-- column argument, seed 0
query
SELECT shuffle(arr, 0) FROM test_shuffle_seed_int

-- column argument, negative seed
query
SELECT shuffle(arr, -12345) FROM test_shuffle_seed_int

-- literal argument, fixed seed
query
SELECT shuffle(array(1, 20, 3, 5), 42)

-- larger literal array, fixed seed
query
SELECT shuffle(array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 123456789)

-- literal array with NULLs, fixed seed
query
SELECT shuffle(array(1, 20, NULL, 3), 7)

-- single element
query
SELECT shuffle(array(42), 1)

-- empty array
query
SELECT shuffle(CAST(array() AS array<int>), 1)

-- NULL input
query
SELECT shuffle(CAST(NULL AS array<int>), 1)

-- ===== LONG arrays =====

statement
CREATE TABLE test_shuffle_seed_long(arr array<bigint>) USING parquet

statement
INSERT INTO test_shuffle_seed_long VALUES
  (array(1, 2, 3, 4, 5)),
  (NULL),
  (array(NULL, 1, NULL, 2)),
  (array(-9223372036854775808, 9223372036854775807, 0))

query
SELECT shuffle(arr, 42) FROM test_shuffle_seed_long

query
SELECT shuffle(array(CAST(-9223372036854775808 AS BIGINT), CAST(0 AS BIGINT), CAST(9223372036854775807 AS BIGINT)), 99)

-- ===== STRING arrays =====

statement
CREATE TABLE test_shuffle_seed_string(arr array<string>) USING parquet

statement
INSERT INTO test_shuffle_seed_string VALUES
  (array('a', 'b', 'c', 'd', 'e')),
  (array('')),
  (NULL),
  (array(NULL, 'a', NULL, 'b')),
  (array('hello', 'world', '中文', 'é', '\t'))

query
SELECT shuffle(arr, 42) FROM test_shuffle_seed_string

query
SELECT shuffle(array('a', 'b', 'c', 'd', 'e', 'f'), 3)

-- multibyte and special characters
query
SELECT shuffle(array('中文', 'é', '\t', 'x'), 5)

-- ===== BOOLEAN arrays =====

statement
CREATE TABLE test_shuffle_seed_bool(arr array<boolean>) USING parquet

statement
INSERT INTO test_shuffle_seed_bool VALUES
  (array(true, false, true, false, true)),
  (NULL),
  (array(NULL, true, NULL, false))

query
SELECT shuffle(arr, 42) FROM test_shuffle_seed_bool

-- ===== DOUBLE arrays (NaN, Infinity, -0.0) =====

statement
CREATE TABLE test_shuffle_seed_double(arr array<double>) USING parquet

statement
INSERT INTO test_shuffle_seed_double VALUES
  (array(1.1, 2.2, 3.3, 4.4, 5.5)),
  (NULL),
  (array(CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE), 0.0, -0.0))

query
SELECT shuffle(arr, 42) FROM test_shuffle_seed_double

query
SELECT shuffle(array(CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE), 0.0, -0.0), 8)

-- ===== DECIMAL arrays =====

statement
CREATE TABLE test_shuffle_seed_decimal(arr array<decimal(10,2)>) USING parquet

statement
INSERT INTO test_shuffle_seed_decimal VALUES
  (array(1.10, 2.20, 3.30, 4.40)),
  (NULL),
  (array(NULL, 1.10, NULL, 2.20))

query
SELECT shuffle(arr, 42) FROM test_shuffle_seed_decimal

-- ===== DATE arrays =====

statement
CREATE TABLE test_shuffle_seed_date(arr array<date>) USING parquet

statement
INSERT INTO test_shuffle_seed_date VALUES
  (array(DATE '2020-01-01', DATE '2021-06-15', DATE '1999-12-31', DATE '2000-02-29'))

query
SELECT shuffle(arr, 42) FROM test_shuffle_seed_date

-- ===== Nested arrays (array of arrays) =====

query
SELECT shuffle(array(array(1, 2), array(3, 4), array(5, 6), array(7, 8)), 42)

query
SELECT shuffle(array(array(1, 2), CAST(NULL AS array<int>), array(5, 6)), 4)
