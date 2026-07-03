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

-- shuffle(array) with no seed resolves to a fresh random seed on every analysis, so the Spark and
-- Comet runs use different seeds and their raw output cannot be compared directly. These queries
-- therefore either assert only that shuffle runs natively while preserving array size (size is
-- deterministic), or compare seed independent projections (sort_array over the shuffled result).
-- Exact permutation equality with a fixed seed lives in shuffle_with_seed.sql (Spark 4.0+, where
-- the two argument shuffle(array, seed) form exists).

-- ===== INT arrays =====

statement
CREATE TABLE test_shuffle_int(arr array<int>) USING parquet

statement
INSERT INTO test_shuffle_int VALUES
  (array(1, 2, 3, 4, 5)),
  (array()),
  (NULL),
  (array(NULL, 1, NULL, 2)),
  (array(1)),
  (array(-2147483648, 2147483647, 0)),
  (array(10, 20, 30, 40, 50, 60, 70, 80, 90, 100))

-- shuffle preserves size and runs natively (column argument)
query
SELECT size(shuffle(arr)) FROM test_shuffle_int

-- shuffle preserves size and runs natively (literal argument)
query
SELECT size(shuffle(array(1, 20, 3, 5)))

-- single element
query
SELECT size(shuffle(array(42)))

-- empty array
query
SELECT size(shuffle(CAST(array() AS array<int>)))

-- NULL input
query
SELECT size(shuffle(CAST(NULL AS array<int>)))

-- a shuffle is a permutation, so sorting it matches sorting the input (seed independent)
query spark_answer_only
SELECT sort_array(shuffle(arr)) FROM test_shuffle_int

-- ===== LONG arrays =====

statement
CREATE TABLE test_shuffle_long(arr array<bigint>) USING parquet

statement
INSERT INTO test_shuffle_long VALUES
  (array(1, 2, 3, 4, 5)),
  (NULL),
  (array(NULL, 1, NULL, 2)),
  (array(-9223372036854775808, 9223372036854775807, 0))

query
SELECT size(shuffle(arr)) FROM test_shuffle_long

query spark_answer_only
SELECT sort_array(shuffle(arr)) FROM test_shuffle_long

-- ===== STRING arrays =====

statement
CREATE TABLE test_shuffle_string(arr array<string>) USING parquet

statement
INSERT INTO test_shuffle_string VALUES
  (array('a', 'b', 'c', 'd', 'e')),
  (array('')),
  (NULL),
  (array(NULL, 'a', NULL, 'b')),
  (array('hello', 'world', '中文', 'é', '\t'))

query
SELECT size(shuffle(arr)) FROM test_shuffle_string

query spark_answer_only
SELECT sort_array(shuffle(arr)) FROM test_shuffle_string

-- ===== BOOLEAN arrays =====

statement
CREATE TABLE test_shuffle_bool(arr array<boolean>) USING parquet

statement
INSERT INTO test_shuffle_bool VALUES
  (array(true, false, true, false, true)),
  (NULL),
  (array(NULL, true, NULL, false))

query
SELECT size(shuffle(arr)) FROM test_shuffle_bool

query spark_answer_only
SELECT sort_array(shuffle(arr)) FROM test_shuffle_bool

-- ===== DOUBLE arrays (NaN, Infinity, -0.0) =====

statement
CREATE TABLE test_shuffle_double(arr array<double>) USING parquet

statement
INSERT INTO test_shuffle_double VALUES
  (array(1.1, 2.2, 3.3, 4.4, 5.5)),
  (NULL),
  (array(CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE), 0.0, -0.0))

query
SELECT size(shuffle(arr)) FROM test_shuffle_double

query spark_answer_only
SELECT sort_array(shuffle(arr)) FROM test_shuffle_double

-- ===== DECIMAL arrays =====

statement
CREATE TABLE test_shuffle_decimal(arr array<decimal(10,2)>) USING parquet

statement
INSERT INTO test_shuffle_decimal VALUES
  (array(1.10, 2.20, 3.30, 4.40)),
  (NULL),
  (array(NULL, 1.10, NULL, 2.20))

query
SELECT size(shuffle(arr)) FROM test_shuffle_decimal

query spark_answer_only
SELECT sort_array(shuffle(arr)) FROM test_shuffle_decimal

-- ===== DATE arrays =====

statement
CREATE TABLE test_shuffle_date(arr array<date>) USING parquet

statement
INSERT INTO test_shuffle_date VALUES
  (array(DATE '2020-01-01', DATE '2021-06-15', DATE '1999-12-31', DATE '2000-02-29'))

query
SELECT size(shuffle(arr)) FROM test_shuffle_date

query spark_answer_only
SELECT sort_array(shuffle(arr)) FROM test_shuffle_date

-- ===== Nested arrays (array of arrays) =====

query
SELECT size(shuffle(array(array(1, 2), array(3, 4), array(5, 6), array(7, 8))))

query spark_answer_only
SELECT sort_array(shuffle(array(array(1, 2), array(3, 4), array(5, 6))))
