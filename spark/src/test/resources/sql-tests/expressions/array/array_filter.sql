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

statement
CREATE TABLE test_array_filter(arr array<int>) USING parquet

statement
INSERT INTO test_array_filter VALUES (array(1, 2, 3, 4, 5)), (array(-1, 0, 1)), (array(10)), (NULL)

query
SELECT filter(arr, x -> x > 2) FROM test_array_filter

query
SELECT filter(arr, x -> x >= 0) FROM test_array_filter

query
SELECT filter(arr, (x, i) -> i > 0) FROM test_array_filter

statement
CREATE TABLE test_array_filter_captured(arr array<int>, c int) USING parquet

statement
INSERT INTO test_array_filter_captured VALUES
  (array(1, NULL, 2), 5),
  (array(3, NULL, 4), NULL),
  (array(NULL, NULL), 7),
  (NULL, 1)

-- Genuine array_compact fast path: IsNotNull on the lambda variable drops the null elements.
query
SELECT filter(arr, x -> x IS NOT NULL) FROM test_array_filter_captured

-- Regression for #4830: IsNotNull on captured column `c` is not array_compact and must not drop
-- the null elements of `arr`.
query
SELECT filter(arr, x -> c IS NOT NULL) FROM test_array_filter_captured

-- IsNotNull on an expression of the lambda var: the operand is not a bare NamedLambdaVariable,
-- so the guard must reject it and the codegen dispatcher must run Spark's semantics.
query
SELECT filter(arr, x -> (x + 1) IS NOT NULL) FROM test_array_filter_captured

-- Compound predicate combining IsNotNull with another condition: structural match must fail so
-- Spark's own evaluation is used.
query
SELECT filter(arr, x -> x IS NOT NULL AND x > 1) FROM test_array_filter_captured

-- Boundary cases for the fast path: null array input and an all-nulls array. array_compact must
-- propagate the row-level null and turn the all-nulls array into an empty array.
statement
CREATE TABLE test_array_filter_boundary(arr array<int>) USING parquet

statement
INSERT INTO test_array_filter_boundary VALUES (NULL), (array()), (array(NULL, NULL))

query
SELECT filter(arr, x -> x IS NOT NULL) FROM test_array_filter_boundary
