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

-- `exists` with a lambda whose body Comet evaluates natively runs through DataFusion's
-- `array_any_match`. Each query below must return what Spark returns, including for NULL arrays,
-- empty arrays, NULL elements, and predicates that are NULL for some elements and false for the
-- rest, where Spark's three-valued logic returns NULL.

-- Config: spark.sql.ansi.enabled=false
-- Config: spark.sql.legacy.followThreeValuedLogicInArrayExists=true

statement
CREATE TABLE test_exists_lambda(
  id int,
  strs array<string>,
  ints array<int>,
  dbls array<double>,
  flts array<float>,
  nested array<array<string>>,
  threshold int,
  code string,
  flag boolean
) USING parquet

statement
INSERT INTO test_exists_lambda VALUES
  (1, array('a', NULL, '', 'b'), array(1, NULL, 0, 5, -3), array(1.5, NULL, CAST('NaN' AS DOUBLE), -0.0, 0.0, 95.0),
   array(CAST(1.5 AS FLOAT), -2.5, CAST(2.5 AS FLOAT)), array(array('x', ''), array('y')), 3, 'US', true),
  (2, array(), array(), array(), array(), array(), 0, 'CN', false),
  (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (4, array(NULL, NULL), array(NULL, NULL), array(NULL, NULL), array(NULL, NULL), array(array(NULL), NULL), 1, '', true),
  (5, array('x', 'y'), array(7, 8, 9), array(7.0, 8.0), array(CAST(7 AS FLOAT)), array(array('x', 'y')), 8, 'IN', true),
  (6, array('b'), array(2, 4), array(40.5, 41.0), array(CAST(1 AS FLOAT)), array(array('b')), 100, 'US', false)

-- Range checks over an array of coordinates.
query expect_native(exists)
SELECT id, exists(dbls, x -> isnull(x) OR x < -90.0 OR x > 90.0 OR x = 0.0) FROM test_exists_lambda

query expect_native(exists)
SELECT id, size(dbls) IN (0, -1) OR exists(dbls, x -> isnull(x) OR x = 0.0) FROM test_exists_lambda

query expect_native(exists)
SELECT id, exists(flts, x -> x < 0.0 OR isnull(x)) FROM test_exists_lambda

-- Strings, with a NULL or empty-string element.
query expect_native(exists)
SELECT id, exists(strs, x -> isnull(x) OR x = '') FROM test_exists_lambda

query expect_native(exists)
SELECT id, exists(strs, x -> x IN ('a', 'b', 'c')) FROM test_exists_lambda

query expect_native(exists)
SELECT id, exists(strs, x -> x = 'a') FROM test_exists_lambda

-- Integers, and the NULL result when no element matches but the predicate was NULL for some.
query expect_native(exists)
SELECT id, exists(ints, x -> x > 4) FROM test_exists_lambda

query expect_native(exists)
SELECT id, exists(ints, x -> x > 100) FROM test_exists_lambda

query expect_native(exists)
SELECT id, exists(ints, x -> CAST(NULL AS BOOLEAN)) FROM test_exists_lambda

query expect_native(exists)
SELECT id, exists(ints, x -> true), exists(ints, x -> false) FROM test_exists_lambda

-- A captured fixed-width column.
query expect_native(exists)
SELECT id, exists(ints, x -> x > threshold) FROM test_exists_lambda

-- The argument is a native expression of its own.
query expect_native(exists)
SELECT id, exists(flatten(nested), x -> isnull(x) OR x = '') FROM test_exists_lambda

-- In a filter, in an aggregate, and combined with other conditions.
query expect_native(exists)
SELECT id FROM test_exists_lambda WHERE exists(ints, x -> x > 4) ORDER BY id

-- Inside an aggregate the harness counts the expression as dispatched as well as native, so these only
-- compare with Spark.
query
SELECT count(CASE WHEN exists(dbls, x -> isnull(x) OR x = 0.0) AND flag THEN 1 END)
FROM test_exists_lambda

query
SELECT count(CASE WHEN exists(strs, x -> x = '') AND code = 'US' THEN 1 END)
FROM test_exists_lambda

-- A body that converts each element to a date, all of it natively supported expressions.
query expect_native(exists)
SELECT id, exists(dbls, x -> datediff(DATE '2024-03-15', CAST(timestamp_seconds(CAST(x AS BIGINT)) AS DATE)) >= 2)
FROM test_exists_lambda

-- exists next to another lambda, as the right operand of OR, and inside an argument that is evaluated only
-- for some rows.
query expect_native(exists)
SELECT id, size(ints) < 1 OR exists(ints, x -> x > 4) FROM test_exists_lambda

query expect_native(exists)
SELECT id, array_contains(ints, size(filter(ints, x -> x > 1))) OR exists(ints, x -> x > 100) FROM test_exists_lambda
