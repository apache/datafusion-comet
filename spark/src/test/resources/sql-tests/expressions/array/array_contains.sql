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
CREATE TABLE test_array_contains(arr array<int>, val int) USING parquet

statement
INSERT INTO test_array_contains VALUES (array(1, 2, 3), 2), (array(1, 2, 3), 4), (array(1, NULL, 3), NULL), (array(), 1), (NULL, 1)

query
SELECT array_contains(arr, val) FROM test_array_contains

-- column + literal
query
SELECT array_contains(arr, 2) FROM test_array_contains

-- literal + column
query
SELECT array_contains(array(1, 2, 3), val) FROM test_array_contains

-- literal + literal
query
SELECT array_contains(array(1, 2, 3), 2), array_contains(array(1, 2, 3), 4), array_contains(array(), 1), array_contains(cast(NULL as array<int>), 1)

-- NULL element/value cases
query
SELECT array_contains(array(1, NULL, 3), 2), array_contains(array(1, NULL, 3), 1), array_contains(array(1, 2, 3), cast(NULL as int)), array_contains(array(1, NULL, 3), cast(NULL as int))

-- Additional NULL array tests (issue #3345 fix verification)
-- NULL array with integer value
query
SELECT array_contains(cast(NULL as array<int>), 1)

-- NULL array with string value
query
SELECT array_contains(cast(NULL as array<string>), 'test')

-- NULL array with NULL value
query
SELECT array_contains(cast(NULL as array<int>), cast(NULL as int))

-- NULL array with column value
query
SELECT array_contains(cast(NULL as array<int>), val) FROM test_array_contains

-- ============================================================
-- Floating-point elements: Spark compares with ordering.equiv, so -0.0 == +0.0 and NaN == NaN.
-- Native array_contains compares raw Arrow values bitwise, so float/double element types route
-- through the JVM codegen dispatcher (Spark's own doGenCode) to stay native and Spark-exact.
-- ============================================================

statement
CREATE TABLE test_array_contains_fp(arr array<double>, val double) USING parquet

statement
INSERT INTO test_array_contains_fp VALUES
  (array(0.0D, 1.0D), -0.0D),
  (array(-0.0D, 2.0D), 0.0D),
  (array(cast('NaN' as double), 3.0D), cast('NaN' as double)),
  (array(1.0D, 2.0D), 5.0D),
  (array(1.0D, NULL), cast(NULL as double)),
  (array(1.0D, NULL), 4.0D),
  (NULL, 1.0D)
query
SELECT array_contains(arr, val) FROM test_array_contains_fp

-- literal forms: -0.0 vs +0.0 and NaN vs NaN, for DOUBLE and FLOAT
query
SELECT array_contains(array(-0.0D, 1.0D), 0.0D),
       array_contains(array(0.0D, 1.0D), -0.0D),
       array_contains(array(cast('NaN' as double), 1.0D), cast('NaN' as double)),
       array_contains(array(cast('-0.0' as float), 1.0F), cast('0.0' as float)),
       array_contains(array(cast('NaN' as float), 1.0F), cast('NaN' as float))

-- nested float element: the element type is array<double>, so the float decline must recurse
query
SELECT array_contains(array(array(0.0D), array(1.0D)), array(-0.0D))
