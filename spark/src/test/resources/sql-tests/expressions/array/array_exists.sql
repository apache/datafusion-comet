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
CREATE TABLE test_array_exists(arr array<int>, sarr array<string>) USING parquet

statement
INSERT INTO test_array_exists VALUES
  (array(1, 2, 3, 4, 5), array('a', 'b', 'c')),
  (array(-1, 0, 1), array('foo')),
  (array(10), array('bar', 'baz')),
  (array(1, NULL, 3), array('x', NULL, 'y')),
  (array(), array()),
  (NULL, NULL)

-- predicate matches at least one element / none / empty / NULL row
query
SELECT exists(arr, x -> x > 2) FROM test_array_exists

query
SELECT exists(arr, x -> x >= 0) FROM test_array_exists

query
SELECT exists(arr, x -> x < -10) FROM test_array_exists

-- three-valued logic: array with NULL element, predicate is NULL on NULL element
-- no element evaluates to true, at least one is NULL -> result NULL
query
SELECT exists(array(1, cast(NULL as int), 3), x -> x > 5)

-- same shape but an element satisfies the predicate -> result true
query
SELECT exists(array(1, cast(NULL as int), 3), x -> x > 2)

-- string elements
query
SELECT exists(sarr, s -> s = 'foo') FROM test_array_exists

query
SELECT exists(sarr, s -> s IS NULL) FROM test_array_exists

-- literal array, column predicate
query
SELECT exists(array(1, 2, 3), x -> x = cast(NULL as int))

