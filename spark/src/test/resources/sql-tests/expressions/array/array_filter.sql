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

-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false

statement
CREATE TABLE test_array_filter(arr array<int>, threshold int) USING parquet

statement
INSERT INTO test_array_filter VALUES (array(1, 2, 3, 4, 5), 10), (array(-1, 0, 1, NULL), 10), (array(NULL, NULL), 10), (array(10), 10), (NULL, 10), (array(), 10)

query
SELECT filter(arr, x -> x > 2) FROM test_array_filter

query
SELECT filter(arr, x -> x >= 0) FROM test_array_filter

query
SELECT filter(filter(arr, x -> x < threshold), y -> y > 0) FROM test_array_filter

query
SELECT filter(arr, x -> size(filter(array(1, 2, 3), y -> y > threshold)) > 0) FROM test_array_filter

query
SELECT filter(arr, x -> x % 2 = 0) FROM test_array_filter

query
SELECT filter(arr, x -> x IS NOT NULL AND x > 0) FROM test_array_filter

query
SELECT filter(arr, x -> x IS NULL OR x < 0) FROM test_array_filter

-- Test case: Filter with multiple conditions using AND/OR
query
SELECT filter(arr, x -> x > 0 AND x < 5) FROM test_array_filter

query
SELECT filter(arr, x -> x <= 0 OR x >= 5) FROM test_array_filter

-- Test case: Filter with arithmetic operations
query
SELECT filter(arr, x -> x * 2 > threshold) FROM test_array_filter

query
SELECT filter(arr, x -> x + threshold > 10) FROM test_array_filter

-- Test case: Filter with nested functions
query
SELECT filter(arr, x -> abs(x) > 2) FROM test_array_filter

query
SELECT filter(arr, x -> coalesce(x, 0) > 0) FROM test_array_filter

statement
CREATE TABLE test_array_filter_struct(arr array<struct<id:int,name:string>>) USING parquet

statement
INSERT INTO test_array_filter_struct VALUES
(array(struct(1, 'alice'), struct(2, 'bob'), struct(3, 'charlie'))),
(array(struct(4, 'dave'), struct(5, NULL))),
(array(struct(NULL, 'eve'))),
(NULL),
(array())

query
SELECT filter(arr, x -> x.id > 2) FROM test_array_filter_struct

query
SELECT filter(arr, x -> x.id IS NOT NULL AND x.name LIKE 'a%') FROM test_array_filter_struct

-- Test case: Filter with array of arrays
statement
CREATE TABLE test_array_filter_nested(arr array<array<int>>) USING parquet

statement
INSERT INTO test_array_filter_nested VALUES
(array(array(1,2), array(3,4), array(5,6))),
(array(array(10), array(20,30), array(40,50,60))),
(array(array(NULL), array(NULL, NULL))),
(NULL),
(array())

query
SELECT filter(arr, x -> size(x) > 1) FROM test_array_filter_nested

query
SELECT filter(arr, x -> x[0] > 5) FROM test_array_filter_nested

query
SELECT filter(arr, x -> array_contains(x, 10)) FROM test_array_filter_nested

