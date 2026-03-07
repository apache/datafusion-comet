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
CREATE TABLE test_array_exists(arr_int array<int>, arr_str array<string>, arr_double array<double>, arr_bool array<boolean>, arr_long array<bigint>, threshold int) USING parquet

statement
INSERT INTO test_array_exists VALUES (array(1, 2, 3), array('a', 'bb', 'ccc'), array(1.5, 2.5, 3.5), array(false, false, true), array(100, 200, 300), 2), (array(1, 2), array('a', 'b'), array(0.5, 1.5), array(false, false), array(10, 20), 5), (array(), array(), array(), array(), array(), 0), (NULL, NULL, NULL, NULL, NULL, 1), (array(1, NULL, 3), array('a', NULL, 'ccc'), array(1.0, NULL, 3.0), array(true, NULL, false), array(100, NULL, 300), 2)

-- basic: element satisfies predicate
query
SELECT exists(arr_int, x -> x > 2) FROM test_array_exists

-- no match
query
SELECT exists(arr_int, x -> x > 100) FROM test_array_exists

-- empty array returns false
query
SELECT exists(arr_int, x -> x > 0) FROM test_array_exists

-- null array returns null
query
SELECT exists(arr_int, x -> x > 0) FROM test_array_exists WHERE arr_int IS NULL

-- predicate referencing outer column
query
SELECT exists(arr_int, x -> x > threshold) FROM test_array_exists

-- three-valued logic: null elements with no match -> null
query
SELECT exists(arr_int, x -> x > 5) FROM test_array_exists

-- null elements but match exists -> true
query
SELECT exists(arr_int, x -> x > 2) FROM test_array_exists

-- string type
query
SELECT exists(arr_str, x -> length(x) > 2) FROM test_array_exists

-- double type
query
SELECT exists(arr_double, x -> x > 2.0) FROM test_array_exists

-- boolean type
query
SELECT exists(arr_bool, x -> x) FROM test_array_exists

-- long type
query
SELECT exists(arr_long, x -> x > 250) FROM test_array_exists

-- literal arrays
query
SELECT exists(array(1, 2, 3), x -> x > 2)

query
SELECT exists(array(1, 2, 3), x -> x > 5)

-- empty literal array has NullType element type, which is unsupported
query spark_answer_only
SELECT exists(array(), x -> cast(x as int) > 0)

query
SELECT exists(cast(NULL as array<int>), x -> x > 0)

-- null elements in literal array with three-valued logic
query
SELECT exists(array(1, NULL, 3), x -> x > 5)

-- null elements in literal array with match
query
SELECT exists(array(1, NULL, 3), x -> x > 2)

-- complex predicate
query
SELECT exists(arr_int, x -> x > 1 AND x < 3) FROM test_array_exists

-- predicate with modulo
query
SELECT exists(arr_int, x -> x % 2 = 0) FROM test_array_exists
