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
CREATE TABLE test_sort_array_int(arr array<int>) USING parquet

statement
INSERT INTO test_sort_array_int VALUES (array(3, 1, 2)), (array()), (NULL), (array(NULL, 3, 1, NULL, 2)), (array(-2147483648, 2147483647, 0))

-- ascending (default)
query
SELECT sort_array(arr) FROM test_sort_array_int

-- descending
query
SELECT sort_array(arr, false) FROM test_sort_array_int

-- literal arguments
query
SELECT sort_array(array(3, 1, 2))

query
SELECT sort_array(array(3, 1, 2), false)

-- NULL array
query
SELECT sort_array(cast(NULL as array<int>))

-- empty array
query
SELECT sort_array(array())

-- array with NULLs
query
SELECT sort_array(array(NULL, 3, NULL, 1, 2))

query
SELECT sort_array(array(NULL, 3, NULL, 1, 2), false)

-- ===== STRING arrays =====

statement
CREATE TABLE test_sort_array_string(arr array<string>) USING parquet

statement
INSERT INTO test_sort_array_string VALUES (array('banana', 'apple', 'cherry')), (array()), (NULL), (array(NULL, 'b', NULL, 'a'))

query
SELECT sort_array(arr) FROM test_sort_array_string

query
SELECT sort_array(arr, false) FROM test_sort_array_string

-- ===== DOUBLE arrays (NaN/Infinity handling) =====

statement
CREATE TABLE test_sort_array_double(arr array<double>) USING parquet

statement
INSERT INTO test_sort_array_double VALUES (array(3.0, 1.0, 2.0)), (NULL), (array(NULL, 2.0, NULL, 1.0)), (array(CAST('NaN' AS DOUBLE), 1.0, 2.0)), (array(CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE), 0.0))

query
SELECT sort_array(arr) FROM test_sort_array_double

query spark_answer_only
SELECT sort_array(arr, false) FROM test_sort_array_double

-- ===== BIGINT arrays =====

query
SELECT sort_array(array(CAST(3 AS BIGINT), CAST(1 AS BIGINT), CAST(2 AS BIGINT)))

query
SELECT sort_array(array(CAST(9223372036854775807 AS BIGINT), CAST(-9223372036854775808 AS BIGINT), CAST(0 AS BIGINT)))

-- ===== BOOLEAN arrays =====

query
SELECT sort_array(array(true, false, true, false))

query
SELECT sort_array(array(true, false, true, false), false)

-- ===== DECIMAL arrays =====

query
SELECT sort_array(array(CAST(3.14 AS DECIMAL(10,2)), CAST(1.41 AS DECIMAL(10,2)), CAST(2.72 AS DECIMAL(10,2))))
