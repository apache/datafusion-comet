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

-- array_prepend is a RuntimeReplaceable that rewrites to array_insert(arr, 1, val),
-- so it runs natively through Comet's ArrayInsert implementation.

-- array_prepend was added in Spark 3.5.
-- MinSparkVersion: 3.5

statement
CREATE TABLE test_array_prepend(arr array<int>, val int) USING parquet

statement
INSERT INTO test_array_prepend VALUES (array(1, 2, 3), 4), (array(), 1), (NULL, 1), (array(1, 2), NULL)

-- column + column
query
SELECT array_prepend(arr, val) FROM test_array_prepend

-- column + literal
query
SELECT array_prepend(arr, 99) FROM test_array_prepend

-- literal + column
query
SELECT array_prepend(array(1, 2, 3), val) FROM test_array_prepend

-- ============================================================
-- Literal arguments (all-literal queries test native eval
-- because CometSqlFileTestSuite disables constant folding)
-- ============================================================

-- integer arrays
query
SELECT array_prepend(array(1, 2, 3), 4)

-- string arrays
query
SELECT array_prepend(array('a', 'b', 'c'), 'd')

-- ============================================================
-- NULL handling
-- ============================================================

-- prepend NULL to an array that already contains NULL
query
SELECT array_prepend(array(1, 2, 3, NULL), NULL)

-- prepend NULL to a string array that already contains NULL
query
SELECT array_prepend(array('a', 'b', 'c', NULL), NULL)

-- NULL array yields NULL
query
SELECT array_prepend(CAST(NULL AS ARRAY<STRING>), 'a')

-- NULL array and NULL value yields NULL
query
SELECT array_prepend(CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING))

-- prepend into empty array
query
SELECT array_prepend(array(), 1)

-- prepend NULL into empty string array
query
SELECT array_prepend(CAST(array() AS ARRAY<STRING>), CAST(NULL AS STRING))

-- prepend NULL into a single-NULL array
query
SELECT array_prepend(array(CAST(NULL AS STRING)), CAST(NULL AS STRING))

-- ============================================================
-- Other element types
-- ============================================================

-- boolean
query
SELECT array_prepend(array(true, false), true)

-- double, including special values
query
SELECT array_prepend(array(CAST(1.0 AS DOUBLE), CAST(2.0 AS DOUBLE)), CAST('NaN' AS DOUBLE))

-- long
query
SELECT array_prepend(array(CAST(1 AS BIGINT), CAST(2 AS BIGINT)), CAST(3 AS BIGINT))

-- multibyte UTF-8
query
SELECT array_prepend(array('hello', 'world'), '中文')

-- ============================================================
-- Type coercion: array<int> with a double element coerces the
-- whole array to double (Spark inserts an implicit cast before
-- the array_insert rewrite)
-- ============================================================

query
SELECT array_prepend(array(1, 2), 1.23D)

-- ============================================================
-- Nested array elements (prepend an array into an array<array<int>>)
-- ============================================================

query
SELECT array_prepend(array(array(1, 2), array(3, 4)), array(5, 6))

query
SELECT array_prepend(array(array(1, 2), array(3, 4)), CAST(array() AS ARRAY<INT>))

-- ============================================================
-- Binary elements
-- ============================================================

statement
CREATE TABLE test_array_prepend_bin(arr array<binary>, val binary) USING parquet

statement
INSERT INTO test_array_prepend_bin VALUES (array(X'0102', X'0304'), X'0506'), (array(X'0102'), NULL), (NULL, X'0506')

query
SELECT array_prepend(arr, val) FROM test_array_prepend_bin

query
SELECT array_prepend(arr, element_at(array(9), idx))
FROM test_array_prepend
