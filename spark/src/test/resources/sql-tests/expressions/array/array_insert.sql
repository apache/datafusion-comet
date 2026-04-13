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
-- Config: spark.comet.expression.ArrayInsert.allowIncompatible=true

-- ============================================================
-- Integer arrays with column arguments
-- ============================================================

statement
CREATE TABLE test_array_insert(arr array<int>, pos int, val int) USING parquet

statement
INSERT INTO test_array_insert VALUES
  (array(1, 2, 3), 2, 10),
  (array(1, 2, 3), 1, 10),
  (array(1, 2, 3), 4, 10),
  (array(), 1, 10),
  (NULL, 1, 10),
  (array(1, 2, 3), NULL, 10),
  (array(1, 2, 3), 3, NULL)

-- basic column arguments
query
SELECT array_insert(arr, pos, val) FROM test_array_insert

-- ============================================================
-- Literal arguments (all-literal queries test native eval
-- because CometSqlFileTestSuite disables constant folding)
-- ============================================================

-- basic insert at various positions
query
SELECT array_insert(array(1, 2, 3), 2, 10)

-- prepend (pos=1)
query
SELECT array_insert(array(1, 2, 3), 1, 10)

-- append (pos = len+1)
query
SELECT array_insert(array(1, 2, 3), 4, 10)

-- insert into empty array
query
SELECT array_insert(array(), 1, 10)

-- ============================================================
-- NULL handling
-- ============================================================

-- null array
query
SELECT array_insert(CAST(NULL AS ARRAY<INT>), 1, 10)

-- null position
query
SELECT array_insert(array(1, 2, 3), CAST(NULL AS INT), 10)

-- null value (insert null into array)
query
SELECT array_insert(array(1, 2, 3), 2, CAST(NULL AS INT))

-- null value appended beyond end
query
SELECT array_insert(array(1, 2, 3, NULL), 4, CAST(NULL AS INT))

-- array with existing nulls
query
SELECT array_insert(array(1, NULL, 3), 2, 10)

-- ============================================================
-- Positive out-of-bounds (null padding)
-- ============================================================

-- one beyond end
query
SELECT array_insert(array(1, 2, 3), 5, 99)

-- far beyond end
query
SELECT array_insert(array(1, 2, 3), 10, 99)

-- ============================================================
-- Negative indices (non-legacy mode, which is the default)
-- ============================================================

-- -1 appends after last element
query
SELECT array_insert(array(1, 2, 3), -1, 10)

-- -2 inserts before last element
query
SELECT array_insert(array(1, 2, 3), -2, 10)

-- -3 inserts before second-to-last
query
SELECT array_insert(array(1, 2, 3), -3, 10)

-- -4 inserts before first element (len = 3, so -4 = before start)
query
SELECT array_insert(array(1, 2, 3), -4, 10)

-- negative beyond start (null padding)
query
SELECT array_insert(array(1, 2, 3), -5, 10)

-- far negative beyond start
query
SELECT array_insert(array(1, 2, 3), -10, 10)

-- ============================================================
-- pos=0 error
-- Note: Spark throws INVALID_INDEX_OF_ZERO, Comet throws a different message.
-- Cannot use expect_error because patterns differ. This is a known incompatibility.
-- ============================================================

-- ============================================================
-- String arrays
-- ============================================================

statement
CREATE TABLE test_array_insert_str(arr array<string>, pos int, val string) USING parquet

statement
INSERT INTO test_array_insert_str VALUES
  (array('a', 'b', 'c'), 2, 'd'),
  (array('a', 'b', 'c'), 1, 'd'),
  (array('a', 'b', 'c'), 4, 'd'),
  (array(), 1, 'x'),
  (NULL, 1, 'x'),
  (array('a', NULL, 'c'), 2, 'z'),
  (array('a', 'b', 'c'), 3, NULL)

-- column arguments with strings
query
SELECT array_insert(arr, pos, val) FROM test_array_insert_str

-- literal string arrays
query
SELECT array_insert(array('hello', 'world'), 1, 'first')

query
SELECT array_insert(array('hello', 'world'), 3, 'last')

-- empty strings
query
SELECT array_insert(array('', 'a', ''), 2, '')

-- multibyte UTF-8 characters
query
SELECT array_insert(array('hello', 'world'), 2, 'cafe\u0301')

query
SELECT array_insert(array('abc', 'def'), 1, '中文')

-- ============================================================
-- Boolean arrays
-- ============================================================

statement
CREATE TABLE test_array_insert_bool(arr array<boolean>, pos int, val boolean) USING parquet

statement
INSERT INTO test_array_insert_bool VALUES
  (array(true, false, true), 2, false),
  (array(true, false), 3, true),
  (NULL, 1, true),
  (array(true), 1, NULL)

query
SELECT array_insert(arr, pos, val) FROM test_array_insert_bool

query
SELECT array_insert(array(true, false, true), 3, true)

-- ============================================================
-- Double arrays
-- ============================================================

statement
CREATE TABLE test_array_insert_double(arr array<double>, pos int, val double) USING parquet

statement
INSERT INTO test_array_insert_double VALUES
  (array(1.1, 2.2, 3.3), 2, 4.4),
  (array(1.1, 2.2), 3, 0.0),
  (NULL, 1, 1.0),
  (array(1.1, 2.2), 1, NULL)

query
SELECT array_insert(arr, pos, val) FROM test_array_insert_double

-- special float values
query
SELECT array_insert(array(CAST(1.0 AS DOUBLE), CAST(2.0 AS DOUBLE)), 2, CAST('NaN' AS DOUBLE))

query
SELECT array_insert(array(CAST(1.0 AS DOUBLE), CAST(2.0 AS DOUBLE)), 2, CAST('Infinity' AS DOUBLE))

query
SELECT array_insert(array(CAST(1.0 AS DOUBLE), CAST(2.0 AS DOUBLE)), 2, CAST('-Infinity' AS DOUBLE))

-- negative zero
query
SELECT array_insert(array(CAST(1.0 AS DOUBLE), CAST(2.0 AS DOUBLE)), 1, CAST(-0.0 AS DOUBLE))

-- ============================================================
-- Long arrays
-- ============================================================

query
SELECT array_insert(array(CAST(1 AS BIGINT), CAST(2 AS BIGINT)), 2, CAST(3 AS BIGINT))

-- ============================================================
-- Short arrays
-- ============================================================

query
SELECT array_insert(array(CAST(1 AS SMALLINT), CAST(2 AS SMALLINT)), 2, CAST(3 AS SMALLINT))

-- ============================================================
-- Byte arrays
-- ============================================================

query
SELECT array_insert(array(CAST(1 AS TINYINT), CAST(2 AS TINYINT)), 2, CAST(3 AS TINYINT))

-- ============================================================
-- Float arrays
-- ============================================================

query
SELECT array_insert(array(CAST(1.1 AS FLOAT), CAST(2.2 AS FLOAT)), 2, CAST(3.3 AS FLOAT))
