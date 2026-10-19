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

-- ArrayAppend is RuntimeReplaceable from Spark 4.0, so CometArrayAppend is only reachable here.
-- MaxSparkVersion: 3.5

-- Spark's generated code for array_append evaluates the item on every row and guards only the
-- assignment of the result, so an item that raises under ANSI raises on a row whose array is NULL
-- as well. https://github.com/apache/datafusion-comet/issues/6086
-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE array_append_eager(_1 INT) USING parquet

statement
INSERT INTO array_append_eager SELECT /*+ COALESCE(1) */ * FROM VALUES (0), (1), (2), (3)

-- Control: the ANSI divide raises on its own, so the query below is not vacuous
query expect_error_class(DIVIDE_BY_ZERO)
SELECT _1, 1 / (_1 - 1) AS d FROM array_append_eager

-- At _1 = 1 the array is NULL and the item divides by zero
query expect_error_class(DIVIDE_BY_ZERO)
SELECT _1, array_append(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), 1 / (_1 - 1)) AS a
FROM array_append_eager

-- The item still raises on a row whose array is not NULL
query expect_error_class(DIVIDE_BY_ZERO)
SELECT _1, array_append(IF(_1 % 2 = 1, array(1), CAST(NULL AS ARRAY<INT>)), 1 / (_1 - 1)) AS a
FROM array_append_eager

-- An item that does not raise on any row is unaffected, and stays native
query expect_native(array_append)
SELECT _1, array_append(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), 1 / (_1 + 1)) AS a
FROM array_append_eager

-- A column or literal item cannot raise, so it needs no extra evaluation
query expect_native(array_append)
SELECT _1, array_append(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), _1) AS a,
       array_append(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), 5) AS b
FROM array_append_eager

-- A nondeterministic operand is declined when the array can be NULL: the native guard would
-- serialize it more than once, and each copy would see only a subset of the rows. This covers the
-- item and the array alike.
query expect_fallback(nondeterministic operand)
SELECT _1, array_append(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), CAST(rand(7L) * 3 AS INT)) AS a
FROM array_append_eager

query expect_fallback(nondeterministic operand)
SELECT _1, array_append(IF(rand(7L) < 2, CAST(NULL AS ARRAY<INT>), array(1)), _1) AS a
FROM array_append_eager

-- A non-nullable array needs no guard, so `convert` emits none and the item is evaluated once over
-- the whole batch. A nondeterministic item therefore stays native and in step with Spark.
query expect_native(array_append)
SELECT _1, array_append(array(1, 2), CAST(rand(7L) * 3 AS INT)) AS a
FROM array_append_eager

-- A non-nullable array still raises for a throwing item
query expect_error_class(DIVIDE_BY_ZERO)
SELECT _1, array_append(array(1, 2), 1 / (_1 - 1)) AS a
FROM array_append_eager

-- Every array NULL: the batch only ever takes the ELSE branch, which must still evaluate the item
statement
CREATE TABLE array_append_all_null(_1 INT, arr ARRAY<INT>) USING parquet

statement
INSERT INTO array_append_all_null SELECT /*+ COALESCE(1) */ * FROM VALUES (0, NULL), (1, NULL), (2, NULL), (3, NULL)

query expect_error_class(DIVIDE_BY_ZERO)
SELECT _1, array_append(arr, 1 / (_1 - 1)) AS a FROM array_append_all_null

query expect_native(array_append)
SELECT _1, array_append(arr, 1 / (_1 + 1)) AS a FROM array_append_all_null

-- No array NULL: the batch only ever takes the THEN branch
statement
CREATE TABLE array_append_no_null(_1 INT, arr ARRAY<INT>) USING parquet

statement
INSERT INTO array_append_no_null SELECT /*+ COALESCE(1) */ * FROM VALUES (0, array(1)), (1, array(2)), (2, array()), (3, array(4, 5))

query expect_error_class(DIVIDE_BY_ZERO)
SELECT _1, array_append(arr, 1 / (_1 - 1)) AS a FROM array_append_no_null

query expect_native(array_append)
SELECT _1, array_append(arr, 1 / (_1 + 1)) AS a FROM array_append_no_null

-- Complex element types: the item is itself an array, and the ELSE branch's NULL has to be typed as
-- the list of arrays that THEN returns. The item divides by zero only where the array is NULL.
query expect_error_class(DIVIDE_BY_ZERO)
SELECT _1, array_append(IF(_1 % 2 = 0, array(array(1)), CAST(NULL AS ARRAY<ARRAY<INT>>)), array(1 / (_1 - 1))) AS a
FROM array_append_eager

query expect_native(array_append)
SELECT _1, array_append(IF(_1 % 2 = 0, array(array(1)), CAST(NULL AS ARRAY<ARRAY<INT>>)), array(1 / (_1 + 1))) AS a
FROM array_append_eager

-- Without ANSI nothing raises, and the default on Spark 3.4 and 3.5 is without ANSI. A computed
-- item still goes through the extra evaluation for rows whose array is NULL, whose NULL has to be
-- typed as the list the other branch returns, or the two cannot be merged. The division by zero
-- returns NULL, and the second query has it land on a row whose array is not NULL, so [1, NULL] is
-- visible in the result.
statement
SET spark.sql.ansi.enabled=false

query expect_native(array_append)
SELECT _1, array_append(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), _1 + 1) AS a
FROM array_append_eager

query expect_native(array_append)
SELECT _1, array_append(IF(_1 % 2 = 1, array(1), CAST(NULL AS ARRAY<INT>)), 1 / (_1 - 1)) AS a
FROM array_append_eager

query expect_native(array_append)
SELECT _1, array_append(arr, _1 + 1) AS a FROM array_append_all_null
