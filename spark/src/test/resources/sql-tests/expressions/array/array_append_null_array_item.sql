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

-- `CometArrayAppend` reproduces Spark's NULL propagation with a `CASE WHEN array IS NOT NULL`
-- guard, and the item sits inside the guard's THEN branch. DataFusion evaluates that branch only
-- on the rows the guard selects, while Spark's codegen evaluates the item on every row. An item
-- that raises on a row whose array is NULL therefore raises in Spark and stays silent on the
-- native path, with ANSI off too: `element_at` raises on index 0 whatever the ANSI setting. The
-- serde reports a nullable array with an item that is not a literal or column as incompatible, so
-- by default the expression runs through the JVM codegen dispatcher, which raises like Spark.
-- `array_append_ansi_null_array.sql` covers the same rules with ANSI on.
--
-- Spark 4.0 rewrites `array_append` to `array_insert(-1)` before serde, so `CometArrayAppend` is
-- only reachable on Spark 3.x.

-- MaxSparkVersion: 3.5

-- Config: spark.sql.ansi.enabled=false
-- Config: spark.sql.subexpressionElimination.enabled=true

statement
CREATE TABLE test_array_append_item(_1 int, arr array<int>) USING parquet

statement
INSERT INTO test_array_append_item
SELECT id, IF(id = 1, NULL, array(id)) FROM range(0, 4)

-- `element_at(array(1), _1 - 1)` reads index 0 exactly on the row whose array is NULL, so Spark
-- raises and the dispatcher raises with it.
query expect_error_class(INVALID_INDEX_OF_ZERO)
SELECT _1, array_append(arr, element_at(array(1), _1 - 1)) AS a
FROM test_array_append_item

-- The same with a conditional array that is NULL on every odd row.
query expect_error_class(INVALID_INDEX_OF_ZERO)
SELECT _1, array_append(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)),
  element_at(array(1), _1 - 1)) AS a
FROM test_array_append_item

-- The lookup raises on its own, so the errors above are not an artifact of the fixture.
query expect_error(INVALID_INDEX_OF_ZERO)
SELECT _1, element_at(array(1), _1 - 1) AS e
FROM test_array_append_item

-- A column or literal item cannot raise, so a nullable array keeps the native guarded path.
query expect_native(array_append)
SELECT _1, array_append(arr, _1) AS a
FROM test_array_append_item

query expect_native(array_append)
SELECT _1, array_append(arr, 7) AS a
FROM test_array_append_item

-- Any other item over a nullable array is routed through the dispatcher, even one that cannot
-- raise.
query expect_dispatch(array_append)
SELECT _1, array_append(arr, _1 + 1) AS a
FROM test_array_append_item

-- A non-nullable array evaluates the item on every row on both paths, so it stays native.
query expect_native(array_append)
SELECT _1, array_append(array(1), _1 + 1) AS a
FROM test_array_append_item

-- The native guard evaluates the item on every row of a non-nullable array, so it raises too.
query expect_error_class(INVALID_INDEX_OF_ZERO)
SELECT _1, array_append(array(1), element_at(array(1), _1 - 1)) AS a
FROM test_array_append_item

statement
CREATE TABLE test_array_append_item_shared(
  _1 int, a1 array<int>, a2 array<int>, m1 map<int, int>, m2 map<int, int>)
USING parquet

statement
INSERT INTO test_array_append_item_shared VALUES
  (1, CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<INT>),
   CAST(NULL AS MAP<INT, INT>), CAST(NULL AS MAP<INT, INT>)),
  (2, array(2), array(3), map(1, 5), map(1, 6))

-- The two calls share `element_at(array(1), _1 - 1)`, which Spark's projection evaluates on every
-- row, so it raises on the row whose arrays and maps are NULL. Each item is a map lookup that
-- skips its key on a NULL map, so the dispatcher would not raise there, and the calls fall back
-- to Spark instead.
query expect_error(INVALID_INDEX_OF_ZERO)
SELECT array_append(a1, element_at(m1, element_at(array(1), _1 - 1))),
  array_append(a2, element_at(m2, element_at(array(1), _1 - 1)))
FROM test_array_append_item_shared

-- On a row that does not raise, the same calls show the fallback.
query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT array_append(a1, element_at(m1, element_at(array(1), _1 - 1))),
  array_append(a2, element_at(m2, element_at(array(1), _1 - 1)))
FROM test_array_append_item_shared
WHERE _1 = 2
