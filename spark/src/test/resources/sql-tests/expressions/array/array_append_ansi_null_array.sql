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
-- on the rows the guard selects, while Spark's codegen evaluates the item on every row. Under
-- ANSI mode an item that raises on a row whose array is NULL therefore raises in Spark and stays
-- silent on the native path. The serde reports a nullable array under ANSI as incompatible, so
-- by default the expression runs through the JVM codegen dispatcher, which raises like Spark.
-- A non-nullable array evaluates the item on every row on both paths, so it stays native.
--
-- Spark 4.0 rewrites `array_append` to `array_insert(-1)` before serde, so `CometArrayAppend` is
-- only reachable on Spark 3.x.

-- MaxSparkVersion: 3.5

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_array_append_ansi(_1 int, arr array<int>) USING parquet

statement
INSERT INTO test_array_append_ansi
SELECT id, IF(id = 1, NULL, array(id)) FROM range(0, 4)

-- The item divides by zero exactly on the row whose array is NULL, so Spark raises and the
-- dispatcher raises with it.
query expect_error(DIVIDE_BY_ZERO)
SELECT _1, array_append(arr, 1 / (_1 - 1)) AS a
FROM test_array_append_ansi

-- The divide raises on its own, so the error above is not an artifact of the fixture.
query expect_error(DIVIDE_BY_ZERO)
SELECT _1, 1 / (_1 - 1) AS d
FROM test_array_append_ansi

-- A nullable array under ANSI is routed through the dispatcher even when the item cannot raise.
query expect_dispatch(array_append)
SELECT _1, array_append(arr, _1) AS a
FROM test_array_append_ansi

-- A non-nullable array literal cannot hit the gap and keeps the native guarded path.
query expect_native(array_append)
SELECT _1, array_append(array(1), _1) AS a
FROM test_array_append_ansi
