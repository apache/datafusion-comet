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
-- guard that serializes the array twice. A stateful array advances each copy independently, so
-- the guard and the `array_append` see different rows and the result silently drifts from Spark.
-- The item sits inside the guard's THEN branch, which DataFusion evaluates only on the selected
-- rows while Spark evaluates it on every row, so a stateful item drifts too. The serde declines
-- either nondeterministic operand and routes it through the JVM codegen dispatcher, which
-- evaluates each once. A deterministic nullable array keeps the native guard.
--
-- Spark 4.0 rewrites `array_append` to `array_insert(-1)` before serde, so `CometArrayAppend` is
-- only reachable on Spark 3.x.

-- MaxSparkVersion: 3.5

statement
CREATE TABLE test_array_append_nondet(_1 int) USING parquet

statement
INSERT INTO test_array_append_nondet VALUES
  (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15)

-- Spark returns [1, 2] on every row whose array is non-NULL and NULL on the rest.
query expect_dispatch(array_append)
SELECT _1, array_append(IF(monotonically_increasing_id() % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), 2) AS a
FROM test_array_append_nondet

-- A non-nullable stateful array is declined too, rather than relying on the guard matching
-- every row.
query expect_dispatch(array_append)
SELECT _1, array_append(array(monotonically_increasing_id()), 2) AS a
FROM test_array_append_nondet

-- A stateful item over a nullable array: Spark advances the counter on all 16 rows, so the
-- even rows carry [1, 0], [1, 2], [1, 4] and so on, which the filtered native branch cannot
-- reproduce.
query expect_dispatch(array_append)
SELECT _1, array_append(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), monotonically_increasing_id()) AS a
FROM test_array_append_nondet

-- A stateful item over a non-nullable array is declined the same way.
query expect_dispatch(array_append)
SELECT _1, array_append(array(1), monotonically_increasing_id()) AS a
FROM test_array_append_nondet

-- A deterministic nullable array stays on the native guarded path.
query expect_native(array_append)
SELECT _1, array_append(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), 2) AS a
FROM test_array_append_nondet
