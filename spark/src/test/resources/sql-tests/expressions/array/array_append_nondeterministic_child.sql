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
-- The serde declines a nondeterministic array and routes it through the JVM codegen
-- dispatcher, which evaluates it once. A deterministic nullable array keeps the native guard.
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

-- A deterministic nullable array stays on the native guarded path.
-- A non-nullable stateful array is declined too, rather than relying on the guard matching
-- every row.
query expect_dispatch(array_append)
SELECT _1, array_append(array(monotonically_increasing_id()), 2) AS a
FROM test_array_append_nondet

-- Only the array operand sits under the guard; a stateful item stays native.
query expect_native(array_append)
SELECT _1, array_append(array(1), monotonically_increasing_id()) AS a
FROM test_array_append_nondet

query expect_native(array_append)
SELECT _1, array_append(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), 2) AS a
FROM test_array_append_nondet
