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

-- `CometMapFromArrays` reproduces Spark's evaluation order with a
-- `CASE WHEN keys IS NOT NULL THEN map_from_arrays(keys, values) END` guard, which serializes the
-- keys a second time. A stateful keys expression advances each copy independently: the guard's
-- copy sees every row while the constructor's copy sees only the rows the guard selected, so the
-- result would silently drift from Spark (#5781). The serde declines a nondeterministic keys
-- expression and the projection falls back to Spark, which evaluates it once.

statement
CREATE TABLE test_map_from_arrays_nondet(_1 int) USING parquet

statement
INSERT INTO test_map_from_arrays_nondet SELECT id FROM range(0, 16)

-- Spark returns {1 -> 2} on every row whose keys array is non-NULL and NULL on the rest.
query expect_fallback(nondeterministic operand)
SELECT _1, map_from_arrays(IF(monotonically_increasing_id() % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), array(2)) AS m
FROM test_map_from_arrays_nondet

-- A non-nullable stateful keys expression is declined too, rather than relying on the guard
-- matching every row.
query expect_fallback(nondeterministic operand)
SELECT _1, map_from_arrays(array(monotonically_increasing_id()), array(2)) AS m
FROM test_map_from_arrays_nondet

-- The values are serialized once, inside the call, and evaluated only on the rows whose keys
-- array is non-NULL, which are the rows Spark evaluates them on. So a stateful values expression
-- stays native and numbers the same rows Spark does.
query expect_native(map_from_arrays)
SELECT _1, map_from_arrays(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), array(monotonically_increasing_id())) AS m
FROM test_map_from_arrays_nondet

query expect_native(map_from_arrays)
SELECT _1, map_from_arrays(array(1), IF(monotonically_increasing_id() % 2 = 0, array(2), CAST(NULL AS ARRAY<INT>))) AS m
FROM test_map_from_arrays_nondet

-- A deterministic nullable keys expression stays on the native guarded path.
query expect_native(map_from_arrays)
SELECT _1, map_from_arrays(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), array(2)) AS m
FROM test_map_from_arrays_nondet
