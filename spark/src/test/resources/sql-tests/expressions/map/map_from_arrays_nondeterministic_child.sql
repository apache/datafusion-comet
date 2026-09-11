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

-- `CometMapFromArrays` reproduces Spark's NULL propagation with a `CASE WHEN keys IS NOT NULL AND
-- values IS NOT NULL` guard that serializes both children twice. A stateful child advances each
-- copy independently, so the guard and the `map` see different rows and the result silently
-- drifts from Spark. The serde declines a nondeterministic child and routes it through
-- the JVM codegen dispatcher, which evaluates it once. A deterministic nullable child keeps the
-- native guard.

statement
CREATE TABLE test_map_from_arrays_nondet(_1 int) USING parquet

statement
INSERT INTO test_map_from_arrays_nondet VALUES
  (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15)

-- Spark returns {1 -> 2} on every row whose keys array is non-NULL and NULL on the rest.
query expect_dispatch(map_from_arrays)
SELECT _1, map_from_arrays(IF(monotonically_increasing_id() % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), array(2)) AS m
FROM test_map_from_arrays_nondet

-- The guard covers both children, so a stateful values array is declined the same way.
query expect_dispatch(map_from_arrays)
SELECT _1, map_from_arrays(array(1), IF(monotonically_increasing_id() % 2 = 0, array(2), CAST(NULL AS ARRAY<INT>))) AS m
FROM test_map_from_arrays_nondet

-- A deterministic nullable child stays on the native guarded path.
-- A non-nullable stateful child is declined too, rather than relying on the guard matching
-- every row.
query expect_dispatch(map_from_arrays)
SELECT _1, map_from_arrays(array(monotonically_increasing_id()), array(2)) AS m
FROM test_map_from_arrays_nondet

query expect_native(map_from_arrays)
SELECT _1, map_from_arrays(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), array(2)) AS m
FROM test_map_from_arrays_nondet
