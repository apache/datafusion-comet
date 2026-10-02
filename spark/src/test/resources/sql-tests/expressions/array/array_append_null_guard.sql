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

-- On Spark 4.0 and later array_append rewrites to array_insert, which has its own serde.
-- MaxSparkVersion: 3.5

statement
CREATE TABLE test_array_append_guard(arr array<int>, val int) USING parquet

statement
INSERT INTO test_array_append_guard VALUES (NULL, 1), (array(1), 2), (array(2), 3), (array(1, 2), NULL)

-- The serde guards the array with CASE WHEN arr IS NOT NULL and serializes it twice, so a
-- non-deterministic array stays in Spark.
query expect_fallback(non-deterministic child under a null guard is evaluated on different rows than Spark's)
SELECT array_append(IF(monotonically_increasing_id() % 2 = 0, arr, NULL), val) FROM test_array_append_guard

-- The item is serialized once, inside the THEN branch, so native code evaluates it only for a
-- non-null array. Spark's generated code evaluates it on every row, so a stateful item would
-- read a different counter after the NULL array in the first row; it stays in Spark as well.
query expect_fallback(non-deterministic child under a null guard is evaluated on different rows than Spark's)
SELECT array_append(arr, IF(monotonically_increasing_id() % 2 = 0, val, NULL)) FROM test_array_append_guard

-- A non-nullable array selects every row, so the item is evaluated on every row in both engines
-- and a stateful item stays native.
query expect_native(array_append)
SELECT array_append(array(val), monotonically_increasing_id()) FROM test_array_append_guard
