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

statement
CREATE TABLE test_coalesce(a int, b int, c int) USING parquet

statement
INSERT INTO test_coalesce VALUES (1, 2, 3), (NULL, 2, 3), (NULL, NULL, 3), (NULL, NULL, NULL), (1, NULL, NULL)

query
SELECT coalesce(a, b, c) FROM test_coalesce

query
SELECT coalesce(a) FROM test_coalesce

query
SELECT coalesce(a, 99) FROM test_coalesce

-- literal arguments
query
SELECT coalesce(NULL, NULL, 99), coalesce(1, NULL, 99), coalesce(NULL)

-- The serde guards every argument but the last with CASE WHEN arg IS NOT NULL THEN arg, and the
-- two copies of a non-deterministic argument advance their state independently: the THEN copy
-- can answer NULL for a row the guard selected, in a column declared non-nullable.
query expect_fallback(non-deterministic child under a null guard is evaluated on different rows than Spark's)
SELECT coalesce(IF(monotonically_increasing_id() % 2 = 0, a, NULL), b, 0) FROM test_coalesce

-- A NullType result stays in Spark: the serde builds a native CASE, which merges the rows of its
-- branches through Arrow's merge_n and cannot build a NullArray with a validity bitmap.
query expect_fallback(native CASE cannot merge NullType branches)
SELECT coalesce(aggregate(array(a), NULL, (acc, x) -> NULL), aggregate(array(b), NULL, (acc, x) -> NULL)) FROM test_coalesce
