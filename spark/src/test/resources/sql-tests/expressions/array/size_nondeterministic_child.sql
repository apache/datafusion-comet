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

-- The native NULL guard serializes the child twice, so a nondeterministic child would drift
-- between the copies and is dispatched instead.

-- ConfigMatrix: spark.sql.legacy.sizeOfNull=true,false

statement
CREATE TABLE test_size_nondet(_1 int, arr array<int>, m map<double, array<int>>, x bigint) USING parquet

statement
INSERT INTO test_size_nondet
SELECT id, IF(id % 4 = 3, NULL, array(id, id + 1)), map(1D, array(10, 20)),
  IF(id = 0, NULL, id - 1)
FROM range(0, 16, 1, 1)

query expect_dispatch(size)
SELECT _1, size(IF(monotonically_increasing_id() % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>))) AS s
FROM test_size_nondet

query expect_native(size)
SELECT _1, size(array(monotonically_increasing_id())) AS s
FROM test_size_nondet

query expect_native(size)
SELECT _1, size(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>))) AS s
FROM test_size_nondet

-- A column array makes the dispatched kernel copy a ListVector
query expect_dispatch(size)
SELECT _1, size(IF(monotonically_increasing_id() % 2 = 0, arr, CAST(NULL AS ARRAY<INT>))) AS s
FROM test_size_nondet

-- Only the second row matches key 1. If coalesce evaluates the lookup again on that
-- row alone, the second counter starts at 0 and the lookup returns NULL.
query expect_dispatch(size)
SELECT _1, size(coalesce(m[CAST(monotonically_increasing_id() AS DOUBLE)], array(0)))
FROM test_size_nondet

query expect_dispatch(size)
SELECT _1, size(coalesce(IF(monotonically_increasing_id() % 2 = 1, arr, NULL), array(0)))
FROM test_size_nondet

-- arrays_zip returns a list of non-null structs. Rust array_repeat rebuilds a nullable
-- item field and fails Arrow's type check if it consumes the dispatched list.
query expect_dispatch(size)
SELECT _1, size(array_repeat(arrays_zip(array(map(_1, monotonically_increasing_id()))), 2))
FROM test_size_nondet

-- Equality skips the counter on the first row, where x is NULL. The next row matches 0.
query expect_dispatch(size)
SELECT _1, size(IF(x = monotonically_increasing_id(), array(1), array(1, 2)))
FROM test_size_nondet
