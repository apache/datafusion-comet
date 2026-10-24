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

-- The native NULL guard serializes each child twice, so a nondeterministic child would drift
-- between the copies and is dispatched instead.

statement
CREATE TABLE test_arrays_zip_nondet(_1 int, arr array<int>) USING parquet

statement
INSERT INTO test_arrays_zip_nondet
SELECT id, IF(id % 4 = 3, NULL, array(id, id + 1)) FROM range(0, 16)

query expect_dispatch(arrays_zip)
SELECT _1, arrays_zip(IF(monotonically_increasing_id() % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), array(2)) AS z
FROM test_arrays_zip_nondet

query expect_dispatch(arrays_zip)
SELECT _1, arrays_zip(array(2), IF(monotonically_increasing_id() % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>))) AS z
FROM test_arrays_zip_nondet

-- Declined even when non-nullable, rather than relying on the guard matching every row
query expect_dispatch(arrays_zip)
SELECT _1, arrays_zip(array(monotonically_increasing_id()), array(2)) AS z
FROM test_arrays_zip_nondet

-- Column arrays make the kernel copy ListVectors, and every fourth row brings a NULL array from
-- the input rather than from the IF
query expect_dispatch(arrays_zip)
SELECT _1, arrays_zip(IF(monotonically_increasing_id() % 2 = 0, arr, CAST(NULL AS ARRAY<INT>)), arr) AS z
FROM test_arrays_zip_nondet

query expect_native(arrays_zip)
SELECT _1, arrays_zip(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), array(2)) AS z
FROM test_arrays_zip_nondet

-- The two subtrees bind to the same ordinals, yet each keeps its own counter as in Spark (0 to 3
-- in both). One partition writes one file, so all four rows come from one task.
statement
CREATE TABLE test_arrays_zip_nondet_pair(x int, y int) USING parquet

statement
INSERT INTO test_arrays_zip_nondet_pair SELECT id, id + 100 FROM range(0, 4, 1, 1)

query expect_dispatch(arrays_zip, monotonically_increasing_id)
SELECT arrays_zip(array(map(x, monotonically_increasing_id()))) AS a,
  arrays_zip(array(map(y, monotonically_increasing_id()))) AS b
FROM test_arrays_zip_nondet_pair
