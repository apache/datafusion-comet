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
CREATE TABLE test_size_nondet(_1 int, arr array<int>) USING parquet

statement
INSERT INTO test_size_nondet
SELECT id, IF(id % 4 = 3, NULL, array(id, id + 1)) FROM range(0, 16)

query expect_dispatch(size)
SELECT _1, size(IF(monotonically_increasing_id() % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>))) AS s
FROM test_size_nondet

-- Declined even when non-nullable, rather than relying on the guard matching every row
query expect_dispatch(size)
SELECT _1, size(array(monotonically_increasing_id())) AS s
FROM test_size_nondet

query expect_native(size)
SELECT _1, size(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>))) AS s
FROM test_size_nondet

-- A column array makes the dispatched kernel copy a ListVector
query expect_dispatch(size)
SELECT _1, size(IF(monotonically_increasing_id() % 2 = 0, arr, CAST(NULL AS ARRAY<INT>))) AS s
FROM test_size_nondet
