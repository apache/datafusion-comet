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
-- A TRY cast whose key cast can fail turns an out-of-range key into a null key, and Spark keeps
-- the row. Arrow cannot hold a null key, and Spark's own readers disagree about it, so any tree
-- holding such a cast must fall back to Spark. See #6172.

statement
CREATE TABLE test_null_map_key(id int, m map<bigint, int>) USING parquet

statement
INSERT INTO test_null_map_key VALUES
  (1, map(1, 10, 9999999999, 20)),
  (2, map(2, 30)),
  (3, map()),
  (4, NULL)

-- Before the fix these returned [1, 0] for the first row, where Spark returns [1, NULL]
query expect_fallback(null map key)
SELECT id, map_keys(transform_values(try_cast(m AS map<int, int>), (k, v) -> v + 1))
FROM test_null_map_key ORDER BY id

query expect_fallback(null map key)
SELECT id, map_keys(map_filter(try_cast(m AS map<int, int>), (k, v) -> true))
FROM test_null_map_key ORDER BY id

-- Refused even though the result carries no map, since the tree still holds the cast
query expect_fallback(null map key)
SELECT id, cast(try_cast(m AS map<int, int>) AS string) FROM test_null_map_key ORDER BY id

-- A widening key cast cannot fail, so the tree stays in the dispatcher
query expect_dispatch(transform_values)
SELECT id, map_keys(transform_values(try_cast(m AS map<decimal(20, 0), int>), (k, v) -> v))
FROM test_null_map_key ORDER BY id

-- Spark counts date to timestamp as an upcast, but it overflows for extreme dates and so can
-- still produce a null key under TRY
statement
CREATE TABLE test_null_map_key_date(id int, m map<date, int>) USING parquet

statement
INSERT INTO test_null_map_key_date VALUES
  (1, map(date_from_unix_date(1), 10, date_from_unix_date(2000000000), 20)),
  (2, NULL)

query expect_fallback(null map key)
SELECT id, map_keys(transform_values(try_cast(m AS map<timestamp, int>), (k, v) -> v))
FROM test_null_map_key_date ORDER BY id
