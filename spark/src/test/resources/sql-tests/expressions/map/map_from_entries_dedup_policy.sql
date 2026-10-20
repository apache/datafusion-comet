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

-- Verifies that `map_from_entries` runs natively under `spark.sql.mapKeyDedupPolicy` = `LAST_WIN`
-- and keeps the last value for each duplicate key. The default `EXCEPTION` mode is covered by
-- `map_from_entries.sql`.

-- Config: spark.sql.mapKeyDedupPolicy=LAST_WIN

statement
CREATE TABLE test_map_from_entries_dedup(entries array<struct<key:string, value:int>>) USING parquet

statement
INSERT INTO test_map_from_entries_dedup VALUES
  (array(struct('a', 1), struct('b', 2), struct('c', 3))),
  (array(struct('a', 1), struct('a', 2), struct('b', 3))),
  (array(struct('x', 10), struct('x', 20))),
  (array(struct('a', 1), struct('a', 2), struct('a', 3))),
  (array(struct('a', 1), struct('b', 2), struct('a', 3))),
  (array(struct('a', 1), struct('a', CAST(NULL AS INT)), struct('b', 3))),
  (array()),
  (NULL)

-- literal arguments, for the all-scalar path
query
SELECT map_from_entries(array(struct('a', 1), struct('a', 2), struct('b', 3)))

-- A repeated key keeps the position of its first occurrence and takes its last value, as
-- `ArrayBasedMapBuilder` does, so ('a', 'b', 'a') gives {a -> 3, b -> 2}; a NULL can be the value
-- that wins. Maps compare equal in any entry order, so `map_keys` and `map_values` pin the order.
-- `expect_native` also rules out the JVM codegen dispatcher, which a plain `query` would accept.
query expect_native(map_from_entries)
SELECT map_keys(map_from_entries(entries)), map_values(map_from_entries(entries)) FROM test_map_from_entries_dedup

-- LAST_WIN does not weaken the NULL key check
query expect_error(NULL_MAP_KEY)
SELECT map_from_entries(array(struct(CAST(NULL AS STRING), 1), struct('b', 2)))
