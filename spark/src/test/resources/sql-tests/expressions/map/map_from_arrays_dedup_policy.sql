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

-- Verifies that `map_from_arrays` follows `spark.sql.mapKeyDedupPolicy` = `LAST_WIN`, keeping
-- the last value for each duplicate key. Comet forwards the policy to the native builder as
-- `datafusion.spark.map_key_dedup_policy`, so the query stays native rather than falling back.
-- The default `EXCEPTION` mode is covered by `map_from_arrays.sql`.

-- Config: spark.sql.mapKeyDedupPolicy=LAST_WIN

statement
CREATE TABLE test_map_from_arrays_dedup(k array<string>, v array<int>) USING parquet

statement
INSERT INTO test_map_from_arrays_dedup VALUES
  (array('a', 'b', 'c'), array(1, 2, 3)),
  (array('a', 'a', 'b'), array(1, 2, 3)),
  (array('x', 'x'), array(10, 20)),
  (array(), array()),
  (NULL, array(99))

-- literal duplicate keys: the last value wins
query
SELECT map_from_arrays(array('a', 'a', 'b'), array(1, 2, 3))

-- three occurrences of the same key collapse to the last one
query
SELECT map_from_arrays(array('a', 'a', 'a'), array(1, 2, 3))

-- column input, including rows without duplicates and a NULL row
query
SELECT map_from_arrays(k, v) FROM test_map_from_arrays_dedup

-- LAST_WIN does not weaken the NULL key check
query expect_error(NULL_MAP_KEY)
SELECT map_from_arrays(array('a', NULL), array(1, 2))
