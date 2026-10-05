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

-- Verifies that `map_from_arrays` leaves the native path when `spark.sql.mapKeyDedupPolicy` is
-- set to `LAST_WIN`. Spark's ArrayBasedMapBuilder keeps the last occurrence of each duplicate key
-- and Comet's native `map` scalar has no LAST_WIN path, so `CometMapFromArrays` reports the
-- policy as `Incompatible` and its `CodegenDispatchFallback` routes the call through the JVM
-- codegen dispatcher, which runs Spark's own builder. The dispatcher-off fallback is pinned by
-- `map_from_arrays_dedup_policy_dispatcher_off.sql`; the default `EXCEPTION` mode agrees with
-- Comet and is covered by `map_from_arrays.sql`.

-- Config: spark.sql.mapKeyDedupPolicy=LAST_WIN

statement
CREATE TABLE test_map_from_arrays_dedup(k array<string>, v array<int>) USING parquet

statement
INSERT INTO test_map_from_arrays_dedup VALUES
  (array('a', 'b', 'c'), array(1, 2, 3)),
  (array('a', 'a', 'b'), array(1, 2, 3)),
  (array('x', 'x'), array(10, 20))

-- literal duplicate keys under LAST_WIN: Spark keeps the last value; the dispatcher runs it.
query expect_dispatch(map_from_arrays)
SELECT map_from_arrays(array('a', 'a', 'b'), array(1, 2, 3))

-- column input is dispatched the same way; the incompat branch is triggered by the SQLConf value,
-- not per-row content.
query expect_dispatch(map_from_arrays)
SELECT map_from_arrays(k, v) FROM test_map_from_arrays_dedup

-- A calendar interval value would overflow the dispatcher's Arrow nanoseconds beyond about 292
-- years, so the incompatible call falls back to Spark instead of dispatching.
-- https://github.com/apache/datafusion-comet/issues/5279
query expect_fallback(holds a calendar interval)
SELECT map_from_arrays(array('a'), array(make_interval(0, 0, 0, 0, 0, 0, 10000000000.000000)))
