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

-- Verifies that `map_from_entries` falls back to Spark when `spark.sql.mapKeyDedupPolicy` is set
-- to `LAST_WIN`. `CometMapFromEntries` mixes in `CodegenDispatchFallback`, so its native
-- `Incompatible` normally routes through the JVM codegen dispatcher; we disable the dispatcher
-- here so the incompat branch surfaces as a genuine Spark fallback rather than in-pipeline
-- codegen. The default `EXCEPTION` mode agrees with Comet and is covered by
-- `map_from_entries.sql`.

-- Config: spark.sql.mapKeyDedupPolicy=LAST_WIN
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false

statement
CREATE TABLE test_map_from_entries_dedup(entries array<struct<key:string, value:int>>) USING parquet

statement
INSERT INTO test_map_from_entries_dedup VALUES
  (array(struct('a', 1), struct('b', 2), struct('c', 3))),
  (array(struct('a', 1), struct('a', 2), struct('b', 3))),
  (array(struct('x', 10), struct('x', 20)))

-- literal duplicate keys under LAST_WIN: Spark keeps the last value; Comet must fall back.
query expect_fallback(mapKeyDedupPolicy)
SELECT map_from_entries(array(struct('a', 1), struct('a', 2), struct('b', 3)))

-- column input falls back the same way; the incompat branch is triggered by the SQLConf value,
-- not per-row content.
query expect_fallback(mapKeyDedupPolicy)
SELECT map_from_entries(entries) FROM test_map_from_entries_dedup
