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

-- Verifies that `map_from_arrays` falls back to Spark when the map-key dedup policy is switched
-- away from its default (EXCEPTION) to LAST_WIN. Spark's ArrayBasedMapBuilder honours both null-key
-- rejection and the dedup policy; Comet's native `map` scalar does neither, so the two paths only
-- agree at the default policy. The default-mode compatible case is covered by
-- `map_from_arrays.sql`.

-- Config: spark.sql.mapKeyDedupPolicy=LAST_WIN

statement
CREATE TABLE test_map_from_arrays_dedup(k array<string>, v array<int>) USING parquet

statement
INSERT INTO test_map_from_arrays_dedup VALUES
  (array('a', 'b', 'c'), array(1, 2, 3)),
  (array('a', 'a', 'b'), array(1, 2, 3)),
  (array('x', 'x'), array(10, 20))

-- literal duplicate keys under LAST_WIN: Spark keeps the last value; Comet must fall back.
query expect_fallback(mapKeyDedupPolicy)
SELECT map_from_arrays(array('a', 'a', 'b'), array(1, 2, 3))

-- column input falls back the same way; the incompat branch is triggered by the SQLConf value,
-- not per-row content.
query expect_fallback(mapKeyDedupPolicy)
SELECT map_from_arrays(k, v) FROM test_map_from_arrays_dedup
