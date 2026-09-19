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

-- MinSparkVersion: 4.0

-- Spark 4.0+ supports string collations. `ArrayBasedMapBuilder` keys its dedup map on
-- `TypeUtils.getInterpretedOrdering` once the key type contains a string, so under `UTF8_LCASE`
-- the keys 'a' and 'A' are one key and Spark raises `DUPLICATED_MAP_KEY`. Comet's native
-- builders compare the raw Arrow bytes and would keep both, so both constructors decline a
-- collated key type outright, whether or not a given row actually collides.
--
-- The keys below are distinct under `UTF8_LCASE` so both engines return a map and the queries
-- can check where the expression ran. `CometMapFromArrays` has no codegen dispatcher, so it
-- falls back to Spark; `CometMapFromEntries` mixes in `CodegenDispatchFallback`, so it stays in
-- the Comet pipeline running Spark's own generated code.
--
-- `size` wraps each call so the projection's output type is an `int`. A map with a collated key
-- is not a supported Comet output type, and that check runs first: returning the map itself
-- takes the whole plan off Comet with no expression-level reason, testing nothing here.

statement
CREATE TABLE test_map_builders_collation(k string) USING parquet

statement
INSERT INTO test_map_builders_collation VALUES ('a'), ('b')

query expect_fallback(cannot honour a non-default collation)
SELECT size(map_from_arrays(
         array(CAST(k AS STRING COLLATE UTF8_LCASE),
               CAST(concat(k, 'z') AS STRING COLLATE UTF8_LCASE)),
         array(1, 2)))
FROM test_map_builders_collation

query expect_dispatch(map_from_entries)
SELECT size(map_from_entries(array(
         struct(CAST(k AS STRING COLLATE UTF8_LCASE) AS key, 1 AS value),
         struct(CAST(concat(k, 'z') AS STRING COLLATE UTF8_LCASE) AS key, 2 AS value))))
FROM test_map_builders_collation
