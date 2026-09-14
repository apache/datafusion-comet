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

-- map_concat has no native implementation; it is routed through the JVM codegen dispatcher, which
-- runs Spark's own doGenCode and must produce Spark-identical maps (a prior dispatcher bug emitted
-- a wrong map key). `query` mode asserts both native execution and answer parity.

statement
CREATE TABLE test_map_concat(m1 map<string, int>, m2 map<string, int>) USING parquet

statement
INSERT INTO test_map_concat VALUES
  (map('a', 1, 'b', 2), map('c', 3, 'd', 4)),
  (map(), map('x', 9)),
  (map('k', 1), NULL),
  (NULL, map('y', 2)),
  (NULL, NULL)

-- column inputs covering empty maps and NULL maps
query
SELECT map_concat(m1, m2) FROM test_map_concat

-- single map argument
query
SELECT map_concat(m1) FROM test_map_concat

-- literal arguments with distinct keys across three maps
query
SELECT map_concat(map('a', 1), map('b', 2), map('c', 3))

-- integer-keyed maps
query
SELECT map_concat(map(1, 'x', 2, 'y'), map(3, 'z'))

-- a NULL literal map makes the whole result NULL
query
SELECT map_concat(map('a', 1), CAST(NULL AS map<string, int>))
