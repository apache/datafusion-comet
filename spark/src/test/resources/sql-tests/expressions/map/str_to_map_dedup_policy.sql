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

-- Verifies that `str_to_map` runs natively under `spark.sql.mapKeyDedupPolicy` = `LAST_WIN` and
-- keeps the last value for each duplicate key. The default `EXCEPTION` mode is covered by
-- `str_to_map.sql`.

-- Config: spark.sql.mapKeyDedupPolicy=LAST_WIN

statement
CREATE TABLE test_str_to_map_dedup(s string) USING parquet

statement
INSERT INTO test_str_to_map_dedup VALUES
  ('a:1,b:2,a:3'),
  ('a:1,b:2,c:3'),
  ('x:1,x:2,x:3'),
  (NULL)

-- literal arguments, for the all-scalar path
query
SELECT str_to_map('a:1,b:2,a:3')

-- `a` keeps the position of its first occurrence and takes its last value, as
-- `ArrayBasedMapBuilder` does, so 'a:1,b:2,a:3' gives {a -> 3, b -> 2}. Maps compare equal in any
-- entry order, so `map_keys` and `map_values` pin the order. `expect_native` also rules out the
-- JVM codegen dispatcher, which a plain `query` would accept.
query expect_native(str_to_map)
SELECT map_keys(str_to_map(s)), map_values(str_to_map(s)) FROM test_str_to_map_dedup
