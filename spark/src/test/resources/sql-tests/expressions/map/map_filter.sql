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

-- Higher-order function map_filter. Runs through Comet's codegen dispatcher.

statement
CREATE TABLE test_map_filter(m map<string, int>, threshold int) USING parquet

statement
INSERT INTO test_map_filter VALUES
  (map('a', 1, 'b', 2, 'c', 3), 1),
  (map('x', -1), 0),
  (map(), 0),
  (NULL, NULL)

-- filter on value
query
SELECT map_filter(m, (k, v) -> v > 1) FROM test_map_filter

-- filter on key
query
SELECT map_filter(m, (k, v) -> k = 'a') FROM test_map_filter

-- column capture
query
SELECT map_filter(m, (k, v) -> v > threshold) FROM test_map_filter

-- all literals
query
SELECT map_filter(map('a', 1, 'b', 2), (k, v) -> v > 1)
