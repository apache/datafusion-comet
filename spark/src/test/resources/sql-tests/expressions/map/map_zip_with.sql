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

-- Higher-order function map_zip_with. Runs through Comet's codegen dispatcher.

statement
CREATE TABLE test_map_zip_with(m map<string, int>, n map<string, int>) USING parquet

statement
INSERT INTO test_map_zip_with VALUES
  (map('a', 1, 'b', 2), map('a', 10, 'c', 30)),
  (map('x', -1), map('x', 5)),
  (map(), map()),
  (map('k', 1), NULL),
  (NULL, NULL)

-- combine values present in either map (missing side is NULL)
query
SELECT map_zip_with(m, n, (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0)) FROM test_map_zip_with

-- build a struct of both values
query
SELECT map_zip_with(m, n, (k, v1, v2) -> struct(v1 AS left, v2 AS right)) FROM test_map_zip_with

-- all literals
query
SELECT map_zip_with(map('a', 1), map('a', 2, 'b', 3), (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0))
