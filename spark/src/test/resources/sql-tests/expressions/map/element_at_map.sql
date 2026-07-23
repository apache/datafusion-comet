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

statement
CREATE TABLE test_element_at_map(m map<string, int>, mi map<int, string>) USING parquet

statement
INSERT INTO test_element_at_map VALUES
  (map('a', 1, 'b', 2, 'c', 3), map(1, 'x', 2, 'y')),
  (map('x', 10), map(99, 'z')),
  (NULL, NULL)

-- key found
query
SELECT element_at(m, 'a'), element_at(m, 'b') FROM test_element_at_map

-- key not found → NULL
query
SELECT element_at(m, 'missing') FROM test_element_at_map

-- null map → NULL
query
SELECT element_at(CAST(NULL AS MAP<STRING, INT>), 'a')

-- null key → NULL
query
SELECT element_at(m, CAST(NULL AS STRING)) FROM test_element_at_map

-- integer key type
query
SELECT element_at(mi, 1), element_at(mi, 2), element_at(mi, 99) FROM test_element_at_map

-- key type coercion
query
SELECT element_at(mi, CAST(1 AS BIGINT)), element_at(mi, CAST(2 AS SMALLINT)) FROM test_element_at_map

-- literal map arguments
query
SELECT element_at(map('a', 1, 'b', 2), 'a'), element_at(map('a', 1, 'b', 2), 'missing'), element_at(map('a', 1, 'b', 2), NULL)
