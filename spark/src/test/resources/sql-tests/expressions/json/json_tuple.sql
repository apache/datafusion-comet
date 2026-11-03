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

-- json_tuple has no native implementation. It runs on the JVM inside the native generate operator,
-- one row per input row, with NULL for a field the document lacks and a row of NULLs for a NULL or
-- invalid document.

statement
CREATE TABLE test_json_tuple(id int, js string) USING parquet

statement
INSERT INTO test_json_tuple VALUES
  (1, '{"a": 1, "b": "x", "c": true}'),
  (2, '{"a": null, "b": "y"}'),
  (3, '{"b": "multi word", "d": {"e": 5}}'),
  (4, '{"a": [1, 2, 3], "b": 1.5}'),
  (5, 'not json'),
  (6, ''),
  (7, NULL),
  (8, '{"a": "é日本", "b": ""}'),
  (NULL, '{"a": 9}')

query
SELECT id, json_tuple(js, 'a', 'b') FROM test_json_tuple

query
SELECT json_tuple(js, 'a', 'b', 'c', 'd') FROM test_json_tuple

-- a field the documents lack, and a repeated field
query
SELECT id, json_tuple(js, 'missing', 'a', 'a') FROM test_json_tuple

query
SELECT id, a, b FROM test_json_tuple LATERAL VIEW json_tuple(js, 'a', 'b') j AS a, b

-- the outer form keeps the rows that yield no values
query
SELECT id, a FROM test_json_tuple LATERAL VIEW OUTER json_tuple(js, 'a') j AS a

-- a filter and an aggregate above the generator
query
SELECT id, a FROM test_json_tuple LATERAL VIEW json_tuple(js, 'a') j AS a WHERE a IS NOT NULL

query
SELECT b, count(*) FROM test_json_tuple LATERAL VIEW json_tuple(js, 'b') j AS b GROUP BY b
