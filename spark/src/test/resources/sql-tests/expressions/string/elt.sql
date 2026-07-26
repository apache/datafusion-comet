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
CREATE TABLE test_elt(n int) USING parquet

statement
INSERT INTO test_elt VALUES (1), (2), (3), (NULL)

query
SELECT elt(0, 'a', 'b', 'c'), elt(-1, 'a', 'b', 'c'), elt(4, 'a', 'b', 'c')

query
SELECT elt(n + 1, 'x', 'y', 'z', 'w') FROM test_elt

query
SELECT elt(1, 'a', NULL, 'c'), elt(2, 'a', NULL, 'c'), elt(3, 'a', NULL, 'c')

statement
CREATE TABLE test_elt_edge (idx int, v1 string, v2 string, v3 string) USING parquet

statement
INSERT INTO test_elt_edge VALUES
  (1, 'foo', 'bar', 'baz'),
  (2, 'foo', NULL, 'baz'),
  (3, NULL, 'bar', 'baz'),
  (4, 'foo', 'bar', 'baz'),
  (NULL, 'foo', 'bar', 'baz'),
  (0, 'foo', 'bar', 'baz'),
  (-1, 'foo', 'bar', 'baz')

query
SELECT elt(idx, v1, v2, v3) FROM test_elt_edge