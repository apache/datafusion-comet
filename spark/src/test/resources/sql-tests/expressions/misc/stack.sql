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

-- stack(n, v1, ..., vk) emits n rows of ceil(k / n) columns for every input
-- row, filling them row by row and padding the last row with NULLs. Comet plans
-- it as inline over an array holding one struct per output row.

statement
CREATE TABLE test_stack(id int, x int, y int, s1 string, s2 string) USING parquet

statement
INSERT INTO test_stack VALUES
  (1, 10, 20, 'a', 'b'),
  (2, NULL, 21, NULL, 'c'),
  (3, 12, NULL, 'd', NULL),
  (NULL, NULL, NULL, NULL, NULL)

-- unpivot two columns into (name, value) pairs
query
SELECT id, stack(2, 'x', x, 'y', y) AS (name, value) FROM test_stack

query
SELECT id, name, value FROM test_stack LATERAL VIEW stack(2, 'x', x, 'y', y) t AS name, value

-- uneven argument count: the last row is padded with NULLs
query
SELECT id, stack(3, x, y, id, x, y) FROM test_stack

-- more rows than values
query
SELECT id, stack(3, x) FROM test_stack

-- a single row
query
SELECT id, stack(1, x, s1, y, s2) FROM test_stack

-- one column per row
query
SELECT id, stack(4, s1, s2, s1, s2) FROM test_stack

-- a NULL literal takes the column's type
query
SELECT id, stack(2, x, NULL) FROM test_stack

query
SELECT id, stack(2, NULL, s1, 'k', NULL) FROM test_stack

-- expressions as arguments
query
SELECT id, stack(2, x + 1, upper(s1), y * 2, concat(s1, s2)) FROM test_stack

-- a filter and an aggregate above the generator
query
SELECT name, count(value), sum(value) FROM test_stack LATERAL VIEW stack(2, 'x', x, 'y', y) t AS name, value WHERE id IS NOT NULL GROUP BY name

-- only the generated columns
query
SELECT stack(2, x, y) FROM test_stack

-- complex values
statement
CREATE TABLE test_stack_nested(id int, a1 array<int>, a2 array<int>, st1 struct<p: int, q: string>, st2 struct<p: int, q: string>) USING parquet

statement
INSERT INTO test_stack_nested VALUES
  (1, array(1, 2), array(3), named_struct('p', 1, 'q', 'one'), named_struct('p', 2, 'q', NULL)),
  (2, NULL, array(), NULL, named_struct('p', NULL, 'q', 'two'))

query
SELECT id, stack(2, a1, st1, a2, st2) FROM test_stack_nested

query
SELECT id, stack(3, a1, a2) FROM test_stack_nested
