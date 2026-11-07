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

-- inline and inline_outer run natively: the planner unnests the array of
-- structs and flattens each struct into one column per field. A NULL struct
-- element yields a row of NULLs. inline_outer emits one row of NULLs for a
-- NULL or empty array, and plain inline drops both.

statement
CREATE TABLE test_inline(id int, arr array<struct<a: int, b: string>>) USING parquet

statement
INSERT INTO test_inline VALUES
  (1, array(named_struct('a', 1, 'b', 'x'), named_struct('a', 2, 'b', 'y'))),
  (2, array(named_struct('a', 3, 'b', NULL))),
  (3, array(named_struct('a', NULL, 'b', NULL))),
  (4, array(NULL, named_struct('a', 4, 'b', 'z'), NULL)),
  (5, array()),
  (6, NULL),
  (NULL, array(named_struct('a', 5, 'b', 'w')))

query
SELECT id, inline(arr) FROM test_inline

query
SELECT id, inline_outer(arr) FROM test_inline

query
SELECT id, a, b FROM test_inline LATERAL VIEW inline(arr) t AS a, b

query
SELECT id, a, b FROM test_inline LATERAL VIEW OUTER inline(arr) t AS a, b

-- only the generated columns
query
SELECT inline(arr) FROM test_inline

-- a filter and an aggregate above the generator
query
SELECT id, a FROM test_inline LATERAL VIEW inline(arr) t AS a, b WHERE a > 1

query
SELECT b, count(*), sum(a) FROM test_inline LATERAL VIEW OUTER inline(arr) t AS a, b GROUP BY b

-- a struct field named like a carried column
statement
CREATE TABLE test_inline_dup(id int, arr array<struct<id: int, v: string>>) USING parquet

statement
INSERT INTO test_inline_dup VALUES
  (1, array(named_struct('id', 10, 'v', 'a'), named_struct('id', 11, 'v', 'b'))),
  (2, array(named_struct('id', 20, 'v', NULL)))

query
SELECT * FROM test_inline_dup LATERAL VIEW inline(arr) t AS id2, v

-- single-field structs
statement
CREATE TABLE test_inline_single(id int, arr array<struct<v: bigint>>) USING parquet

statement
INSERT INTO test_inline_single VALUES
  (1, array(named_struct('v', 1L), NULL, named_struct('v', NULL))),
  (2, array())

query
SELECT id, inline_outer(arr) FROM test_inline_single

-- nested and wide field types
statement
CREATE TABLE test_inline_nested(
  id int,
  arr array<struct<
    d: double,
    dec: decimal(20, 3),
    dt: date,
    ts: timestamp,
    bin: binary,
    ints: array<int>,
    m: map<string, int>,
    s: struct<x: int, y: string>>>) USING parquet

statement
INSERT INTO test_inline_nested VALUES
  (1, array(
    named_struct('d', 1.5D, 'dec', 12345.678BD, 'dt', date'2024-01-01',
      'ts', timestamp'2024-01-01 12:34:56', 'bin', X'CAFE', 'ints', array(1, NULL, 3),
      'm', map('k', 1), 's', named_struct('x', 1, 'y', 'one')),
    NULL,
    named_struct('d', double('NaN'), 'dec', NULL, 'dt', NULL, 'ts', NULL, 'bin', NULL,
      'ints', array(), 'm', NULL, 's', NULL))),
  (2, NULL)

query
SELECT id, inline_outer(arr) FROM test_inline_nested

-- an array built in the query rather than read from the table
query
SELECT id, inline(array(named_struct('k', id, 'v', 'p'), named_struct('k', id * 10, 'v', 'q'))) FROM test_inline WHERE id IS NOT NULL

query
SELECT id, inline(arrays_zip(array(1, 2, 3), array('a', 'b'))) FROM test_inline WHERE id = 1

-- NULL struct elements produced by an expression rather than read from Parquet
query
SELECT id, inline_outer(array(IF(id % 2 = 0, NULL, named_struct('a', id, 'b', concat('v', id))), named_struct('a', -1, 'b', NULL))) FROM test_inline
