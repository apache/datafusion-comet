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

-- posexplode_outer is gated behind allowIncompatible=true (DataFusion #19053).
-- Setting it at file scope does not affect the non-outer cases.
-- Config: spark.comet.operator.GenerateExec.allowIncompatible=true

statement
CREATE TABLE test_posexplode_int(id int, arr array<int>) USING parquet

statement
INSERT INTO test_posexplode_int VALUES
  (1, array(10, 20, 30)),
  (2, array(40, 50)),
  (3, array(60)),
  (4, array()),
  (5, NULL)

-- basic posexplode over an int array column
query
SELECT id, posexplode(arr) FROM test_posexplode_int

-- posexplode with explicit pos/value aliasing
query
SELECT id, pos, value FROM test_posexplode_int LATERAL VIEW posexplode(arr) p AS pos, value

-- posexplode_outer keeps rows whose array is NULL. Empty arrays are excluded by
-- the WHERE clause because DataFusion #19053 drops empty arrays under
-- preserve_nulls=true; that is the documented Incompatible behavior.
query
SELECT id, posexplode_outer(arr) FROM test_posexplode_int WHERE id != 4

-- posexplode of a literal array (constant folding is disabled by the test runner)
query
SELECT id, posexplode(array(100, 200, 300)) FROM test_posexplode_int WHERE id = 1

statement
CREATE TABLE test_posexplode_str(id int, arr array<string>) USING parquet

statement
INSERT INTO test_posexplode_str VALUES
  (1, array('a', 'b', 'c')),
  (2, array('d', 'e')),
  (3, array('f'))

-- posexplode over a string array
query
SELECT id, posexplode(arr) FROM test_posexplode_str

statement
CREATE TABLE test_posexplode_nullable(id int, arr array<int>) USING parquet

statement
INSERT INTO test_posexplode_nullable VALUES
  (1, array(1, NULL, 3)),
  (2, array(NULL, 5)),
  (3, array(6))

-- posexplode preserves null elements within the array
query
SELECT id, posexplode(arr) FROM test_posexplode_nullable

statement
CREATE TABLE test_posexplode_struct(id int, arr array<struct<v1: int, v2: string>>) USING parquet

statement
INSERT INTO test_posexplode_struct VALUES
  (1, array(named_struct('v1', 10, 'v2', 'a'), named_struct('v1', 20, 'v2', 'b'))),
  (2, array(named_struct('v1', 30, 'v2', 'c'))),
  (3, array())

-- posexplode over an array of structs, then project struct fields out of the unnested column
query
SELECT id, pos, value.v1 AS v1, value.v2 AS v2 FROM test_posexplode_struct LATERAL VIEW posexplode(arr) p AS pos, value

statement
CREATE TABLE test_posexplode_map(id int, m map<string, int>) USING parquet

statement
INSERT INTO test_posexplode_map VALUES
  (1, map('a', 1, 'b', 2)),
  (2, map('c', 3))

-- posexplode over a map falls back to Spark (Comet only supports array inputs)
query expect_fallback(size does not support map inputs)
SELECT id, posexplode(m) FROM test_posexplode_map
