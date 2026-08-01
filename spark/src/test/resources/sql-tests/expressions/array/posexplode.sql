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

-- posexplode_outer is now supported natively; see DataFusion #19053 handling
-- via ListEmptyToNullExpr in the planner.

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

-- posexplode_outer keeps rows whose array is NULL or empty. Comet emits one
-- row with a NULL position and NULL value for each such input row.
query
SELECT id, posexplode_outer(arr) FROM test_posexplode_int

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

-- posexplode over a map falls back to Spark (Comet only supports array inputs, not maps)
query expect_fallback(Comet only supports explode/explode_outer for arrays, not maps)
SELECT id, posexplode(m) FROM test_posexplode_map

-- ===== posexplode_outer across non-int element types =====

-- posexplode_outer with nullable int elements (in-array NULLs plus outer null row)
query
SELECT id, posexplode_outer(arr) FROM test_posexplode_nullable

-- posexplode_outer over an array of strings
query
SELECT id, posexplode_outer(arr) FROM test_posexplode_str

-- posexplode_outer over an array of structs via LATERAL VIEW OUTER, then project fields
query
SELECT id, pos, value.v1 AS v1, value.v2 AS v2
FROM test_posexplode_struct LATERAL VIEW OUTER posexplode(arr) p AS pos, value

statement
CREATE TABLE test_posexplode_outer_types(
  id int,
  arr_bool array<boolean>,
  arr_bi array<bigint>,
  arr_dbl array<double>,
  arr_dec array<decimal(18,4)>,
  arr_bin array<binary>,
  arr_dt array<date>,
  arr_ts array<timestamp>) USING parquet

statement
INSERT INTO test_posexplode_outer_types VALUES
  (1,
   array(true, false, NULL),
   array(-9223372036854775808L, 0L, 9223372036854775807L),
   array(cast('NaN' as double), cast(0.0 as double), cast(-0.0 as double), cast('Infinity' as double), cast('-Infinity' as double)),
   array(cast('99999999999999.9999' as decimal(18,4)), cast('-99999999999999.9999' as decimal(18,4)), cast(NULL as decimal(18,4))),
   array(cast('a' as binary), NULL, cast('bc' as binary)),
   array(date '1970-01-01', date '9999-12-31', NULL),
   array(timestamp '1970-01-01 00:00:00', timestamp '2024-06-15 12:34:56.789', NULL)),
  (2, array(), array(), array(), array(), array(), array(), array()),
  (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL)

query
SELECT id, posexplode_outer(arr_bool) FROM test_posexplode_outer_types

query
SELECT id, posexplode_outer(arr_bi) FROM test_posexplode_outer_types

query
SELECT id, posexplode_outer(arr_dbl) FROM test_posexplode_outer_types

query
SELECT id, posexplode_outer(arr_dec) FROM test_posexplode_outer_types

query
SELECT id, posexplode_outer(arr_bin) FROM test_posexplode_outer_types

query
SELECT id, posexplode_outer(arr_dt) FROM test_posexplode_outer_types

query
SELECT id, posexplode_outer(arr_ts) FROM test_posexplode_outer_types

-- posexplode_outer over an array of structs (explicit projection form)
query
SELECT id, posexplode_outer(arr) FROM test_posexplode_struct

-- LATERAL VIEW OUTER posexplode over int arrays: covers the analyzer's
-- `MultiAlias(GeneratorOuter(g), names)` branch alongside the direct
-- `posexplode_outer(...)` projection form above.
query
SELECT id, pos, value
FROM test_posexplode_int LATERAL VIEW OUTER posexplode(arr) p AS pos, value

-- posexplode_outer batch of only-empty arrays exercises the slow path with an
-- all-zeros non-empty bitmap; only-null exercises the fast-path passthrough.
statement
CREATE TABLE test_posexplode_all_empty(id int, arr array<int>) USING parquet

statement
INSERT INTO test_posexplode_all_empty VALUES (1, array()), (2, array()), (3, array())

query
SELECT id, posexplode_outer(arr) FROM test_posexplode_all_empty

statement
CREATE TABLE test_posexplode_all_null(id int, arr array<int>) USING parquet

statement
INSERT INTO test_posexplode_all_null VALUES (1, NULL), (2, NULL), (3, NULL)

query
SELECT id, posexplode_outer(arr) FROM test_posexplode_all_null
