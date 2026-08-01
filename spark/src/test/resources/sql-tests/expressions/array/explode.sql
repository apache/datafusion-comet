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

-- Exercises explode and explode_outer across every primitive data type Spark
-- supports for array element input, including null arrays, empty arrays, null
-- array elements, floating-point specials, integer/decimal boundaries, nested
-- arrays, and arrays of structs. Comet's explode_outer must emit exactly one
-- row (with a null value) for both null and empty inputs, matching Spark's
-- GenerateExec `outer` semantics.

-- ===== INT arrays: null id, null / empty / non-empty arrays =====

statement
CREATE TABLE test_explode_int(id int, arr array<int>) USING parquet

statement
INSERT INTO test_explode_int VALUES
  (1, array(10, 20, 30)),
  (2, array(40)),
  (3, array()),
  (4, NULL),
  (5, array(NULL, 50, NULL)),
  (NULL, array(60, 70)),
  (NULL, array()),
  (NULL, NULL)

query
SELECT id, explode(arr) AS v FROM test_explode_int

query
SELECT id, explode_outer(arr) AS v FROM test_explode_int

-- LATERAL VIEW forms round-trip through the same Generate operator
query
SELECT id, v FROM test_explode_int LATERAL VIEW explode(arr) t AS v

query
SELECT id, v FROM test_explode_int LATERAL VIEW OUTER explode(arr) t AS v

-- explode / explode_outer of a literal array (constant folding is disabled by
-- the test runner, so the array reaches the operator as an expression)
query
SELECT id, explode_outer(array(100, 200, 300)) FROM test_explode_int WHERE id = 1

query
SELECT id, explode_outer(cast(NULL as array<int>)) FROM test_explode_int WHERE id = 1

query
SELECT id, explode_outer(array()) FROM test_explode_int WHERE id = 1

-- ===== BOOLEAN =====

statement
CREATE TABLE test_explode_bool(id int, arr array<boolean>) USING parquet

statement
INSERT INTO test_explode_bool VALUES
  (1, array(true, false, true)),
  (2, array(NULL, true)),
  (3, array()),
  (4, NULL)

query
SELECT id, explode_outer(arr) FROM test_explode_bool

-- ===== TINYINT / SMALLINT / BIGINT with min/max boundaries =====

statement
CREATE TABLE test_explode_tinyint(id int, arr array<tinyint>) USING parquet

statement
INSERT INTO test_explode_tinyint VALUES
  (1, array(cast(-128 as tinyint), cast(0 as tinyint), cast(127 as tinyint))),
  (2, array(cast(NULL as tinyint))),
  (3, array()),
  (4, NULL)

query
SELECT id, explode_outer(arr) FROM test_explode_tinyint

statement
CREATE TABLE test_explode_smallint(id int, arr array<smallint>) USING parquet

statement
INSERT INTO test_explode_smallint VALUES
  (1, array(cast(-32768 as smallint), cast(0 as smallint), cast(32767 as smallint))),
  (2, array()),
  (3, NULL)

query
SELECT id, explode_outer(arr) FROM test_explode_smallint

statement
CREATE TABLE test_explode_bigint(id int, arr array<bigint>) USING parquet

statement
INSERT INTO test_explode_bigint VALUES
  (1, array(-9223372036854775808L, 0L, 9223372036854775807L)),
  (2, array(NULL, 1L)),
  (3, array()),
  (4, NULL)

query
SELECT id, explode_outer(arr) FROM test_explode_bigint

-- Integer min/max boundaries for the default INT case
statement
CREATE TABLE test_explode_int_bounds(id int, arr array<int>) USING parquet

statement
INSERT INTO test_explode_int_bounds VALUES
  (1, array(-2147483648, 0, 2147483647)),
  (2, array()),
  (3, NULL)

query
SELECT id, explode_outer(arr) FROM test_explode_int_bounds

-- ===== FLOAT / DOUBLE: NaN, +/-Infinity, +/-0.0, NULL, empty, null-array =====

statement
CREATE TABLE test_explode_float(id int, arr array<float>) USING parquet

statement
INSERT INTO test_explode_float VALUES
  (1, array(cast('NaN' as float), cast(0.0 as float), cast(-0.0 as float))),
  (2, array(cast('Infinity' as float), cast('-Infinity' as float))),
  (3, array(cast(1.5 as float), NULL, cast(-1.5 as float))),
  (4, array()),
  (5, NULL)

query
SELECT id, explode_outer(arr) FROM test_explode_float

statement
CREATE TABLE test_explode_double(id int, arr array<double>) USING parquet

statement
INSERT INTO test_explode_double VALUES
  (1, array(cast('NaN' as double), cast(0.0 as double), cast(-0.0 as double))),
  (2, array(cast('Infinity' as double), cast('-Infinity' as double))),
  (3, array(1.5, NULL, -1.5)),
  (4, array()),
  (5, NULL)

query
SELECT id, explode_outer(arr) FROM test_explode_double

-- ===== DECIMAL boundaries =====

statement
CREATE TABLE test_explode_decimal(id int, arr array<decimal(18,4)>) USING parquet

statement
INSERT INTO test_explode_decimal VALUES
  (1, array(cast('99999999999999.9999' as decimal(18,4)),
            cast('-99999999999999.9999' as decimal(18,4)),
            cast('0.0000' as decimal(18,4)))),
  (2, array(cast(NULL as decimal(18,4)), cast('1.2345' as decimal(18,4)))),
  (3, array()),
  (4, NULL)

query
SELECT id, explode_outer(arr) FROM test_explode_decimal

-- Large-precision decimal
statement
CREATE TABLE test_explode_decimal38(id int, arr array<decimal(38,10)>) USING parquet

statement
INSERT INTO test_explode_decimal38 VALUES
  (1, array(cast('9999999999999999999999999999.9999999999' as decimal(38,10)),
            cast('-9999999999999999999999999999.9999999999' as decimal(38,10)))),
  (2, array()),
  (3, NULL)

query
SELECT id, explode_outer(arr) FROM test_explode_decimal38

-- ===== STRING =====

statement
CREATE TABLE test_explode_string(id int, arr array<string>) USING parquet

statement
INSERT INTO test_explode_string VALUES
  (1, array('a', 'bb', 'ccc')),
  (2, array('', ' ', 'unicode: éü')),
  (3, array(NULL, 'x')),
  (4, array()),
  (5, NULL)

query
SELECT id, explode_outer(arr) FROM test_explode_string

-- ===== BINARY =====

statement
CREATE TABLE test_explode_binary(id int, arr array<binary>) USING parquet

statement
INSERT INTO test_explode_binary VALUES
  (1, array(cast('a' as binary), cast('bc' as binary))),
  (2, array(NULL, cast('x' as binary))),
  (3, array()),
  (4, NULL)

query
SELECT id, explode_outer(arr) FROM test_explode_binary

-- ===== DATE / TIMESTAMP =====

statement
CREATE TABLE test_explode_date(id int, arr array<date>) USING parquet

statement
INSERT INTO test_explode_date VALUES
  (1, array(date '1970-01-01', date '9999-12-31', date '2024-06-15')),
  (2, array(NULL, date '2024-02-29')),
  (3, array()),
  (4, NULL)

query
SELECT id, explode_outer(arr) FROM test_explode_date

statement
CREATE TABLE test_explode_ts(id int, arr array<timestamp>) USING parquet

statement
INSERT INTO test_explode_ts VALUES
  (1, array(timestamp '1970-01-01 00:00:00', timestamp '2024-06-15 12:34:56.789')),
  (2, array(NULL, timestamp '9999-12-31 23:59:59.999999')),
  (3, array()),
  (4, NULL)

query
SELECT id, explode_outer(arr) FROM test_explode_ts

-- ===== Nested arrays: array<array<int>> =====

statement
CREATE TABLE test_explode_nested(id int, arr array<array<int>>) USING parquet

statement
INSERT INTO test_explode_nested VALUES
  (1, array(array(1, 2), array(3))),
  (2, array(array(), array(NULL))),
  (3, array(NULL, array(4, 5))),
  (4, array()),
  (5, NULL)

-- explode_outer of the outer array; inner arrays are emitted verbatim
query
SELECT id, explode_outer(arr) FROM test_explode_nested

-- ===== Array of structs =====

statement
CREATE TABLE test_explode_struct(id int, arr array<struct<a: int, b: string>>) USING parquet

statement
INSERT INTO test_explode_struct VALUES
  (1, array(named_struct('a', 10, 'b', 'x'), named_struct('a', 20, 'b', NULL))),
  (2, array(named_struct('a', cast(NULL as int), 'b', 'y'))),
  (3, array()),
  (4, NULL)

query
SELECT id, explode_outer(arr) FROM test_explode_struct

-- Access struct fields after exploding
query
SELECT id, v.a AS a, v.b AS b FROM test_explode_struct LATERAL VIEW OUTER explode(arr) t AS v

-- ===== Multiple projected columns, several of them nullable =====

statement
CREATE TABLE test_explode_multi(id int, name string, extra bigint, arr array<int>) USING parquet

statement
INSERT INTO test_explode_multi VALUES
  (1, 'A', 100L, array(1, 2, 3)),
  (2, NULL, 200L, array()),
  (3, 'C', NULL, array(4)),
  (4, NULL, NULL, NULL),
  (NULL, 'E', 500L, array(5, 6))

query
SELECT id, name, extra, explode_outer(arr) AS v FROM test_explode_multi

-- ===== Empty table (zero input rows) =====

statement
CREATE TABLE test_explode_empty(id int, arr array<int>) USING parquet

query
SELECT id, explode_outer(arr) FROM test_explode_empty

-- ===== Map falls back to Spark: outer form must also fall back =====

statement
CREATE TABLE test_explode_map(id int, m map<string, int>) USING parquet

statement
INSERT INTO test_explode_map VALUES
  (1, map('k1', 1, 'k2', 2)),
  (2, map()),
  (3, NULL)

query expect_fallback(Comet only supports explode/explode_outer for arrays, not maps)
SELECT id, explode_outer(m) FROM test_explode_map
