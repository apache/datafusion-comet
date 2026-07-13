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

-- Default (non-legacy) formatting for array / map / struct → string:
-- `{f1, f2, ...}` for structs, `[e1, e2, ...]` for arrays, `{k1 -> v1, k2 -> v2}` for maps,
-- with NULL elements rendered as the literal "null". The legacy `[...]`-wrapped /
-- NULL-omitting mode is covered separately in cast_complex_types_to_string_legacy.sql.

-- Config: spark.sql.legacy.castComplexTypesToString.enabled=false
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_cast_struct_to_string(
  id int,
  s_unnamed struct<col1: int, col2: string>,
  s_named struct<a: int, b: string, c: boolean>,
  s_floats struct<f: float, d: double>,
  s_bounds struct<b: tinyint, s: smallint, i: int, l: bigint>,
  s_decimal struct<d1: decimal(10, 2), d2: decimal(38, 18)>,
  s_temporal struct<dt: date, ts: timestamp>,
  s_binary struct<b: binary>,
  s_nested struct<inner: struct<x: int, y: string>, tag: string>,
  s_with_array struct<arr: array<int>, label: string>,
  s_all_null struct<a: int, b: string>
) USING parquet

statement
INSERT INTO test_cast_struct_to_string VALUES
  (
    1,
    named_struct('col1', 1, 'col2', 'hello'),
    named_struct('a', 42, 'b', 'world', 'c', true),
    named_struct('f', cast(1.5 as float), 'd', cast(2.5 as double)),
    named_struct('b', cast(127 as tinyint), 's', cast(32767 as smallint), 'i', 2147483647, 'l', 9223372036854775807),
    named_struct('d1', cast('12345678.90' as decimal(10, 2)), 'd2', cast('1.234567890123456789' as decimal(38, 18))),
    named_struct('dt', date '2024-01-15', 'ts', timestamp '2024-01-15 10:30:45'),
    named_struct('b', X'616263'),
    named_struct('inner', named_struct('x', 10, 'y', 'inner'), 'tag', 'outer'),
    named_struct('arr', array(1, 2, 3), 'label', 'three'),
    named_struct('a', 1, 'b', 'present')
  ),
  (
    2,
    named_struct('col1', cast(null as int), 'col2', 'with-null-int'),
    named_struct('a', 0, 'b', cast(null as string), 'c', false),
    named_struct('f', cast('NaN' as float), 'd', cast('NaN' as double)),
    named_struct('b', cast(-128 as tinyint), 's', cast(-32768 as smallint), 'i', -2147483648, 'l', -9223372036854775808),
    named_struct('d1', cast('-12345678.90' as decimal(10, 2)), 'd2', cast('-1.234567890123456789' as decimal(38, 18))),
    named_struct('dt', date '1970-01-01', 'ts', timestamp '1970-01-01 00:00:00'),
    named_struct('b', X''),
    named_struct('inner', named_struct('x', cast(null as int), 'y', cast(null as string)), 'tag', cast(null as string)),
    named_struct('arr', array(cast(null as int), 1, cast(null as int)), 'label', cast(null as string)),
    named_struct('a', cast(null as int), 'b', cast(null as string))
  ),
  (
    3,
    named_struct('col1', -1, 'col2', ''),
    named_struct('a', cast(null as int), 'b', '', 'c', cast(null as boolean)),
    named_struct('f', cast('Infinity' as float), 'd', cast('-Infinity' as double)),
    named_struct('b', cast(0 as tinyint), 's', cast(0 as smallint), 'i', 0, 'l', cast(0 as bigint)),
    named_struct('d1', cast(0 as decimal(10, 2)), 'd2', cast(0 as decimal(38, 18))),
    named_struct('dt', date '9999-12-31', 'ts', timestamp '9999-12-31 23:59:59'),
    named_struct('b', X'00FF7F80'),
    named_struct('inner', named_struct('x', 0, 'y', ''), 'tag', ''),
    named_struct('arr', cast(array() as array<int>), 'label', ''),
    cast(null as struct<a: int, b: string>)
  ),
  (
    4,
    named_struct('col1', cast(null as int), 'col2', cast(null as string)),
    named_struct('a', cast(null as int), 'b', cast(null as string), 'c', cast(null as boolean)),
    named_struct('f', cast(-0.0 as float), 'd', cast(-0.0 as double)),
    named_struct('b', cast(null as tinyint), 's', cast(null as smallint), 'i', cast(null as int), 'l', cast(null as bigint)),
    named_struct('d1', cast(null as decimal(10, 2)), 'd2', cast(null as decimal(38, 18))),
    named_struct('dt', cast(null as date), 'ts', cast(null as timestamp)),
    named_struct('b', cast(null as binary)),
    named_struct('inner', cast(null as struct<x: int, y: string>), 'tag', cast(null as string)),
    named_struct('arr', cast(null as array<int>), 'label', cast(null as string)),
    cast(null as struct<a: int, b: string>)
  )

-- Anonymous struct fields are auto-named col1, col2, ... by `struct(...)`.
query
SELECT cast(s_unnamed as string), id FROM test_cast_struct_to_string ORDER BY id

-- Named struct fields propagate user-supplied names into the formatted output.
query
SELECT cast(s_named as string), id FROM test_cast_struct_to_string ORDER BY id

-- Floating-point: NaN, ±0, ±Infinity, NULL.
query
SELECT cast(s_floats as string), id FROM test_cast_struct_to_string ORDER BY id

-- Integer min/max for byte, short, int, long.
query
SELECT cast(s_bounds as string), id FROM test_cast_struct_to_string ORDER BY id

-- Decimal at the small and the 38-precision limit, positive / negative / zero / NULL.
query
SELECT cast(s_decimal as string), id FROM test_cast_struct_to_string ORDER BY id

-- Date and timestamp at common values plus the date range edges.
query
SELECT cast(s_temporal as string), id FROM test_cast_struct_to_string ORDER BY id

-- Binary content including empty bytes and non-printable values.
query
SELECT cast(s_binary as string), id FROM test_cast_struct_to_string ORDER BY id

-- Nested struct: inner struct rendered inside the outer braces.
query
SELECT cast(s_nested as string), id FROM test_cast_struct_to_string ORDER BY id

-- Struct containing an array field.
query
SELECT cast(s_with_array as string), id FROM test_cast_struct_to_string ORDER BY id

-- Whole struct is NULL vs. all inner fields NULL.
query
SELECT cast(s_all_null as string), id FROM test_cast_struct_to_string ORDER BY id

-- Literal anonymous struct, mixed types with NULL.
query
SELECT cast(struct(1, 'two', cast(null as double)) as string)

-- Literal named struct, mixed types.
query
SELECT cast(named_struct('k', 'key', 'v', 100, 'flag', true) as string)

-- Deeply nested literal struct (3 levels).
query
SELECT cast(named_struct('a', named_struct('b', named_struct('c', 1, 'd', 'leaf'))) as string)

-- Empty-string and whitespace string-field rendering.
query
SELECT cast(named_struct('s1', '', 's2', ' ', 's3', cast(null as string)) as string)

-- Map-valued field: not supported, falls back to Spark.
query expect_fallback(to StringType is not supported)
SELECT cast(named_struct('m', map('k', 1)) as string)

-- ----------------------------------------------------------------------------
-- Array → string
-- ----------------------------------------------------------------------------

statement
CREATE TABLE test_cast_array_to_string(
  id int,
  a_int array<int>,
  a_string array<string>,
  a_bool array<boolean>,
  a_bounds array<bigint>,
  a_decimal array<decimal(38, 18)>,
  a_date array<date>,
  a_ts array<timestamp>,
  a_binary array<binary>,
  a_struct array<struct<x: int, y: string>>,
  a_nested array<array<int>>
) USING parquet

statement
INSERT INTO test_cast_array_to_string VALUES
  (
    1,
    array(1, 2, 3),
    array('a', 'b', 'c'),
    array(true, false, true),
    array(9223372036854775807, -9223372036854775808, 0),
    array(cast('1.234567890123456789' as decimal(38, 18)), cast('-1.234567890123456789' as decimal(38, 18))),
    array(date '2024-01-15', date '1970-01-01'),
    array(timestamp '2024-01-15 10:30:45', timestamp '1970-01-01 00:00:00'),
    array(X'616263', X'', X'00FF7F80'),
    array(named_struct('x', 1, 'y', 'first'), named_struct('x', 2, 'y', 'second')),
    array(array(1, 2), array(3, 4, 5))
  ),
  (
    2,
    array(cast(null as int), 1, cast(null as int)),
    array(cast(null as string), '', ' '),
    array(cast(null as boolean), true),
    array(cast(null as bigint), 0),
    array(cast(null as decimal(38, 18))),
    array(cast(null as date)),
    array(cast(null as timestamp)),
    array(cast(null as binary), X'00'),
    array(named_struct('x', cast(null as int), 'y', cast(null as string)), cast(null as struct<x: int, y: string>)),
    array(cast(null as array<int>), array(cast(null as int)))
  ),
  (
    3,
    cast(array() as array<int>),
    cast(array() as array<string>),
    cast(array() as array<boolean>),
    cast(array() as array<bigint>),
    cast(array() as array<decimal(38, 18)>),
    cast(array() as array<date>),
    cast(array() as array<timestamp>),
    cast(array() as array<binary>),
    cast(array() as array<struct<x: int, y: string>>),
    cast(array() as array<array<int>>)
  ),
  (
    4,
    cast(null as array<int>),
    cast(null as array<string>),
    cast(null as array<boolean>),
    cast(null as array<bigint>),
    cast(null as array<decimal(38, 18)>),
    cast(null as array<date>),
    cast(null as array<timestamp>),
    cast(null as array<binary>),
    cast(null as array<struct<x: int, y: string>>),
    cast(null as array<array<int>>)
  )

query
SELECT cast(a_int as string), id FROM test_cast_array_to_string ORDER BY id

query
SELECT cast(a_string as string), id FROM test_cast_array_to_string ORDER BY id

query
SELECT cast(a_bool as string), id FROM test_cast_array_to_string ORDER BY id

query
SELECT cast(a_bounds as string), id FROM test_cast_array_to_string ORDER BY id

query
SELECT cast(a_decimal as string), id FROM test_cast_array_to_string ORDER BY id

query
SELECT cast(a_date as string), id FROM test_cast_array_to_string ORDER BY id

query
SELECT cast(a_ts as string), id FROM test_cast_array_to_string ORDER BY id

query
SELECT cast(a_binary as string), id FROM test_cast_array_to_string ORDER BY id

-- Array of structs: each element rendered as `{f1, f2, ...}`.
query
SELECT cast(a_struct as string), id FROM test_cast_array_to_string ORDER BY id

-- Nested array<array<int>>: outer `[...]` containing inner `[...]`.
query
SELECT cast(a_nested as string), id FROM test_cast_array_to_string ORDER BY id

-- Array of floats / doubles with NaN / ±0 / ±Infinity / NULL.
query
SELECT cast(array(cast(1.5 as float), cast('NaN' as float), cast(-0.0 as float), cast(null as float)) as string)

query
SELECT cast(array(cast(1.5 as double), cast('NaN' as double), cast('-Infinity' as double), cast(null as double)) as string)

-- Deeply nested literal array (3 levels).
query
SELECT cast(array(array(array(1, 2), array(3)), array(array(cast(null as int)))) as string)

-- Array of map: not supported, falls back to Spark.
query expect_fallback(to StringType is not supported)
SELECT cast(array(map('k', 1)) as string)

-- ----------------------------------------------------------------------------
-- Map → string
-- ----------------------------------------------------------------------------
-- Comet does not implement map-to-string casts, so every map → string falls back to Spark.
-- Note: maps materialized through parquet have nondeterministic entry order, so map column
-- tests use literal maps directly rather than reading from a parquet table.

-- Map with string keys, int values.
query expect_fallback(Cast from MapType)
SELECT cast(map('a', 1, 'b', 2, 'c', 3) as string)

-- Map with NULL values rendered as "null".
query expect_fallback(Cast from MapType)
SELECT cast(map('a', 1, 'b', cast(null as int), 'c', 3) as string)

-- Map with int keys, string values.
query expect_fallback(Cast from MapType)
SELECT cast(map(1, 'one', 2, 'two', 3, 'three') as string)

-- Map with boolean values.
query expect_fallback(Cast from MapType)
SELECT cast(map('t', true, 'f', false, 'n', cast(null as boolean)) as string)

-- Map with bigint values at min/max.
query expect_fallback(Cast from MapType)
SELECT cast(map('max', 9223372036854775807, 'min', -9223372036854775808, 'zero', cast(0 as bigint)) as string)

-- Map with decimal values.
query expect_fallback(Cast from MapType)
SELECT cast(map('pos', cast('1.234567890123456789' as decimal(38, 18)), 'neg', cast('-1.234567890123456789' as decimal(38, 18)), 'null', cast(null as decimal(38, 18))) as string)

-- Map with date and timestamp values.
query expect_fallback(Cast from MapType)
SELECT cast(map('a', date '2024-01-15', 'b', date '1970-01-01', 'c', cast(null as date)) as string)

query expect_fallback(Cast from MapType)
SELECT cast(map('a', timestamp '2024-01-15 10:30:45', 'b', cast(null as timestamp)) as string)

-- Map with binary values.
query expect_fallback(Cast from MapType)
SELECT cast(map('a', X'616263', 'b', X'', 'c', cast(null as binary)) as string)

-- Map with float / double values: NaN / ±0 / ±Infinity / NULL.
query expect_fallback(Cast from MapType)
SELECT cast(map('nan', cast('NaN' as float), 'neg0', cast(-0.0 as float), 'null', cast(null as float)) as string)

query expect_fallback(Cast from MapType)
SELECT cast(map('nan', cast('NaN' as double), 'inf', cast('Infinity' as double), 'ninf', cast('-Infinity' as double), 'null', cast(null as double)) as string)

-- Map with struct values: each value rendered as `{f1, f2, ...}`.
query expect_fallback(Cast from MapType)
SELECT cast(map('a', named_struct('x', 1, 'y', 'first'), 'b', cast(null as struct<x: int, y: string>)) as string)

-- Map with array values.
query expect_fallback(Cast from MapType)
SELECT cast(map('a', array(1, 2, 3), 'b', array(cast(null as int)), 'c', cast(null as array<int>)) as string)

-- Empty map.
query expect_fallback(Cast from MapType)
SELECT cast(map() as string)

-- NULL map: Spark constant-folds this to a literal NULL, so the cast never reaches Comet
-- and there is no fallback.
query
SELECT cast(cast(null as map<string, int>) as string)

-- Map of map.
query expect_fallback(Cast from MapType)
SELECT cast(map('outer', map('inner', 1)) as string)
