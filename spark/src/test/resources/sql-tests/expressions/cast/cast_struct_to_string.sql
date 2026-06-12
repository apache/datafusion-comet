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

-- Default (non-legacy) struct-to-string formatting: `{f1, f2, ...}` with NULL elements
-- rendered as "null". The legacy `[...]` mode is covered separately in
-- cast_struct_to_string_legacy.sql.

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
