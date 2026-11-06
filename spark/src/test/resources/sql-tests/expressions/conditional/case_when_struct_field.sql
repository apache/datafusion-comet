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

-- CASE WHEN, IF and COALESCE whose branches read struct fields, and AND, OR and IN whose operands
-- do. A struct field cannot fail, so Comet evaluates these for the whole batch and then picks each
-- row's value, as it does for top-level columns. A field of a NULL struct is NULL. A branch or an
-- operand that divides still runs only for the rows that reach it, which matters with ANSI mode on.

-- ConfigMatrix: spark.sql.ansi.enabled=false,true

statement
CREATE TABLE test_case_struct_field(k int, s struct<
  p: boolean, b: boolean, i: int, l: bigint, d: double, dec: decimal(38, 18), dt: date,
  ts: timestamp, str: string, bin: binary, n: struct<x: bigint, y: bigint>>) USING parquet

statement
INSERT INTO test_case_struct_field
SELECT k, IF(k = 3, NULL, named_struct(
  'p', p, 'b', b, 'i', i, 'l', l, 'd', d, 'dec', dec, 'dt', dt, 'ts', ts, 'str', str,
  'bin', bin, 'n', IF(k = 4, NULL, named_struct('x', x, 'y', CAST(y AS BIGINT)))))
FROM VALUES
  (0, true, true, 1, 10, 2.5D, 1.000000000000000001, DATE '2024-01-01',
   TIMESTAMP '2024-01-01 10:00:00', 'apple', X'01', 7, 2),
  (1, false, false, CAST(-2147483648 AS INT), CAST(-9223372036854775808 AS BIGINT),
   CAST('NaN' AS DOUBLE), -99999999999999999999.999999999999999999, DATE '1901-01-01',
   TIMESTAMP '1901-01-01 00:00:01', '', X'', 9223372036854775807, 0),
  (2, true, false, 2147483647, 9223372036854775807, CAST('Infinity' AS DOUBLE),
   99999999999999999999.999999999999999999, DATE '9999-12-31', TIMESTAMP '2262-04-11 23:47:16',
   'a value that is longer than sixteen bytes', X'00112233445566778899AABBCCDDEEFF0011',
   CAST(-9223372036854775808 AS BIGINT), 3),
  (3, true, true, 5, 50, 1.0D, 0.0, DATE '2000-01-01', TIMESTAMP '2000-01-01 00:00:00', 'x',
   X'02', 1, 1),
  (4, NULL, true, NULL, -1, CAST('-Infinity' AS DOUBLE), NULL, NULL, NULL, NULL, NULL, 4, 4),
  (5, true, NULL, 0, NULL, -0.0D, 0.000000000000000001, DATE '1970-01-01',
   TIMESTAMP '1970-01-01 00:00:00', NULL, X'FFFE', NULL, 0),
  (6, false, true, -7, 0, 0.0D, -2.5, NULL, NULL, '日本語', NULL, 0, -1),
  (7, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
AS t(k, p, b, i, l, d, dec, dt, ts, str, bin, x, y)

query
SELECT k,
  CASE WHEN s.p THEN s.b END,
  CASE WHEN s.p THEN s.b ELSE NOT s.b END,
  CASE WHEN s.p THEN s.i WHEN s.b THEN -1 END,
  CASE WHEN s.i > 0 THEN s.l ELSE s.n.x END,
  IF(s.b, s.l, s.n.y),
  coalesce(s.n.x, s.l, 0)
FROM test_case_struct_field

query
SELECT k,
  CASE WHEN s.p THEN s.d END,
  CASE WHEN s.p THEN s.d WHEN s.b THEN s.d * 2 ELSE s.d / 2 END,
  IF(s.b, s.d, s.d / 2),
  coalesce(s.d, 0.0D)
FROM test_case_struct_field

query
SELECT k,
  CASE WHEN s.p THEN s.dec END,
  IF(s.b, s.dec, NULL),
  CASE WHEN s.p THEN s.dt WHEN s.b THEN DATE '2000-01-01' END,
  coalesce(s.ts, TIMESTAMP '2000-01-01 00:00:00')
FROM test_case_struct_field

query
SELECT k,
  CASE WHEN s.p THEN s.str WHEN s.b THEN 'b' END,
  IF(s.p, s.str, ''),
  CASE WHEN s.b THEN s.bin END,
  coalesce(s.str, 'none')
FROM test_case_struct_field

-- a field of a field, which is NULL when the inner struct is
query
SELECT k, CASE WHEN s.n.x > s.n.y THEN s.n.x ELSE s.n.y END, IF(s.n IS NULL, -1L, s.n.y)
FROM test_case_struct_field

query
SELECT k, s.p AND s.b, s.p OR s.i > 0, s.l IN (s.n.x, s.n.y), s.i IN (1, 2147483647)
FROM test_case_struct_field

query
SELECT k FROM test_case_struct_field WHERE s.p AND s.i > 0

-- a division by a zero that the WHEN or the left operand of AND excludes
query
SELECT k, CASE WHEN s.n.y <> 0 THEN s.n.x div s.n.y ELSE -1 END FROM test_case_struct_field

query
SELECT k, s.n.y <> 0 AND s.n.x div s.n.y > 1 FROM test_case_struct_field
