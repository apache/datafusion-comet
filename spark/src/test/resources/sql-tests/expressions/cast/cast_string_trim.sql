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

-- ConfigMatrix: parquet.enable.dictionary=false,true

-- Whitespace trimming in cast(string as <fixed type>). Spark has two trim regimes and neither
-- trims non-ASCII whitespace, so DEL is trimmed for boolean/int/date but not for double/decimal,
-- and NBSP / U+3000 are trimmed for nothing. See conversion_funcs::trim in the native crate and
-- https://github.com/apache/datafusion-comet/issues/5149.
--
-- Per-codepoint parity across all three eval modes is covered by the CometNativeCastSuite
-- "whitespace trim parity" tests; this fixture covers the same regimes over a Parquet column.

statement
CREATE TABLE cast_trim_pad(name string, pad string) USING parquet

-- The padding is materialized here rather than in the queries, so that the cast operands below
-- stay inside expressions Comet runs natively. The two multi-byte pads are spelled as casts of
-- their UTF-8 bytes: `decode(bin, charset)` resolves to a RuntimeReplaceable on Spark 3.5 and
-- 4.x, which is not foldable, and an inline table only accepts foldable expressions.
statement
INSERT INTO cast_trim_pad VALUES
  ('a_none', ''),
  ('b_nul_0x00', chr(0)),
  ('c_vtab_0x0b', chr(11)),
  ('d_us_0x1f', chr(31)),
  ('e_space_0x20', ' '),
  ('f_del_0x7f', chr(127)),
  ('g_nbsp_u00a0', cast(X'C2A0' as string)),
  ('h_ideographic_u3000', cast(X'E38080' as string))

query
SELECT
  name,
  cast(concat(pad, 'true', pad) as boolean),
  cast(concat(pad, '12', pad) as tinyint),
  cast(concat(pad, '12', pad) as smallint),
  cast(concat(pad, '12', pad) as int),
  cast(concat(pad, '12', pad) as bigint),
  cast(concat(pad, '1.5', pad) as float),
  cast(concat(pad, '1.5', pad) as double),
  cast(concat(pad, '1.5', pad) as decimal(10,2)),
  cast(concat(pad, '2020-01-01', pad) as date)
FROM cast_trim_pad

-- Padding on its own, and padding in the interior, must never parse
query
SELECT
  name,
  cast(pad as boolean),
  cast(pad as int),
  cast(pad as double),
  cast(pad as decimal(10,2)),
  cast(pad as date),
  cast(concat('1', pad, '2') as int),
  cast(concat('1', pad, '.5') as double)
FROM cast_trim_pad
