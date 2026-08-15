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

-- MinSparkVersion: 4.1
-- Config: spark.sql.timeType.enabled=true

statement
CREATE TABLE test_to_time(s STRING) USING parquet

statement
INSERT INTO test_to_time VALUES
  ('00:00'),
  ('12:30'),
  ('23:59'),
  ('12:30:45'),
  ('00:00:00'),
  ('23:59:59'),
  ('00:00:00.1'),
  ('00:00:00.001'),
  ('00:00:00.000001'),
  ('00:00:00.1234567'),
  ('23:59:59.999999'),
  ('1:2:3'),
  ('1:2:3.04'),
  ('T12:30:45'),
  ('T1:02:3.04'),
  ('12:00:00 AM'),
  ('1:00:00 AM'),
  ('12:00:00 PM'),
  ('1:00:00 PM'),
  ('11:59:59 PM'),
  ('12:59:59.999999 PM'),
  ('12:00:00AM'),
  ('1:00:00PM'),
  (' 12:30:45'),
  ('   12:30 PM'),
  (NULL)

-- column argument: basic time formats (spark_answer_only: shuffle does not support TimeType yet;
-- TODO: promote to full native-verification once SPARK-51779 lands)
query spark_answer_only
SELECT s, to_time(s) FROM test_to_time ORDER BY s

-- literal HH:mm
query
SELECT to_time('00:00')

query
SELECT to_time('12:30')

query
SELECT to_time('23:59')

-- literal HH:mm:ss
query
SELECT to_time('12:30:45')

query
SELECT to_time('00:00:00')

query
SELECT to_time('23:59:59')

-- fractional seconds
query
SELECT to_time('00:00:00.1')

query
SELECT to_time('00:00:00.001')

query
SELECT to_time('00:00:00.000001')

query
SELECT to_time('23:59:59.999999')

-- more than 6 fractional digits (truncated to microseconds)
query
SELECT to_time('00:00:00.1234567')

-- single digit hour/min/sec
query
SELECT to_time('1:2:3')

query
SELECT to_time('1:2:3.04')

-- T-prefix
query
SELECT to_time('T12:30:45')

query
SELECT to_time('T1:02:3.04')

-- AM/PM
query
SELECT to_time('12:00:00 AM')

query
SELECT to_time('1:00:00 AM')

query
SELECT to_time('11:59:59 AM')

query
SELECT to_time('12:00:00 PM')

query
SELECT to_time('1:00:00 PM')

query
SELECT to_time('11:59:59 PM')

-- AM/PM case insensitive
query
SELECT to_time('12:00:00 am')

query
SELECT to_time('12:00:00 pm')

-- AM/PM without space
query
SELECT to_time('12:00:00AM')

query
SELECT to_time('1:00:00PM')

-- AM/PM with fractional seconds
query
SELECT to_time('12:59:59.999999 PM')

-- null input
query
SELECT to_time(NULL)

-- trailing whitespace
query
SELECT to_time('12:30:45  ')

-- leading whitespace
query
SELECT to_time(' 12:30:45')

query
SELECT to_time('  12:30:45')

query
SELECT to_time(' 12:30:45 ')

query
SELECT to_time('  12:30')

query
SELECT to_time('   12:30 PM')

query
SELECT to_time(' 1:00:00 PM')

-- leading tab and newline
query
SELECT to_time('\t12:30:45')

query
SELECT to_time('\n12:30:45')

-- leading whitespace with T-prefix is rejected by Spark
query expect_error(cannot be parsed to a TIME value)
SELECT to_time('  T12:30:45')

-- invalid inputs - should throw error with to_time
query expect_error(cannot be parsed to a TIME value)
SELECT to_time('')

query expect_error(cannot be parsed to a TIME value)
SELECT to_time('XYZ')

query expect_error(cannot be parsed to a TIME value)
SELECT to_time('24:00:00')

query expect_error(cannot be parsed to a TIME value)
SELECT to_time('23:60:00')

query expect_error(cannot be parsed to a TIME value)
SELECT to_time('23:00:60')

query expect_error(cannot be parsed to a TIME value)
SELECT to_time('120000')

-- invalid AM/PM - should throw error
query expect_error(cannot be parsed to a TIME value)
SELECT to_time('0:00:00 AM')

query expect_error(cannot be parsed to a TIME value)
SELECT to_time('13:00:00 AM')

query expect_error(cannot be parsed to a TIME value)
SELECT to_time('13:00:00 PM')

-- try_to_time: returns null for invalid inputs
query
SELECT try_to_time('12:30:45')

query
SELECT try_to_time('')

query
SELECT try_to_time('XYZ')

query
SELECT try_to_time('24:00:00')

query
SELECT try_to_time('23:60:00')

query
SELECT try_to_time(NULL)

query
SELECT try_to_time('0:00:00 AM')

query
SELECT try_to_time('13:00:00 PM')

-- try_to_time: leading whitespace parses successfully
query
SELECT try_to_time(' 12:30:45')

query
SELECT try_to_time(' 1:00:00 PM')

-- Spark's time parser trims ASCII control characters, spaces, and DELETE after detecting AM/PM,
-- but never trims non-ASCII whitespace. Materialize the padding so every query remains native.
statement
CREATE TABLE test_to_time_trim(name STRING, pad STRING) USING parquet

statement
INSERT INTO test_to_time_trim VALUES
  ('a_none', ''),
  ('b_nul_0x00', chr(0)),
  ('c_soh_0x01', chr(1)),
  ('d_tab_0x09', chr(9)),
  ('e_vtab_0x0b', chr(11)),
  ('f_us_0x1f', chr(31)),
  ('g_space_0x20', ' '),
  ('h_del_0x7f', chr(127)),
  ('i_nbsp_u00a0', cast(X'C2A0' as string)),
  ('j_ideographic_u3000', cast(X'E38080' as string))

-- Leading, trailing, both-sided, and interior padding must match Spark for every codepoint.
query
SELECT
  name,
  try_to_time(concat(pad, '12:30:45')),
  try_to_time(concat('12:30:45', pad)),
  try_to_time(concat(pad, '12:30:45', pad)),
  try_to_time(concat('12:', pad, '30:45'))
FROM test_to_time_trim

-- Leading trimAll padding invalidates a T prefix. Controls before AM/PM are valid, but only ASCII
-- spaces after AM/PM are trimmed before Spark checks the suffix.
query
SELECT
  name,
  try_to_time(concat(pad, 'T12:30:45')),
  try_to_time(concat('T12:30:45', pad)),
  try_to_time(concat('1:00:00', pad, 'PM')),
  try_to_time(concat('1:00:00 PM', pad))
FROM test_to_time_trim

-- The throwing variant must accept the same valid ASCII controls as try_to_time.
query
SELECT
  name,
  to_time(concat(pad, '12:30:45', pad)),
  to_time(concat('1:00:00', pad, 'PM'))
FROM test_to_time_trim
WHERE name IN (
  'a_none', 'b_nul_0x00', 'c_soh_0x01', 'd_tab_0x09', 'e_vtab_0x0b',
  'f_us_0x1f', 'g_space_0x20', 'h_del_0x7f')

-- The throwing variant must reject Unicode whitespace instead of silently parsing it.
query expect_error(cannot be parsed to a TIME value)
SELECT to_time(concat(pad, '12:30:45'))
FROM test_to_time_trim
WHERE name = 'i_nbsp_u00a0'

query expect_error(cannot be parsed to a TIME value)
SELECT to_time(concat('12:30:45', pad))
FROM test_to_time_trim
WHERE name = 'j_ideographic_u3000'

-- Only literal ASCII spaces are removed before AM/PM suffix detection.
query expect_error(cannot be parsed to a TIME value)
SELECT to_time(concat('1:00:00 PM', pad))
FROM test_to_time_trim
WHERE name = 'd_tab_0x09'

-- to_time with format pattern falls back to Spark (not supported natively)
query expect_fallback(invoke is not supported)
SELECT to_time('12:30:45', 'HH:mm:ss')

statement
CREATE TABLE test_to_time_col_fmt(s STRING, f STRING) USING parquet

statement
INSERT INTO test_to_time_col_fmt VALUES
  ('14.30.00', 'HH.mm.ss'),
  ('1230', 'HHmm')

-- A non-foldable format column should fall back to Spark because Comet does
-- not implement the format-pattern variant of to_time.
query expect_fallback(invoke is not supported)
SELECT to_time(s, f) FROM test_to_time_col_fmt
