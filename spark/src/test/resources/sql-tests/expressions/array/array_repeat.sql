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

-- ============================================================
-- Mixed-column table covers per-type element with parameterized count.
-- ============================================================

statement
CREATE TABLE test_array_repeat(
  byte_v tinyint,
  short_v smallint,
  int_v int,
  long_v bigint,
  float_v float,
  double_v double,
  dec_v decimal(38, 0),
  bool_v boolean,
  str_v string,
  bin_v binary,
  date_v date,
  ts_v timestamp,
  arr_v array<int>,
  cnt int
) USING parquet

statement
INSERT INTO test_array_repeat VALUES
  (CAST(127 AS TINYINT), CAST(32767 AS SMALLINT), 2147483647, 9223372036854775807, CAST('Infinity' AS FLOAT), CAST('Infinity' AS DOUBLE), CAST(99999999999999999999999999999999999999 AS DECIMAL(38, 0)), true, 'a', X'01', DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00', array(1, 2, 3), 3),
  (CAST(-128 AS TINYINT), CAST(-32768 AS SMALLINT), -2147483648, -9223372036854775808, CAST('-Infinity' AS FLOAT), CAST('-Infinity' AS DOUBLE), CAST(-99999999999999999999999999999999999999 AS DECIMAL(38, 0)), false, 'é', X'02FF', DATE '9999-12-31', TIMESTAMP '9999-12-31 23:59:59', array(), 2),
  (CAST(0 AS TINYINT), CAST(0 AS SMALLINT), 0, 0, CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE), CAST(0 AS DECIMAL(38, 0)), false, '', X'', DATE '2024-02-29', TIMESTAMP '2024-02-29 12:34:56', array(NULL, 1, NULL), 1),
  (NULL, NULL, NULL, NULL, CAST(0.0 AS FLOAT), CAST(-0.0 AS DOUBLE), NULL, NULL, '日本', X'00', NULL, NULL, NULL, 0),
  (CAST(1 AS TINYINT), CAST(1 AS SMALLINT), 1, 1, CAST(-0.0 AS FLOAT), CAST(0.0 AS DOUBLE), CAST(1 AS DECIMAL(38, 0)), true, NULL, NULL, DATE '2000-01-01', TIMESTAMP '2000-01-01 00:00:00', array(7), -1),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)

-- ============================================================
-- Per-type column repeated with a column count
-- ============================================================

query
SELECT array_repeat(byte_v, cnt) FROM test_array_repeat

query
SELECT array_repeat(short_v, cnt) FROM test_array_repeat

query
SELECT array_repeat(int_v, cnt) FROM test_array_repeat

query
SELECT array_repeat(long_v, cnt) FROM test_array_repeat

query
SELECT array_repeat(float_v, cnt) FROM test_array_repeat

query
SELECT array_repeat(double_v, cnt) FROM test_array_repeat

query
SELECT array_repeat(dec_v, cnt) FROM test_array_repeat

query
SELECT array_repeat(bool_v, cnt) FROM test_array_repeat

query
SELECT array_repeat(str_v, cnt) FROM test_array_repeat

query
SELECT array_repeat(bin_v, cnt) FROM test_array_repeat

query
SELECT array_repeat(date_v, cnt) FROM test_array_repeat

query
SELECT array_repeat(ts_v, cnt) FROM test_array_repeat

query
SELECT array_repeat(arr_v, cnt) FROM test_array_repeat

-- ============================================================
-- column + literal counts: verify count edge cases
-- (NULL → row NULL, 0 / negative → empty array, large positive → array of N).
-- ============================================================

query
SELECT array_repeat(int_v, 3) FROM test_array_repeat

query
SELECT array_repeat(int_v, 0) FROM test_array_repeat

query
SELECT array_repeat(int_v, -5) FROM test_array_repeat

query
SELECT array_repeat(int_v, CAST(NULL AS INT)) FROM test_array_repeat

-- ============================================================
-- literal element + column count
-- ============================================================

query
SELECT array_repeat(42, cnt) FROM test_array_repeat

query
SELECT array_repeat('hello', cnt) FROM test_array_repeat

-- NULL element repeated count times — result is array of NULLs (NOT a NULL row)
query
SELECT array_repeat(CAST(NULL AS INT), cnt) FROM test_array_repeat

query
SELECT array_repeat(CAST(NULL AS STRING), cnt) FROM test_array_repeat
