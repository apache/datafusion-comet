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

-- Keep the wide-range fallback fixture independent of far-future JVM/native timezone rules.
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.sql.parquet.int96RebaseModeInWrite=CORRECTED
-- Config: spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED
-- Config: spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS
-- Dictionary-encoded timestamps reuse the scalar timestamp path for dictionary values.
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_trunc_ts(ts timestamp) USING parquet

statement
INSERT INTO test_trunc_ts VALUES
  (timestamp('2024-05-17 12:34:56.123456')),
  (timestamp('2024-02-29 23:59:59.999999')),
  (timestamp('2000-02-29 00:00:00')),
  (timestamp('1900-02-28 00:00:00')),
  (timestamp('1969-12-31 23:59:59.123456')),
  -- Below the nanosecond lower bound and inside its 370-day truncation margin.
  (timestamp('1500-06-15 12:34:56.123456')),
  (timestamp('1678-06-01 12:34:56.123456')),
  -- Valid Spark timestamp outside TimestampNanosecond's range.
  (timestamp('3333-05-17 12:34:56.123456')),
  (NULL)

query
SELECT ts, date_trunc('YEAR', ts), date_trunc('YYYY', ts), date_trunc('YY', ts) FROM test_trunc_ts ORDER BY ts

query
SELECT ts, date_trunc('QUARTER', ts) FROM test_trunc_ts ORDER BY ts

query
SELECT ts, date_trunc('MONTH', ts), date_trunc('MON', ts), date_trunc('MM', ts) FROM test_trunc_ts ORDER BY ts

query
SELECT ts, date_trunc('WEEK', ts), date_trunc('DAY', ts), date_trunc('DD', ts) FROM test_trunc_ts ORDER BY ts

query
SELECT
  ts,
  date_trunc('HOUR', ts),
  date_trunc('MINUTE', ts),
  date_trunc('SECOND', ts),
  date_trunc('MILLISECOND', ts),
  date_trunc('MICROSECOND', ts)
FROM test_trunc_ts
ORDER BY ts

query
SELECT
  ts,
  date_trunc('year', ts),
  date_trunc('Year', ts),
  date_trunc('yEaR', ts),
  date_trunc('month', ts),
  date_trunc('Mon', ts),
  date_trunc('week', ts)
FROM test_trunc_ts
ORDER BY ts

-- NULL format is Incompatible on the native path. Without allowIncompatible the
-- codegen dispatcher runs Spark's TruncTimestamp and returns NULL.
query
SELECT ts, date_trunc(NULL, ts) FROM test_trunc_ts ORDER BY ts

query
SELECT date_trunc('YEAR', NULL), date_trunc(NULL, NULL)

-- Literal arguments exercise the scalar timestamp input branch.
query
SELECT
  date_trunc('YEAR', TIMESTAMP '2024-05-17 12:34:56.123456'),
  date_trunc('QUARTER', TIMESTAMP '2024-05-17 12:34:56.123456'),
  date_trunc('MONTH', TIMESTAMP '2024-05-17 12:34:56.123456'),
  date_trunc('WEEK', TIMESTAMP '2024-05-17 12:34:56.123456'),
  date_trunc('DAY', TIMESTAMP '2024-05-17 12:34:56.123456'),
  date_trunc('HOUR', TIMESTAMP '2024-05-17 12:34:56.123456'),
  date_trunc('MINUTE', TIMESTAMP '2024-05-17 12:34:56.123456'),
  date_trunc('SECOND', TIMESTAMP '2024-05-17 12:34:56.123456'),
  date_trunc('MILLISECOND', TIMESTAMP '2024-05-17 12:34:56.123456'),
  date_trunc('MICROSECOND', TIMESTAMP '2024-05-17 12:34:56.123456')

-- Literal arguments exercise the scalar timestamp input branch.
query
SELECT
  date_trunc('YEAR', TIMESTAMP '3333-05-17 12:34:56.123456'),
  date_trunc('QUARTER', TIMESTAMP '3333-05-17 12:34:56.123456'),
  date_trunc('MONTH', TIMESTAMP '3333-05-17 12:34:56.123456'),
  date_trunc('WEEK', TIMESTAMP '3333-05-17 12:34:56.123456'),
  date_trunc('DAY', TIMESTAMP '3333-05-17 12:34:56.123456'),
  date_trunc('HOUR', TIMESTAMP '3333-05-17 12:34:56.123456'),
  date_trunc('MINUTE', TIMESTAMP '3333-05-17 12:34:56.123456'),
  date_trunc('SECOND', TIMESTAMP '3333-05-17 12:34:56.123456'),
  date_trunc('MILLISECOND', TIMESTAMP '3333-05-17 12:34:56.123456'),
  date_trunc('MICROSECOND', TIMESTAMP '3333-05-17 12:34:56.123456')

-- Long.MaxValue is used as an end-of-time sentinel and exceeds chrono's range.
-- Use microsecond storage to preserve the extreme value without an INT96 conversion.
-- Direct date_trunc projections must execute natively and match Spark exactly.
-- Wrapping them in unix_micros would dispatch the whole subtree to the JVM.
statement
CREATE TABLE test_trunc_ts_extreme(ts timestamp) USING parquet

statement
INSERT INTO test_trunc_ts_extreme VALUES
  (timestamp_micros(9223372036854775807)),
  (timestamp_micros(-9000000000000000000)),
  (timestamp('2024-05-17 12:34:56.123456')),
  (timestamp('3333-05-17 12:34:56.123456')),
  (NULL)

query expect_native(date_trunc)
SELECT
  date_trunc('YEAR', ts),
  date_trunc('QUARTER', ts),
  date_trunc('MONTH', ts),
  date_trunc('WEEK', ts)
FROM test_trunc_ts_extreme
ORDER BY ts

-- A valid microsecond input can truncate below Long.MinValue. Spark raises an error.
statement
CREATE TABLE test_trunc_ts_overflow(ts timestamp) USING parquet

statement
INSERT INTO test_trunc_ts_overflow VALUES (timestamp_micros(-9223372036854775808))

query expect_error(long overflow)
SELECT date_trunc('YEAR', ts) FROM test_trunc_ts_overflow

query expect_error(long overflow)
SELECT date_trunc('QUARTER', ts) FROM test_trunc_ts_overflow

query expect_error(long overflow)
SELECT date_trunc('MONTH', ts) FROM test_trunc_ts_overflow

query expect_error(long overflow)
SELECT date_trunc('WEEK', ts) FROM test_trunc_ts_overflow

query expect_error(long overflow)
SELECT date_trunc('MINUTE', ts) FROM test_trunc_ts_overflow

query expect_error(long overflow)
SELECT date_trunc('HOUR', ts) FROM test_trunc_ts_overflow

query expect_error(long overflow)
SELECT date_trunc('DAY', ts) FROM test_trunc_ts_overflow

-- Spark intentionally wraps Long subtraction for SECOND/MILLISECOND at the lower bound.
query expect_native(date_trunc)
SELECT date_trunc('SECOND', ts), date_trunc('MILLISECOND', ts)
FROM test_trunc_ts_overflow
