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

-- Spark 4.2 and later (SPARK-56663) raise when SECOND or MILLISECOND truncation falls below
-- Long.MinValue, like every other unit. trunc_timestamp_fine_overflow.sql covers 4.1 and
-- earlier, where Spark wraps the Long subtraction instead.
-- MinSparkVersion: 4.2

-- Use microsecond storage to preserve the extreme value without an INT96 conversion.
-- Config: spark.sql.session.timeZone=UTC
-- A format column is Incompatible, because an invalid format throws instead of returning NULL.
-- Config: spark.comet.expression.TruncTimestamp.allowIncompatible=true
-- Config: spark.sql.parquet.int96RebaseModeInWrite=CORRECTED
-- Config: spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED
-- Config: spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_trunc_ts_fine_overflow(ts timestamp) USING parquet

-- Long.MinValue, and an ordinary value.
statement
INSERT INTO test_trunc_ts_fine_overflow VALUES
  (timestamp_micros(-9223372036854775808)),
  (timestamp('2024-05-17 12:34:56.123456'))

-- Shows date_trunc runs natively here, so the errors below come from Comet, not a fallback.
query expect_native(date_trunc)
SELECT date_trunc('SECOND', ts), date_trunc('MILLISECOND', ts)
FROM test_trunc_ts_fine_overflow
WHERE ts > timestamp_micros(-9223372036854775808)

query expect_error(long overflow)
SELECT date_trunc('SECOND', ts) FROM test_trunc_ts_fine_overflow

query expect_error(long overflow)
SELECT date_trunc('MILLISECOND', ts) FROM test_trunc_ts_fine_overflow

statement
CREATE TABLE test_trunc_ts_fine_overflow_fmt(ts timestamp, fmt string) USING parquet

statement
INSERT INTO test_trunc_ts_fine_overflow_fmt VALUES
  (timestamp_micros(-9223372036854775808), 'SECOND'),
  (timestamp_micros(-9223372036854775808), 'MILLISECOND'),
  (timestamp('2024-05-17 12:34:56.123456'), 'SECOND')

-- With the format in a column. The first query shows it runs natively.
query expect_native(date_trunc)
SELECT date_trunc(fmt, ts) FROM test_trunc_ts_fine_overflow_fmt
WHERE ts > timestamp_micros(-9223372036854775808)

query expect_error(long overflow)
SELECT date_trunc(fmt, ts) FROM test_trunc_ts_fine_overflow_fmt

query expect_error(long overflow)
SELECT date_trunc(fmt, ts) FROM test_trunc_ts_fine_overflow_fmt WHERE fmt = 'MILLISECOND'
