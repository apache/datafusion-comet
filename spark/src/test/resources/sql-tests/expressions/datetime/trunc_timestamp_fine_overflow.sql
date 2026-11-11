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

-- Before 4.2, Spark wraps the Long subtraction when SECOND or MILLISECOND truncation falls below
-- Long.MinValue. Spark 4.2 and later (SPARK-56663) raise instead, which
-- trunc_timestamp_fine_overflow_spark42.sql covers.
-- MaxSparkVersion: 4.1

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

query expect_native(date_trunc)
SELECT date_trunc('SECOND', ts), date_trunc('MILLISECOND', ts)
FROM test_trunc_ts_fine_overflow

statement
CREATE TABLE test_trunc_ts_fine_overflow_fmt(ts timestamp, fmt string) USING parquet

statement
INSERT INTO test_trunc_ts_fine_overflow_fmt VALUES
  (timestamp_micros(-9223372036854775808), 'SECOND'),
  (timestamp_micros(-9223372036854775808), 'MILLISECOND'),
  (timestamp('2024-05-17 12:34:56.123456'), 'SECOND')

-- The same truncation with the format in a column.
query expect_native(date_trunc)
SELECT fmt, date_trunc(fmt, ts) FROM test_trunc_ts_fine_overflow_fmt ORDER BY fmt, ts
