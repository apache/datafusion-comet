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


-- date_trunc with the format in a column, around DST overlaps, gaps and historical offsets. Each
-- row is truncated with the rules a literal format applies (see trunc_timestamp_dst_ambiguous.sql
-- for what each zone covers), and a NULL format gives NULL. A non-literal format is Incompatible,
-- because an invalid format throws instead of returning NULL.

-- Config: spark.comet.expression.TruncTimestamp.allowIncompatible=true
-- Config: spark.sql.parquet.int96RebaseModeInWrite=CORRECTED
-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles,America/New_York,America/Sao_Paulo,Africa/Monrovia,Asia/Aden,America/Havana,America/Toronto,America/Asuncion

statement
CREATE TABLE test_trunc_fmt_ts(ts timestamp) USING parquet

statement
INSERT INTO test_trunc_fmt_ts VALUES
  (timestamp('1919-04-02T16:00:00Z')),
  (timestamp('1919-03-31T04:30:00Z')),
  (timestamp('1919-03-31T04:45:00Z')),
  (timestamp('2023-10-15T12:00:00Z')),
  (TIMESTAMP '1960-06-15 10:30:45'),
  (timestamp('1947-03-13T20:53:30.123Z')),
  (timestamp('1972-01-07T00:44:45Z')),
  (timestamp('2018-11-04T02:30:15.123456Z')),
  (timestamp('2018-11-04T03:30:15.123456Z')),
  (timestamp('2024-03-10T09:30:15.123456Z')),
  (timestamp('2024-03-10T10:30:15.123456Z')),
  (timestamp('2024-11-03T08:30:15.123456Z')),
  (timestamp('2024-11-03T09:30:15.123456Z')),
  (timestamp('2020-11-01T04:30:00Z')),
  (timestamp('2020-11-01T05:30:00Z')),
  (timestamp('2020-11-15T12:00:00Z')),
  (NULL)

statement
CREATE TABLE test_trunc_fmt_unit(fmt string) USING parquet

statement
INSERT INTO test_trunc_fmt_unit VALUES
  ('YEAR'), ('yyyy'), ('YY'), ('QUARTER'), ('MONTH'), ('mon'), ('MM'), ('WEEK'), ('DAY'), ('dd'),
  ('HOUR'), ('MINUTE'), ('SECOND'), ('MILLISECOND'), ('MICROSECOND'), (NULL)

statement
CREATE TABLE test_trunc_fmt(ts timestamp, fmt string) USING parquet

statement
INSERT INTO test_trunc_fmt SELECT ts, fmt FROM test_trunc_fmt_ts CROSS JOIN test_trunc_fmt_unit

query expect_native(date_trunc)
SELECT ts, fmt, date_trunc(fmt, ts) FROM test_trunc_fmt ORDER BY ts, fmt

-- A NULL literal format gives NULL.
query expect_native(date_trunc)
SELECT ts, date_trunc(NULL, ts) FROM test_trunc_fmt_ts ORDER BY ts

-- A literal timestamp with the format in a column. 2024-11-03 01:30 is in the US fall-back overlap.
query expect_native(date_trunc)
SELECT fmt, date_trunc(fmt, TIMESTAMP '2024-11-03 01:30:00') FROM test_trunc_fmt_unit ORDER BY fmt
