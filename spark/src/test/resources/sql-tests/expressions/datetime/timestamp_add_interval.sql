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

-- Days and months apply on local time, so America/Los_Angeles is pinned to cross DST.
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.comet.shuffle.mode=native

statement
CREATE TABLE test_timestamp_add_interval(
  ts timestamp,
  ts_ntz timestamp_ntz,
  d date,
  dd int,
  h int,
  mi int,
  s decimal(18, 6),
  k int) USING parquet

statement
INSERT INTO test_timestamp_add_interval VALUES
  (timestamp'2024-01-15 10:30:45.123456', timestamp_ntz'2024-01-15 10:30:45.123456',
   date'2024-01-15', 1, 2, 3, 4.5, 1),
  (timestamp'2024-01-31 23:00:00', timestamp_ntz'2024-01-31 23:00:00',
   date'2024-01-31', 1, 1, 0, 0, 1),
  (timestamp'2024-03-09 12:00:00', timestamp_ntz'2024-03-09 12:00:00',
   date'2024-03-09', 1, 0, 0, 0, 2),
  (timestamp'2024-03-09 12:00:00', timestamp_ntz'2024-03-09 12:00:00',
   date'2024-03-09', 0, 24, 0, 0, 2),
  (timestamp'2024-11-02 12:00:00', timestamp_ntz'2024-11-02 12:00:00',
   date'2024-11-02', 1, 0, 0, 0, 3),
  (timestamp'2024-11-02 12:00:00', timestamp_ntz'2024-11-02 12:00:00',
   date'2024-11-02', 0, 24, 0, 0, 3),
  (timestamp'2024-12-31 23:59:59.999999', timestamp_ntz'2024-12-31 23:59:59.999999',
   date'2024-12-31', 0, 0, 0, 0.000001, 4),
  (timestamp'1970-01-01 00:00:00', timestamp_ntz'1970-01-01 00:00:00',
   date'1970-01-01', -1, -1, -1, -1.5, 4),
  (timestamp'2024-06-15 08:00:00', timestamp_ntz'2024-06-15 08:00:00',
   date'2024-06-15', NULL, 1, 1, 1, 5),
  (timestamp'2024-06-15 08:00:00', timestamp_ntz'2024-06-15 08:00:00',
   date'2024-06-15', 1, 1, 1, NULL, 5),
  (NULL, NULL, NULL, 1, 1, 1, 1, 6)

-- the root is TimeAdd before Spark 4.1 and TimestampAddInterval from 4.1, so the sentinel pins
-- the interval builder, which is dispatched only inside the same kernel
query expect_dispatch(make_dt_interval)
SELECT ts, dd, h, mi, s, ts + make_dt_interval(dd, h, mi, s), make_dt_interval(dd, h, mi, s) + ts
FROM test_timestamp_add_interval

query
SELECT ts + make_interval(0, 1, 0, dd, h, mi, s), make_interval(0, 1, 0, dd, h, mi, s) + ts
FROM test_timestamp_add_interval

query
SELECT ts_ntz + make_dt_interval(dd, h, mi, s), ts_ntz + make_interval(0, 1, 0, dd, h, mi, s)
FROM test_timestamp_add_interval

query
SELECT d + make_dt_interval(dd, h, mi, s), d + INTERVAL '1 12:00:00' DAY TO SECOND
FROM test_timestamp_add_interval

-- make_interval, as the parser rejects literals mixing year-month and day-time units
query
SELECT
  ts + INTERVAL '1' DAY,
  ts + INTERVAL '36' HOUR,
  ts + INTERVAL '1 02:30:00.5' DAY TO SECOND,
  ts + INTERVAL '1 day 2 hours',
  ts + make_interval(0, 1, 0, 1, 2),
  ts - INTERVAL '1' DAY,
  ts - make_interval(0, 1, 0, 1, 2),
  ts_ntz + INTERVAL '1' DAY,
  ts_ntz - make_interval(0, 1, 0, 1)
FROM test_timestamp_add_interval

query
SELECT
  timestamp'2024-03-09 12:00:00' + INTERVAL '1' DAY,
  timestamp'2024-03-09 12:00:00' + INTERVAL '24' HOUR,
  timestamp'2024-01-31 23:00:00' + make_interval(0, 1, 0, 1),
  timestamp_ntz'2024-03-09 12:00:00' + INTERVAL '1' DAY

query
SELECT k, ts + make_dt_interval(dd, h, mi, s) AS r, ts_ntz + make_interval(0, 1, 0, dd) AS r_ntz
FROM test_timestamp_add_interval
DISTRIBUTE BY k
