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

-- Run once per session timezone to exercise TZ-sensitive casts (NTZ↔Timestamp)
-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles,America/New_York,Asia/Kolkata

statement
CREATE TABLE test_ts_ntz(ts_ntz timestamp_ntz, ts timestamp, d date, id int) USING parquet

statement
INSERT INTO test_ts_ntz VALUES
  (TIMESTAMP_NTZ'2020-01-01 00:00:00', TIMESTAMP'2020-01-01 00:00:00 UTC', DATE'2020-01-01', 1),
  (TIMESTAMP_NTZ'2023-06-15 12:30:45.123456', TIMESTAMP'2023-06-15 12:30:45.123456 UTC', DATE'2023-06-15', 2),
  (TIMESTAMP_NTZ'1970-01-01 00:00:00', TIMESTAMP'1970-01-01 00:00:00 UTC', DATE'1970-01-01', 3),
  (TIMESTAMP_NTZ'2024-03-10 02:30:00', TIMESTAMP'2024-03-10 10:00:00 UTC', DATE'2024-03-10', 4),
  (TIMESTAMP_NTZ'2020-06-15 23:00:00', TIMESTAMP'2020-06-15 23:00:00 UTC', DATE'2020-06-15', 5),
  (NULL, NULL, NULL, 6)

-- NTZ → String (timezone-independent: formats local time as-is)
query
SELECT cast(ts_ntz as string), id FROM test_ts_ntz ORDER BY id

-- NTZ → Date (timezone-independent: extracts date treating NTZ value as UTC)
-- Row 5 (23:00) would produce 2020-06-16 in Kolkata (+5:30) if TZ were wrongly applied
query
SELECT cast(ts_ntz as date), id FROM test_ts_ntz ORDER BY id

-- NTZ → Timestamp (session-TZ dependent: interprets NTZ as local time, converts to UTC epoch)
query
SELECT cast(ts_ntz as timestamp), id FROM test_ts_ntz ORDER BY id

-- Date → NTZ (timezone-independent: pure days * 86400 * 1000000 arithmetic)
query
SELECT cast(d as timestamp_ntz), id FROM test_ts_ntz ORDER BY id

-- Timestamp → NTZ (session-TZ dependent: shifts UTC epoch to local time, stores as local epoch)
query
SELECT cast(ts as timestamp_ntz), id FROM test_ts_ntz ORDER BY id

-- Literal casts
query
SELECT cast(TIMESTAMP_NTZ'2020-01-01 12:34:56.789' as string)

query
SELECT cast(TIMESTAMP_NTZ'2020-01-01 12:34:56' as date)

query
SELECT cast(DATE'2020-01-15' as timestamp_ntz)
