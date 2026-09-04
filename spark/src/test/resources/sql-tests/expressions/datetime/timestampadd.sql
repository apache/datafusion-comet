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

-- timestampadd runs through the codegen dispatcher so results match Spark exactly.
-- America/Los_Angeles is pinned so the DST cases below straddle real transitions.
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_timestampadd(ts timestamp, ts_ntz timestamp_ntz, q int) USING parquet

statement
INSERT INTO test_timestampadd VALUES
  (timestamp'2024-01-15 10:30:45', timestamp_ntz'2024-01-15 10:30:45', 3),
  (timestamp'2024-01-31 23:00:00', timestamp_ntz'2024-01-31 23:00:00', 1),
  (timestamp'2024-02-29 12:00:00', timestamp_ntz'2024-02-29 12:00:00', 12),
  (timestamp'2024-12-31 23:59:59', timestamp_ntz'2024-12-31 23:59:59', 2),
  (timestamp'1970-01-01 00:00:00', timestamp_ntz'1970-01-01 00:00:00', -5),
  (NULL, NULL, 1),
  (timestamp'2024-06-15 00:00:00', timestamp_ntz'2024-06-15 00:00:00', NULL)

-- Fully read NTZ Parquet input before testing native kernels.
statement
CACHE TABLE test_timestampadd

-- column quantity across a range of units, including month-end and leap-day rollover
query
SELECT timestampadd(HOUR, q, ts) FROM test_timestampadd

query
SELECT timestampadd(MONTH, q, ts) FROM test_timestampadd

-- every unit accepted by DateTimeUtils.timestampAdd. DAYOFYEAR shares a case arm with DAY,
-- so it is covered here to prove the alias both parses and dispatches.
query
SELECT
  timestampadd(YEAR, 1, ts),
  timestampadd(QUARTER, 1, ts),
  timestampadd(WEEK, 2, ts),
  timestampadd(DAY, -10, ts),
  timestampadd(DAYOFYEAR, -10, ts),
  timestampadd(MINUTE, 90, ts),
  timestampadd(SECOND, 30, ts),
  timestampadd(MILLISECOND, 1500, ts),
  timestampadd(MICROSECOND, 500, ts)
FROM test_timestampadd

-- TIMESTAMP_NTZ input. TimestampAdd.dataType is timestamp.dataType, so this yields an NTZ
-- output, which compiles a separate kernel and resolves zoneIdForType to UTC.
query
SELECT
  timestampadd(HOUR, q, ts_ntz),
  timestampadd(MONTH, q, ts_ntz),
  timestampadd(DAY, 1, ts_ntz),
  timestampadd(MICROSECOND, 500, ts_ntz)
FROM test_timestampadd

-- the grammar routes TIMESTAMPADD and DATEADD to the same TimestampAdd node whenever the
-- first argument is a datetimeUnit keyword, so the alias spelling must land on this serde
-- rather than on the two-argument DateAdd. DATE_ADD joined the rule in Spark 3.5 and is
-- covered by date_add_unit_alias.sql.
query
SELECT
  dateadd(DAY, q, ts),
  dateadd(MONTH, 1, ts),
  dateadd(HOUR, 6, ts)
FROM test_timestampadd

-- DST boundaries. timestampadd goes through timestampAddInterval, which does calendar
-- arithmetic on local time (.atZone(zoneId).plusDays(...)), so adding a day across the
-- spring-forward transition advances the local clock by one day rather than by 24 hours.
query
SELECT
  timestampadd(DAY, 1, timestamp'2024-03-09 12:00:00'),
  timestampadd(HOUR, 24, timestamp'2024-03-09 12:00:00'),
  timestampadd(DAY, 1, timestamp'2024-11-02 12:00:00'),
  timestampadd(HOUR, 24, timestamp'2024-11-02 12:00:00')

-- fall back: 2024-11-03 01:30 is an ambiguous local time
query
SELECT
  timestampadd(HOUR, 1, timestamp'2024-11-03 00:30:00'),
  timestampadd(HOUR, 1, timestamp'2024-11-03 01:30:00'),
  timestampadd(MINUTE, 90, timestamp'2024-11-03 00:45:00')

-- spring forward: 2024-03-10 02:30 is a nonexistent local time
query
SELECT
  timestampadd(HOUR, 1, timestamp'2024-03-10 01:30:00'),
  timestampadd(MINUTE, 45, timestamp'2024-03-10 01:30:00'),
  timestampadd(DAY, 1, timestamp'2024-03-09 02:30:00')

-- literal arguments (constant folding is disabled by the test suite)
query
SELECT
  timestampadd(HOUR, 3, timestamp'2024-01-01 10:00:00'),
  timestampadd(MONTH, 1, timestamp'2024-01-31 00:00:00'),
  timestampadd(YEAR, 1, timestamp'2024-02-29 00:00:00')

-- DateTimeUtils.timestampAdd maps ArithmeticException and DateTimeException to
-- timestampAddOverflowError, which must cross out of the generated kernel unchanged.
query expect_error(DATETIME_OVERFLOW)
SELECT timestampadd(YEAR, 1000000000, timestamp'2024-01-15 10:30:45')
