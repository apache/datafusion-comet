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

-- timestamp - timestamp in legacy interval mode resolves to SubtractTimestamps with a
-- CalendarIntervalType result holding the elapsed microseconds. The codegen dispatcher's
-- calendar-interval output cannot carry that span past about 292 years, so the expression falls
-- back to Spark. America/Los_Angeles is pinned so the DST rows below straddle real transitions.
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.legacy.interval.enabled=true

statement
CREATE TABLE test_subtract_timestamps_legacy(
  ts1 timestamp,
  ts2 timestamp,
  ntz1 timestamp_ntz,
  ntz2 timestamp_ntz,
  d date,
  k int) USING parquet

-- rows 3 and 4 span the spring-forward and fall-back transitions
statement
INSERT INTO test_subtract_timestamps_legacy VALUES
  (timestamp'2024-03-15 10:30:45.123456', timestamp'2024-01-01 00:00:00',
   timestamp_ntz'2024-03-15 10:30:45.123456', timestamp_ntz'2024-01-01 00:00:00',
   date'2024-01-01', 1),
  (timestamp'2024-01-01 00:00:00', timestamp'2024-03-15 10:30:45.123456',
   timestamp_ntz'2024-01-01 00:00:00', timestamp_ntz'2024-03-15 10:30:45.123456',
   date'2024-06-30', 1),
  (timestamp'2024-03-10 12:00:00', timestamp'2024-03-09 12:00:00',
   timestamp_ntz'2024-03-10 12:00:00', timestamp_ntz'2024-03-09 12:00:00',
   date'2024-03-09', 2),
  (timestamp'2024-11-03 12:00:00', timestamp'2024-11-02 12:00:00',
   timestamp_ntz'2024-11-03 12:00:00', timestamp_ntz'2024-11-02 12:00:00',
   date'2024-11-02', 2),
  (timestamp'1969-12-31 23:59:59.999999', timestamp'1970-01-01 00:00:00',
   timestamp_ntz'1969-12-31 23:59:59.999999', timestamp_ntz'1970-01-01 00:00:00',
   date'1970-01-01', 3),
  (timestamp'2024-06-01 08:00:00', timestamp'2024-06-01 08:00:00',
   timestamp_ntz'2024-06-01 08:00:00', timestamp_ntz'2024-06-01 08:00:00',
   date'2024-06-01', 3),
  (NULL, timestamp'2024-01-01 00:00:00', NULL, timestamp_ntz'2024-01-01 00:00:00',
   date'2024-01-01', 4),
  (timestamp'2024-01-01 00:00:00', NULL, timestamp_ntz'2024-01-01 00:00:00', NULL, NULL, 4),
  (NULL, NULL, NULL, NULL, NULL, 5)

-- TIMESTAMP columns in both directions. Across a DST transition legacy mode reports the elapsed
-- 23 or 25 hours rather than one calendar day.
query expect_fallback(cannot carry a span past about 292 years)
SELECT ts1, ts2, ts1 - ts2, ts2 - ts1 FROM test_subtract_timestamps_legacy

-- TIMESTAMP_NTZ columns never see the session time zone
query expect_fallback(cannot carry a span past about 292 years)
SELECT ntz1 - ntz2, ntz2 - ntz1 FROM test_subtract_timestamps_legacy

-- a DATE operand is implicitly cast to TIMESTAMP
query expect_fallback(cannot carry a span past about 292 years)
SELECT ts1 - d, d - ts1 FROM test_subtract_timestamps_legacy

-- literal on either side
query expect_fallback(cannot carry a span past about 292 years)
SELECT
  ts1 - timestamp'2024-01-01 00:00:00',
  timestamp'2024-01-01 00:00:00' - ts2,
  ntz1 - timestamp_ntz'2024-01-01 00:00:00'
FROM test_subtract_timestamps_legacy

-- all-literal operands (constant folding is disabled by the test suite)
query expect_fallback(cannot carry a span past about 292 years)
SELECT
  timestamp'2024-03-15 10:30:45.123456' - timestamp'2024-01-01 00:00:00',
  timestamp'2024-01-01 00:00:00' - timestamp'2024-03-15 10:30:45.123456',
  timestamp_ntz'2024-03-10 12:00:00' - timestamp_ntz'2024-03-09 12:00:00'
