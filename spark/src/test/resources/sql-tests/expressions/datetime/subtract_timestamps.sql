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

-- Legacy mode falls back and is covered by subtract_timestamps_legacy.sql.
-- America/Los_Angeles is pinned so the DST rows straddle real transitions.
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.comet.shuffle.mode=native
-- Config: spark.sql.legacy.interval.enabled=false

statement
CREATE TABLE test_subtract_timestamps(
  ts1 timestamp,
  ts2 timestamp,
  ntz1 timestamp_ntz,
  ntz2 timestamp_ntz,
  d date,
  k int) USING parquet

-- rows 3 and 4 span the spring-forward and fall-back transitions
statement
INSERT INTO test_subtract_timestamps VALUES
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

-- across a DST transition the result is one calendar day, not the elapsed 23 or 25 hours
query expect_dispatch(subtracttimestamps)
SELECT ts1, ts2, ts1 - ts2, ts2 - ts1 FROM test_subtract_timestamps

-- compared with a literal of the same interval type, so no cast is added; the fall-back row
-- elapses 25 hours but equals one day, so it drops out
query expect_dispatch(subtracttimestamps)
SELECT ts1, ts2 FROM test_subtract_timestamps
WHERE ts1 - ts2 > INTERVAL '1 00:00:00' DAY TO SECOND

-- across DST, 23 hours from noon lands on 12:00 or 10:00 while one day keeps noon
query expect_native(hour, cast)
SELECT
  hour(ts2 + INTERVAL '23' HOUR),
  hour(ts2 + INTERVAL '1' DAY),
  CAST(ts1 + INTERVAL '12' HOUR AS DATE)
FROM test_subtract_timestamps

-- spark_answer_only: native shuffle cannot hash-partition on an interval key
query spark_answer_only
SELECT ts1 - ts2 AS i, count(*) FROM test_subtract_timestamps GROUP BY ts1 - ts2

-- here, not in subtract_dates.sql, which also runs legacy mode: Spark 3.x rejects grouping on
-- CalendarIntervalType
query spark_answer_only
SELECT d - date'2024-01-01' AS i, count(*) FROM test_subtract_timestamps
GROUP BY d - date'2024-01-01'

query
SELECT ntz1 - ntz2, ntz2 - ntz1 FROM test_subtract_timestamps

query
SELECT ts1 - d, d - ts1 FROM test_subtract_timestamps

query
SELECT
  ts1 - timestamp'2024-01-01 00:00:00',
  timestamp'2024-01-01 00:00:00' - ts2,
  ntz1 - timestamp_ntz'2024-01-01 00:00:00'
FROM test_subtract_timestamps

-- no NULL literal: NullPropagation folds it before Comet sees the plan
query
SELECT
  timestamp'2024-03-15 10:30:45.123456' - timestamp'2024-01-01 00:00:00',
  timestamp'2024-01-01 00:00:00' - timestamp'2024-03-15 10:30:45.123456',
  timestamp_ntz'2024-03-10 12:00:00' - timestamp_ntz'2024-03-09 12:00:00'

query
SELECT k, ts1 - ts2 AS i, named_struct('i', ntz1 - ntz2) AS s
FROM test_subtract_timestamps
DISTRIBUTE BY k
