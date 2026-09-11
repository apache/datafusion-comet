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

-- timestamp + year-month interval resolves to TimestampAddYMInterval and runs through the
-- codegen dispatcher so results match Spark exactly. TIMESTAMP values are shifted on local
-- time in the session zone, so America/Los_Angeles is pinned and two rows cross a DST change.
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.comet.shuffle.mode=native

statement
CREATE TABLE test_timestamp_add_ym_interval(
  ts timestamp,
  ts_ntz timestamp_ntz,
  y int,
  m int,
  k int) USING parquet

-- rows 5 and 6 cross the spring-forward and fall-back transitions
statement
INSERT INTO test_timestamp_add_ym_interval VALUES
  (timestamp'2024-01-31 10:30:45.123456', timestamp_ntz'2024-01-31 10:30:45.123456', 0, 1, 1),
  (timestamp'2024-01-31 10:30:45.123456', timestamp_ntz'2024-01-31 10:30:45.123456', 0, 13, 1),
  (timestamp'2024-02-29 23:59:59', timestamp_ntz'2024-02-29 23:59:59', 1, 0, 2),
  (timestamp'2024-03-31 00:00:00', timestamp_ntz'2024-03-31 00:00:00', 0, -1, 2),
  (timestamp'2024-02-10 12:00:00', timestamp_ntz'2024-02-10 12:00:00', 0, 1, 3),
  (timestamp'2024-10-15 12:00:00', timestamp_ntz'2024-10-15 12:00:00', 0, 1, 3),
  (timestamp'1970-01-01 00:00:00', timestamp_ntz'1970-01-01 00:00:00', -1, -1, 4),
  (timestamp'2024-06-15 08:00:00', timestamp_ntz'2024-06-15 08:00:00', NULL, 1, 5),
  (timestamp'2024-06-15 08:00:00', timestamp_ntz'2024-06-15 08:00:00', 1, NULL, 5),
  (NULL, NULL, 1, 1, 6)

-- TIMESTAMP column plus an interval built from columns, both directions
query
SELECT ts, y, m, ts + make_ym_interval(y, m), make_ym_interval(y, m) + ts
FROM test_timestamp_add_ym_interval

-- TIMESTAMP_NTZ compiles a separate kernel and never sees the session time zone
query
SELECT ts_ntz, ts_ntz + make_ym_interval(y, m), make_ym_interval(y, m) + ts_ntz
FROM test_timestamp_add_ym_interval

-- interval literals in the unit, unit-to-unit and multi-unit spellings. Subtraction rewrites
-- to an addition of the negated interval.
query
SELECT
  ts + INTERVAL '1' YEAR,
  ts + INTERVAL '1' MONTH,
  ts + INTERVAL '1-2' YEAR TO MONTH,
  ts + INTERVAL '1 year 1 month',
  ts - INTERVAL '1' MONTH,
  ts_ntz + INTERVAL '1' YEAR,
  ts_ntz - INTERVAL '1' MONTH
FROM test_timestamp_add_ym_interval

-- all-literal operands (constant folding is disabled by the test suite)
query
SELECT
  timestamp'2024-01-31 10:30:45.123456' + INTERVAL '1' MONTH,
  timestamp'2024-02-29 23:59:59' + INTERVAL '1' YEAR,
  timestamp_ntz'2024-01-31 10:30:45.123456' + INTERVAL '1' MONTH,
  timestamp'2024-03-31 00:00:00' - INTERVAL '1' MONTH,
  CAST(NULL AS TIMESTAMP) + INTERVAL '1' MONTH,
  timestamp'2024-01-31 00:00:00' + CAST(NULL AS INTERVAL YEAR TO MONTH)

-- timestamp output through native shuffle
query
SELECT k, ts + make_ym_interval(y, m) AS r, ts_ntz + make_ym_interval(y, m) AS r_ntz
FROM test_timestamp_add_ym_interval
DISTRIBUTE BY k
