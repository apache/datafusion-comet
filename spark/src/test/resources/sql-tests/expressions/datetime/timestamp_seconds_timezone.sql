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

-- timestamp_seconds returns TimestampType. These queries use the result instead of only
-- projecting it. Expressions that depend on the session timezone must see an instant rather
-- than TIMESTAMP_NTZ wall-clock time, and comparisons, CASE and coalesce need the result to have
-- the same Arrow type as other timestamp columns.
-- https://github.com/apache/datafusion-comet/issues/6328

-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles,Asia/Kolkata

statement
CREATE TABLE test_ts_seconds_tz(s bigint, i int, d double, ts timestamp) USING parquet

-- ts equals timestamp_seconds(s) except in the row for 2024-03-10, where it is an hour later
statement
INSERT INTO test_ts_seconds_tz VALUES
  (0, 0, 0.5, TIMESTAMP'1970-01-01 00:00:00Z'),
  (1640995200, 1640995200, 1640995200.25, TIMESTAMP'2022-01-01 00:00:00Z'),
  (-86400, -86400, -86400.5, TIMESTAMP'1969-12-31 00:00:00Z'),
  (1710063000, 1710063000, 1710063000.0, TIMESTAMP'2024-03-10 10:30:00Z'),
  (1730622600, 1730622600, 1730622600.0, TIMESTAMP'2024-11-03 08:30:00Z'),
  (1719790200, 1719790200, 1719790200.0, TIMESTAMP'2024-06-30 23:30:00Z'),
  (NULL, NULL, NULL, NULL)

-- fields and casts that depend on the session timezone
query expect_native(timestamp_seconds)
SELECT s, hour(timestamp_seconds(s)), minute(timestamp_seconds(s)) FROM test_ts_seconds_tz

query expect_native(timestamp_seconds)
SELECT s, CAST(timestamp_seconds(s) AS STRING), CAST(timestamp_seconds(s) AS DATE) FROM test_ts_seconds_tz

query expect_native(timestamp_seconds)
SELECT s, CAST(timestamp_seconds(s) AS TIMESTAMP_NTZ) FROM test_ts_seconds_tz

-- int and double inputs take their own native paths
query expect_native(timestamp_seconds)
SELECT i, hour(timestamp_seconds(i)), CAST(timestamp_seconds(i) AS STRING) FROM test_ts_seconds_tz

query expect_native(timestamp_seconds)
SELECT d, CAST(timestamp_seconds(d) AS STRING) FROM test_ts_seconds_tz

-- comparisons with a timestamp column and a timestamp literal
query expect_native(timestamp_seconds)
SELECT s, timestamp_seconds(s) = ts, timestamp_seconds(s) < ts FROM test_ts_seconds_tz

query expect_native(timestamp_seconds)
SELECT s FROM test_ts_seconds_tz WHERE timestamp_seconds(s) >= TIMESTAMP'2022-01-01 00:00:00Z'

-- conditionals that mix the result with a timestamp column
query expect_native(timestamp_seconds)
SELECT s, CASE WHEN s > 0 THEN timestamp_seconds(s) ELSE ts END FROM test_ts_seconds_tz

query expect_native(timestamp_seconds)
SELECT s, coalesce(timestamp_seconds(s), ts) FROM test_ts_seconds_tz

-- literal argument, which takes the scalar path because the suite disables constant folding
query expect_native(timestamp_seconds)
SELECT s, CAST(timestamp_seconds(1800) AS STRING), timestamp_seconds(0) = ts FROM test_ts_seconds_tz
