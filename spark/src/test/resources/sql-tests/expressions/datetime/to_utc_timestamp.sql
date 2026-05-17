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

-- The result of to_utc_timestamp is a shift of the underlying microsecond
-- value, so it must not depend on the session timezone. Verify across two
-- representative session zones.
-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles

-- CometToUTCTimestamp is marked Incompatible because Comet's native timezone
-- parser does not accept Spark's legacy timezone forms; enable allowIncompatible
-- to force the native path for the timezones covered here.
-- Config: spark.comet.expression.ToUTCTimestamp.allowIncompatible=true

statement
CREATE TABLE test_to_utc_timestamp(ts timestamp, tz string) USING parquet

-- Includes a summer and a winter row for the LA timezone so that both DST
-- branches are exercised. The third row uses a fixed-offset tz to match the
-- form Spark's own DateFunctionsSuite covers via CEST = +02:00.
statement
INSERT INTO test_to_utc_timestamp VALUES
  (timestamp('2015-07-24 00:00:00'), 'America/Los_Angeles'),
  (timestamp('2015-01-24 00:00:00'), 'America/Los_Angeles'),
  (timestamp('2024-06-15 10:30:45'), '+02:00'),
  (timestamp('2024-01-01 00:00:00'), 'Asia/Seoul'),
  (timestamp('1969-12-31 23:59:59'), 'UTC'),
  (NULL, 'UTC'),
  (timestamp('2024-06-15 10:30:45'), NULL),
  (NULL, NULL)

-- column timestamp, literal IANA timezone
query
SELECT to_utc_timestamp(ts, 'America/Los_Angeles') FROM test_to_utc_timestamp

query
SELECT to_utc_timestamp(ts, 'Asia/Seoul') FROM test_to_utc_timestamp

query
SELECT to_utc_timestamp(ts, 'UTC') FROM test_to_utc_timestamp

-- column timestamp, literal fixed-offset timezone
query
SELECT to_utc_timestamp(ts, '+02:00') FROM test_to_utc_timestamp

-- column timestamp, column timezone (mix of IANA and fixed-offset values)
query
SELECT to_utc_timestamp(ts, tz) FROM test_to_utc_timestamp

-- literal arguments
query
SELECT to_utc_timestamp(timestamp('2017-07-14 02:40:00'), 'Etc/GMT-1')

query
SELECT to_utc_timestamp(timestamp('2016-08-31 00:00:00'), 'Asia/Seoul')

-- null handling
query
SELECT to_utc_timestamp(NULL, 'UTC'), to_utc_timestamp(timestamp('2024-01-01 00:00:00'), NULL), to_utc_timestamp(NULL, NULL)
