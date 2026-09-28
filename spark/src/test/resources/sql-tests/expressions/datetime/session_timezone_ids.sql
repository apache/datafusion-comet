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

-- Spark accepts session timezone IDs that native code cannot parse as written: `Z`, offsets
-- such as `+8`, `-08` and `+08:00:00`, prefixed offsets such as `GMT+8`, and short IDs such as
-- `PST`. Comet normalizes them before passing them to native code.
-- https://github.com/apache/datafusion-comet/issues/6329

-- ConfigMatrix: spark.sql.session.timeZone=GMT+8,UTC+08:00,+8,-08,+08:00:00,Z,PST,IST

statement
CREATE TABLE test_session_tz_ids(ts timestamp, ntz timestamp_ntz, d date, s string) USING parquet

statement
INSERT INTO test_session_tz_ids VALUES
  (TIMESTAMP'2024-01-15 18:30:45Z', TIMESTAMP_NTZ'2024-01-15 18:30:45', DATE'2024-01-15', '2024-01-15 18:30:45'),
  (TIMESTAMP'2024-06-30 23:30:00Z', TIMESTAMP_NTZ'2024-06-30 23:30:00', DATE'2024-06-30', '2024-06-30 23:30:00'),
  (NULL, NULL, NULL, NULL)

query
SELECT CAST(ts AS STRING), CAST(ts AS DATE), hour(ts), minute(ts) FROM test_session_tz_ids

query
SELECT year(ts), month(ts), dayofmonth(ts) FROM test_session_tz_ids

query
SELECT CAST(s AS TIMESTAMP), CAST(d AS TIMESTAMP), unix_timestamp(d) FROM test_session_tz_ids

query
SELECT CAST(ntz AS TIMESTAMP), CAST(ts AS TIMESTAMP_NTZ) FROM test_session_tz_ids

query
SELECT date_trunc('HOUR', ts), date_format(ts, 'yyyy-MM-dd HH:mm') FROM test_session_tz_ids
