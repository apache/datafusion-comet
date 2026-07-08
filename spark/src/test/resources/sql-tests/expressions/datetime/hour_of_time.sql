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

-- MinSparkVersion: 4.1
-- Config: spark.sql.timeType.enabled=true
-- ConfigMatrix: spark.sql.session.timeZone=UTC,Asia/Kolkata

-- hour() with TIME input resolves to HoursOfTime, which expands to a
-- StaticInvoke(DateTimeUtils, "getHoursOfTime", TimeType). TIME cannot be stored in Parquet,
-- so column-input TIME values are synthesized via to_time() on a string column.
-- ConfigMatrix pins timezone-independence: TIME is a local time-of-day, so the extractor
-- must return the same value under any session timezone (Asia/Kolkata has a half-hour
-- offset that would surface any accidental zone conversion).

statement
CREATE TABLE test_hour_of_time(s STRING) USING parquet

statement
INSERT INTO test_hour_of_time VALUES
  ('00:00:00'),
  ('00:00:00.000001'),
  ('12:30:45'),
  ('12:30:45.123456'),
  ('23:59:59'),
  ('23:59:59.999999'),
  (NULL)

-- column-input TIME derived from to_time(string)
query
SELECT s, hour(to_time(s)) FROM test_hour_of_time ORDER BY s

-- literal TIME (constant-folded at analysis; still exercises the parser path)
query
SELECT hour(TIME '00:00:00')

query
SELECT hour(TIME '12:30:45.123456')

query
SELECT hour(TIME '23:59:59.999999')

-- explicit NULL cast
query
SELECT hour(CAST(NULL AS TIME))

-- HoursOfTime on lower-precision TimeType(0)
query
SELECT hour(CAST(TIME '13:00:00' AS TIME(0)))

-- EXTRACT(HOUR FROM ...) rewrites to HoursOfTime(source) via TimeExtract.parseExtractField,
-- so the shim must fire on this grammar too.
query
SELECT EXTRACT(HOUR FROM to_time(s)) FROM test_hour_of_time ORDER BY s
