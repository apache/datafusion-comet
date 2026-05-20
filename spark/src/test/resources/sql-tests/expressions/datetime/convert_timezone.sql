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

-- Config: spark.comet.expression.ConvertTimezone.allowIncompatible=true
-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles

statement
CREATE TABLE test_convert_timezone(ts timestamp_ntz, src string, tgt string) USING parquet

statement
INSERT INTO test_convert_timezone VALUES
  (timestamp_ntz'2021-12-06 08:00:00', 'UTC', 'America/Los_Angeles'),
  (timestamp_ntz'2021-07-01 12:00:00', 'America/New_York', 'Asia/Tokyo'),
  (timestamp_ntz'2023-01-15 09:30:00', 'America/Los_Angeles', 'UTC'),
  (NULL, 'UTC', 'Asia/Tokyo'),
  (timestamp_ntz'2021-12-06 08:00:00', NULL, 'Asia/Tokyo'),
  (timestamp_ntz'2021-12-06 08:00:00', 'UTC', NULL)

query
SELECT convert_timezone('UTC', 'America/Los_Angeles', timestamp_ntz'2021-12-06 08:00:00')

query
SELECT convert_timezone('Asia/Tokyo', 'Europe/Berlin', timestamp_ntz'2021-12-06 12:00:00')

query
SELECT convert_timezone('America/Los_Angeles', 'Asia/Tokyo', timestamp_ntz'2023-01-15 20:00:00')

query
SELECT convert_timezone(CAST(NULL AS STRING), 'Asia/Tokyo', timestamp_ntz'2021-12-06 08:00:00')

query
SELECT convert_timezone('UTC', CAST(NULL AS STRING), timestamp_ntz'2021-12-06 08:00:00')

query
SELECT convert_timezone(src, tgt, ts) FROM test_convert_timezone
