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

-- Config: spark.sql.legacy.timeParserPolicy=CORRECTED
-- ConfigMatrix: parquet.enable.dictionary=false,true
-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles

statement
CREATE TABLE test_unix_ts_strings(s string, fmt string) USING parquet

statement
INSERT INTO test_unix_ts_strings VALUES
  ('1970-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss'),
  ('1969-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss'),
  ('2024-02-29 12:30:45', 'yyyy-MM-dd HH:mm:ss'),
  ('2024-03-10 02:30:00', 'yyyy-MM-dd HH:mm:ss'),
  ('2024-11-03 01:30:00', 'yyyy-MM-dd HH:mm:ss'),
  ('1969-12-31 23:59:59.999999', 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
  ('2024/06/15', 'yyyy/MM/dd'),
  ('2024-06-15T10:30:45+05:30', "yyyy-MM-dd'T'HH:mm:ssXXX"),
  ('1582-10-04', 'yyyy-MM-dd'),
  ('0001-01-01', 'yyyy-MM-dd'),
  ('9999-12-31', 'yyyy-MM-dd'),
  ('not a date', 'yyyy-MM-dd'),
  ('2024-02-30', 'yyyy-MM-dd'),
  ('', 'yyyy-MM-dd'),
  (NULL, 'yyyy-MM-dd'),
  ('2024-06-15', NULL),
  ('2024-06-15', ''),
  (NULL, NULL)

-- Exercise both the cached literal formatter and the per-row formatter.
query
SELECT unix_timestamp(s), unix_timestamp(s, 'yyyy-MM-dd HH:mm:ss') FROM test_unix_ts_strings

query
SELECT unix_timestamp(s, fmt) FROM test_unix_ts_strings

query
SELECT unix_timestamp('2024-06-15', fmt) FROM test_unix_ts_strings

-- Constant folding is disabled by the SQL test harness.
query
SELECT unix_timestamp('2024-06-15', 'yyyy-MM-dd'), unix_timestamp(''), unix_timestamp(CAST(NULL AS STRING)), unix_timestamp('2024-06-15', CAST(NULL AS STRING))
