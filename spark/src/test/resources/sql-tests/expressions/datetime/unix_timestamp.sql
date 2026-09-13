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

-- Config: spark.sql.session.timeZone=UTC

statement
CREATE TABLE test_unix_ts(ts timestamp) USING parquet

statement
INSERT INTO test_unix_ts VALUES (timestamp('1970-01-01 00:00:00')), (timestamp('2024-06-15 10:30:45')), (NULL)

query expect_native(unix_timestamp)
SELECT unix_timestamp(ts) FROM test_unix_ts

-- literal arguments
query ignore(https://github.com/apache/datafusion-comet/issues/3336)
SELECT unix_timestamp(timestamp('1970-01-01 00:00:00')), unix_timestamp(timestamp('2024-06-15 10:30:45'))

-- Native timestamp conversion truncates fractional seconds toward zero, including before epoch.
statement
CREATE TABLE test_unix_ts_fractional(ts timestamp, ntz timestamp_ntz) USING parquet

statement
INSERT INTO test_unix_ts_fractional VALUES
  (CAST('1969-12-31 23:59:58.500000' AS TIMESTAMP), CAST('1969-12-31 23:59:58.500000' AS TIMESTAMP_NTZ)),
  (CAST('1969-12-31 23:59:59.000000' AS TIMESTAMP), CAST('1969-12-31 23:59:59.000000' AS TIMESTAMP_NTZ)),
  (CAST('1969-12-31 23:59:59.999999' AS TIMESTAMP), CAST('1969-12-31 23:59:59.999999' AS TIMESTAMP_NTZ)),
  (CAST('1970-01-01 00:00:00.000000' AS TIMESTAMP), CAST('1970-01-01 00:00:00.000000' AS TIMESTAMP_NTZ)),
  (CAST('1970-01-01 00:00:00.000001' AS TIMESTAMP), CAST('1970-01-01 00:00:00.000001' AS TIMESTAMP_NTZ)),
  (CAST('1970-01-01 00:00:01.500000' AS TIMESTAMP), CAST('1970-01-01 00:00:01.500000' AS TIMESTAMP_NTZ)),
  (NULL, NULL)

query expect_native(unix_timestamp)
SELECT unix_timestamp(ts), unix_timestamp(ntz) FROM test_unix_ts_fractional

query expect_native(unix_timestamp)
SELECT unix_timestamp(ts), unix_timestamp(ntz) FROM test_unix_ts_fractional WHERE ts IS NOT NULL
