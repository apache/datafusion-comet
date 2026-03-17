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

-- ConfigMatrix: parquet.enable.dictionary=false,true

-- Test casting string to timestamp_ntz
-- https://github.com/apache/datafusion-comet/issues/3179

statement
CREATE TABLE test_cast_ts_ntz(s string) USING parquet

statement
INSERT INTO test_cast_ts_ntz VALUES ('2020-01-01T12:34:56.123456'), ('2020-01-01'), ('2020-01-01T12:34:56'), ('2020'), ('2020-01'), (NULL), ('not_a_timestamp'), ('2020-01-01T12:34:56+05:00')

-- Cast string to timestamp_ntz: valid formats should parse, invalid should be null
query
SELECT s, cast(s AS timestamp_ntz) FROM test_cast_ts_ntz

-- Verify that timestamp_ntz values are not affected by session timezone
query
SELECT s, cast(s AS timestamp_ntz) FROM test_cast_ts_ntz WHERE s = '2020-01-01T12:34:56.123456'

-- Compare timestamp_ntz vs timestamp (with timezone) to show they differ
query
SELECT s, cast(s AS timestamp_ntz) as ts_ntz, cast(s AS timestamp) as ts FROM test_cast_ts_ntz WHERE s IS NOT NULL AND s != 'not_a_timestamp' AND s != '2020-01-01T12:34:56+05:00'
