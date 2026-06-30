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

statement
CREATE TABLE test_unix_ts(ts timestamp) USING parquet

statement
INSERT INTO test_unix_ts VALUES (timestamp('1970-01-01 00:00:00')), (timestamp('2024-06-15 10:30:45')), (NULL)

query
SELECT unix_timestamp(ts) FROM test_unix_ts

-- literal arguments
query ignore(https://github.com/apache/datafusion-comet/issues/3336)
SELECT unix_timestamp(timestamp('1970-01-01 00:00:00')), unix_timestamp(timestamp('2024-06-15 10:30:45'))

-- String input is not supported by the native unix_timestamp, so the expression falls back to Spark.
statement
CREATE TABLE test_unix_ts_str(ts_str string) USING parquet

statement
INSERT INTO test_unix_ts_str VALUES ('2020-01-01 00:00:00'), ('2021-06-15 12:30:45'), ('2022-12-31 23:59:59'), (NULL)

query expect_fallback(unix_timestamp does not support input type: StringType)
SELECT ts_str, unix_timestamp(ts_str) FROM test_unix_ts_str ORDER BY ts_str

query expect_fallback(unix_timestamp does not support input type: StringType)
SELECT ts_str, unix_timestamp(ts_str, 'yyyy-MM-dd HH:mm:ss') FROM test_unix_ts_str ORDER BY ts_str
