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

-- date_trunc truncates in the session timezone, but its result must keep the UTC label that
-- every other timestamp column has, so that it can be compared with them and mixed with them in
-- conditionals. Etc/UTC takes the native path by default. The other zones take it through
-- allowIncompatible, and have no DST transitions, which keeps these queries clear of
-- https://github.com/apache/datafusion-comet/issues/5633.
-- https://github.com/apache/datafusion-comet/issues/6330

-- Config: spark.comet.expression.TruncTimestamp.allowIncompatible=true
-- ConfigMatrix: spark.sql.session.timeZone=UTC,Etc/UTC,Asia/Tokyo,Asia/Kolkata

statement
CREATE TABLE test_trunc_ts_label(id int, ts timestamp, other timestamp) USING parquet

statement
INSERT INTO test_trunc_ts_label VALUES
  (0, TIMESTAMP'2024-01-15 18:30:45Z', TIMESTAMP'2024-01-15 18:00:00Z'),
  (1, TIMESTAMP'2024-06-30 23:30:00Z', TIMESTAMP'2024-06-01 00:00:00Z'),
  (2, TIMESTAMP'1969-12-31 23:59:59.999999Z', TIMESTAMP'1969-12-31 23:00:00Z'),
  (3, NULL, TIMESTAMP'2024-01-01 00:00:00Z')

-- comparisons with a timestamp column and a timestamp literal
query expect_native(date_trunc)
SELECT id, date_trunc('HOUR', ts) = other, date_trunc('DAY', ts) >= other FROM test_trunc_ts_label

query expect_native(date_trunc)
SELECT id FROM test_trunc_ts_label WHERE date_trunc('DAY', ts) >= TIMESTAMP'2024-06-01 00:00:00Z'

query expect_native(date_trunc)
SELECT id, date_trunc('HOUR', ts) BETWEEN other AND ts, date_trunc('HOUR', ts) <=> other FROM test_trunc_ts_label

query expect_native(date_trunc)
SELECT id, nullif(date_trunc('HOUR', ts), other) FROM test_trunc_ts_label

-- conditionals that mix the result with a timestamp column
query expect_native(date_trunc)
SELECT id, CASE WHEN id > 0 THEN date_trunc('HOUR', ts) ELSE other END FROM test_trunc_ts_label

query expect_native(date_trunc)
SELECT id, coalesce(date_trunc('HOUR', ts), other) FROM test_trunc_ts_label

-- a join condition
query expect_native(date_trunc)
SELECT a.id FROM test_trunc_ts_label a JOIN test_trunc_ts_label b ON a.id = b.id AND date_trunc('DAY', a.ts) <= b.other
