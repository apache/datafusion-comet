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

-- CometConvertTimezone mixes in CodegenDispatchFallback, so with allowIncompatible unset it routes
-- through the JVM codegen dispatcher and matches Spark exactly, including the legacy timezone forms
-- the native parser rejects. Verified across two session zones.
-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles

statement
CREATE TABLE test_ct_dispatch(ts timestamp_ntz, src string, tgt string) USING parquet

statement
INSERT INTO test_ct_dispatch VALUES
  (timestamp_ntz'2021-12-06 08:00:00', 'UTC', 'America/Los_Angeles'),
  (timestamp_ntz'2021-07-01 12:00:00', 'America/New_York', 'Asia/Tokyo'),
  (NULL, 'UTC', 'Asia/Tokyo')

query
SELECT convert_timezone(src, tgt, ts) FROM test_ct_dispatch

query
SELECT convert_timezone('UTC', 'America/Los_Angeles', timestamp_ntz'2021-12-06 08:00:00')

-- legacy forms the native parser rejects but the dispatcher handles
query
SELECT convert_timezone('GMT+1', 'PST', timestamp_ntz'2021-12-06 08:00:00')
