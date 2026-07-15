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

-- CometFromUTCTimestamp mixes in CodegenDispatchFallback, so with allowIncompatible unset it
-- routes through the JVM codegen dispatcher (Spark's own doGenCode) and matches Spark exactly,
-- including the legacy timezone forms the native parser rejects (e.g. PST, GMT+1). Verified across
-- two session zones to confirm the resolved timeZoneId survives closure serialization.
-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles

statement
CREATE TABLE test_futc_dispatch(ts timestamp) USING parquet

statement
INSERT INTO test_futc_dispatch VALUES
  (timestamp('2015-07-24 00:00:00')),
  (timestamp('2015-01-24 00:00:00')),
  (timestamp('1969-12-31 23:59:59')),
  (NULL)

query
SELECT from_utc_timestamp(ts, 'America/Los_Angeles') FROM test_futc_dispatch

query
SELECT from_utc_timestamp(ts, '+02:00') FROM test_futc_dispatch

-- legacy forms the native parser rejects but the dispatcher handles
query
SELECT from_utc_timestamp(ts, 'PST') FROM test_futc_dispatch

query
SELECT from_utc_timestamp(timestamp('2017-07-14 02:40:00'), 'GMT+1'), from_utc_timestamp(timestamp('2017-07-14 02:40:00'), 'Etc/GMT-1')
