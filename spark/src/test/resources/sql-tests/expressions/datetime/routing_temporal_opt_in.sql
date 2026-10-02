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

-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.comet.expression.FromUTCTimestamp.allowIncompatible=true
-- Config: spark.comet.expression.ToUTCTimestamp.allowIncompatible=true
-- Config: spark.comet.expression.FromUnixTime.allowIncompatible=true
-- Config: spark.comet.expression.TruncDate.allowIncompatible=true
-- Config: spark.comet.expression.TruncTimestamp.allowIncompatible=true

statement
CREATE TABLE routing_temporal(ts TIMESTAMP, d DATE, fmt STRING, seconds BIGINT) USING parquet

statement
INSERT INTO routing_temporal VALUES (TIMESTAMP '2024-06-15 12:34:56', DATE '2024-06-15', 'YEAR', 0), (NULL, NULL, NULL, NULL)

query expect_native(trunc)
SELECT trunc(d, 'YEAR') FROM routing_temporal

query expect_native(date_trunc)
SELECT date_trunc('YEAR', ts) FROM routing_temporal

query expect_native(date_format)
SELECT date_format(ts, 'yyyy-MM-dd') FROM routing_temporal

query expect_native(from_utc_timestamp)
SELECT from_utc_timestamp(ts, 'UTC') FROM routing_temporal

query expect_native(to_utc_timestamp)
SELECT to_utc_timestamp(ts, 'UTC') FROM routing_temporal

query expect_native(from_unixtime)
SELECT from_unixtime(seconds) FROM routing_temporal

query expect_fallback(from_unixtime: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT from_unixtime(seconds, 'yyyy') FROM routing_temporal

-- Use non-null formats for native opt-in; the dispatcher cases retain null-format coverage.
query expect_native(trunc)
SELECT trunc(d, fmt) FROM routing_temporal WHERE fmt IS NOT NULL

query expect_fallback(trunc: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT trunc(d, 'day') FROM routing_temporal

query expect_native(date_trunc)
SELECT date_trunc(fmt, ts) FROM routing_temporal WHERE fmt IS NOT NULL

query expect_fallback(date_trunc: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT date_trunc('not_a_unit', ts) FROM routing_temporal

query expect_fallback(date_format: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT date_format(ts, 'yyyy-MM-dd EEEE') FROM routing_temporal
