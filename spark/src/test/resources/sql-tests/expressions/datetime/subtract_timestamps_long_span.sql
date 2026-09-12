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

-- timestamp - timestamp over a span past about 292 years. In default mode the result is a
-- DayTimeIntervalType that holds the microseconds in a long, so the span fits and the expression
-- stays on the codegen dispatcher. UTC is pinned so the elapsed microseconds are exact.
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.legacy.interval.enabled=false

statement
CREATE TABLE test_subtract_timestamps_long_span(
  ts1 timestamp,
  ts2 timestamp,
  ntz1 timestamp_ntz,
  ntz2 timestamp_ntz) USING parquet

-- 2300-01-01 - 1970-01-01 is 10413792000000000 microseconds, about 330 years
statement
INSERT INTO test_subtract_timestamps_long_span VALUES
  (timestamp'2300-01-01 00:00:00', timestamp'1970-01-01 00:00:00',
   timestamp_ntz'2300-01-01 00:00:00', timestamp_ntz'1970-01-01 00:00:00'),
  (timestamp'1970-01-01 00:00:00', timestamp'2300-01-01 00:00:00',
   timestamp_ntz'1970-01-01 00:00:00', timestamp_ntz'2300-01-01 00:00:00')

-- TIMESTAMP columns in both operand orders
query
SELECT ts1, ts2, ts1 - ts2, ts2 - ts1 FROM test_subtract_timestamps_long_span

-- TIMESTAMP_NTZ columns in both operand orders
query
SELECT ntz1 - ntz2, ntz2 - ntz1 FROM test_subtract_timestamps_long_span
