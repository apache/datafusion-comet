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

-- ConfigMatrix: spark.sql.session.timeZone=America/Los_Angeles,Australia/Lord_Howe,Asia/Kathmandu
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.sql.execution.reuseSubquery=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE clock_subq(id INT, ts TIMESTAMP, ntz TIMESTAMP_NTZ) USING parquet

statement
INSERT INTO clock_subq VALUES
  (1, TIMESTAMP '1969-12-31 23:59:59.123456', TIMESTAMP_NTZ '2024-02-29 12:34:56.654321'),
  (2, NULL, NULL),
  (3, NULL, NULL),
  (4, TIMESTAMP '2024-02-29 12:34:56.654321', TIMESTAMP_NTZ '1969-12-31 23:59:59.123456'),
  (5, NULL, NULL),
  (6, NULL, NULL),
  (7, NULL, NULL),
  (8, NULL, NULL)

-- Runtime Timestamp scalars use Spark in regional zones, even when an aggregate is empty.
-- hour: plain scalar and paired aggregate subqueries, including runtime NULL results.
query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT id, hour((SELECT max(ts) AS maximum FROM clock_subq))
FROM clock_subq

query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT id, hour((SELECT max(ts) AS maximum FROM clock_subq)),
       hour((SELECT min(ts) AS minimum FROM clock_subq))
FROM clock_subq

query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT id, hour((SELECT max(ts) AS maximum FROM clock_subq WHERE id < 0))
FROM clock_subq

query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT id, hour((SELECT max(ts) AS maximum FROM clock_subq WHERE id < 0)),
       hour((SELECT min(ts) AS minimum FROM clock_subq WHERE id < 0))
FROM clock_subq

-- minute: plain scalar and paired aggregate subqueries, including runtime NULL results.
query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT id, minute((SELECT max(ts) AS maximum FROM clock_subq))
FROM clock_subq

query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT id, minute((SELECT max(ts) AS maximum FROM clock_subq)),
       minute((SELECT min(ts) AS minimum FROM clock_subq))
FROM clock_subq

query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT id, minute((SELECT max(ts) AS maximum FROM clock_subq WHERE id < 0))
FROM clock_subq

query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT id, minute((SELECT max(ts) AS maximum FROM clock_subq WHERE id < 0)),
       minute((SELECT min(ts) AS minimum FROM clock_subq WHERE id < 0))
FROM clock_subq

-- second: plain scalar and paired aggregate subqueries, including runtime NULL results.
query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT id, second((SELECT max(ts) AS maximum FROM clock_subq))
FROM clock_subq

query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT id, second((SELECT max(ts) AS maximum FROM clock_subq)),
       second((SELECT min(ts) AS minimum FROM clock_subq))
FROM clock_subq

query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT id, second((SELECT max(ts) AS maximum FROM clock_subq WHERE id < 0))
FROM clock_subq

query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT id, second((SELECT max(ts) AS maximum FROM clock_subq WHERE id < 0)),
       second((SELECT min(ts) AS minimum FROM clock_subq WHERE id < 0))
FROM clock_subq

-- TimestampNTZ clocks have no session timezone conversion, including runtime NULLs.
query expect_native(hour,minute,second)
SELECT id, hour((SELECT max(ntz) AS maximum FROM clock_subq)),
       minute((SELECT max(ntz) AS maximum FROM clock_subq)),
       second((SELECT max(ntz) AS maximum FROM clock_subq))
FROM clock_subq

query expect_native(hour,minute,second)
SELECT id, hour((SELECT max(ntz) AS maximum FROM clock_subq)),
       hour((SELECT min(ntz) AS minimum FROM clock_subq)),
       minute((SELECT max(ntz) AS maximum FROM clock_subq)),
       minute((SELECT min(ntz) AS minimum FROM clock_subq)),
       second((SELECT max(ntz) AS maximum FROM clock_subq)),
       second((SELECT min(ntz) AS minimum FROM clock_subq))
FROM clock_subq

query expect_native(hour,minute,second)
SELECT id, hour((SELECT max(ntz) AS maximum FROM clock_subq WHERE id < 0)),
       minute((SELECT max(ntz) AS maximum FROM clock_subq WHERE id < 0)),
       second((SELECT max(ntz) AS maximum FROM clock_subq WHERE id < 0))
FROM clock_subq

query expect_native(hour,minute,second)
SELECT id, hour((SELECT max(ntz) AS maximum FROM clock_subq WHERE id < 0)),
       hour((SELECT min(ntz) AS minimum FROM clock_subq WHERE id < 0)),
       minute((SELECT max(ntz) AS maximum FROM clock_subq WHERE id < 0)),
       minute((SELECT min(ntz) AS minimum FROM clock_subq WHERE id < 0)),
       second((SELECT max(ntz) AS maximum FROM clock_subq WHERE id < 0)),
       second((SELECT min(ntz) AS minimum FROM clock_subq WHERE id < 0))
FROM clock_subq

-- TimestampNTZ epoch conversion also remains native.
query expect_native(unix_timestamp)
SELECT id, unix_timestamp((SELECT max(ntz) AS maximum FROM clock_subq))
FROM clock_subq

query expect_native(unix_timestamp)
SELECT id, unix_timestamp((SELECT max(ntz) AS maximum FROM clock_subq)),
       unix_timestamp((SELECT min(ntz) AS minimum FROM clock_subq))
FROM clock_subq

query expect_native(unix_timestamp)
SELECT id, unix_timestamp((SELECT max(ntz) AS maximum FROM clock_subq WHERE id < 0))
FROM clock_subq

query expect_native(unix_timestamp)
SELECT id, unix_timestamp((SELECT max(ntz) AS maximum FROM clock_subq WHERE id < 0)),
       unix_timestamp((SELECT min(ntz) AS minimum FROM clock_subq WHERE id < 0))
FROM clock_subq

-- Future IANA transitions must use Spark's rules rather than a last-known fixed offset.
statement
SET spark.comet.enabled=false

statement
CREATE TABLE future_clock USING parquet AS
SELECT id, timestamp_micros(7273800000000000L + id * 3600000000L) AS ts
FROM range(2)

statement
SET spark.comet.enabled=true

query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT id, hour((SELECT max(ts) AS maximum FROM future_clock)),
       hour((SELECT min(ts) AS minimum FROM future_clock))
FROM future_clock

-- Epoch arithmetic on Timestamp does not depend on regional timezone rules.
query expect_native(unix_timestamp)
SELECT id, unix_timestamp((SELECT max(ts) AS maximum FROM clock_subq))
FROM clock_subq

query expect_native(unix_timestamp)
SELECT id, unix_timestamp((SELECT max(ts) AS maximum FROM clock_subq)),
       unix_timestamp((SELECT min(ts) AS minimum FROM clock_subq))
FROM clock_subq

query expect_native(unix_timestamp)
SELECT id, unix_timestamp((SELECT max(ts) AS maximum FROM clock_subq WHERE id < 0))
FROM clock_subq

query expect_native(unix_timestamp)
SELECT id, unix_timestamp((SELECT max(ts) AS maximum FROM clock_subq WHERE id < 0)),
       unix_timestamp((SELECT min(ts) AS minimum FROM clock_subq WHERE id < 0))
FROM clock_subq

-- Wrapping a timestamp scalar in coalesce or IF must not bypass the timezone guard.
query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT id, hour(coalesce((SELECT max(ts) AS maximum FROM future_clock), ts)),
       minute(IF(id = 0, (SELECT min(ts) AS minimum FROM future_clock), ts))
FROM future_clock

-- A plain NTZ-to-Timestamp cast follows the session zone when consumed by a clock function.
query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT id, hour(CAST((SELECT max(ntz) AS maximum FROM clock_subq) AS TIMESTAMP))
FROM clock_subq
