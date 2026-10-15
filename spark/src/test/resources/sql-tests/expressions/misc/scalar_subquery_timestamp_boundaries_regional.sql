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
-- Config: spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS
-- Config: spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED
-- Config: spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED

statement
CREATE TABLE timestamp_boundary(id INT, ts TIMESTAMP) USING parquet

-- Write with Spark so only scalar transport and clock extraction are under test.
statement
SET spark.comet.enabled=false

statement
INSERT INTO timestamp_boundary
SELECT id, timestamp_micros(micros)
FROM VALUES
  (0, -9223372036854775808L),
  (1, 9223372036854775807L),
  (2, -8334601228800000001L),
  (3, -8334601228800000000L),
  (4, 8210266876799999999L),
  (5, 8210266876800000000L)
AS boundaries(id, micros)

statement
SET spark.comet.enabled=true

-- Regional rules use Spark for runtime values, including the full i64 endpoints.
query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT hour((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id < 2)),
       hour((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id < 2)),
       minute((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id < 2)),
       minute((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id < 2)),
       second((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id < 2)),
       second((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id < 2))
FROM timestamp_boundary

-- Immediately before and at chrono's minimum.
query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT hour((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id IN (2, 3))),
       hour((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id IN (2, 3))),
       minute((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id IN (2, 3))),
       minute((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id IN (2, 3))),
       second((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id IN (2, 3))),
       second((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id IN (2, 3)))
FROM timestamp_boundary

-- At and immediately after chrono's maximum.
query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT hour((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id IN (4, 5))),
       hour((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id IN (4, 5))),
       minute((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id IN (4, 5))),
       minute((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id IN (4, 5))),
       second((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id IN (4, 5))),
       second((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id IN (4, 5)))
FROM timestamp_boundary

-- An empty source still produces NULL scalar fields.
query expect_fallback(Timezone-dependent scalar subquery operations require Spark for non-fixed timezones)
SELECT hour((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id < 0)),
       hour((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id < 0)),
       minute((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id < 0)),
       minute((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id < 0)),
       second((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id < 0)),
       second((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id < 0))
FROM timestamp_boundary
