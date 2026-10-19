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

-- ConfigMatrix: spark.sql.session.timeZone=UTC,+01:00,-03:30
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

-- The full i64 microsecond endpoints.
query expect_native(hour,minute,second)
SELECT hour((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id < 2)),
       hour((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id < 2)),
       minute((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id < 2)),
       minute((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id < 2)),
       second((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id < 2)),
       second((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id < 2))
FROM timestamp_boundary

-- Row-dependent wrappers turn merged scalar fields into arrays. Their full-range clock
-- components must remain native and must not become NULL through calendar conversion.
query expect_native(hour,minute,second)
SELECT id,
       hour(IF(id % 2 = 0, (SELECT max(ts) FROM timestamp_boundary), ts)),
       minute(IF(id % 2 = 0, (SELECT max(ts) FROM timestamp_boundary), ts)),
       second(IF(id % 2 = 0, (SELECT max(ts) FROM timestamp_boundary), ts)),
       hour((SELECT min(ts) FROM timestamp_boundary))
FROM timestamp_boundary

query expect_native(hour,minute,second)
SELECT id,
       hour(CASE WHEN id % 2 = 0 THEN (SELECT min(ts) FROM timestamp_boundary) ELSE ts END),
       minute(CASE WHEN id % 2 = 0 THEN (SELECT min(ts) FROM timestamp_boundary) ELSE ts END),
       second(CASE WHEN id % 2 = 0 THEN (SELECT min(ts) FROM timestamp_boundary) ELSE ts END),
       hour((SELECT max(ts) FROM timestamp_boundary))
FROM timestamp_boundary

-- Null array elements must keep their validity, including sparse and dense null inputs.
query expect_native(hour,minute,second)
SELECT id,
       hour(IF(id = 0, (SELECT max(ts) FROM timestamp_boundary), CAST(NULL AS TIMESTAMP))),
       minute(IF(id = 0, (SELECT max(ts) FROM timestamp_boundary), CAST(NULL AS TIMESTAMP))),
       second(IF(id = 0, (SELECT max(ts) FROM timestamp_boundary), CAST(NULL AS TIMESTAMP))),
       hour((SELECT min(ts) FROM timestamp_boundary))
FROM timestamp_boundary

query expect_native(hour,minute,second)
SELECT id,
       hour(IF(id = 0, CAST(NULL AS TIMESTAMP), (SELECT min(ts) FROM timestamp_boundary))),
       minute(IF(id = 0, CAST(NULL AS TIMESTAMP), (SELECT min(ts) FROM timestamp_boundary))),
       second(IF(id = 0, CAST(NULL AS TIMESTAMP), (SELECT min(ts) FROM timestamp_boundary))),
       hour((SELECT max(ts) FROM timestamp_boundary))
FROM timestamp_boundary

-- A wrong NULL would also silently lose rows and change aggregate results.
query expect_native(hour,minute,second)
SELECT count(*), sum(h), sum(m), sum(s)
FROM (
  SELECT hour(IF(id % 2 = 0, (SELECT max(ts) FROM timestamp_boundary), ts)) AS h,
         minute(IF(id % 2 = 0, (SELECT max(ts) FROM timestamp_boundary), ts)) AS m,
         second(IF(id % 2 = 0, (SELECT max(ts) FROM timestamp_boundary), ts)) AS s,
         hour((SELECT min(ts) FROM timestamp_boundary)) AS lower_hour
  FROM timestamp_boundary
) clocks
WHERE h IS NOT NULL AND lower_hour IS NOT NULL

-- Immediately before and at chrono's minimum.
query expect_native(hour,minute,second)
SELECT hour((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id IN (2, 3))),
       hour((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id IN (2, 3))),
       minute((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id IN (2, 3))),
       minute((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id IN (2, 3))),
       second((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id IN (2, 3))),
       second((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id IN (2, 3)))
FROM timestamp_boundary

-- At and immediately after chrono's maximum.
query expect_native(hour,minute,second)
SELECT hour((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id IN (4, 5))),
       hour((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id IN (4, 5))),
       minute((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id IN (4, 5))),
       minute((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id IN (4, 5))),
       second((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id IN (4, 5))),
       second((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id IN (4, 5)))
FROM timestamp_boundary

-- An empty source still produces NULL scalar fields.
query expect_native(hour,minute,second)
SELECT hour((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id < 0)),
       hour((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id < 0)),
       minute((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id < 0)),
       minute((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id < 0)),
       second((SELECT max(ts) AS maximum FROM timestamp_boundary WHERE id < 0)),
       second((SELECT min(ts) AS minimum FROM timestamp_boundary WHERE id < 0))
FROM timestamp_boundary
