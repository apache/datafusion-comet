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

-- ConfigMatrix: spark.sql.session.timeZone=UTC,+01:00,-03:30,America/Los_Angeles,Australia/Lord_Howe,Asia/Kathmandu
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.sql.execution.reuseSubquery=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS
-- Config: spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED
-- Config: spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED

-- Store with Spark so these assertions isolate consumers of runtime scalar fields.
statement
SET spark.comet.enabled=false

statement
CREATE TABLE calendar_subq USING parquet AS
SELECT id, timestamp_micros(7273783800000000L + id * 900000000L) AS ts,
       CAST('2200-07-01 00:30:00' AS TIMESTAMP_NTZ) AS ntz,
       date_from_unix_date(CAST(100000000L + id AS INT)) AS d,
       timestamp_micros(8640000000000000000L + id * 3600000000L) AS ts_extreme
FROM range(2)

statement
SET spark.comet.enabled=true

-- In Los Angeles these future instants fall just after midnight on July 1.
-- A stale fixed offset would change the date and month as well as the clock.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, CAST((SELECT max(ts) AS maximum FROM calendar_subq) AS STRING),
       CAST((SELECT min(ts) AS minimum FROM calendar_subq) AS STRING)
FROM calendar_subq

query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, CAST((SELECT max(ts) AS maximum FROM calendar_subq) AS DATE),
       CAST((SELECT min(ts) AS minimum FROM calendar_subq) AS DATE)
FROM calendar_subq

query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, CAST((SELECT max(ts) AS maximum FROM calendar_subq) AS TIMESTAMP_NTZ),
       CAST((SELECT min(ts) AS minimum FROM calendar_subq) AS TIMESTAMP_NTZ)
FROM calendar_subq

query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, month((SELECT max(ts) AS maximum FROM calendar_subq)),
       dayofmonth((SELECT min(ts) AS minimum FROM calendar_subq))
FROM calendar_subq

-- Nested expressions must not hide the runtime struct field from the calendar guard.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, CAST(coalesce((SELECT max(ts) AS maximum FROM calendar_subq), ts) AS STRING),
       CAST(IF(id = 0, (SELECT min(ts) AS minimum FROM calendar_subq), ts) AS DATE)
FROM calendar_subq

-- Casting a whole scalar struct also recursively converts its timestamp fields.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, CAST((SELECT named_struct('instant', max(ts), 'local_time', max(ntz))
                FROM calendar_subq) AS STRING)
FROM calendar_subq

-- Merged NTZ fields have an unknown calendar range in every zone, including UTC.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, CAST((SELECT max(ntz) AS maximum FROM calendar_subq) AS TIMESTAMP),
       CAST((SELECT min(ntz) AS minimum FROM calendar_subq) AS TIMESTAMP)
FROM calendar_subq

-- Runtime NULLs retain the same guard rather than relying on constant folding.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, CAST((SELECT max(ts) AS maximum FROM calendar_subq WHERE id < 0) AS STRING),
       CAST((SELECT min(ts) AS minimum FROM calendar_subq WHERE id < 0) AS DATE)
FROM calendar_subq

-- Date32 can exceed the native calendar range even in UTC.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, year((SELECT max(d) AS maximum FROM calendar_subq)),
       month((SELECT min(d) AS minimum FROM calendar_subq)),
       dayofmonth((SELECT min(d) AS minimum FROM calendar_subq))
FROM calendar_subq

query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, unix_date(last_day((SELECT max(d) AS maximum FROM calendar_subq))),
       unix_date(last_day((SELECT min(d) AS minimum FROM calendar_subq)))
FROM calendar_subq

-- Formatting also needs the calendar guard in UTC when runtime values exceed chrono.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, date_format((SELECT max(ts_extreme) AS maximum FROM calendar_subq),
                       'yyyy-MM-dd HH:mm:ss'),
       date_format((SELECT min(ts_extreme) AS minimum FROM calendar_subq),
                   'yyyy-MM-dd HH:mm:ss')
FROM calendar_subq

-- next_day uses calendar conversion even though it has no timezone argument.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, unix_date(next_day((SELECT max(d) AS maximum FROM calendar_subq), 'MONDAY')),
       unix_date(next_day((SELECT min(d) AS minimum FROM calendar_subq), 'MONDAY'))
FROM calendar_subq

-- Transport and comparison of the raw timestamp remain native in every zone.
query
SELECT id, (SELECT max(ts) AS maximum FROM calendar_subq) >
           (SELECT min(ts) AS minimum FROM calendar_subq),
       ts = (SELECT min(ts) AS minimum FROM calendar_subq)
FROM calendar_subq

-- Timestamp-to-integer casts use epoch arithmetic rather than calendar conversion.
query expect_native(cast)
SELECT id, CAST((SELECT max(ts) AS maximum FROM calendar_subq) AS BIGINT),
       CAST((SELECT min(ts) AS minimum FROM calendar_subq) AS BIGINT)
FROM calendar_subq
