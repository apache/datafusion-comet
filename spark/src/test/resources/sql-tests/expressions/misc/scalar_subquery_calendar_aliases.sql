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

-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles
-- ConfigMatrix: spark.sql.adaptive.enabled=false,true
-- Config: spark.sql.execution.reuseSubquery=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS
-- Config: spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED
-- Config: spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED

statement
SET spark.comet.enabled=false

-- These future instants are July 1 just after midnight in Los Angeles.
statement
CREATE TABLE calendar_alias_subq USING parquet AS
SELECT id, named_struct(
  'instant', timestamp_micros(7273783800000000L + id * 900000000L),
  'local_time', CAST('2200-07-01 00:30:00' AS TIMESTAMP_NTZ)) AS payload
FROM range(2)

statement
SET spark.comet.enabled=true

-- Reusing the struct alias keeps a Project between the scalar subquery and its consumers.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, CAST(s.instant AS STRING), CAST(s.instant AS DATE),
       CAST(s.instant AS TIMESTAMP_NTZ), month(s.instant), hour(s.instant),
       s.instant = timestamp_micros(7273783800000000L)
FROM (SELECT id, (SELECT payload FROM calendar_alias_subq WHERE id = 0) AS s
      FROM calendar_alias_subq)

-- Reused struct wrappers retain two alias layers; outer wrappers must keep their provenance.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, CAST(coalesce(first.instant, second.instant) AS STRING),
       CAST(IF(id = 0, first.instant, second.instant) AS DATE)
FROM (SELECT id, IF(id = 0, s, payload) AS first, IF(id = 0, payload, s) AS second
      FROM (SELECT id, payload,
                   (SELECT payload FROM calendar_alias_subq WHERE id = 0) AS s
            FROM calendar_alias_subq))

-- The scalar-derived alias remains identifiable across an exchange boundary.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, CAST(s.instant AS STRING), month(s.instant)
FROM (SELECT /*+ REPARTITION(1) */ id,
             (SELECT payload FROM calendar_alias_subq WHERE id = 0) AS s
      FROM calendar_alias_subq)

-- The limit keeps this calendar predicate above the alias-producing Project.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT s.instant
FROM (SELECT id, (SELECT payload FROM calendar_alias_subq WHERE id = 0) AS s
      FROM calendar_alias_subq LIMIT 1)
WHERE CAST(s.instant AS DATE) = DATE '2200-07-01' AND month(s.instant) = 7

-- Raw comparisons, NTZ clocks and timestamp epoch arithmetic retain native execution.
query expect_native(hour,unix_timestamp)
SELECT id, s.instant = payload.instant, s.instant < payload.instant,
       hour(s.local_time), unix_timestamp(s.instant)
FROM (SELECT id, payload,
             (SELECT payload FROM calendar_alias_subq WHERE id = 0) AS s
      FROM calendar_alias_subq)

-- An ordinary stored struct field must not acquire scalar-subquery provenance.
query expect_native(cast)
SELECT id, CAST(payload.local_time AS STRING)
FROM calendar_alias_subq

-- The projection is above the sort so TopK carries the calendar consumers itself.
-- Unique ids make the selected row deterministic.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, CAST(s.instant AS STRING), month(s.instant)
FROM (SELECT id, (SELECT payload FROM calendar_alias_subq WHERE id = 0) AS s
      FROM calendar_alias_subq ORDER BY id)
LIMIT 1
