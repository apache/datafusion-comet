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

-- ConfigMatrix: spark.sql.adaptive.enabled=false,true
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.sql.execution.reuseSubquery=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS
-- Config: spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED
-- Config: spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED

statement
SET spark.comet.enabled=false

-- One file contains both Date32 and timestamp-microsecond endpoints, ordinary
-- values, and NULLs. Only the consumers below are evaluated with Comet.
statement
CREATE TABLE calendar_allow_subq USING parquet AS
SELECT /*+ COALESCE(1) */ id, date_from_unix_date(CAST(epoch_day AS INT)) AS d,
       timestamp_micros(micros) AS ts,
       CASE WHEN id <> 2 THEN date_add(DATE '2024-06-15', id) END AS ordinary_d
FROM VALUES
  (0, -2147483648L, -9223372036854775808L),
  (1, 2147483647L, 9223372036854775807L),
  (2, NULL, NULL),
  (3, -100000000L, -8640000000000000000L),
  (4, 0L, 0L),
  (5, 100000000L, 8640000000000000000L)
AS boundaries(id, epoch_day, micros)

statement
SET spark.comet.enabled=true

-- DateDiff returns INT and inherits neither GetDateField nor TimeZoneAwareExpression.
-- This is an allow-list policy regression, not a claim of a wrong DateDiff result:
-- consumers outside the reviewed set must keep Spark execution by default.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, datediff((SELECT max(d) FROM calendar_allow_subq), ordinary_d),
       datediff((SELECT min(d) FROM calendar_allow_subq), ordinary_d)
FROM calendar_allow_subq

-- DateAdd is also outside the reviewed set. A permitted outer epoch conversion
-- must still inspect its date-producing child.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, unix_date(date_add((SELECT max(d) FROM calendar_allow_subq), 1)),
       unix_date(date_add((SELECT min(d) FROM calendar_allow_subq), -1))
FROM calendar_allow_subq

-- Keep the same policy after aliases and conditional wrappers cross a Project.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, datediff(coalesce(s.high, ordinary_d), ordinary_d),
       unix_date(date_add(IF(id = 0, s.low, d), 1))
FROM (SELECT id, d, ordinary_d,
             (SELECT named_struct('high', max(d), 'low', min(d))
              FROM calendar_allow_subq) AS s
      FROM calendar_allow_subq ORDER BY id LIMIT 5)

-- Ordinary date columns without struct-subquery context retain their existing
-- native implementations, including NULLs.
query expect_native(datediff,date_add,unix_date)
SELECT id, datediff(ordinary_d, DATE '2024-06-15'),
       unix_date(date_add(ordinary_d, 1))
FROM calendar_allow_subq

-- Epoch-day transport and weekday arithmetic support both Date32 endpoints.
query expect_native(unix_date,dayofweek,weekday)
SELECT id, unix_date((SELECT max(d) FROM calendar_allow_subq)),
       unix_date((SELECT min(d) FROM calendar_allow_subq)),
       dayofweek((SELECT max(d) FROM calendar_allow_subq)),
       dayofweek((SELECT min(d) FROM calendar_allow_subq)),
       weekday((SELECT max(d) FROM calendar_allow_subq)),
       weekday((SELECT min(d) FROM calendar_allow_subq))
FROM calendar_allow_subq

-- Field access, aliases, IF/CASE, coalesce and min/max-style comparisons only
-- transport dates. The row-dependent branches include the stored NULL.
query expect_native(unix_date,dayofweek,weekday)
SELECT id, unix_date(coalesce(s.high, d)),
       dayofweek(IF(id % 2 = 0, s.low, d)),
       weekday(CASE WHEN id % 2 = 0 THEN s.high ELSE d END),
       unix_date(greatest(s.low, d)), unix_date(least(s.high, d))
FROM (SELECT id, d,
             (SELECT named_struct('high', max(d), 'low', min(d))
              FROM calendar_allow_subq) AS s
      FROM calendar_allow_subq ORDER BY id LIMIT 5)

-- Comparing raw dates and timestamps does not interpret a calendar.
query expect_native(equalto,equalnullsafe,lessthan,greaterthan)
SELECT id, d = (SELECT max(d) FROM calendar_allow_subq),
       d < (SELECT max(d) FROM calendar_allow_subq),
       d > (SELECT min(d) FROM calendar_allow_subq),
       ts <=> (SELECT max(ts) FROM calendar_allow_subq)
FROM calendar_allow_subq

-- Timestamp-to-BIGINT casts retain full-range epoch arithmetic.
query expect_native(cast)
SELECT id, CAST((SELECT max(ts) FROM calendar_allow_subq) AS BIGINT),
       CAST((SELECT min(ts) FROM calendar_allow_subq) AS BIGINT),
       CAST(IF(id % 2 = 0, (SELECT max(ts) FROM calendar_allow_subq), ts) AS BIGINT)
FROM calendar_allow_subq

-- Empty aggregates produce runtime NULL fields; transport and allowed consumers
-- must keep their NULL behavior rather than relying on constant folding.
query expect_native(unix_date,dayofweek,weekday,cast)
SELECT id, unix_date((SELECT max(d) FROM calendar_allow_subq WHERE id < 0)),
       dayofweek((SELECT max(d) FROM calendar_allow_subq WHERE id < 0)),
       weekday((SELECT min(d) FROM calendar_allow_subq WHERE id < 0)),
       CAST((SELECT max(ts) FROM calendar_allow_subq WHERE id < 0) AS BIGINT),
       CAST((SELECT min(ts) FROM calendar_allow_subq WHERE id < 0) AS BIGINT)
FROM calendar_allow_subq

-- Raw-value aggregation is transport/comparison too. Its result must retain
-- the scalar-subquery context for any later consumer.
query expect_native(unix_date,dayofweek,weekday)
SELECT unix_date(max(high)), dayofweek(min(low)), weekday(max(high)), count(high)
FROM (SELECT (SELECT max(d) FROM calendar_allow_subq) AS high,
             (SELECT min(d) FROM calendar_allow_subq) AS low
      FROM calendar_allow_subq)
