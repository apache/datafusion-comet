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

-- ConfigMatrix: spark.sql.session.timeZone=UTC,+01:00,America/Los_Angeles
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.sql.execution.reuseSubquery=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED
-- Config: spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED

statement
SET spark.comet.enabled=false

statement
CREATE TABLE date_unix_subq USING parquet AS
SELECT id, date_from_unix_date(CAST(100000000L + id AS INT)) AS d
FROM range(2)

statement
SET spark.comet.enabled=true

-- Date32 scalar values may exceed the native calendar or timestamp range in any zone.
-- Keep plain, paired, and runtime NULL paths covered independently.
query expect_fallback(Date scalar subqueries in unix_timestamp require Spark because their runtime values may exceed the native calendar range)
SELECT id, unix_timestamp((SELECT max(d) AS maximum FROM date_unix_subq))
FROM date_unix_subq

query expect_fallback(Date scalar subqueries in unix_timestamp require Spark because their runtime values may exceed the native calendar range)
SELECT id, unix_timestamp((SELECT max(d) AS maximum FROM date_unix_subq)),
       unix_timestamp((SELECT min(d) AS minimum FROM date_unix_subq))
FROM date_unix_subq

query expect_fallback(Date scalar subqueries in unix_timestamp require Spark because their runtime values may exceed the native calendar range)
SELECT id, unix_timestamp((SELECT max(d) AS maximum FROM date_unix_subq WHERE id < 0))
FROM date_unix_subq

query expect_fallback(Date scalar subqueries in unix_timestamp require Spark because their runtime values may exceed the native calendar range)
SELECT id, unix_timestamp((SELECT max(d) AS maximum FROM date_unix_subq WHERE id < 0)),
       unix_timestamp((SELECT min(d) AS minimum FROM date_unix_subq WHERE id < 0))
FROM date_unix_subq
