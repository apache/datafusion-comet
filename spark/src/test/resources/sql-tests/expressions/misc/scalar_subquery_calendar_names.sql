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

-- MinSparkVersion: 4.0
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.sql.execution.reuseSubquery=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED
-- Config: spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED

statement
SET spark.comet.enabled=false

statement
CREATE TABLE calendar_name_subq USING parquet AS
SELECT id, date_from_unix_date(CAST(100000000L + id AS INT)) AS d
FROM range(2)

statement
SET spark.comet.enabled=true

-- Spark 4.x calendar-name expressions must hit the shared guard before version-specific serde.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, dayname((SELECT max(d) AS maximum FROM calendar_name_subq)),
       dayname((SELECT min(d) AS minimum FROM calendar_name_subq)),
       monthname((SELECT max(d) AS maximum FROM calendar_name_subq)),
       monthname((SELECT min(d) AS minimum FROM calendar_name_subq))
FROM calendar_name_subq

query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, dayname((SELECT max(d) AS maximum FROM calendar_name_subq WHERE id < 0)),
       dayname((SELECT min(d) AS minimum FROM calendar_name_subq WHERE id < 0)),
       monthname((SELECT max(d) AS maximum FROM calendar_name_subq WHERE id < 0)),
       monthname((SELECT min(d) AS minimum FROM calendar_name_subq WHERE id < 0))
FROM calendar_name_subq
