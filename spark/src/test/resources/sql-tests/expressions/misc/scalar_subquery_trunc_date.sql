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

-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.sql.execution.reuseSubquery=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED
-- Config: spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED

-- Spark writes dates beyond chrono's range so only the runtime consumer is under test.
statement
SET spark.comet.enabled=false

statement
CREATE TABLE trunc_date_subq USING parquet AS
SELECT id, date_from_unix_date(100000000) AS d,
       date_from_unix_date(-100000000) AS negative_d,
       CASE WHEN id < 2 THEN date_add(DATE '2024-06-15', CAST(id AS INT)) END AS ordinary_d,
       named_struct('d', DATE '2024-06-15') AS payload
FROM range(3)

statement
SET spark.comet.enabled=true

-- Calendar consumers above a struct-consuming filter remain newly admitted even
-- when the final projection drops the struct itself.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT unix_date(trunc(d, 'YEAR'))
FROM (SELECT id, d,
             (SELECT named_struct('x', max(ordinary_d), 'y', min(ordinary_d))
              FROM trunc_date_subq) AS s
      FROM trunc_date_subq ORDER BY id LIMIT 2)
WHERE s.x IS NOT NULL

-- A merged struct in a sibling projection can admit the entire Project. Its
-- ordinary date input still needs calendar protection on that newly native path.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT unix_date(trunc(d, 'YEAR')),
       (SELECT max(ordinary_d) FROM trunc_date_subq),
       (SELECT min(ordinary_d) FROM trunc_date_subq)
FROM trunc_date_subq

-- Merged max/min results become fields of a struct scalar subquery. Spark returns
-- epoch day 99999744 for both YEAR expressions; the unguarded native path returns NULL.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT unix_date(trunc((SELECT max(d) FROM trunc_date_subq), 'YEAR')),
       unix_date(trunc((SELECT min(d) FROM trunc_date_subq), 'YEAR'))
FROM trunc_date_subq

query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT unix_date(trunc((SELECT max(d) FROM trunc_date_subq), 'MONTH')),
       unix_date(trunc((SELECT min(d) FROM trunc_date_subq), 'MONTH'))
FROM trunc_date_subq

query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT unix_date(trunc((SELECT max(d) FROM trunc_date_subq), 'QUARTER')),
       unix_date(trunc((SELECT min(d) FROM trunc_date_subq), 'QUARTER'))
FROM trunc_date_subq

query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT unix_date(trunc((SELECT max(d) FROM trunc_date_subq), 'WEEK')),
       unix_date(trunc((SELECT min(d) FROM trunc_date_subq), 'WEEK'))
FROM trunc_date_subq

-- The same calendar conversion fails for dates before chrono's minimum year.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT unix_date(trunc((SELECT max(negative_d) FROM trunc_date_subq), 'YEAR')),
       unix_date(trunc((SELECT min(negative_d) FROM trunc_date_subq), 'YEAR')),
       unix_date(trunc((SELECT max(negative_d) FROM trunc_date_subq), 'MONTH')),
       unix_date(trunc((SELECT min(negative_d) FROM trunc_date_subq), 'MONTH')),
       unix_date(trunc((SELECT max(negative_d) FROM trunc_date_subq), 'QUARTER')),
       unix_date(trunc((SELECT min(negative_d) FROM trunc_date_subq), 'QUARTER')),
       unix_date(trunc((SELECT max(negative_d) FROM trunc_date_subq), 'WEEK')),
       unix_date(trunc((SELECT min(negative_d) FROM trunc_date_subq), 'WEEK'))
FROM trunc_date_subq

-- Empty aggregates produce runtime NULLs and must retain the same provenance guard.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT trunc((SELECT max(d) FROM trunc_date_subq WHERE id < 0), 'YEAR'),
       trunc((SELECT min(d) FROM trunc_date_subq WHERE id < 0), 'MONTH'),
       trunc((SELECT max(d) FROM trunc_date_subq WHERE id < 0), 'QUARTER'),
       trunc((SELECT min(d) FROM trunc_date_subq WHERE id < 0), 'WEEK')
FROM trunc_date_subq

-- Wrappers around the date input and the truncation must not hide the calendar consumer.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id,
       coalesce(unix_date(trunc(
         coalesce((SELECT max(d) FROM trunc_date_subq), ordinary_d), 'MONTH')), -1),
       IF(id = 0, unix_date(trunc((SELECT min(d) FROM trunc_date_subq), 'WEEK')),
          unix_date(ordinary_d))
FROM trunc_date_subq

-- The limit keeps an alias-producing Project below the consumers. Reusing the struct
-- and wrapping its fields checks provenance after native conversion of that Project.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id, unix_date(trunc(s.high, 'YEAR')), unix_date(trunc(s.low, 'MONTH')),
       unix_date(trunc(coalesce(s.high, ordinary_d), 'QUARTER')),
       unix_date(trunc(IF(id = 0, s.low, s.high), 'WEEK'))
FROM (SELECT id, ordinary_d,
             (SELECT named_struct('high', max(d), 'low', min(negative_d))
              FROM trunc_date_subq) AS s
      FROM trunc_date_subq ORDER BY id LIMIT 2)

-- A calendar consumer in a predicate must preserve rows that NULL truncation would drop.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT id
FROM (SELECT id,
             (SELECT named_struct('high', max(d), 'low', min(negative_d))
              FROM trunc_date_subq) AS s
      FROM trunc_date_subq ORDER BY id LIMIT 2)
WHERE unix_date(trunc(s.high, 'YEAR')) = 99999744
  AND trunc(s.low, 'MONTH') IS NOT NULL

-- Ordinary safe date columns, including stored struct fields and NULLs, stay native.
query expect_native(trunc)
SELECT id, trunc(ordinary_d, 'YEAR'), trunc(ordinary_d, 'MONTH'),
       trunc(ordinary_d, 'QUARTER'), trunc(ordinary_d, 'WEEK'),
       trunc(payload.d, 'YEAR')
FROM trunc_date_subq

-- A generator materializes a scalar-derived date as a new column, whose source
-- must remain visible to the calendar guard above it.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT unix_date(trunc(v, 'YEAR'))
FROM (SELECT explode(array((SELECT named_struct('d', max(d))
                            FROM trunc_date_subq).d)) AS v
      FROM trunc_date_subq)

-- posexplode also creates fresh output attributes and must retain date provenance.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT pos, unix_date(trunc(v, 'MONTH'))
FROM (SELECT posexplode(array((SELECT named_struct('d', max(d))
                               FROM trunc_date_subq).d)) AS (pos, v)
      FROM trunc_date_subq)

-- Calendar protection also remains conservative for in-range sibling columns
-- after the final projection drops the struct. Independent paths are tested above.
query expect_fallback(Calendar operations on struct scalar subqueries require Spark because their runtime values may exceed the native calendar range)
SELECT trunc(ordinary_d, 'YEAR')
FROM (SELECT id, ordinary_d,
             (SELECT named_struct('d', max(d)) FROM trunc_date_subq) AS s
      FROM trunc_date_subq ORDER BY id LIMIT 2)
WHERE unix_date(s.d) > 0
