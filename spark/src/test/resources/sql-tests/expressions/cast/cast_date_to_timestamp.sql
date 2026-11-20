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

-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.datetime.java8API.enabled=true
-- Config: spark.sql.session.timeZone=GMT+05:30
-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE wide_dates(y INT, m INT, d INT) USING parquet

-- The first two years exceed chrono's range but fit in timestamp microseconds.
-- July 2101 is beyond chrono-tz's last DST transition for Los Angeles.
statement
INSERT INTO wide_dates VALUES
  (262143, 1, 1), (-262144, 1, 1), (2101, 7, 4), (1970, 1, 1), (NULL, 1, 1)

query expect_native(cast, make_date)
SELECT CAST(make_date(y, m, d) AS TIMESTAMP),
       CAST(array(named_struct('d', make_date(y, m, d))) AS ARRAY<STRUCT<d: TIMESTAMP>>)
FROM wide_dates

statement
SET spark.sql.session.timeZone=America/Los_Angeles

query expect_dispatch(cast)
SELECT CAST(make_date(y, m, d) AS TIMESTAMP) FROM wide_dates

query expect_native(cast, make_date)
SELECT CAST(make_date(y, m, d) AS TIMESTAMP_NTZ) FROM wide_dates

statement
SET spark.sql.session.timeZone=UTC

statement
INSERT INTO wide_dates VALUES (300000, 6, 15), (-300000, 6, 15)

query expect_native(try_cast)
SELECT TRY_CAST(make_date(y, m, d) AS TIMESTAMP), TRY_CAST(make_date(y, m, d) AS TIMESTAMP_NTZ)
FROM wide_dates

-- Under ANSI, make_date with non-nullable inputs is non-nullable. Spark infers the
-- same for the TRY_CAST, even though 300000-06-15 overflows.
query expect_fallback(TRY_CAST from date can overflow)
SELECT TRY_CAST(make_date(coalesce(y, 300000), 6, 15) AS TIMESTAMP) FROM wide_dates

statement
SET spark.sql.legacy.castComplexTypesToString.enabled=true

-- The outer cast would otherwise send this subtree through the dispatcher.
query expect_fallback(TRY_CAST from date can overflow)
SELECT CAST(array(TRY_CAST(make_date(coalesce(y, 300000), 6, 15) AS TIMESTAMP_NTZ)) AS STRING)
FROM wide_dates

statement
CREATE TABLE nested_dates USING parquet AS
SELECT y = 1970 AS keep, named_struct('d', dt) AS s, map(1, dt) AS m, map(dt, 1) AS k,
       array(named_struct('d', dt)) AS a
FROM (SELECT y, make_date(y, m, d) AS dt FROM wide_dates WHERE y IN (1970, 300000))

-- IF can null a container without clearing the overflowing date in its child buffer.
-- Read each container twice to keep the optimizer from moving the cast inside the IF.
query expect_native(cast)
SELECT s, CAST(s AS STRUCT<d: TIMESTAMP>), m, CAST(m AS MAP<INT, TIMESTAMP_NTZ>),
       k, CAST(k AS MAP<TIMESTAMP, INT>), a, CAST(a AS ARRAY<STRUCT<d: TIMESTAMP_NTZ>>)
FROM (SELECT IF(keep, s, NULL) AS s, IF(keep, m, NULL) AS m, IF(keep, k, NULL) AS k,
             IF(keep, a, NULL) AS a FROM nested_dates)

query expect_native(try_cast)
SELECT TRY_CAST(s AS STRUCT<d: TIMESTAMP>), TRY_CAST(m AS MAP<INT, TIMESTAMP_NTZ>),
       TRY_CAST(a AS ARRAY<STRUCT<d: TIMESTAMP>>)
FROM nested_dates

query expect_fallback(null map key)
SELECT TRY_CAST(k AS MAP<TIMESTAMP, INT>) FROM nested_dates

statement
SET spark.sql.session.timeZone=America/Los_Angeles

statement
SET spark.comet.exec.scalaUDF.codegen.enabled=false

query expect_fallback(cast: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT CAST(make_date(y, m, d) AS TIMESTAMP) FROM wide_dates WHERE y = 262143
