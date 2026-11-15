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

-- Timestamp-producing casts use distinct kernels for strings, dates, numeric inputs and the
-- other timestamp type. Consume their results before any boundary can repair the Arrow label.
-- Compare TIMESTAMP_NTZ with a TIMESTAMP_NTZ column: mixing timestamp types would insert a cast
-- that could hide a wrong label. NTZ must remain unlabelled in every session timezone.
-- ConfigMatrix: spark.sql.session.timeZone=UTC,Etc/UTC,America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.ansi.enabled=false

statement
CREATE TABLE cast_timestamp_labels(s STRING, dt DATE, ts TIMESTAMP, ntz TIMESTAMP_NTZ, n BIGINT, f DOUBLE, dec DECIMAL(20,6)) USING parquet

-- Local strings and NTZ inputs include the spring gap and autumn overlap.
statement
INSERT INTO cast_timestamp_labels VALUES
  ('1970-01-01 00:00:00', DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00Z', TIMESTAMP_NTZ '1970-01-01 00:00:00', 0, 0.0, 0.0),
  ('1969-12-31 23:59:59.5', DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.5Z', TIMESTAMP_NTZ '1969-12-31 23:59:59.5', -1, -0.5, -0.5),
  ('2024-01-15 18:00:00.25', DATE '2024-01-15', TIMESTAMP '2024-01-15 18:00:00.25Z', TIMESTAMP_NTZ '2024-01-15 18:00:00.25', 1705341600, 1705341600.25, 1705341600.25),
  ('2024-06-30 23:30:00.5', DATE '2024-06-30', TIMESTAMP '2024-06-30 23:30:00.5Z', TIMESTAMP_NTZ '2024-06-30 23:30:00.5', 1719790200, 1719790200.5, 1719790200.5),
  ('2024-03-10 02:30:00', DATE '2024-03-10', TIMESTAMP '2024-03-10 10:30:00Z', TIMESTAMP_NTZ '2024-03-10 02:30:00', 1710066600, 1710066600.0, 1710066600.0),
  ('2024-11-03 01:30:00', DATE '2024-11-03', TIMESTAMP '2024-11-03 08:30:00Z', TIMESTAMP_NTZ '2024-11-03 01:30:00', 1730622600, 1730622600.0, 1730622600.0),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL)

-- STRING → TIMESTAMP.
query expect_native(cast, equalto, lessthan, hour)
SELECT
  CAST(s AS TIMESTAMP) = ts,
  CAST(s AS TIMESTAMP) < ts,
  hour(CAST(s AS TIMESTAMP)),
  CAST(CAST(s AS TIMESTAMP) AS STRING)
FROM cast_timestamp_labels

-- DATE → TIMESTAMP.
query expect_native(cast, equalto, lessthan, hour)
SELECT
  CAST(dt AS TIMESTAMP) = ts,
  CAST(dt AS TIMESTAMP) < ts,
  hour(CAST(dt AS TIMESTAMP)),
  CAST(CAST(dt AS TIMESTAMP) AS STRING)
FROM cast_timestamp_labels

-- TIMESTAMP_NTZ → TIMESTAMP.
query expect_native(cast, equalto, lessthan, hour)
SELECT
  CAST(ntz AS TIMESTAMP) = ts,
  CAST(ntz AS TIMESTAMP) < ts,
  hour(CAST(ntz AS TIMESTAMP)),
  CAST(CAST(ntz AS TIMESTAMP) AS STRING)
FROM cast_timestamp_labels

-- Integral seconds → TIMESTAMP.
query expect_native(cast, equalto, lessthan, hour)
SELECT
  CAST(n AS TIMESTAMP) = ts,
  CAST(n AS TIMESTAMP) < ts,
  hour(CAST(n AS TIMESTAMP)),
  CAST(CAST(n AS TIMESTAMP) AS STRING)
FROM cast_timestamp_labels

-- Floating-point seconds → TIMESTAMP.
query expect_native(cast, equalto, lessthan, hour)
SELECT
  CAST(f AS TIMESTAMP) = ts,
  CAST(f AS TIMESTAMP) < ts,
  hour(CAST(f AS TIMESTAMP)),
  CAST(CAST(f AS TIMESTAMP) AS STRING)
FROM cast_timestamp_labels

-- Decimal seconds → TIMESTAMP.
query expect_native(cast, equalto, lessthan, hour)
SELECT
  CAST(dec AS TIMESTAMP) = ts,
  CAST(dec AS TIMESTAMP) < ts,
  hour(CAST(dec AS TIMESTAMP)),
  CAST(CAST(dec AS TIMESTAMP) AS STRING)
FROM cast_timestamp_labels

-- STRING → TIMESTAMP_NTZ.
query expect_native(cast, equalto, lessthan, hour)
SELECT
  CAST(s AS TIMESTAMP_NTZ) = ntz,
  CAST(s AS TIMESTAMP_NTZ) < ntz,
  hour(CAST(s AS TIMESTAMP_NTZ)),
  CAST(CAST(s AS TIMESTAMP_NTZ) AS STRING)
FROM cast_timestamp_labels

-- DATE → TIMESTAMP_NTZ.
query expect_native(cast, equalto, lessthan, hour)
SELECT
  CAST(dt AS TIMESTAMP_NTZ) = ntz,
  CAST(dt AS TIMESTAMP_NTZ) < ntz,
  hour(CAST(dt AS TIMESTAMP_NTZ)),
  CAST(CAST(dt AS TIMESTAMP_NTZ) AS STRING)
FROM cast_timestamp_labels

-- TIMESTAMP → TIMESTAMP_NTZ.
query expect_native(cast, equalto, lessthan, hour)
SELECT
  CAST(ts AS TIMESTAMP_NTZ) = ntz,
  CAST(ts AS TIMESTAMP_NTZ) < ntz,
  hour(CAST(ts AS TIMESTAMP_NTZ)),
  CAST(CAST(ts AS TIMESTAMP_NTZ) AS STRING)
FROM cast_timestamp_labels

-- BOOLEAN has a separate native timestamp builder and is supported only in legacy mode.
statement
CREATE TABLE boolean_timestamp_labels(b BOOLEAN, ts TIMESTAMP) USING parquet

statement
INSERT INTO boolean_timestamp_labels VALUES
  (true, TIMESTAMP '1970-01-01 00:00:00.000001Z'),
  (false, TIMESTAMP '1970-01-01 00:00:00Z'),
  (true, TIMESTAMP '1970-01-01 00:00:00.000002Z'),
  (NULL, NULL)

query expect_native(cast, equalto, lessthan, hour)
SELECT
  CAST(b AS TIMESTAMP) = ts,
  CAST(b AS TIMESTAMP) < ts,
  hour(CAST(b AS TIMESTAMP)),
  CAST(CAST(b AS TIMESTAMP) AS STRING)
FROM boolean_timestamp_labels
