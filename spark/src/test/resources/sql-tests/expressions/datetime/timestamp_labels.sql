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

-- Keep timestamp producers and their consumers in the same native projection: scan/shuffle
-- boundaries and conditionals can repair an incorrect Arrow timezone label.
-- TIMESTAMP results must carry exactly "UTC", including in an Etc/UTC session.
-- See docs/source/contributor-guide/timezones.md and issue #6849.
-- ConfigMatrix: spark.sql.session.timeZone=UTC,Etc/UTC,America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.comet.expression.TruncTimestamp.allowIncompatible=true
-- Config: spark.comet.expression.FromUTCTimestamp.allowIncompatible=true
-- Config: spark.comet.expression.ToUTCTimestamp.allowIncompatible=true

statement
CREATE TABLE timestamp_labels(s BIGINT, i INT, d DOUBLE, f FLOAT, dec DECIMAL(20,6), ts TIMESTAMP) USING parquet

-- Epoch, pre-epoch, winter/summer, both sides of the LA spring gap and autumn overlap, and NULL.
-- The comparison column deliberately includes both equal and later instants.
statement
INSERT INTO timestamp_labels VALUES
  (0, 0, 0.0, 0.0, 0.0, TIMESTAMP '1970-01-01 00:00:00Z'),
  (-1, -1, -0.5, -0.5, -0.5, TIMESTAMP '1970-01-01 00:00:00Z'),
  (1705341600, 1705341600, 1705341600.25, 1705341600, 1705341600.25, TIMESTAMP '2024-01-15 18:00:00.25Z'),
  (1719790200, 1719790200, 1719790200.5, 1719790200, 1719790200.5, TIMESTAMP '2024-06-30 23:30:00.5Z'),
  (1710063000, 1710063000, 1710063000.125, 1710063000, 1710063000.125, TIMESTAMP '2024-03-10 09:30:00.125Z'),
  (1710066600, 1710066600, 1710066600.125, 1710066600, 1710066600.125, TIMESTAMP '2024-03-10 10:30:00.125Z'),
  (1730622600, 1730622600, 1730622600.125, 1730622600, 1730622600.125, TIMESTAMP '2024-11-03 08:30:00.125Z'),
  (1730626200, 1730626200, 1730626200.125, 1730626200, 1730626200.125, TIMESTAMP '2024-11-03 09:30:00.125Z'),
  (NULL, NULL, NULL, NULL, NULL, NULL)

-- BIGINT has its own timestamp_seconds input path.
query expect_native(timestamp_seconds, equalto, lessthan, hour, cast)
SELECT
  timestamp_seconds(s) = ts,
  timestamp_seconds(s) < ts,
  hour(timestamp_seconds(s)),
  CAST(timestamp_seconds(s) AS STRING)
FROM timestamp_labels

-- INT has its own timestamp_seconds input path.
query expect_native(timestamp_seconds, equalto, lessthan, hour, cast)
SELECT
  timestamp_seconds(i) = ts,
  timestamp_seconds(i) < ts,
  hour(timestamp_seconds(i)),
  CAST(timestamp_seconds(i) AS STRING)
FROM timestamp_labels

-- DOUBLE has its own timestamp_seconds input path.
query expect_native(timestamp_seconds, equalto, lessthan, hour, cast)
SELECT
  timestamp_seconds(d) = ts,
  timestamp_seconds(d) < ts,
  hour(timestamp_seconds(d)),
  CAST(timestamp_seconds(d) AS STRING)
FROM timestamp_labels

-- FLOAT has its own timestamp_seconds input path.
query expect_native(timestamp_seconds, equalto, lessthan, hour, cast)
SELECT
  timestamp_seconds(f) = ts,
  timestamp_seconds(f) < ts,
  hour(timestamp_seconds(f)),
  CAST(timestamp_seconds(f) AS STRING)
FROM timestamp_labels

-- Truncate to HOUR in the session timezone, then consume the UTC-labelled result.
query expect_native(date_trunc, equalto, lessthan, hour, cast)
SELECT
  date_trunc('HOUR', ts) = ts,
  date_trunc('HOUR', ts) < ts,
  hour(date_trunc('HOUR', ts)),
  CAST(date_trunc('HOUR', ts) AS STRING)
FROM timestamp_labels

-- Truncate to DAY in the session timezone, then consume the UTC-labelled result.
query expect_native(date_trunc, equalto, lessthan, hour, cast)
SELECT
  date_trunc('DAY', ts) = ts,
  date_trunc('DAY', ts) < ts,
  hour(date_trunc('DAY', ts)),
  CAST(date_trunc('DAY', ts) AS STRING)
FROM timestamp_labels

-- The opt-in native timezone conversion must also return a UTC-labelled TIMESTAMP.
query expect_native(from_utc_timestamp, equalto, lessthan, hour, cast)
SELECT
  from_utc_timestamp(ts, 'America/Los_Angeles') = ts,
  from_utc_timestamp(ts, 'America/Los_Angeles') < ts,
  hour(from_utc_timestamp(ts, 'America/Los_Angeles')),
  CAST(from_utc_timestamp(ts, 'America/Los_Angeles') AS STRING)
FROM timestamp_labels

-- The opt-in native timezone conversion must also return a UTC-labelled TIMESTAMP.
-- Use a zone without DST here to isolate labels from the opt-in kernel's DST value differences.
-- The session still varies, so hour and STRING must interpret the output as an instant.
query expect_native(to_utc_timestamp, equalto, lessthan, hour, cast)
SELECT
  to_utc_timestamp(ts, 'Asia/Tokyo') = ts,
  to_utc_timestamp(ts, 'Asia/Tokyo') < ts,
  hour(to_utc_timestamp(ts, 'Asia/Tokyo')),
  CAST(to_utc_timestamp(ts, 'Asia/Tokyo') AS STRING)
FROM timestamp_labels

-- DECIMAL dispatches to Spark codegen; its output schema must attach the UTC label.
query expect_dispatch(timestamp_seconds)
SELECT
  timestamp_seconds(dec) = ts,
  timestamp_seconds(dec) < ts,
  hour(timestamp_seconds(dec)),
  CAST(timestamp_seconds(dec) AS STRING)
FROM timestamp_labels

-- A query has one assertion mode, so separately pin the dispatch result's consumers to native.
query expect_native(equalto, lessthan, hour, cast)
SELECT
  timestamp_seconds(dec) = ts,
  timestamp_seconds(dec) < ts,
  hour(timestamp_seconds(dec)),
  CAST(timestamp_seconds(dec) AS STRING)
FROM timestamp_labels
