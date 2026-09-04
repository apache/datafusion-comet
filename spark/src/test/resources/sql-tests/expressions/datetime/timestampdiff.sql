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

-- timestampdiff runs through the codegen dispatcher so results match Spark exactly.
-- America/Los_Angeles is pinned so the DST cases below straddle real transitions.
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_timestampdiff(a timestamp, b timestamp, a_ntz timestamp_ntz, b_ntz timestamp_ntz) USING parquet

statement
INSERT INTO test_timestampdiff VALUES
  (timestamp'2024-01-01 00:00:00', timestamp'2024-03-15 12:30:00', timestamp_ntz'2024-01-01 00:00:00', timestamp_ntz'2024-03-15 12:30:00'),
  (timestamp'2024-03-15 12:30:00', timestamp'2024-01-01 00:00:00', timestamp_ntz'2024-03-15 12:30:00', timestamp_ntz'2024-01-01 00:00:00'),
  (timestamp'2024-01-31 00:00:00', timestamp'2024-02-29 00:00:00', timestamp_ntz'2024-01-31 00:00:00', timestamp_ntz'2024-02-29 00:00:00'),
  (timestamp'2020-02-29 00:00:00', timestamp'2024-02-29 00:00:00', timestamp_ntz'2020-02-29 00:00:00', timestamp_ntz'2024-02-29 00:00:00'),
  (NULL, timestamp'2024-01-01 00:00:00', NULL, timestamp_ntz'2024-01-01 00:00:00'),
  (timestamp'2024-01-01 00:00:00', NULL, timestamp_ntz'2024-01-01 00:00:00', NULL)

-- Fully read NTZ Parquet input before testing native kernels.
statement
CACHE TABLE test_timestampdiff

-- whole-unit differences are truncated toward zero, matching Spark
query
SELECT
  timestampdiff(YEAR, a, b),
  timestampdiff(MONTH, a, b),
  timestampdiff(WEEK, a, b),
  timestampdiff(DAY, a, b),
  timestampdiff(HOUR, a, b),
  timestampdiff(MINUTE, a, b),
  timestampdiff(SECOND, a, b)
FROM test_timestampdiff

-- the remaining entries of timestampDiffMap. QUARTER is the only entry with arithmetic of
-- its own (MONTHS.between(...) / 3, integer division toward zero).
query
SELECT
  timestampdiff(QUARTER, a, b),
  timestampdiff(MILLISECOND, a, b),
  timestampdiff(MICROSECOND, a, b)
FROM test_timestampdiff

-- TIMESTAMP_NTZ inputs. inputTypes is Seq(TimestampType, TimestampType), so NTZ is cast up.
query
SELECT
  timestampdiff(YEAR, a_ntz, b_ntz),
  timestampdiff(MONTH, a_ntz, b_ntz),
  timestampdiff(HOUR, a_ntz, b_ntz),
  timestampdiff(MICROSECOND, a_ntz, b_ntz)
FROM test_timestampdiff

-- the grammar routes TIMESTAMPDIFF and DATEDIFF to the same TimestampDiff node whenever the
-- first argument is a datetimeUnit keyword, so the alias spelling must land on this serde
-- rather than on the two-argument DateDiff. DATE_DIFF joined the rule in Spark 3.5 and is
-- covered by date_add_unit_alias.sql.
query
SELECT
  datediff(DAY, a, b),
  datediff(MONTH, a, b),
  datediff(HOUR, a, b)
FROM test_timestampdiff

-- DST boundaries. timestampdiff converts both sides with getLocalDateTime and then calls
-- ChronoUnit.X.between on the local values, so a day spanning the spring-forward transition
-- still reports 24 hours even though only 23 real hours elapse.
query
SELECT
  timestampdiff(HOUR, timestamp'2024-03-09 12:00:00', timestamp'2024-03-10 12:00:00'),
  timestampdiff(DAY, timestamp'2024-03-09 12:00:00', timestamp'2024-03-10 12:00:00'),
  timestampdiff(HOUR, timestamp'2024-11-02 12:00:00', timestamp'2024-11-03 12:00:00'),
  timestampdiff(DAY, timestamp'2024-11-02 12:00:00', timestamp'2024-11-03 12:00:00')

-- across the transition instants themselves
query
SELECT
  timestampdiff(MINUTE, timestamp'2024-03-10 01:30:00', timestamp'2024-03-10 03:30:00'),
  timestampdiff(SECOND, timestamp'2024-11-03 00:30:00', timestamp'2024-11-03 02:30:00'),
  timestampdiff(MICROSECOND, timestamp'2024-03-10 01:59:59', timestamp'2024-03-10 03:00:00')

-- literal arguments (constant folding is disabled by the test suite)
query
SELECT
  timestampdiff(MONTH, timestamp'2024-01-31 00:00:00', timestamp'2024-02-29 00:00:00'),
  timestampdiff(HOUR, timestamp'2024-01-01 00:00:00', timestamp'2024-01-02 06:00:00'),
  timestampdiff(QUARTER, timestamp'2024-01-01 00:00:00', timestamp'2024-08-15 00:00:00'),
  timestampdiff(DAY, timestamp'2024-03-15 12:30:00', timestamp'2024-01-01 00:00:00')
