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

-- Zone suffixes that Spark resolves through ZoneId.of(id, ZoneId.SHORT_IDS): SHORT_IDS
-- aliases, region ids and offsets with a seconds field. Summer dates separate the aliases
-- that map to a region (PST follows PDT) from the fixed-offset ones (EST stays -05:00).
-- Config: spark.sql.ansi.enabled=false
-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles

statement
CREATE TABLE zone_id_valid(s string) USING parquet

statement
INSERT INTO zone_id_valid VALUES
  ('2020-01-01 12:34:56 PST'), ('2020-07-01 12:34:56 PST'),
  ('2020-01-01 12:34:56 EST'), ('2020-07-01 12:34:56 EST'),
  ('2020-07-01 12:34:56 CST'), ('2020-07-01 12:34:56 JST'), ('2020-07-01 12:34:56 HST'),
  ('2020-01-01 12:34:56 CET'), ('2020-07-01 12:34:56 CET'),
  ('2020-01-01 12:34:56Zulu'), ('2020-01-01 12:34:56 Zulu'),
  ('2020-07-01 12:34:56America/Los_Angeles'), ('2020-07-01 12:34:56.123 America/Los_Angeles'),
  ('2020-01-01 12:34:56 Etc/GMT+8'), ('2020-01-01 12:34:56 Etc/GMT-8'),
  ('2020-01-01 12:34:56 Etc/UTC'), ('2020-01-01 12:34:56 UTC'), ('2020-01-01 12:34:56GMT'),
  ('2020-01-01 12:34:56 +08:00:00'), ('2020-01-01 12:34:56 +8:00:00'),
  ('2020-01-01 12:34:56 UTC+8:00:00'), ('2020-01-01 12:34:56-08:00:00'),
  (NULL)

statement
CREATE TABLE zone_id_invalid(s string) USING parquet

statement
INSERT INTO zone_id_invalid VALUES
  ('2020-01-01 12:34:56 pst'), ('2020-01-01 12:34:56 XYZ'),
  ('2020-01-01 12:34:56 +08:00:0'), ('2020-01-01 12:34:56 +8:0:00')

query expect_native(cast)
SELECT s, cast(s AS timestamp), cast(s AS timestamp_ntz) FROM zone_id_valid

query expect_native(cast)
SELECT s, cast(s AS timestamp), cast(s AS timestamp_ntz) FROM zone_id_invalid

query expect_native(try_cast)
SELECT s, try_cast(s AS timestamp), try_cast(s AS timestamp_ntz)
FROM (SELECT s FROM zone_id_valid UNION ALL SELECT s FROM zone_id_invalid)

query
SELECT cast('2020-07-01 12:34:56 PST' AS timestamp), cast('2020-07-01 12:34:56 EST' AS timestamp)

query
SELECT cast('2020-07-01 12:34:56 PST' AS timestamp_ntz), cast('2020-01-01 12:34:56 +8:00:00' AS timestamp_ntz)

statement
SET spark.sql.ansi.enabled=true

query expect_native(cast)
SELECT s, cast(s AS timestamp), cast(s AS timestamp_ntz) FROM zone_id_valid

query expect_error(CAST_INVALID_INPUT)
SELECT cast(s AS timestamp) FROM zone_id_invalid WHERE s = '2020-01-01 12:34:56 pst'

query expect_error(CAST_INVALID_INPUT)
SELECT cast(s AS timestamp) FROM zone_id_invalid WHERE s = '2020-01-01 12:34:56 +8:0:00'

query expect_error(CAST_INVALID_INPUT)
SELECT cast('2020-01-01 12:34:56 XYZ' AS timestamp)
