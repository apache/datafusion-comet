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
-- aliases, region ids and offsets with a seconds field. Every one of the 28 SHORT_IDS aliases
-- appears with a January and a July date, so Spark is the reference for each alias target,
-- including the daylight saving time of the region ones (PST follows PDT, NST follows NZST/NZDT)
-- and the fixed-offset ones (EST stays -05:00).
-- Config: spark.sql.ansi.enabled=false
-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles

statement
CREATE TABLE zone_id_valid(s string) USING parquet

statement
INSERT INTO zone_id_valid VALUES
  ('2020-01-15 12:34:56 ACT'), ('2020-07-15 12:34:56 ACT'),
  ('2020-01-15 12:34:56 AET'), ('2020-07-15 12:34:56 AET'),
  ('2020-01-15 12:34:56 AGT'), ('2020-07-15 12:34:56 AGT'),
  ('2020-01-15 12:34:56 ART'), ('2020-07-15 12:34:56 ART'),
  ('2020-01-15 12:34:56 AST'), ('2020-07-15 12:34:56 AST'),
  ('2020-01-15 12:34:56 BET'), ('2020-07-15 12:34:56 BET'),
  ('2020-01-15 12:34:56 BST'), ('2020-07-15 12:34:56 BST'),
  ('2020-01-15 12:34:56 CAT'), ('2020-07-15 12:34:56 CAT'),
  ('2020-01-15 12:34:56 CNT'), ('2020-07-15 12:34:56 CNT'),
  ('2020-01-15 12:34:56 CST'), ('2020-07-15 12:34:56 CST'),
  ('2020-01-15 12:34:56 CTT'), ('2020-07-15 12:34:56 CTT'),
  ('2020-01-15 12:34:56 EAT'), ('2020-07-15 12:34:56 EAT'),
  ('2020-01-15 12:34:56 ECT'), ('2020-07-15 12:34:56 ECT'),
  ('2020-01-15 12:34:56 IET'), ('2020-07-15 12:34:56 IET'),
  ('2020-01-15 12:34:56 IST'), ('2020-07-15 12:34:56 IST'),
  ('2020-01-15 12:34:56 JST'), ('2020-07-15 12:34:56 JST'),
  ('2020-01-15 12:34:56 MIT'), ('2020-07-15 12:34:56 MIT'),
  ('2020-01-15 12:34:56 NET'), ('2020-07-15 12:34:56 NET'),
  ('2020-01-15 12:34:56 NST'), ('2020-07-15 12:34:56 NST'),
  ('2020-01-15 12:34:56 PLT'), ('2020-07-15 12:34:56 PLT'),
  ('2020-01-15 12:34:56 PNT'), ('2020-07-15 12:34:56 PNT'),
  ('2020-01-15 12:34:56 PRT'), ('2020-07-15 12:34:56 PRT'),
  ('2020-01-15 12:34:56 PST'), ('2020-07-15 12:34:56 PST'),
  ('2020-01-15 12:34:56 SST'), ('2020-07-15 12:34:56 SST'),
  ('2020-01-15 12:34:56 VST'), ('2020-07-15 12:34:56 VST'),
  ('2020-01-15 12:34:56 EST'), ('2020-07-15 12:34:56 EST'),
  ('2020-01-15 12:34:56 MST'), ('2020-07-15 12:34:56 MST'),
  ('2020-01-15 12:34:56 HST'), ('2020-07-15 12:34:56 HST'),
  ('2020-01-01 12:34:56 CET'), ('2020-07-01 12:34:56 CET'),
  ('2020-01-01 12:34:56Zulu'), ('2020-01-01 12:34:56 Zulu'),
  ('2020-07-01 12:34:56America/Los_Angeles'), ('2020-07-01 12:34:56.123 America/Los_Angeles'),
  ('2020-01-01 12:34:56 Etc/GMT+8'), ('2020-01-01 12:34:56 Etc/GMT-8'),
  ('2020-01-01 12:34:56 Etc/UTC'), ('2020-01-01 12:34:56 UTC'), ('2020-01-01 12:34:56GMT'),
  ('2020-01-01 12:34:56 +08:00:00'), ('2020-01-01 12:34:56 +8:00:00'),
  ('2020-01-01 12:34:56 UTC+8:00:00'), ('2020-01-01 12:34:56-08:00:00'),
  ('2020-01-01 12:34:56 +080000'), ('2020-01-01 12:34:56 UTC-080000'),
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
