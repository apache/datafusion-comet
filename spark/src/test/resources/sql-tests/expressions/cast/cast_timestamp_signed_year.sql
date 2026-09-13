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

-- Config: spark.sql.ansi.enabled=false
-- ConfigMatrix: spark.sql.session.timeZone=UTC,+05:30

-- Parquet columns exercise the native parser, including every date/time segment shape.
statement
CREATE TABLE signed_year_valid(s string) USING parquet

statement
INSERT INTO signed_year_valid VALUES
  ('+7528'), ('+00463'), ('+79821'), ('+2976'), ('+0000'), ('+002020'),
  ('+2020-1'), ('+2020-1-2'), ('+2020-1-2T3'), ('+2020-1-2 3:4'),
  ('+2020-1-2T3:4:5'), ('+2020-1-2 3:4:5.'), ('+2020-1-2T3:4:5.123456789'),
  ('+2020-1-2T3:4:5Z'), ('+2020-1-2T3:4:5+05:30'), ('+2020-1-2T3:4:5-08:00'),
  ('+2020-1-2T3:4:5 UTC'), (' +7528 '), ('-0001'), ('-0001-1-2T3:4:5'), (NULL)

statement
CREATE TABLE signed_year_invalid(s string) USING parquet

statement
INSERT INTO signed_year_invalid VALUES
  ('+'), ('++2020'), ('+-2020'), ('-+2020'), ('--2020'), ('+020'), ('+0002020'),
  ('+ 2020'), ('+２０２０'), ('+2020-+1'), ('+2020--1'), ('+2020-1-+2'),
  ('+12:12:12'), ('+T12:12:12'), ('+2020Z'), ('+2020-1-2Z'), ('+2020-1-2T3:4Z'),
  ('++2020-1-2'), ('+-2020-1-2')

query expect_native(cast)
SELECT s, cast(s AS timestamp), cast(s AS timestamp_ntz) FROM signed_year_valid

query expect_native(cast)
SELECT s, cast(s AS timestamp), cast(s AS timestamp_ntz) FROM signed_year_invalid

query expect_native(try_cast)
SELECT s, try_cast(s AS timestamp), try_cast(s AS timestamp_ntz)
FROM (SELECT s FROM signed_year_valid UNION ALL SELECT s FROM signed_year_invalid)

statement
SET spark.sql.ansi.enabled=true

query expect_native(cast)
SELECT s, cast(s AS timestamp), cast(s AS timestamp_ntz) FROM signed_year_valid

query expect_native(try_cast)
SELECT s, try_cast(s AS timestamp), try_cast(s AS timestamp_ntz)
FROM (SELECT s FROM signed_year_valid UNION ALL SELECT s FROM signed_year_invalid)

-- Each malformed sign must throw independently in ANSI mode.
query expect_error(CAST_INVALID_INPUT)
SELECT cast(s AS timestamp) FROM signed_year_invalid WHERE s = '++2020'

query expect_error(CAST_INVALID_INPUT)
SELECT cast(s AS timestamp_ntz) FROM signed_year_invalid WHERE s = '++2020'

query expect_error(CAST_INVALID_INPUT)
SELECT cast(s AS timestamp) FROM signed_year_invalid WHERE s = '+-2020-1-2'

query expect_error(CAST_INVALID_INPUT)
SELECT cast(s AS timestamp_ntz) FROM signed_year_invalid WHERE s = '+-2020-1-2'

query expect_error(CAST_INVALID_INPUT)
SELECT cast(s AS timestamp) FROM signed_year_invalid WHERE s = '+12:12:12'

query expect_error(CAST_INVALID_INPUT)
SELECT cast(s AS timestamp_ntz) FROM signed_year_invalid WHERE s = '+12:12:12'
