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

-- cast(string as date) for years outside chrono's NaiveDate range (+/-262142). Spark accepts a
-- year of up to 7 digits and only rejects a date whose epoch day overflows Int, so these strings
-- are valid dates in every eval mode. unix_date makes the epoch day visible, since collected
-- dates past year 9999 can display truncated.

-- ConfigMatrix: spark.sql.ansi.enabled=false,true

statement
CREATE TABLE test_wide_year_date_str(s string) USING parquet

statement
INSERT INTO test_wide_year_date_str VALUES
  ('262143-01-01'),
  ('-262144-01-01'),
  ('294248-01-01'),
  ('-290309-01-01'),
  ('999999-01-01'),
  ('1000000-01-01'),
  ('+1000000-01-01T12:34:56'),
  ('-0973250'),
  ('300000-02-29'),
  ('5881580-07-11'),
  ('-5877641-06-23'),
  ('2020-01-01'),
  (NULL)

query
SELECT s, unix_date(cast(s AS date)) FROM test_wide_year_date_str

query
SELECT s, unix_date(try_cast(s AS date)) FROM test_wide_year_date_str

-- The date values themselves. The Int-boundary rows are left out because Spark cannot convert
-- them to java.sql.Date when collecting.
query
SELECT s, cast(s AS date), try_cast(s AS date) FROM test_wide_year_date_str
WHERE s NOT IN ('5881580-07-11', '-5877641-06-23')

-- to_date routes through the same cast
query
SELECT s, unix_date(to_date(s)) FROM test_wide_year_date_str

-- One day past either Int boundary, and an invalid calendar date past chrono's range, are
-- invalid in Spark too, so try_cast returns NULL.
statement
CREATE TABLE test_wide_year_date_invalid(s string) USING parquet

statement
INSERT INTO test_wide_year_date_invalid VALUES
  ('5881580-07-12'),
  ('-5877641-06-22'),
  ('300001-02-29'),
  ('10000000-01-01')

query
SELECT s, try_cast(s AS date) FROM test_wide_year_date_invalid
