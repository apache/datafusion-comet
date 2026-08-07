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

-- DATE_ADD and DATE_DIFF joined the timestampadd/timestampdiff grammar rules in Spark 3.5.
-- With a datetimeUnit keyword as the first argument they parse to TimestampAdd/TimestampDiff
-- and run through the codegen dispatcher, not to the two-argument DateAdd/DateDiff. The
-- two-argument forms stay native and are covered here so both spellings are pinned together.
-- See timestampadd.sql and timestampdiff.sql for the unit and DST coverage.
-- MinSparkVersion: 3.5
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_date_add_unit(ts timestamp, d date, q int) USING parquet

statement
INSERT INTO test_date_add_unit VALUES
  (timestamp'2024-01-15 10:30:45', date'2024-01-15', 3),
  (timestamp'2024-01-31 23:00:00', date'2024-01-31', 1),
  (timestamp'2024-02-29 12:00:00', date'2024-02-29', -5),
  (NULL, NULL, 1),
  (timestamp'2024-06-15 00:00:00', date'2024-06-15', NULL)

-- unit form parses to TimestampAdd
query
SELECT
  date_add(DAY, q, ts),
  date_add(MONTH, 1, ts),
  date_add(HOUR, 6, ts),
  date_add(MICROSECOND, 500, ts)
FROM test_date_add_unit

-- unit form parses to TimestampDiff
query
SELECT
  date_diff(DAY, ts, timestamp'2024-07-01 00:00:00'),
  date_diff(MONTH, ts, timestamp'2024-07-01 00:00:00'),
  date_diff(HOUR, ts, timestamp'2024-07-01 00:00:00'),
  date_diff(QUARTER, ts, timestamp'2024-07-01 00:00:00')
FROM test_date_add_unit

-- the two-argument forms are unaffected and stay on DateAdd / DateDiff
query
SELECT
  date_add(d, q),
  date_diff(d, date'2024-07-01')
FROM test_date_add_unit
