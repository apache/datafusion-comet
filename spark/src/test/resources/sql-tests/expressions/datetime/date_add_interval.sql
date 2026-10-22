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

-- ANSI errors are in date_add_interval_ansi.sql. America/Los_Angeles is pinned so the 25-hour
-- row crosses DST.
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.comet.shuffle.mode=native

statement
CREATE TABLE test_date_add_interval(d date, y int, m int, dd int, h int, k int) USING parquet

statement
INSERT INTO test_date_add_interval VALUES
  (date'2024-01-31', 0, 1, 0, 0, 1),
  (date'2024-01-31', 1, 1, 1, 0, 1),
  (date'2024-02-29', 1, 0, 0, 0, 2),
  (date'2024-03-31', 0, -1, 0, 0, 2),
  (date'2024-12-31', 0, 0, 1, 0, 3),
  (date'2024-03-09', 0, 0, 1, 0, 3),
  (date'2024-03-09', 0, 0, 0, 25, 4),
  (date'1970-01-01', -1, -1, -1, -1, 4),
  (date'2024-06-15', NULL, 1, 1, 0, 5),
  (date'2024-06-15', 0, NULL, 1, 0, 5),
  (date'2024-06-15', 0, 0, NULL, 0, 6),
  (date'2024-06-15', 0, 0, 0, NULL, 6),
  (NULL, 1, 1, 1, 0, 7)

-- month arithmetic clamps to the end of the shorter month before the day part is added
query expect_dispatch(dateaddinterval)
SELECT d, y, m, dd, d + make_interval(y, m, 0, dd) FROM test_date_add_interval

query
SELECT make_interval(y, m, 0, dd) + d FROM test_date_add_interval

-- with ANSI off the hour part is applied on the timestamp and truncated away again
query
SELECT d, h, d + make_interval(y, m, 0, dd, h) FROM test_date_add_interval

-- make_interval, as the parser rejects literals mixing year-month and day-time units
query
SELECT
  d + make_interval(1, 0, 0, 1),
  d + make_interval(-1, 0, 0, -1),
  d + make_interval(0, 1, 0, 1),
  d + make_interval(0, 1, 0, 1, 12),
  d - make_interval(1, 0, 0, 1),
  d - make_interval(0, 1, 0, 1)
FROM test_date_add_interval

query
SELECT
  date'2024-01-31' + make_interval(0, 1, 0, 1),
  date'2024-02-29' + make_interval(1, 0, 0, 1),
  date'2024-01-31' - make_interval(0, 1, 0, 1)

query
SELECT k, d + make_interval(y, m, 0, dd) AS r
FROM test_date_add_interval
DISTRIBUTE BY k
