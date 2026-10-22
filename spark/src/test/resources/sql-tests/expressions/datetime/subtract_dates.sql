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

-- The result is DayTimeIntervalType(DAY) by default and CalendarIntervalType in legacy mode.
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.comet.shuffle.mode=native
-- ConfigMatrix: spark.sql.legacy.interval.enabled=false,true

statement
CREATE TABLE test_subtract_dates(d1 date, d2 date, k int) USING parquet

-- the 2300 rows span about 330 years, past the 292-year nanosecond limit, which the day count
-- of a date difference never touches
statement
INSERT INTO test_subtract_dates VALUES
  (date'2024-03-15', date'2024-01-01', 1),
  (date'2024-01-01', date'2024-03-15', 1),
  (date'2024-02-29', date'2023-02-28', 2),
  (date'1969-12-31', date'1970-01-02', 2),
  (date'2024-06-01', date'2024-06-01', 3),
  (date'1900-01-01', date'2100-12-31', 3),
  (date'2300-01-01', date'1970-01-01', 6),
  (date'1970-01-01', date'2300-01-01', 6),
  (NULL, date'2024-01-01', 4),
  (date'2024-01-01', NULL, 4),
  (NULL, NULL, 5)

query expect_dispatch(subtractdates)
SELECT d1, d2, d1 - d2, d2 - d1 FROM test_subtract_dates

query
SELECT d1 - date'2024-01-01', date'2024-01-01' - d2 FROM test_subtract_dates

-- no NULL literal: NullPropagation folds it before Comet sees the plan
query
SELECT
  date'2024-03-15' - date'2024-01-01',
  date'2024-01-01' - date'2024-03-15',
  date'2024-01-01' - date'2024-01-01'

query
SELECT k, d1 - d2 AS i, named_struct('i', d2 - d1) AS s
FROM test_subtract_dates
DISTRIBUTE BY k
