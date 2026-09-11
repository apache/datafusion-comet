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

-- date - date resolves to SubtractDates and runs through the codegen dispatcher so results
-- match Spark exactly. The output type follows spark.sql.legacy.interval.enabled: a
-- DayTimeIntervalType(DAY) by default, a CalendarIntervalType in legacy mode.
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.comet.shuffle.mode=native
-- ConfigMatrix: spark.sql.legacy.interval.enabled=false,true

statement
CREATE TABLE test_subtract_dates(d1 date, d2 date, k int) USING parquet

-- the 2300 rows span about 330 years, past the 292-year limit of a nanosecond long, but the
-- day count of a date difference never touches that field
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

-- column - column in both directions, covering negative and zero spans
query
SELECT d1, d2, d1 - d2, d2 - d1 FROM test_subtract_dates

-- literal on either side
query
SELECT d1 - date'2024-01-01', date'2024-01-01' - d2 FROM test_subtract_dates

-- all-literal operands (constant folding is disabled by the test suite). A NULL literal operand
-- is left out: NullPropagation folds it to a null interval literal, and the native literal
-- path rejects CalendarIntervalType (#5058). NULL operands are covered by the column rows above.
query
SELECT
  date'2024-03-15' - date'2024-01-01',
  date'2024-01-01' - date'2024-03-15',
  date'2024-01-01' - date'2024-01-01'

-- interval output through native shuffle, at top level and nested in a struct
query
SELECT k, d1 - d2 AS i, named_struct('i', d2 - d1) AS s
FROM test_subtract_dates
DISTRIBUTE BY k
