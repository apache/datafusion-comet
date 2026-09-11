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

-- date + year-month interval resolves to DateAddYMInterval and runs through the codegen
-- dispatcher so results match Spark exactly. Month arithmetic clamps to the last day of the
-- target month.
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.comet.shuffle.mode=native

statement
CREATE TABLE test_date_add_ym_interval(d date, y int, m int, k int) USING parquet

statement
INSERT INTO test_date_add_ym_interval VALUES
  (date'2024-01-31', 0, 1, 1),
  (date'2024-01-31', 0, 13, 1),
  (date'2024-02-29', 1, 0, 2),
  (date'2024-02-29', 4, 0, 2),
  (date'2024-03-31', 0, -1, 3),
  (date'2024-12-31', 0, 2, 3),
  (date'1970-01-01', -1, -1, 4),
  (date'2024-06-15', NULL, 1, 5),
  (date'2024-06-15', 1, NULL, 5),
  (NULL, 1, 1, 6)

-- column date plus an interval built from columns, both directions
query
SELECT d, y, m, d + make_ym_interval(y, m), make_ym_interval(y, m) + d
FROM test_date_add_ym_interval

-- interval literals in the unit, unit-to-unit and multi-unit spellings. Subtraction rewrites
-- to an addition of the negated interval.
query
SELECT
  d + INTERVAL '1' YEAR,
  d + INTERVAL '1' MONTH,
  d + INTERVAL '1-2' YEAR TO MONTH,
  d + INTERVAL '1 year 1 month',
  d + INTERVAL '-1' MONTH,
  d - INTERVAL '1' MONTH,
  d - INTERVAL '1' YEAR
FROM test_date_add_ym_interval

-- all-literal operands (constant folding is disabled by the test suite)
query
SELECT
  date'2024-01-31' + INTERVAL '1' MONTH,
  date'2024-02-29' + INTERVAL '1' YEAR,
  date'2024-03-31' - INTERVAL '1' MONTH,
  CAST(NULL AS DATE) + INTERVAL '1' MONTH,
  date'2024-01-31' + CAST(NULL AS INTERVAL YEAR TO MONTH)

-- date output through native shuffle
query
SELECT k, d + make_ym_interval(y, m) AS r
FROM test_date_add_ym_interval
DISTRIBUTE BY k
