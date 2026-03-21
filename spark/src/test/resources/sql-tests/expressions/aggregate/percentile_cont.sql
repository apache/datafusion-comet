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

-- Tests for percentile_cont aggregate function
-- Uses similar test data as Spark's percentiles.sql

statement
CREATE TABLE test_percentile(k int, v int) USING parquet

statement
INSERT INTO test_percentile VALUES (0, 0), (0, 10), (0, 20), (0, 30), (0, 40), (1, 10), (1, 20), (2, 10), (2, 20), (2, 25), (2, 30), (3, 60), (4, NULL)

-- Basic percentile_cont (25th percentile) - should match Spark result: 10.0
query
SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY v) FROM test_percentile

-- percentile_cont with DESC ordering - should match Spark result: 30.0
query
SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC) FROM test_percentile

-- percentile_cont with GROUP BY - should match Spark results
query
SELECT k, percentile_cont(0.25) WITHIN GROUP (ORDER BY v) FROM test_percentile GROUP BY k ORDER BY k

-- percentile_cont with GROUP BY and DESC - should match Spark results
query
SELECT k, percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC) FROM test_percentile GROUP BY k ORDER BY k

-- median (50th percentile)
query
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM test_percentile

-- Multiple percentile_cont in same query
query
SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY v), percentile_cont(0.75) WITHIN GROUP (ORDER BY v) FROM test_percentile

-- Tests for interval types
statement
CREATE TABLE test_interval (
  id INT,
  ym INTERVAL YEAR TO MONTH,
  dt INTERVAL DAY TO SECOND
) USING parquet

statement
INSERT INTO test_interval VALUES
  (1, INTERVAL '1' YEAR, INTERVAL '1' DAY),
  (2, INTERVAL '2' YEAR, INTERVAL '2' DAY),
  (3, INTERVAL '3' YEAR, INTERVAL '3' DAY),
  (4, INTERVAL '4' YEAR, INTERVAL '4' DAY),
  (5, INTERVAL '5' YEAR, INTERVAL '5' DAY)

-- percentile_cont with YearMonthIntervalType
query
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY ym) FROM test_interval

-- percentile_cont with DayTimeIntervalType
query
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY dt) FROM test_interval

-- percentile_cont with interval types and GROUP BY
query
SELECT id % 2 AS grp, percentile_cont(0.5) WITHIN GROUP (ORDER BY ym) FROM test_interval GROUP BY id % 2 ORDER BY grp
