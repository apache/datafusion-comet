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

-- MinSparkVersion: 4.1
-- Config: spark.sql.timeType.enabled=true

statement
CREATE TABLE test_make_time(hours int, minutes int, secs decimal(16,6)) USING parquet

statement
INSERT INTO test_make_time VALUES
  (0, 0, 0.000000),
  (12, 30, 45.123456),
  (23, 59, 59.999999),
  (1, 2, 3.500000),
  (0, 0, 0.000001),
  (NULL, 0, 0.000000),
  (12, NULL, 30.000000),
  (12, 30, NULL),
  (NULL, NULL, NULL)

-- column arguments (spark_answer_only: shuffle does not support TimeType yet; TODO: promote to
-- full native-verification once SPARK-51779 lands)
query spark_answer_only
SELECT hours, minutes, secs, make_time(hours, minutes, secs) FROM test_make_time ORDER BY hours, minutes, secs

-- literal hour, column minutes and secs (spark_answer_only: shuffle does not support TimeType yet)
query spark_answer_only
SELECT make_time(10, minutes, secs) FROM test_make_time ORDER BY minutes, secs

-- column hours, literal minutes and secs (spark_answer_only: shuffle does not support TimeType yet)
query spark_answer_only
SELECT make_time(hours, 15, 30.5) FROM test_make_time ORDER BY hours

-- all literals
query
SELECT make_time(0, 0, 0)

query
SELECT make_time(12, 30, 45.123456)

query
SELECT make_time(23, 59, 59.999999)

-- midnight
query
SELECT make_time(0, 0, 0.0)

-- one microsecond after midnight
query
SELECT make_time(0, 0, 0.000001)

-- end of day
query
SELECT make_time(23, 59, 59.999999)

-- null handling with literals
query
SELECT make_time(NULL, 0, 0)

query
SELECT make_time(12, NULL, 0)

query
SELECT make_time(12, 30, NULL)

-- integer seconds (implicit cast to decimal)
query
SELECT make_time(10, 20, 30)

query
SELECT make_time(1, 2, 0)

-- boundary valid values
query
SELECT make_time(0, 0, 0)

query
SELECT make_time(23, 0, 0)

query
SELECT make_time(0, 59, 0)

query
SELECT make_time(0, 0, 59.999999)

-- invalid hours - should throw error
query expect_error(HourOfDay)
SELECT make_time(24, 0, 0)

query expect_error(HourOfDay)
SELECT make_time(25, 2, 23.5)

query expect_error(HourOfDay)
SELECT make_time(-1, 0, 0)

-- invalid minutes - should throw error
query expect_error(MinuteOfHour)
SELECT make_time(12, 60, 0)

query expect_error(MinuteOfHour)
SELECT make_time(23, -1, 23.5)

-- invalid seconds - should throw error
query expect_error(SecondOfMinute)
SELECT make_time(12, 30, 60.0)

query expect_error(SecondOfMinute)
SELECT make_time(23, 12, 100.5)

query expect_error(SecondOfMinute)
SELECT make_time(0, 0, -1.0)

-- overflow seconds
query expect_error(SecondOfMinute)
SELECT make_time(1, 18, 4294967297.999999)

-- time literal in comparison (make_time with all literals is constant-folded to a Time literal)
query
SELECT make_time(12, 30, 0) > make_time(11, 0, 0)

query
SELECT make_time(0, 0, 0) = make_time(0, 0, 0)

-- current_time() is foldable and produces a Time literal
query
SELECT current_time() IS NOT NULL
