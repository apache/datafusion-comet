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

-- Tests for try_to_date, try_to_timestamp, and try_make_timestamp.
-- These RuntimeReplaceable functions rewrite to Cast/GetTimestamp/MakeTimestamp(failOnError=false)
-- and return NULL on invalid input instead of throwing.
-- try_to_date and try_make_timestamp are only available in Spark 4.1+, so gate the file.
-- MinSparkVersion: 4.1
-- Config: spark.sql.session.timeZone=UTC

-- ------------------------------------------------------------
-- Table for try_to_date and try_to_timestamp tests
-- ------------------------------------------------------------

statement
CREATE TABLE test_try_datetime_str(s string) USING parquet

statement
INSERT INTO test_try_datetime_str VALUES
  ('2024-06-15'),
  ('1970-01-01'),
  ('2024-12-31'),
  ('2020-13-99'),
  ('not-a-date'),
  (''),
  (NULL)

-- ------------------------------------------------------------
-- try_to_date: column argument, no format
-- ------------------------------------------------------------

-- column argument, default format, falls back to Spark
query spark_answer_only
SELECT s, try_to_date(s) FROM test_try_datetime_str

-- literal valid date
query spark_answer_only
SELECT try_to_date('2024-06-15')

-- literal invalid date -> NULL
query spark_answer_only
SELECT try_to_date('2020-13-99')

-- literal NULL -> NULL
query spark_answer_only
SELECT try_to_date(NULL)

-- column argument with explicit format
query spark_answer_only
SELECT s, try_to_date(s, 'yyyy-MM-dd') FROM test_try_datetime_str

-- literal with explicit format
query spark_answer_only
SELECT try_to_date('15/06/2024', 'dd/MM/yyyy')

-- literal invalid with format -> NULL
query spark_answer_only
SELECT try_to_date('99/99/9999', 'dd/MM/yyyy')

-- ------------------------------------------------------------
-- Table for try_to_timestamp with timestamp strings
-- ------------------------------------------------------------

statement
CREATE TABLE test_try_datetime_ts(s string) USING parquet

statement
INSERT INTO test_try_datetime_ts VALUES
  ('2024-06-15 10:30:45'),
  ('1970-01-01 00:00:00'),
  ('2024-12-31 23:59:59'),
  ('2020-13-99 25:61:61'),
  ('not-a-timestamp'),
  (''),
  (NULL)

-- ------------------------------------------------------------
-- try_to_timestamp: column argument, no format
-- ------------------------------------------------------------

-- column argument, default format
query spark_answer_only
SELECT s, try_to_timestamp(s) FROM test_try_datetime_ts

-- literal valid timestamp
query spark_answer_only
SELECT try_to_timestamp('2024-06-15 10:30:45')

-- literal invalid timestamp -> NULL
query spark_answer_only
SELECT try_to_timestamp('2020-13-99 25:61:61')

-- literal NULL -> NULL
query spark_answer_only
SELECT try_to_timestamp(NULL)

-- column argument with explicit format
query spark_answer_only
SELECT s, try_to_timestamp(s, 'yyyy-MM-dd HH:mm:ss') FROM test_try_datetime_ts

-- literal with explicit format
query spark_answer_only
SELECT try_to_timestamp('15/06/2024 10:30:45', 'dd/MM/yyyy HH:mm:ss')

-- literal invalid with format -> NULL
query spark_answer_only
SELECT try_to_timestamp('99/99/9999 99:99:99', 'dd/MM/yyyy HH:mm:ss')

-- date-only string with date format
query spark_answer_only
SELECT try_to_timestamp('2024-06-15', 'yyyy-MM-dd')

-- ------------------------------------------------------------
-- Table for try_make_timestamp tests
-- ------------------------------------------------------------

statement
CREATE TABLE test_try_make_ts(y int, mo int, d int, h int, mi int, s decimal(8,6)) USING parquet

statement
INSERT INTO test_try_make_ts VALUES
  (2024, 6, 15, 10, 30, 45.123456),
  (1970, 1, 1, 0, 0, 0.0),
  (2024, 12, 31, 23, 59, 59.999999),
  (2024, 13, 1, 0, 0, 0.0),
  (2024, 6, 32, 0, 0, 0.0),
  (2024, 6, 15, 25, 0, 0.0),
  (NULL, 6, 15, 10, 30, 45.0),
  (2024, NULL, 15, 10, 30, 45.0)

-- ------------------------------------------------------------
-- try_make_timestamp: column arguments, no timezone
-- ------------------------------------------------------------

-- column arguments (invalid rows -> NULL instead of error)
-- Comet runs natively but returns wrong values for invalid inputs instead of NULL
query ignore(https://github.com/apache/datafusion-comet/issues/4554)
SELECT y, mo, d, h, mi, s, try_make_timestamp(y, mo, d, h, mi, s) FROM test_try_make_ts

-- column arguments with explicit UTC timezone (same bug: invalid rows return wrong values)
query ignore(https://github.com/apache/datafusion-comet/issues/4554)
SELECT try_make_timestamp(y, mo, d, h, mi, s, 'UTC') FROM test_try_make_ts

-- literal valid timestamp
query spark_answer_only
SELECT try_make_timestamp(2024, 6, 15, 10, 30, 45.0)

-- literal invalid month -> NULL (Comet returns wrong value instead of NULL)
query ignore(https://github.com/apache/datafusion-comet/issues/4554)
SELECT try_make_timestamp(2024, 13, 1, 0, 0, 0.0)

-- literal invalid day -> NULL (Comet returns wrong value instead of NULL)
query ignore(https://github.com/apache/datafusion-comet/issues/4554)
SELECT try_make_timestamp(2024, 2, 30, 0, 0, 0.0)

-- literal invalid hour -> NULL (Comet returns wrong value instead of NULL)
query ignore(https://github.com/apache/datafusion-comet/issues/4554)
SELECT try_make_timestamp(2024, 6, 15, 25, 0, 0.0)

-- literal NULL component -> NULL
query spark_answer_only
SELECT try_make_timestamp(NULL, 6, 15, 10, 30, 45.0)

-- literal with explicit timezone
query spark_answer_only
SELECT try_make_timestamp(2024, 6, 15, 10, 30, 45.0, 'America/Los_Angeles')
