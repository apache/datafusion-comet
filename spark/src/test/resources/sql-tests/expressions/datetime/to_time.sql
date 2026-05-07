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

statement
CREATE TABLE test_to_time(s STRING) USING parquet

statement
INSERT INTO test_to_time VALUES
  ('00:00'),
  ('12:30'),
  ('23:59'),
  ('12:30:45'),
  ('00:00:00'),
  ('23:59:59'),
  ('00:00:00.1'),
  ('00:00:00.001'),
  ('00:00:00.000001'),
  ('23:59:59.999999'),
  ('1:2:3'),
  ('1:2:3.04'),
  ('T12:30:45'),
  ('T1:02:3.04'),
  ('12:00:00 AM'),
  ('1:00:00 AM'),
  ('12:00:00 PM'),
  ('1:00:00 PM'),
  ('11:59:59 PM'),
  ('12:59:59.999999 PM'),
  ('12:00:00AM'),
  ('1:00:00PM'),
  (NULL)

-- column argument: basic time formats
query
SELECT s, to_time(s) FROM test_to_time ORDER BY s

-- literal HH:mm
query
SELECT to_time('00:00')

query
SELECT to_time('12:30')

query
SELECT to_time('23:59')

-- literal HH:mm:ss
query
SELECT to_time('12:30:45')

query
SELECT to_time('00:00:00')

query
SELECT to_time('23:59:59')

-- fractional seconds
query
SELECT to_time('00:00:00.1')

query
SELECT to_time('00:00:00.001')

query
SELECT to_time('00:00:00.000001')

query
SELECT to_time('23:59:59.999999')

-- single digit hour/min/sec
query
SELECT to_time('1:2:3')

query
SELECT to_time('1:2:3.04')

-- T-prefix
query
SELECT to_time('T12:30:45')

query
SELECT to_time('T1:02:3.04')

-- AM/PM
query
SELECT to_time('12:00:00 AM')

query
SELECT to_time('1:00:00 AM')

query
SELECT to_time('11:59:59 AM')

query
SELECT to_time('12:00:00 PM')

query
SELECT to_time('1:00:00 PM')

query
SELECT to_time('11:59:59 PM')

-- AM/PM case insensitive
query
SELECT to_time('12:00:00 am')

query
SELECT to_time('12:00:00 pm')

-- AM/PM without space
query
SELECT to_time('12:00:00AM')

query
SELECT to_time('1:00:00PM')

-- AM/PM with fractional seconds
query
SELECT to_time('12:59:59.999999 PM')

-- null input
query
SELECT to_time(NULL)

-- trailing whitespace
query
SELECT to_time('12:30:45  ')

-- invalid inputs - should throw error with to_time
query expect_error(cannot be parsed to a TIME value)
SELECT to_time('')

query expect_error(cannot be parsed to a TIME value)
SELECT to_time('XYZ')

query expect_error(cannot be parsed to a TIME value)
SELECT to_time('24:00:00')

query expect_error(cannot be parsed to a TIME value)
SELECT to_time('23:60:00')

query expect_error(cannot be parsed to a TIME value)
SELECT to_time('23:00:60')

query expect_error(cannot be parsed to a TIME value)
SELECT to_time('120000')

-- invalid AM/PM - should throw error
query expect_error(cannot be parsed to a TIME value)
SELECT to_time('0:00:00 AM')

query expect_error(cannot be parsed to a TIME value)
SELECT to_time('13:00:00 AM')

query expect_error(cannot be parsed to a TIME value)
SELECT to_time('13:00:00 PM')

-- try_to_time: returns null for invalid inputs
query
SELECT try_to_time('12:30:45')

query
SELECT try_to_time('')

query
SELECT try_to_time('XYZ')

query
SELECT try_to_time('24:00:00')

query
SELECT try_to_time('23:60:00')

query
SELECT try_to_time(NULL)

query
SELECT try_to_time('0:00:00 AM')

query
SELECT try_to_time('13:00:00 PM')
