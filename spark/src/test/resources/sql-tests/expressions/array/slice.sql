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

statement
CREATE TABLE test_slice(int_arr array<int>, str_arr array<string>, start_idx int, len int) USING parquet

statement
INSERT INTO test_slice VALUES
  (array(1, 2, 3, 4, 5), array('a', 'b', 'c', 'd', 'e'), 2, 3),
  (array(1, 2, NULL, 4), array('a', NULL, 'c'), 1, 2),
  (array(10, 20, 30), array('x', 'y', 'z'), -2, 2),
  (array(), array(), 1, 1),
  (NULL, NULL, 1, 2),
  (array(1, 2, 3), array('a', 'b', 'c'), 4, 1),
  (array(1, 2, 3), array('a', 'b', 'c'), -10, 2),
  (array(1, 2, 3, 4), array('a', 'b', 'c', 'd'), -2, 100),
  (array(7, 8, 9), array('p', 'q', 'r'), NULL, 2),
  (array(7, 8, 9), array('p', 'q', 'r'), 1, NULL)

-- all literal arguments
query
SELECT slice(array(1, 2, 3, 4, 5), 2, 3)

query
SELECT slice(array(1, 2, 3, 4, 5), 1, 5)

query
SELECT slice(array(1, 2, 3, 4, 5), -2, 2)

query
SELECT slice(array(1, 2, 3, 4, 5), -3, 5)

-- length 0 returns empty array
query
SELECT slice(array(1, 2, 3, 4), 1, 0)

-- length larger than remaining elements is clamped
query
SELECT slice(array(1, 2, 3, 4), 2, 100)

-- start past end returns empty array
query
SELECT slice(array(1, 2, 3), 10, 1)

-- empty array input
query
SELECT slice(array(), 1, 1)

-- NULL array returns NULL
query
SELECT slice(cast(NULL as array<int>), 1, 2)

-- NULL start returns NULL
query
SELECT slice(array(1, 2, 3), cast(NULL as int), 2)

-- NULL length returns NULL
query
SELECT slice(array(1, 2, 3), 1, cast(NULL as int))

-- column array + column start + column length
query
SELECT slice(int_arr, start_idx, len) FROM test_slice

-- column array + literal start + literal length
query
SELECT slice(int_arr, 1, 2) FROM test_slice

-- column array + column start + literal length
query
SELECT slice(int_arr, start_idx, 2) FROM test_slice

-- column array + literal start + column length
query
SELECT slice(int_arr, 1, len) FROM test_slice

-- string column array + column args
query
SELECT slice(str_arr, start_idx, len) FROM test_slice

-- string column array + literal args
query
SELECT slice(str_arr, 1, 2) FROM test_slice

-- expressions for start / length
query
SELECT slice(int_arr, start_idx + 1, len - 1) FROM test_slice WHERE start_idx > 0 AND len > 1

-- boolean arrays
statement
CREATE TABLE test_slice_bool(arr array<boolean>) USING parquet

statement
INSERT INTO test_slice_bool VALUES
  (array(true, false, true, false)),
  (array(true)),
  (NULL),
  (array())

query
SELECT slice(arr, 1, 2) FROM test_slice_bool

query
SELECT slice(arr, -1, 1) FROM test_slice_bool

-- tinyint arrays
statement
CREATE TABLE test_slice_byte(arr array<tinyint>) USING parquet

statement
INSERT INTO test_slice_byte VALUES
  (array(cast(1 as tinyint), cast(2 as tinyint), cast(3 as tinyint))),
  (array(cast(-128 as tinyint), cast(0 as tinyint), cast(127 as tinyint))),
  (NULL)

query
SELECT slice(arr, 2, 2) FROM test_slice_byte

-- smallint arrays
statement
CREATE TABLE test_slice_short(arr array<smallint>) USING parquet

statement
INSERT INTO test_slice_short VALUES
  (array(cast(100 as smallint), cast(200 as smallint), cast(300 as smallint))),
  (array(cast(-1 as smallint), cast(0 as smallint))),
  (NULL)

query
SELECT slice(arr, 1, 2) FROM test_slice_short

-- bigint arrays
statement
CREATE TABLE test_slice_long(arr array<bigint>) USING parquet

statement
INSERT INTO test_slice_long VALUES
  (array(cast(1000000000000 as bigint), cast(2000000000000 as bigint), cast(3000000000000 as bigint))),
  (array(cast(-1 as bigint), cast(0 as bigint), cast(1 as bigint))),
  (NULL)

query
SELECT slice(arr, -2, 2) FROM test_slice_long

-- float arrays
statement
CREATE TABLE test_slice_float(arr array<float>) USING parquet

statement
INSERT INTO test_slice_float VALUES
  (array(cast(1.1 as float), cast(2.2 as float), cast(3.3 as float))),
  (array(cast(0.0 as float), cast(-1.5 as float), cast('NaN' as float), cast('Infinity' as float))),
  (NULL)

query
SELECT slice(arr, 1, 2) FROM test_slice_float

-- double arrays
statement
CREATE TABLE test_slice_double(arr array<double>) USING parquet

statement
INSERT INTO test_slice_double VALUES
  (array(1.1, 2.2, 3.3, 4.4)),
  (array(0.0, -1.5, cast('NaN' as double), cast('Infinity' as double))),
  (NULL)

query
SELECT slice(arr, 2, 3) FROM test_slice_double

-- decimal arrays
statement
CREATE TABLE test_slice_decimal(arr array<decimal(10,2)>) USING parquet

statement
INSERT INTO test_slice_decimal VALUES
  (array(cast(1.10 as decimal(10,2)), cast(2.20 as decimal(10,2)), cast(3.30 as decimal(10,2)))),
  (array(cast(0.00 as decimal(10,2)))),
  (NULL)

query
SELECT slice(arr, 1, 2) FROM test_slice_decimal

-- date arrays
statement
CREATE TABLE test_slice_date(arr array<date>) USING parquet

statement
INSERT INTO test_slice_date VALUES
  (array(date '2024-01-01', date '2024-06-15', date '2024-12-31')),
  (array(date '2000-01-01')),
  (NULL)

query
SELECT slice(arr, 2, 1) FROM test_slice_date

-- timestamp arrays
statement
CREATE TABLE test_slice_ts(arr array<timestamp>) USING parquet

statement
INSERT INTO test_slice_ts VALUES
  (array(timestamp '2024-01-01 00:00:00', timestamp '2024-06-15 12:30:00', timestamp '2024-12-31 23:59:59')),
  (array(timestamp '2000-01-01 00:00:00')),
  (NULL)

query
SELECT slice(arr, 1, 2) FROM test_slice_ts

-- timestamp_ntz arrays
statement
CREATE TABLE test_slice_ts_ntz(arr array<timestamp_ntz>) USING parquet

statement
INSERT INTO test_slice_ts_ntz VALUES
  (array(timestamp_ntz '2024-01-01 00:00:00', timestamp_ntz '2024-06-15 12:30:00', timestamp_ntz '2024-12-31 23:59:59')),
  (array(timestamp_ntz '2000-01-01 00:00:00')),
  (NULL)

query
SELECT slice(arr, 1, 2) FROM test_slice_ts_ntz

-- nested int arrays
statement
CREATE TABLE test_slice_nested_int(arr array<array<int>>) USING parquet

statement
INSERT INTO test_slice_nested_int VALUES
  (array(array(1, 2), array(3, 4), array(5, 6))),
  (array(array(1, 2, 3))),
  (NULL)

query
SELECT slice(arr, 1, 2) FROM test_slice_nested_int

-- nested string arrays
statement
CREATE TABLE test_slice_nested_str(arr array<array<string>>) USING parquet

statement
INSERT INTO test_slice_nested_str VALUES
  (array(array('a', 'b'), array('c', 'd'), array('e', 'f'))),
  (array(array('a'))),
  (NULL)

query
SELECT slice(arr, -2, 2) FROM test_slice_nested_str
