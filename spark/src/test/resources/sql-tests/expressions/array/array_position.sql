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

-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_array_position(int_arr array<int>, str_arr array<string>, val int, str_val string) USING parquet

statement
INSERT INTO test_array_position VALUES
  (array(1, 2, 3, 4), array('a', 'b', 'c'), 2, 'b'),
  (array(1, 2, NULL, 3), array('a', NULL, 'c'), 3, 'c'),
  (array(10, 20, 30), array('x', 'y', 'z'), 99, 'w'),
  (array(), array(), 1, 'a'),
  (NULL, NULL, 1, 'a'),
  (array(1, 1, 1), array('a', 'a', 'a'), 1, 'a'),
  (array(5, 6, 7), array('p', 'q', 'r'), NULL, NULL)

-- literal args fall back to Spark
query spark_answer_only
SELECT array_position(array(1, 2, 3, 4), 3)

query spark_answer_only
SELECT array_position(array(1, 2, 3, 4), 5)

query spark_answer_only
SELECT array_position(array('a', 'b', 'c'), 'b')

query spark_answer_only
SELECT array_position(array(1, 2, NULL, 3), 3)

query spark_answer_only
SELECT array_position(array(1, 2, 3), cast(NULL as int))

query spark_answer_only
SELECT array_position(cast(NULL as array<int>), 1)

query spark_answer_only
SELECT array_position(array(), 1)

query spark_answer_only
SELECT array_position(array(1, 2, 1, 3), 1)

-- column array + column value (includes NULL val row)
query
SELECT array_position(int_arr, val) FROM test_array_position

-- column array + literal value
query
SELECT array_position(int_arr, 3) FROM test_array_position

-- literal array + column value
query
SELECT array_position(array(1, 2, 3), val) FROM test_array_position

-- string column array + column value (includes NULL str_val row)
query
SELECT array_position(str_arr, str_val) FROM test_array_position

-- string column array + literal value
query
SELECT array_position(str_arr, 'c') FROM test_array_position

-- expressions in array construction
query
SELECT array_position(array(val, val + 1, val + 2), val) FROM test_array_position

-- boolean arrays
statement
CREATE TABLE test_ap_bool(arr array<boolean>, val boolean) USING parquet

statement
INSERT INTO test_ap_bool VALUES
  (array(true, false, true), false),
  (array(true, true), false),
  (array(false, false), true),
  (NULL, true),
  (array(true, false), NULL)

query
SELECT array_position(arr, val) FROM test_ap_bool

-- tinyint arrays
statement
CREATE TABLE test_ap_byte(arr array<tinyint>, val tinyint) USING parquet

statement
INSERT INTO test_ap_byte VALUES
  (array(cast(1 as tinyint), cast(2 as tinyint), cast(3 as tinyint)), cast(2 as tinyint)),
  (array(cast(-128 as tinyint), cast(127 as tinyint)), cast(127 as tinyint)),
  (NULL, cast(1 as tinyint)),
  (array(cast(1 as tinyint)), NULL)

query
SELECT array_position(arr, val) FROM test_ap_byte

-- smallint arrays
statement
CREATE TABLE test_ap_short(arr array<smallint>, val smallint) USING parquet

statement
INSERT INTO test_ap_short VALUES
  (array(cast(100 as smallint), cast(200 as smallint), cast(300 as smallint)), cast(200 as smallint)),
  (NULL, cast(1 as smallint)),
  (array(cast(1 as smallint)), NULL)

query
SELECT array_position(arr, val) FROM test_ap_short

-- bigint arrays
statement
CREATE TABLE test_ap_long(arr array<bigint>, val bigint) USING parquet

statement
INSERT INTO test_ap_long VALUES
  (array(cast(1000000000000 as bigint), cast(2000000000000 as bigint)), cast(2000000000000 as bigint)),
  (array(cast(-1 as bigint), cast(0 as bigint), cast(1 as bigint)), cast(0 as bigint)),
  (NULL, cast(1 as bigint)),
  (array(cast(1 as bigint)), NULL)

query
SELECT array_position(arr, val) FROM test_ap_long

-- float arrays
statement
CREATE TABLE test_ap_float(arr array<float>, val float) USING parquet

statement
INSERT INTO test_ap_float VALUES
  (array(cast(1.1 as float), cast(2.2 as float), cast(3.3 as float)), cast(2.2 as float)),
  (array(cast(0.0 as float), cast(-1.5 as float)), cast(-1.5 as float)),
  (NULL, cast(1.0 as float)),
  (array(cast(1.0 as float)), NULL)

query
SELECT array_position(arr, val) FROM test_ap_float

-- double arrays
statement
CREATE TABLE test_ap_double(arr array<double>, val double) USING parquet

statement
INSERT INTO test_ap_double VALUES
  (array(1.1, 2.2, 3.3), 2.2),
  (array(0.0, -1.5), -1.5),
  (NULL, 1.0),
  (array(1.0), NULL)

query
SELECT array_position(arr, val) FROM test_ap_double

-- decimal arrays
statement
CREATE TABLE test_ap_decimal(arr array<decimal(10,2)>, val decimal(10,2)) USING parquet

statement
INSERT INTO test_ap_decimal VALUES
  (array(cast(1.10 as decimal(10,2)), cast(2.20 as decimal(10,2)), cast(3.30 as decimal(10,2))), cast(2.20 as decimal(10,2))),
  (array(cast(0.00 as decimal(10,2))), cast(0.00 as decimal(10,2))),
  (NULL, cast(1.00 as decimal(10,2))),
  (array(cast(1.00 as decimal(10,2))), NULL)

query
SELECT array_position(arr, val) FROM test_ap_decimal

-- date arrays
statement
CREATE TABLE test_ap_date(arr array<date>, val date) USING parquet

statement
INSERT INTO test_ap_date VALUES
  (array(date '2024-01-01', date '2024-06-15', date '2024-12-31'), date '2024-06-15'),
  (array(date '2000-01-01'), date '1999-12-31'),
  (NULL, date '2024-01-01'),
  (array(date '2024-01-01'), NULL)

query
SELECT array_position(arr, val) FROM test_ap_date

-- timestamp arrays
statement
CREATE TABLE test_ap_ts(arr array<timestamp>, val timestamp) USING parquet

statement
INSERT INTO test_ap_ts VALUES
  (array(timestamp '2024-01-01 00:00:00', timestamp '2024-06-15 12:30:00'), timestamp '2024-06-15 12:30:00'),
  (array(timestamp '2000-01-01 00:00:00'), timestamp '1999-12-31 23:59:59'),
  (NULL, timestamp '2024-01-01 00:00:00'),
  (array(timestamp '2024-01-01 00:00:00'), NULL)

query
SELECT array_position(arr, val) FROM test_ap_ts
