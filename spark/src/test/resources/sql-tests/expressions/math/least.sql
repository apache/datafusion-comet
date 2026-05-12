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
CREATE TABLE test_least(a int, b int, c int) USING parquet

statement
INSERT INTO test_least VALUES
  (1, 2, 3),
  (3, 2, 1),
  (NULL, 2, 3),
  (1, NULL, 3),
  (1, 2, NULL),
  (NULL, NULL, 3),
  (NULL, NULL, NULL),
  (-1, 0, 1),
  (2147483647, -2147483648, 0)

-- column arguments
query
SELECT least(a, b, c) FROM test_least

-- two column arguments
query
SELECT least(a, b) FROM test_least

-- literal arguments
query
SELECT least(1, 2, 3), least(3, 2, 1), least(-1, 0, 1)

-- NULL literal handling
query
SELECT least(NULL, 2, 3), least(1, NULL, 3), least(NULL, NULL, NULL)

-- mixed column and literal
query
SELECT least(a, 0) FROM test_least

statement
CREATE TABLE test_least_types(f float, d double, s string, dt date) USING parquet

statement
INSERT INTO test_least_types VALUES
  (1.5, 2.5, 'apple', DATE '2023-01-01'),
  (-1.5, -2.5, 'banana', DATE '2023-06-15'),
  (0.0, 0.0, 'cherry', DATE '2023-12-31'),
  (NULL, NULL, NULL, NULL),
  (cast('NaN' as float), 1.0, 'a', DATE '2020-01-01'),
  (cast('Infinity' as float), cast('-Infinity' as double), 'z', DATE '2099-12-31')

-- floating point with NaN and Infinity
query
SELECT least(f, 0.0) FROM test_least_types

query
SELECT least(d, 0.0) FROM test_least_types

-- string type
query
SELECT least(s, 'b') FROM test_least_types

-- date type
query
SELECT least(dt, DATE '2023-06-01') FROM test_least_types

-- literal floating point edge cases
query
SELECT least(cast('NaN' as float), 1.0), least(cast('NaN' as float), cast('NaN' as float))

query
SELECT least(cast('Infinity' as double), 1.0), least(cast('-Infinity' as double), 1.0)


statement
CREATE TABLE test_least_long(a bigint, b bigint) USING parquet

statement
INSERT INTO test_least_long VALUES
  (1, 2),
  (-1, 1),
  (9223372036854775807, -9223372036854775808),
  (NULL, 100),
  (NULL, NULL)

-- long/bigint type
query
SELECT least(a, b) FROM test_least_long

query
SELECT least(cast(1 as bigint), cast(-1 as bigint), cast(-9223372036854775808 as bigint))

statement
CREATE TABLE test_least_decimal(a decimal(20,5), b decimal(20,5)) USING parquet

statement
INSERT INTO test_least_decimal VALUES
  (1.00000, 2.00000),
  (-99999.99999, 99999.99999),
  (0.00001, -0.00001),
  (NULL, 123.45000),
  (NULL, NULL)

-- decimal type
query
SELECT least(a, b) FROM test_least_decimal

query
SELECT least(cast(1.5 as decimal(10,2)), cast(2.5 as decimal(10,2)), cast(-1.0 as decimal(10,2)))

statement
CREATE TABLE test_least_bool(a boolean, b boolean) USING parquet

statement
INSERT INTO test_least_bool VALUES
  (true, false),
  (false, true),
  (true, true),
  (false, false),
  (NULL, true),
  (NULL, NULL)

-- boolean type
query
SELECT least(a, b) FROM test_least_bool

query
SELECT least(true, false), least(false, false), least(true, true)

statement
CREATE TABLE test_least_ts(a timestamp, b timestamp) USING parquet

statement
INSERT INTO test_least_ts VALUES
  (TIMESTAMP '2023-01-01 08:00:00', TIMESTAMP '2023-01-01 10:00:00'),
  (TIMESTAMP '2020-06-15 12:30:00', TIMESTAMP '2020-06-15 12:29:59'),
  (NULL, TIMESTAMP '2023-01-01 00:00:00'),
  (NULL, NULL)

-- timestamp type
query
SELECT least(a, b) FROM test_least_ts

query
SELECT least(TIMESTAMP '2015-07-01 08:00:00', TIMESTAMP '2015-07-01 10:00:00')

-- many arguments (5+)
query
SELECT least(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)

query
SELECT least(a, b, c, a, b) FROM test_least

-- array type (lexicographic ordering)
statement
CREATE TABLE test_least_array(a array<int>, b array<int>) USING parquet

statement
INSERT INTO test_least_array VALUES
  (array(1, 2, 3), array(1, 2, 4)),
  (array(1, 2), array(1, 2, 0)),
  (array(3), array(2, 9, 9)),
  (array(1, 1), array(1, 1)),
  (NULL, array(1)),
  (array(1), NULL),
  (NULL, NULL)

query
SELECT least(a, b) FROM test_least_array

query
SELECT least(array(1, 2), array(1, 3), array(1, 1))

query
SELECT least(array('b', 'a'), array('a', 'z'))

-- struct type (field-by-field ordering)
statement
CREATE TABLE test_least_struct(a struct<x:int, y:string>, b struct<x:int, y:string>) USING parquet

statement
INSERT INTO test_least_struct VALUES
  (named_struct('x', 1, 'y', 'a'), named_struct('x', 2, 'y', 'a')),
  (named_struct('x', 1, 'y', 'b'), named_struct('x', 1, 'y', 'a')),
  (named_struct('x', 3, 'y', 'z'), named_struct('x', 3, 'y', 'z')),
  (NULL, named_struct('x', 1, 'y', 'a')),
  (named_struct('x', 1, 'y', 'a'), NULL),
  (NULL, NULL)

query
SELECT least(a, b) FROM test_least_struct

query
SELECT least(named_struct('x', 1, 'y', 'b'), named_struct('x', 1, 'y', 'a'))

-- nested complex type: array of structs
query
SELECT least(array(named_struct('x', 1)), array(named_struct('x', 2)))
