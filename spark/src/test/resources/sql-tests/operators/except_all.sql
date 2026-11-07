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

-- EXCEPT ALL and INTERSECT ALL are planned with the internal ReplicateRows generator, which
-- repeats each group of rows as many times as the counts left after the set operation. It has no
-- native implementation, so it runs on the JVM inside the native generate operator and carries
-- every column of the row through.

statement
CREATE TABLE test_set_left(
  id int,
  d decimal(20, 2),
  bin binary,
  f float,
  dbl double,
  dt date,
  ts timestamp,
  ts_ntz timestamp_ntz,
  arr array<int>,
  st struct<a: int, b: string>,
  s string
) USING parquet

statement
CREATE TABLE test_set_right LIKE test_set_left

statement
INSERT INTO test_set_left VALUES
  (1, 1.50, X'01', 1.5, 1.5, DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP_NTZ '2024-01-01 00:00:00', array(1, 2), named_struct('a', 1, 'b', 'x'), 'a'),
  (1, 1.50, X'01', 1.5, 1.5, DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP_NTZ '2024-01-01 00:00:00', array(1, 2), named_struct('a', 1, 'b', 'x'), 'a'),
  (1, 1.50, X'01', 1.5, 1.5, DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP_NTZ '2024-01-01 00:00:00', array(1, 2), named_struct('a', 1, 'b', 'x'), 'a'),
  (2, -2.25, X'00FF', CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE), DATE '9999-12-31', TIMESTAMP '9999-12-31 23:59:59', TIMESTAMP_NTZ '9999-12-31 23:59:59', array(NULL, 3), named_struct('a', NULL, 'b', 'y'), 'é日本'),
  (2, -2.25, X'00FF', CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE), DATE '9999-12-31', TIMESTAMP '9999-12-31 23:59:59', TIMESTAMP_NTZ '9999-12-31 23:59:59', array(NULL, 3), named_struct('a', NULL, 'b', 'y'), 'é日本'),
  (3, 0.00, X'', float('-0.0'), double('-0.0'), DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP_NTZ '1970-01-01 00:00:00', array(), named_struct('a', 3, 'b', NULL), ''),
  (4, 99999999999999999.99, X'AB', CAST('Infinity' AS FLOAT), CAST('-Infinity' AS DOUBLE), DATE '2000-02-29', TIMESTAMP '2000-02-29 12:34:56', TIMESTAMP_NTZ '2000-02-29 12:34:56', array(7), named_struct('a', 4, 'b', 'z'), 'w'),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)

statement
INSERT INTO test_set_right VALUES
  (1, 1.50, X'01', 1.5, 1.5, DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP_NTZ '2024-01-01 00:00:00', array(1, 2), named_struct('a', 1, 'b', 'x'), 'a'),
  (2, -2.25, X'00FF', CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE), DATE '9999-12-31', TIMESTAMP '9999-12-31 23:59:59', TIMESTAMP_NTZ '9999-12-31 23:59:59', array(NULL, 3), named_struct('a', NULL, 'b', 'y'), 'é日本'),
  (2, -2.25, X'00FF', CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE), DATE '9999-12-31', TIMESTAMP '9999-12-31 23:59:59', TIMESTAMP_NTZ '9999-12-31 23:59:59', array(NULL, 3), named_struct('a', NULL, 'b', 'y'), 'é日本'),
  (2, -2.25, X'00FF', CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE), DATE '9999-12-31', TIMESTAMP '9999-12-31 23:59:59', TIMESTAMP_NTZ '9999-12-31 23:59:59', array(NULL, 3), named_struct('a', NULL, 'b', 'y'), 'é日本'),
  (3, 0.00, X'', float('0.0'), double('0.0'), DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP_NTZ '1970-01-01 00:00:00', array(), named_struct('a', 3, 'b', NULL), ''),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)

-- every column at once
query
SELECT * FROM test_set_left EXCEPT ALL SELECT * FROM test_set_right

query
SELECT * FROM test_set_left INTERSECT ALL SELECT * FROM test_set_right

-- one type at a time, so a type the carried-column path mishandles shows up on its own
query
SELECT d FROM test_set_left EXCEPT ALL SELECT d FROM test_set_right

query
SELECT bin FROM test_set_left EXCEPT ALL SELECT bin FROM test_set_right

query
SELECT f FROM test_set_left EXCEPT ALL SELECT f FROM test_set_right

query
SELECT dbl FROM test_set_left EXCEPT ALL SELECT dbl FROM test_set_right

query
SELECT dt FROM test_set_left EXCEPT ALL SELECT dt FROM test_set_right

query
SELECT ts FROM test_set_left EXCEPT ALL SELECT ts FROM test_set_right

query
SELECT ts_ntz FROM test_set_left EXCEPT ALL SELECT ts_ntz FROM test_set_right

query
SELECT arr FROM test_set_left EXCEPT ALL SELECT arr FROM test_set_right

query
SELECT st FROM test_set_left EXCEPT ALL SELECT st FROM test_set_right

query
SELECT s FROM test_set_left EXCEPT ALL SELECT s FROM test_set_right

query
SELECT s FROM test_set_left INTERSECT ALL SELECT s FROM test_set_right

-- an empty result and an empty right side
query
SELECT s FROM test_set_left EXCEPT ALL SELECT s FROM test_set_left

query
SELECT s FROM test_set_left EXCEPT ALL SELECT s FROM test_set_right WHERE id > 100
