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
CREATE TABLE test_map_from_arrays(k array<string>, v array<int>) USING parquet

statement
INSERT INTO test_map_from_arrays VALUES
  (array('a', 'b', 'c'), array(1, 2, 3)),
  (array(), array()),
  (NULL, NULL),
  (array('x'), NULL),
  (NULL, array(99))

-- basic functionality
query spark_answer_only
SELECT map_from_arrays(k, v) FROM test_map_from_arrays WHERE k IS NOT NULL AND v IS NOT NULL

-- both inputs NULL should return NULL
query
SELECT map_from_arrays(k, v) FROM test_map_from_arrays WHERE k IS NULL AND v IS NULL

-- keys not null but values null should return NULL (Spark behavior)
query
SELECT map_from_arrays(k, v) FROM test_map_from_arrays WHERE k IS NOT NULL AND v IS NULL

-- keys null but values not null should return NULL (Spark behavior)
query
SELECT map_from_arrays(k, v) FROM test_map_from_arrays WHERE k IS NULL AND v IS NOT NULL

-- all rows including nulls
query spark_answer_only
SELECT map_from_arrays(k, v) FROM test_map_from_arrays

-- literal arguments
query spark_answer_only
SELECT map_from_arrays(array('a', 'b'), array(1, 2))

-- literal null arguments
query
SELECT map_from_arrays(NULL, array(1, 2))

query
SELECT map_from_arrays(array('a'), NULL)

query
SELECT map_from_arrays(NULL, NULL)

-- ArrayBasedMapBuilder checks, in order: the key and value counts, then each key for NULL or a repeat
statement
CREATE TABLE test_map_from_arrays_keys(
  id int,
  i array<int>,
  d array<double>,
  s array<string>,
  st array<struct<a: int, b: string>>,
  v array<string>) USING parquet

statement
INSERT INTO test_map_from_arrays_keys VALUES
  (1,
   array(-2147483648, 2147483647, 0),
   array(double('NaN'), double('Infinity'), double('-Infinity')),
   array('', 'é', '中文'),
   array(named_struct('a', 1, 'b', NULL), named_struct('a', NULL, 'b', 'x'),
     named_struct('a', NULL, 'b', NULL)),
   array('x', NULL, 'z')),
  (2, array(1), array(double('0.0')), array('a'), array(named_struct('a', 1, 'b', 'x')),
   array(NULL)),
  (3, array(2), array(double('-0.0')), array('A'), array(named_struct('a', 1, 'b', 'X')),
   array('y')),
  (4, array(), array(), array(), array(), array()),
  (5, array(1, NULL, 1), array(1.0, NULL, 1.0), array('a', NULL, 'a'), array(NULL, NULL), NULL),
  (6, NULL, NULL, NULL, NULL, array('a', 'b')),
  (7, array(1, 2, 1, NULL), NULL, NULL, NULL, array('a', 'b', 'c', 'd')),
  (8, array(NULL, 1, 1), NULL, NULL, NULL, array('a', 'b', 'c')),
  (9, array(1), NULL, NULL, NULL, array('a', 'b')),
  (10, array(2, 3, 4), NULL, NULL, NULL, array('c', 'd')),
  (11, NULL, NULL, array('a', 'b', 'a'), NULL, array('a', 'b', 'c')),
  (12, NULL, NULL, NULL, array(named_struct('a', 1, 'b', NULL), named_struct('a', 1, 'b', NULL)),
   array('a', 'b')),
  (13, NULL, NULL, NULL, array(named_struct('a', 1, 'b', 'x'), NULL), array('a', 'b'))

-- distinct keys of each type, and NULL key or value arrays whose elements are not checked
query
SELECT id, map_from_arrays(i, v), map_from_arrays(d, v), map_from_arrays(s, v),
  map_from_arrays(st, v)
FROM test_map_from_arrays_keys WHERE id <= 6

-- the repeat comes before the NULL key
query expect_error(Duplicate map key 1 was found)
SELECT map_from_arrays(i, v) FROM test_map_from_arrays_keys WHERE id = 7

-- the NULL key comes before the repeat
query expect_error(NULL_MAP_KEY)
SELECT map_from_arrays(i, v) FROM test_map_from_arrays_keys WHERE id = 8

-- counted per row: across these two rows the key and value counts add up
query expect_error(must have the same length)
SELECT map_from_arrays(i, v) FROM test_map_from_arrays_keys WHERE id IN (9, 10)

query expect_error(DUPLICATED_MAP_KEY)
SELECT map_from_arrays(s, v) FROM test_map_from_arrays_keys WHERE id = 11

-- struct keys whose NULL fields line up are equal
query expect_error(DUPLICATED_MAP_KEY)
SELECT map_from_arrays(st, v) FROM test_map_from_arrays_keys WHERE id = 12

query expect_error(NULL_MAP_KEY)
SELECT map_from_arrays(st, v) FROM test_map_from_arrays_keys WHERE id = 13

statement
CREATE TABLE test_map_from_arrays_types(
  id int,
  bo array<boolean>,
  ti array<tinyint>,
  si array<smallint>,
  bi array<bigint>,
  fl array<float>,
  de array<decimal(38, 18)>,
  dt array<date>,
  ts array<timestamp>,
  ntz array<timestamp_ntz>,
  bin array<binary>,
  arr array<array<int>>,
  v array<boolean>) USING parquet

statement
INSERT INTO test_map_from_arrays_types VALUES
  (1, array(true, false), array(-128Y, 127Y), array(-32768S, 32767S),
   array(-9223372036854775808L, 9223372036854775807L), array(float('NaN'), float('-Infinity')),
   array(99999999999999999999.999999999999999999, -99999999999999999999.999999999999999999),
   array(date'1970-01-01', date'9999-12-31'),
   array(timestamp'1970-01-01 00:00:00', timestamp'2038-01-19 03:14:08.999999'),
   array(timestamp_ntz'1970-01-01 00:00:00', timestamp_ntz'2038-01-19 03:14:08.999999'),
   array(X'', X'FF'), array(array(1, NULL), array()), array(true, NULL)),
  (2, array(false), array(0Y), array(0S), array(0L), array(float('0.0')), array(0.5),
   array(date'2024-02-29'), array(timestamp'2024-02-29 12:00:00'),
   array(timestamp_ntz'2024-02-29 12:00:00'), array(X'00'), array(array(NULL)), array(false)),
  (3, array(true), array(1Y), array(1S), array(1L), array(float('-0.0')), array(-0.5),
   array(date'2000-01-01'), array(timestamp'2000-01-01 00:00:00'),
   array(timestamp_ntz'2000-01-01 00:00:00'), array(X'0000'), array(array(1, 2)), array(NULL)),
  (4, array(), array(), array(), array(), array(), array(), array(), array(), array(), array(),
   array(), array()),
  (5, array(false, true), array(2Y, 3Y), array(2S, 3S), array(2L, 3L),
   array(float('Infinity'), float('1.5')), array(1, 2), array(date'2000-01-02', date'2000-01-03'),
   array(timestamp'2000-01-02 00:00:00', timestamp'2000-01-03 00:00:00'),
   array(timestamp_ntz'2000-01-02 00:00:00', timestamp_ntz'2000-01-03 00:00:00'),
   array(X'01', X'02'), array(array(3), array(4)), array(true, false)),
  (6, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)

-- key types beyond those above, with boolean values
query
SELECT id, map_from_arrays(bo, v), map_from_arrays(ti, v), map_from_arrays(si, v),
  map_from_arrays(bi, v), map_from_arrays(fl, v), map_from_arrays(de, v),
  map_from_arrays(dt, v), map_from_arrays(ts, v), map_from_arrays(ntz, v),
  map_from_arrays(bin, v), map_from_arrays(arr, v)
FROM test_map_from_arrays_types
