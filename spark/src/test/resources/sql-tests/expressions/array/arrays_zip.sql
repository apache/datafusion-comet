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

-- Basic usage with arrays of same length
query
SELECT arrays_zip(array(1, 2, 3), array(2, 3, 4));

-- Arrays with different lengths
query
SELECT arrays_zip(array(1, 2, 3), array('a', 'b'));

-- With floating points
query
SELECT arrays_zip(array(-.1234567E+2BD, CAST('-Infinity' AS DOUBLE), CAST('NaN' AS DOUBLE)), array(CAST('Infinity' AS FLOAT), double('-0.0'), -0.1234567f, CAST('NaN' AS FLOAT)));

-- Preserve both signs of zero in float and double array columns.
statement
CREATE TABLE test_arrays_zip_fp(f array<float>, d array<double>) USING parquet

statement
INSERT INTO test_arrays_zip_fp VALUES
  (array(float('-0.0'), float('0.0')), array(double('0.0'), double('-0.0'))),
  (array(float('0.0'), float('-0.0')), array(double('-0.0')))

query
SELECT arrays_zip(f, d) FROM test_arrays_zip_fp

-- basic: two integer arrays of equal length
query
select arrays_zip(array(1, 2, 3), array(10, 20, 30));

-- basic: two arrays with different element types (int + string)
query
select arrays_zip(array(1, 2, 3), array('a', 'b', 'c'));

-- three arrays of equal length
query
SELECT arrays_zip(array(1, 2), array(2, 3), array(3, 4));

-- three arrays of equal length
query
select arrays_zip(array(1, 2, 3), array(10, 20, 30), array(100, 200, 300));

-- four arrays of equal length
query
select arrays_zip(array(1), array(2), array(3), array(4));

-- mixed element types: float + boolean
query
select arrays_zip(array(1.5, 2.5), array(true, false));

-- different length arrays: shorter array padded with NULLs
query
select arrays_zip(array(1, 2), array(3, 4, 5));

-- different length arrays: first longer
query
select arrays_zip(array(1, 2, 3), array(10));

-- different length: one single element, other three elements
query
select arrays_zip(array(1), array('a', 'b', 'c'));

-- empty arrays
query
select arrays_zip(array(), array());

-- one empty, one non-empty
query
select arrays_zip(array(), array(1, 2, 3));

-- NULL elements inside arrays
query
select arrays_zip(array(1, null, 3), array('a', 'b', 'c'));

-- all NULL elements
query
select arrays_zip(array(cast(NULL AS int), NULL, NULL), array(cast(NULL AS string), NULL, NULL));

-- both args are NULL (entire list null)
query
select arrays_zip(cast(NULL AS array<int>), cast(NULL AS array<int>));

-- single element arrays
query
select arrays_zip(array(42), array('hello'));

-- single argument
query
SELECT arrays_zip(null)

query
select arrays_zip(cast(NULL AS array<int>));

-- NullType
query
select arrays_zip(array());

query
select arrays_zip(array(1, 2, 3));

-- one arg is NULL list, other is real array
query
select arrays_zip(cast(NULL AS array<int>), array(1, 2, 3));

-- real array + NULL list
query
select arrays_zip(array(1, 2), cast(NULL AS array<int>));

-- w/ names
statement
CREATE TABLE test_arrays_zip(a array<int>, b array<int>) USING parquet

-- column-level test with multiple rows
statement
INSERT INTO test_arrays_zip VALUES (array(1, 2), array(10, 20)), (array(3, 4, 5), array(30)), (array(6), array(60, 70))

-- column-level test with NULL rows
statement
INSERT INTO test_arrays_zip VALUES (array(1, 2), array(10, 20)), (cast(NULL AS array<int>), array(30, 40)), (array(5, 6), cast(NULL AS array<int>))

statement
INSERT INTO test_arrays_zip VALUES (array(1), array(10, 20)), (array(2, 3), array(30))

query
select arrays_zip(a, b) FROM test_arrays_zip

query
SELECT arrays_zip(a, b)['a'] FROM (SELECT array(1, 2, 3) as a, array(3, 4, 5) as b)

query
SELECT arrays_zip(a, b)['b'] FROM (SELECT array(1, 2, 3) as a, array(3, 4, 5) as b)

-- single argument
query
select arrays_zip(a) FROM test_arrays_zip

query
select arrays_zip(b) FROM test_arrays_zip

-- real array + NULL list
query
SELECT arrays_zip(a, b) FROM (SELECT array(1, 2, 3) as a, null as b)

query
SELECT arrays_zip(b, a) FROM (SELECT array(1, 2, 3) as a, null as b)

query
SELECT arrays_zip(a) FROM (SELECT array(1, 2, 3) as a, null as b)

query
SELECT arrays_zip(b) FROM (SELECT array(1, 2, 3) as a, null as b)

-- Arrays of arrays
-- +----------------------------------------------------------------------------------+
-- |arrays_zip(array(array(1, 1), array(2, 3)), array(array(3, 4), array(NULL, NULL)))|
-- +----------------------------------------------------------------------------------+
-- |[{[1, 1], [3, 4]}, {[2, 3], [NULL, NULL]}]                                        |
-- +----------------------------------------------------------------------------------+
query
SELECT arrays_zip(array(array(1, 1), array(2, 3)), array(array(3, 4), array(null, null)));

-- Arrays of arrays - single argument
-- +-----------------------------------------------+
-- |arrays_zip(array(array(NULL)), array(array(1)))|
-- +-----------------------------------------------+
-- |[{[NULL], [1]}]                                |
-- +-----------------------------------------------+
query
SELECT arrays_zip(array(array(null)), array(array(1)));

-- Arrays of arrays - different lengths
-- +---------------------------------------------------------------+
-- |arrays_zip(array(array(a, b), array(b, NULL)), array(array(1)))|
-- +---------------------------------------------------------------+
-- |[{[a, b], [1]}, {[b, NULL], NULL}]                             |
-- +---------------------------------------------------------------+
query
SELECT arrays_zip(array(array('a', 'b'), array('b', null)), array(array(1)));

-- Arrays of Dates / Timestamp / TimestampNTZ
query
SELECT arrays_zip(array(DATE '1997', DATE '1998', NULL), array(TIMESTAMP '1997-01-31 09:26:56.123', TIMESTAMP '1997-01-31 09:26:56.66666666UTC+08:00'));

-- Arrays of binary
query
SELECT arrays_zip(array(X'123456', X'123', null), array(array(X'789', X'1', null, null)))

-- Arrays of TIME need Spark 4.1 and spark.sql.timeType.enabled, so they live in
-- arrays_zip_time.sql.

-- Arrays of structs
query
SELECT arrays_zip(array(struct(1, 2, 3), struct(2, 3, 4)));

-- FIXME: COMET: Cast from NullType to IntegerType is not supported, unsupported arguments for CreateArray, unsupported arguments for ArraysZip
-- +-----------------------------------------------------------------------------+
-- |arrays_zip(array(struct(1, 2, 3), struct(2, 3, 4), struct(NULL, NULL, NULL)))|
-- +-----------------------------------------------------------------------------+
-- |[{{1, 2, 3}}, {{2, 3, 4}}, {{NULL, NULL, NULL}}]                             |
-- +-----------------------------------------------------------------------------+
-- query
-- SELECT arrays_zip(array(struct(1, 2, 3), struct(2, 3, 4), struct(null, null, null)));

-- Arrays of maps. No native arrays_zip kernel takes a map element, so these run through the
-- JVM codegen dispatcher.
query expect_dispatch(arrays_zip)
SELECT arrays_zip(array(map(1.0, '2', 3.0, '4')), array(map(1.0, '2', 3.0, '4')));

statement
CREATE TABLE test_arrays_zip_map(m array<map<string, int>>, n array<map<int, string>>) USING parquet

statement
INSERT INTO test_arrays_zip_map VALUES
  (array(map('a', 1, 'b', 2), map('c', NULL)), array(map(1, 'x'))),
  (array(CAST(map() AS map<string, int>), NULL), array(map(2, 'y'), map(3, NULL), NULL)),
  (NULL, array(map(4, 'z'))),
  (array(map('d', 4)), NULL),
  (array(), array(map(5, 'w')))

query expect_dispatch(arrays_zip)
SELECT arrays_zip(m, n) FROM test_arrays_zip_map

-- map column next to an element type the native kernel supports
query expect_dispatch(arrays_zip)
SELECT arrays_zip(m, array(1, 2, 3)) FROM test_arrays_zip_map

-- map inside a struct element and inside an inner array
query expect_dispatch(arrays_zip)
SELECT arrays_zip(array(named_struct('m', m)), array(n)) FROM test_arrays_zip_map

-- Day-time and year-month interval elements have no native kernel either. A Parquet interval
-- column falls back at the scan, so the intervals are built from int columns.
statement
CREATE TABLE test_arrays_zip_interval(d int, h int, y int, mo int) USING parquet

statement
INSERT INTO test_arrays_zip_interval VALUES (1, 2, 1, 2), (-3, 0, -3, -4), (0, 0, 0, 0), (NULL, 5, NULL, 6)

query expect_dispatch(arrays_zip)
SELECT arrays_zip(array(make_dt_interval(d, h), NULL), array(1, 2, 3)) FROM test_arrays_zip_interval

query expect_dispatch(arrays_zip)
SELECT arrays_zip(array(make_ym_interval(y, mo)), array(make_ym_interval(mo, y), NULL)) FROM test_arrays_zip_interval

-- both interval kinds, one of them inside a struct element
query expect_dispatch(arrays_zip)
SELECT arrays_zip(array(make_dt_interval(d)), array(named_struct('i', make_ym_interval(y)))) FROM test_arrays_zip_interval

-- year-month interval inside an inner array
query expect_dispatch(arrays_zip)
SELECT arrays_zip(array(array(make_ym_interval(y), NULL)), array(d)) FROM test_arrays_zip_interval

-- interval literals
query expect_dispatch(arrays_zip)
SELECT arrays_zip(array(INTERVAL '1 02:03:04.5' DAY TO SECOND, NULL), array(INTERVAL '1-2' YEAR TO MONTH))
