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

-- Config: spark.comet.expression.ArrayIntersect.allowIncompatible=true

-- DataFusion's array_intersect emits elements in the order of the longer input
-- (it uses the shorter side as the hash lookup), while Spark emits elements in
-- the order they appear in the left argument. Most cases below have
-- left.length >= right.length so the element order matches Spark; cases that
-- intentionally flip that relation use sort_array on the result so the
-- assertion stays stable.

statement
CREATE TABLE test_array_intersect(a array<int>, b array<int>) USING parquet

statement
INSERT INTO test_array_intersect VALUES (array(1, 2, 3), array(2, 3, 4)), (array(1, 2), array(3, 4)), (array(), array(1)), (NULL, array(1)), (array(1, NULL), array(NULL, 2))

query
SELECT array_intersect(a, b) FROM test_array_intersect

-- column + literal
query
SELECT array_intersect(a, array(2, 3)) FROM test_array_intersect

-- literal + column
query
SELECT array_intersect(array(1, 2, 3), b) FROM test_array_intersect

-- literal + literal
query
SELECT array_intersect(array(1, 2, 3), array(2, 3, 4)), array_intersect(array(1, 2), array(3, 4)), array_intersect(array(), array(1)), array_intersect(cast(NULL as array<int>), array(1))

-- right longer than left: element order diverges from Spark, so sort the result
query
SELECT sort_array(array_intersect(array(2, 1), array(3, 1, 2))), sort_array(array_intersect(array(3, 1), array(1, 2, 3, 4)))

-- duplicate elements within and across arrays
statement
CREATE TABLE test_intersect_dups(a array<int>, b array<int>) USING parquet

statement
INSERT INTO test_intersect_dups VALUES (array(1, 1, 1), array(1, 1)), (array(1, 2, 1, 2), array(2, 1, 2, 1)), (array(1, 2, 3), array(1, 2, 3)), (array(1, 1, 2, 2, 3, 3), array(2, 2))

query
SELECT a, b, array_intersect(a, b) FROM test_intersect_dups

-- both-NULL arrays and all-NULL element arrays
statement
CREATE TABLE test_intersect_nulls(a array<int>, b array<int>) USING parquet

statement
INSERT INTO test_intersect_nulls VALUES (array(NULL), array(NULL)), (array(NULL, NULL), array(NULL)), (array(NULL, NULL), array(NULL, NULL)), (array(1, NULL), array(1, NULL)), (array(1, NULL), array(NULL)), (array(1), array(NULL))

query
SELECT a, b, array_intersect(a, b) FROM test_intersect_nulls

query
SELECT array_intersect(cast(NULL as array<int>), cast(NULL as array<int>))

-- self-intersection (deduplication)
query
SELECT a, array_intersect(a, a) FROM test_intersect_dups

-- empty array combinations
query
SELECT array_intersect(array(), array()), array_intersect(array(), array(1, 2)), array_intersect(array(1, 2), array())

-- boolean arrays
statement
CREATE TABLE test_intersect_bool(a array<boolean>, b array<boolean>) USING parquet

statement
INSERT INTO test_intersect_bool VALUES (array(true, false), array(true)), (array(true, true), array(false)), (array(true, false, NULL), array(false, NULL))

query
SELECT a, b, array_intersect(a, b) FROM test_intersect_bool

-- tinyint arrays
statement
CREATE TABLE test_intersect_byte(a array<tinyint>, b array<tinyint>) USING parquet

statement
INSERT INTO test_intersect_byte VALUES (array(cast(127 as tinyint), cast(-128 as tinyint), cast(0 as tinyint)), array(cast(127 as tinyint), cast(1 as tinyint))), (array(cast(1 as tinyint), cast(2 as tinyint)), array(cast(3 as tinyint), cast(4 as tinyint)))

query
SELECT a, b, array_intersect(a, b) FROM test_intersect_byte

-- smallint arrays
statement
CREATE TABLE test_intersect_short(a array<smallint>, b array<smallint>) USING parquet

statement
INSERT INTO test_intersect_short VALUES (array(cast(32767 as smallint), cast(-32768 as smallint)), array(cast(32767 as smallint))), (array(cast(1 as smallint), cast(2 as smallint)), array(cast(2 as smallint), cast(3 as smallint)))

query
SELECT a, b, array_intersect(a, b) FROM test_intersect_short

-- bigint arrays with boundary values
statement
CREATE TABLE test_intersect_long(a array<bigint>, b array<bigint>) USING parquet

statement
INSERT INTO test_intersect_long VALUES (array(9223372036854775807, 1, -9223372036854775808), array(9223372036854775807, -9223372036854775808)), (array(0, 1, 2), array(3, 4, 5))

query
SELECT a, b, array_intersect(a, b) FROM test_intersect_long

-- float arrays with NaN, Infinity, -Infinity
statement
CREATE TABLE test_intersect_float(a array<float>, b array<float>) USING parquet

statement
INSERT INTO test_intersect_float VALUES (array(1.5, 2.5, float('NaN')), array(2.5, float('NaN'))), (array(float('Infinity'), 1.0, float('-Infinity')), array(float('Infinity'), float('-Infinity'))), (array(cast(0.0 as float), cast(-0.0 as float)), array(cast(0.0 as float)))

query
SELECT a, b, array_intersect(a, b) FROM test_intersect_float

-- double arrays with NaN, Infinity, -Infinity
statement
CREATE TABLE test_intersect_dbl(a array<double>, b array<double>) USING parquet

statement
INSERT INTO test_intersect_dbl VALUES (array(1.0, 2.0, double('NaN')), array(2.0, double('NaN'))), (array(double('Infinity'), 1.0, double('-Infinity')), array(double('Infinity'), double('-Infinity'))), (array(0.0, -0.0), array(0.0)), (array(1.0, 2.0, NULL), array(1.0, NULL))

query
SELECT a, b, array_intersect(a, b) FROM test_intersect_dbl

-- decimal arrays
statement
CREATE TABLE test_intersect_dec(a array<decimal(10,2)>, b array<decimal(10,2)>) USING parquet

statement
INSERT INTO test_intersect_dec VALUES (array(1.00, 2.50, 3.00), array(2.50, 3.00, 4.00)), (array(1.00, 2.00), array(3.00, 4.00)), (array(1.10, NULL), array(1.10, NULL))

query
SELECT a, b, array_intersect(a, b) FROM test_intersect_dec

-- date arrays
statement
CREATE TABLE test_intersect_date(a array<date>, b array<date>) USING parquet

statement
INSERT INTO test_intersect_date VALUES (array(date '2024-01-01', date '2024-06-15', date '2024-12-31'), array(date '2024-06-15', date '2024-12-31')), (array(date '2024-01-01'), array(date '2024-12-31')), (array(date '2024-01-01', NULL), array(date '2024-01-01', NULL))

query
SELECT a, b, array_intersect(a, b) FROM test_intersect_date

-- timestamp arrays
statement
CREATE TABLE test_intersect_ts(a array<timestamp>, b array<timestamp>) USING parquet

statement
INSERT INTO test_intersect_ts VALUES (array(timestamp '2024-01-01 00:00:00', timestamp '2024-06-15 12:00:00', timestamp '2024-12-31 23:59:59'), array(timestamp '2024-06-15 12:00:00', timestamp '2024-12-31 23:59:59')), (array(timestamp '2024-01-01 00:00:00'), array(timestamp '2024-12-31 23:59:59'))

query
SELECT a, b, array_intersect(a, b) FROM test_intersect_ts

-- string arrays including empty strings and multibyte UTF-8
statement
CREATE TABLE test_intersect_str(a array<string>, b array<string>) USING parquet

statement
INSERT INTO test_intersect_str VALUES (array('a', 'b', 'c'), array('b', 'c', 'd')), (array('a', NULL, 'b'), array('a', NULL)), (array('', 'a'), array('', 'b')), (array('café', '中文', 'test'), array('café', 'other')), (NULL, array('a'))

query
SELECT a, b, array_intersect(a, b) FROM test_intersect_str

-- binary arrays
statement
CREATE TABLE test_intersect_bin(a array<binary>, b array<binary>) USING parquet

statement
INSERT INTO test_intersect_bin VALUES (array(binary('abc'), binary('def'), binary('ghi')), array(binary('def'), binary('ghi'))), (array(binary('abc')), array(binary('xyz'))), (array(binary('abc'), NULL), array(binary('abc'), NULL))

query
SELECT a, b, array_intersect(a, b) FROM test_intersect_bin

-- nested array<array<int>>
statement
CREATE TABLE test_intersect_nested(a array<array<int>>, b array<array<int>>) USING parquet

statement
INSERT INTO test_intersect_nested VALUES (array(array(1, 2), array(3, 4), array(5, 6)), array(array(3, 4), array(5, 6))), (array(array(1, 2)), array(array(3, 4))), (array(array(1, 2), cast(NULL as array<int>)), array(array(1, 2), cast(NULL as array<int>)))

query
SELECT a, b, array_intersect(a, b) FROM test_intersect_nested

-- mixed column and literal with NULL elements
query
SELECT array_intersect(a, array(1, 2, NULL)) FROM test_array_intersect

query
SELECT array_intersect(array(1, NULL, 3), b) FROM test_array_intersect

-- conditional (CASE WHEN) arrays
query
SELECT array_intersect(CASE WHEN a IS NOT NULL THEN a ELSE array(0) END, b) FROM test_array_intersect
