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
CREATE TABLE test_array_union(a array<int>, b array<int>) USING parquet

statement
INSERT INTO test_array_union VALUES (array(1, 2, 3), array(3, 4, 5)), (array(1, 2), array()), (array(), array(1)), (NULL, array(1)), (array(1, NULL), array(NULL, 2))

query
SELECT array_union(a, b) FROM test_array_union

-- column + literal
query
SELECT array_union(a, array(3, 4, 5)) FROM test_array_union

-- literal + column
query
SELECT array_union(array(1, 2, 3), b) FROM test_array_union

-- literal + literal
query
SELECT array_union(array(1, 2, 3), array(3, 4, 5)), array_union(array(1, 2), array()), array_union(array(), array(1)), array_union(cast(NULL as array<int>), array(1))

-- NULL element deduplication (NULLs treated as values, kept once in result)
statement
CREATE TABLE test_union_nulls(a array<int>, b array<int>) USING parquet

statement
INSERT INTO test_union_nulls VALUES (array(1, NULL, 3), array(4, NULL, 5)), (array(NULL), array(NULL)), (array(NULL, NULL), array(NULL, NULL)), (array(1, NULL), array(2, NULL)), (array(NULL, 2), array(1, NULL)), (array(1, NULL, 3), array(1, 5))

query
SELECT a, b, array_union(a, b) FROM test_union_nulls

-- empty array combinations
query
SELECT array_union(array(), array()) FROM test_union_nulls

query
SELECT array_union(array(), array(1, 2)) FROM test_union_nulls

query
SELECT array_union(array(1, 2), array()) FROM test_union_nulls

query
SELECT array_union(array(), array(NULL)) FROM test_union_nulls

-- both-NULL arrays
query
SELECT array_union(cast(NULL as array<int>), cast(NULL as array<int>)) FROM test_union_nulls

-- self-union (deduplication)
query
SELECT a, array_union(a, a) FROM test_union_nulls

-- duplicate elements within and across arrays
statement
CREATE TABLE test_union_dups(a array<int>, b array<int>) USING parquet

statement
INSERT INTO test_union_dups VALUES (array(1, 1, 1), array(2, 2, 2)), (array(1, 1, 1), array(1, 2, 2)), (array(1, 2, 1, 2), array(3, 4, 3, 4)), (array(1, 2, 1, 2), array(2, 3, 2, 3)), (array(1, 2, 3), array(1, 2, 3))

query
SELECT a, b, array_union(a, b) FROM test_union_dups

-- single element arrays
query
SELECT array_union(array(1), array(1)) FROM test_union_dups

query
SELECT array_union(array(1), array(2)) FROM test_union_dups

-- string arrays
statement
CREATE TABLE test_union_str(a array<string>, b array<string>) USING parquet

statement
INSERT INTO test_union_str VALUES (array('a', 'b', 'c'), array('c', 'd')), (array('a', 'b'), array('c', 'd')), (array('a', NULL), array('b', NULL)), (array('a', NULL), array('a', 'b')), (NULL, array('a')), (array(''), array('')), (array('', NULL), array('x'))

query
SELECT a, b, array_union(a, b) FROM test_union_str

-- empty string handling
query
SELECT array_union(array('', 'a'), array('', 'b')) FROM test_union_str

-- double arrays with special values
statement
CREATE TABLE test_union_dbl(a array<double>, b array<double>) USING parquet

statement
INSERT INTO test_union_dbl VALUES (array(1.0, 2.0), array(2.0, 3.0)), (array(1.0, double('NaN')), array(double('NaN'), 2.0)), (array(double('Infinity'), 1.0), array(double('Infinity'))), (array(double('-Infinity')), array(double('Infinity'))), (array(0.0), array(-0.0)), (array(1.0, NULL), array(2.0, NULL))

query
SELECT a, b, array_union(a, b) FROM test_union_dbl

-- boolean arrays
query
SELECT array_union(array(true, false), array(false)) FROM test_union_dbl

query
SELECT array_union(array(true), array(false)) FROM test_union_dbl

query
SELECT array_union(array(true, NULL), array(false, NULL)) FROM test_union_dbl

-- bigint arrays
statement
CREATE TABLE test_union_long(a array<bigint>, b array<bigint>) USING parquet

statement
INSERT INTO test_union_long VALUES (array(9223372036854775807, 1), array(9223372036854775807)), (array(-9223372036854775808), array(-9223372036854775808)), (array(0), array(1))

query
SELECT a, b, array_union(a, b) FROM test_union_long

-- decimal arrays
statement
CREATE TABLE test_union_dec(a array<decimal(10,2)>, b array<decimal(10,2)>) USING parquet

statement
INSERT INTO test_union_dec VALUES (array(1.00, 2.50), array(2.50, 3.00)), (array(1.00, 2.00), array(3.00, 4.00)), (array(1.10, NULL), array(2.20, NULL))

query
SELECT a, b, array_union(a, b) FROM test_union_dec

-- date arrays
statement
CREATE TABLE test_union_date(a array<date>, b array<date>) USING parquet

statement
INSERT INTO test_union_date VALUES (array(date '2024-01-01', date '2024-06-15'), array(date '2024-06-15', date '2024-12-31')), (array(date '2024-01-01'), array(date '2024-12-31')), (array(date '2024-01-01', NULL), array(date '2024-12-31'))

query
SELECT a, b, array_union(a, b) FROM test_union_date

-- timestamp arrays
statement
CREATE TABLE test_union_ts(a array<timestamp>, b array<timestamp>) USING parquet

statement
INSERT INTO test_union_ts VALUES (array(timestamp '2024-01-01 00:00:00', timestamp '2024-06-15 12:00:00'), array(timestamp '2024-06-15 12:00:00')), (array(timestamp '2024-01-01 00:00:00'), array(timestamp '2024-12-31 23:59:59'))

query
SELECT a, b, array_union(a, b) FROM test_union_ts

-- nested arrays
statement
CREATE TABLE test_union_nested(a array<array<int>>, b array<array<int>>) USING parquet

statement
INSERT INTO test_union_nested VALUES (array(array(1, 2), array(3, 4)), array(array(3, 4), array(5, 6))), (array(array(1, 2)), array(array(3, 4))), (array(array(1, 2), cast(NULL as array<int>)), array(array(3, 4), cast(NULL as array<int>))), (array(array(1, NULL)), array(array(1, NULL)))

query
SELECT a, b, array_union(a, b) FROM test_union_nested

-- struct element arrays
statement
CREATE TABLE test_union_struct(a array<struct<x:int, y:int>>, b array<struct<x:int, y:int>>) USING parquet

statement
INSERT INTO test_union_struct VALUES (array(named_struct('x', 1, 'y', 2)), array(named_struct('x', 1, 'y', 2))), (array(named_struct('x', 1, 'y', 2)), array(named_struct('x', 3, 'y', 4))), (array(named_struct('x', 1, 'y', cast(NULL as int))), array(named_struct('x', 1, 'y', cast(NULL as int)))), (array(cast(NULL as struct<x:int, y:int>)), array(cast(NULL as struct<x:int, y:int>)))

query
SELECT a, b, array_union(a, b) FROM test_union_struct

-- mixed column and literal with NULL elements
query
SELECT array_union(a, array(99, NULL)) FROM test_array_union

query
SELECT array_union(array(NULL, 99), b) FROM test_array_union

-- conditional (CASE WHEN) arrays
query
SELECT array_union(CASE WHEN a IS NOT NULL THEN a ELSE array(0) END, b) FROM test_array_union
