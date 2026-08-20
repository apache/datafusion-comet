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
CREATE TABLE test_arrays_overlap(a array<int>, b array<int>) USING parquet

statement
INSERT INTO test_arrays_overlap VALUES (array(1, 2, 3), array(3, 4, 5)), (array(1, 2), array(3, 4)), (array(), array(1)), (NULL, array(1)), (array(1, NULL), array(NULL, 2))

query
SELECT arrays_overlap(a, b) FROM test_arrays_overlap

-- column + literal
query
SELECT arrays_overlap(a, array(3, 4, 5)) FROM test_arrays_overlap

-- literal + column
query
SELECT arrays_overlap(array(1, 2, 3), b) FROM test_arrays_overlap

-- literal + literal
query
SELECT arrays_overlap(array(1, 2, 3), array(3, 4, 5)), arrays_overlap(array(1, 2), array(3, 4)), arrays_overlap(array(), array(1)), arrays_overlap(cast(NULL as array<int>), array(1))

-- NULL element semantics (three-valued logic)
-- When no match is found but NULL elements exist, result should be NULL (uncertain)
statement
CREATE TABLE test_overlap_nulls(a array<int>, b array<int>) USING parquet

statement
INSERT INTO test_overlap_nulls VALUES (array(1, NULL, 3), array(4, 5)), (array(1, NULL, 3), array(1, 5)), (array(1, NULL), array(NULL, 2)), (array(NULL), array(NULL)), (array(NULL, NULL), array(NULL, NULL)), (array(1, NULL), array(2, NULL)), (array(NULL, 2), array(1, NULL))

-- no match + has NULL => NULL
query
SELECT arrays_overlap(a, b) FROM test_overlap_nulls WHERE a = array(1, NULL, 3) AND b = array(4, 5)

-- has match + has NULL => true (match found, NULL irrelevant)
query
SELECT arrays_overlap(a, b) FROM test_overlap_nulls WHERE a = array(1, NULL, 3) AND b = array(1, 5)

-- NULL vs NULL elements => NULL (NULL != NULL)
query
SELECT arrays_overlap(a, b) FROM test_overlap_nulls WHERE a = array(NULL) AND b = array(NULL)

-- all rows
query
SELECT a, b, arrays_overlap(a, b) FROM test_overlap_nulls

-- empty array combinations
query
SELECT arrays_overlap(array(), array()) FROM test_overlap_nulls

query
SELECT arrays_overlap(array(), array(1, 2)) FROM test_overlap_nulls

query
SELECT arrays_overlap(array(1, 2), array()) FROM test_overlap_nulls

query
SELECT arrays_overlap(array(), array(NULL)) FROM test_overlap_nulls

-- both-NULL arrays
query
SELECT arrays_overlap(cast(NULL as array<int>), cast(NULL as array<int>)) FROM test_overlap_nulls

-- identical arrays
query
SELECT arrays_overlap(a, a) FROM test_overlap_nulls

-- duplicate elements in arrays
statement
CREATE TABLE test_overlap_dups(a array<int>, b array<int>) USING parquet

statement
INSERT INTO test_overlap_dups VALUES (array(1, 1, 1), array(2, 2, 2)), (array(1, 1, 1), array(1, 2, 2)), (array(1, 2, 1, 2), array(3, 4, 3, 4)), (array(1, 2, 1, 2), array(2, 3, 2, 3))

query
SELECT a, b, arrays_overlap(a, b) FROM test_overlap_dups

-- single element arrays
query
SELECT arrays_overlap(array(1), array(1)) FROM test_overlap_dups

query
SELECT arrays_overlap(array(1), array(2)) FROM test_overlap_dups

-- string arrays
statement
CREATE TABLE test_overlap_str(a array<string>, b array<string>) USING parquet

statement
INSERT INTO test_overlap_str VALUES (array('a', 'b', 'c'), array('c', 'd')), (array('a', 'b'), array('c', 'd')), (array('a', NULL), array('b', NULL)), (array('a', NULL), array('a', 'b')), (NULL, array('a')), (array(''), array('')), (array('', NULL), array('x'))

query
SELECT a, b, arrays_overlap(a, b) FROM test_overlap_str

-- empty string vs NULL
query
SELECT arrays_overlap(array('', 'a'), array('')) FROM test_overlap_str

-- double arrays with special values
statement
CREATE TABLE test_overlap_dbl(a array<double>, b array<double>) USING parquet

statement
INSERT INTO test_overlap_dbl VALUES (array(1.0, 2.0), array(2.0, 3.0)), (array(1.0, double('NaN')), array(double('NaN'), 2.0)), (array(double('Infinity'), 1.0), array(double('Infinity'))), (array(double('-Infinity')), array(double('Infinity'))), (array(0.0), array(-0.0)), (array(1.0, NULL), array(2.0, NULL))

query
SELECT a, b, arrays_overlap(a, b) FROM test_overlap_dbl

-- Flat FLOAT/DOUBLE NaNs must compare equal regardless of their sign or payload.
-- Parquet canonicalizes NaNs, so negate a scanned column to produce a noncanonical
-- NaN at execution time. String casts preserve the sign of the zero controls.
statement
CREATE TABLE test_overlap_floating_point(
  id int, fl float, fr float, dl double, dr double
) USING parquet

statement
INSERT INTO test_overlap_floating_point VALUES
  (0, CAST('NaN' AS FLOAT), CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE)),
  (1, CAST('0.0' AS FLOAT), CAST('-0.0' AS FLOAT), CAST('0.0' AS DOUBLE), CAST('-0.0' AS DOUBLE)),
  (2, CAST('-0.0' AS FLOAT), CAST('0.0' AS FLOAT), CAST('-0.0' AS DOUBLE), CAST('0.0' AS DOUBLE)),
  (3, CAST('0.0' AS FLOAT), CAST('0.0' AS FLOAT), CAST('0.0' AS DOUBLE), CAST('0.0' AS DOUBLE)),
  (4, CAST('-0.0' AS FLOAT), CAST('-0.0' AS FLOAT), CAST('-0.0' AS DOUBLE), CAST('-0.0' AS DOUBLE)),
  (5, CAST(1 AS FLOAT), CAST(2 AS FLOAT), CAST(1 AS DOUBLE), CAST(2 AS DOUBLE)),
  (6, CAST('NaN' AS FLOAT), CAST(1 AS FLOAT), CAST('NaN' AS DOUBLE), CAST(1 AS DOUBLE)),
  (7, CAST(1 AS FLOAT), CAST('NaN' AS FLOAT), CAST(1 AS DOUBLE), CAST('NaN' AS DOUBLE))

-- Short arrays, reversed operands, and direct signed-zero controls. Spark 4.2+
-- normalizes signed zeros before arrays_overlap (SPARK-54918); comparing with the
-- running Spark version checks the appropriate behavior without fixed expectations.
query
SELECT id,
  arrays_overlap(array(fl), array(-fr)) AS float_short,
  arrays_overlap(array(dl), array(-dr)) AS double_short,
  arrays_overlap(array(-fr), array(fl)) AS float_reversed,
  arrays_overlap(array(-dr), array(dl)) AS double_reversed,
  arrays_overlap(array(fl), array(fr)) AS float_direct,
  arrays_overlap(array(dl), array(dr)) AS double_direct
FROM test_overlap_floating_point

-- Seventeen elements on each side exceed the native budget of 256 comparisons
-- and exercise the hash-probe path, including signed-zero and nonmatching rows.
query
SELECT id,
  arrays_overlap(array_repeat(fl, 17), array_repeat(-fr, 17)) AS float_long,
  arrays_overlap(array_repeat(dl, 17), array_repeat(-dr, 17)) AS double_long,
  arrays_overlap(array_repeat(-fr, 17), array_repeat(fl, 17)) AS float_long_reversed,
  arrays_overlap(array_repeat(-dr, 17), array_repeat(dl, 17)) AS double_long_reversed
FROM test_overlap_floating_point

-- A definite NaN match wins over NULL; a nonmatch with NULL remains unknown.
query
SELECT id,
  arrays_overlap(array(fl, CAST(NULL AS FLOAT)), array(-fr)),
  arrays_overlap(array(dl, CAST(NULL AS DOUBLE)), array(-dr)),
  arrays_overlap(array(-fr), array(fl, CAST(NULL AS FLOAT))),
  arrays_overlap(array(-dr), array(dl, CAST(NULL AS DOUBLE)))
FROM test_overlap_floating_point

-- boolean arrays
query
SELECT arrays_overlap(array(true, false), array(false)) FROM test_overlap_dbl

query
SELECT arrays_overlap(array(true), array(false)) FROM test_overlap_dbl

query
SELECT arrays_overlap(array(true, NULL), array(false)) FROM test_overlap_dbl

-- bigint arrays
statement
CREATE TABLE test_overlap_long(a array<bigint>, b array<bigint>) USING parquet

statement
INSERT INTO test_overlap_long VALUES (array(9223372036854775807, 1), array(9223372036854775807)), (array(-9223372036854775808), array(-9223372036854775808)), (array(0), array(1))

query
SELECT a, b, arrays_overlap(a, b) FROM test_overlap_long

-- decimal arrays
statement
CREATE TABLE test_overlap_dec(a array<decimal(10,2)>, b array<decimal(10,2)>) USING parquet

statement
INSERT INTO test_overlap_dec VALUES (array(1.00, 2.50), array(2.50, 3.00)), (array(1.00, 2.00), array(3.00, 4.00)), (array(1.10, NULL), array(2.20, NULL))

query
SELECT a, b, arrays_overlap(a, b) FROM test_overlap_dec

-- date arrays
statement
CREATE TABLE test_overlap_date(a array<date>, b array<date>) USING parquet

statement
INSERT INTO test_overlap_date VALUES (array(date '2024-01-01', date '2024-06-15'), array(date '2024-06-15', date '2024-12-31')), (array(date '2024-01-01'), array(date '2024-12-31')), (array(date '2024-01-01', NULL), array(date '2024-12-31'))

query
SELECT a, b, arrays_overlap(a, b) FROM test_overlap_date

-- timestamp arrays
statement
CREATE TABLE test_overlap_ts(a array<timestamp>, b array<timestamp>) USING parquet

statement
INSERT INTO test_overlap_ts VALUES (array(timestamp '2024-01-01 00:00:00', timestamp '2024-06-15 12:00:00'), array(timestamp '2024-06-15 12:00:00')), (array(timestamp '2024-01-01 00:00:00'), array(timestamp '2024-12-31 23:59:59'))

query
SELECT a, b, arrays_overlap(a, b) FROM test_overlap_ts

-- nested arrays
statement
CREATE TABLE test_overlap_nested(a array<array<int>>, b array<array<int>>) USING parquet

statement
INSERT INTO test_overlap_nested VALUES (array(array(1, 2), array(3, 4)), array(array(3, 4), array(5, 6))), (array(array(1, 2)), array(array(3, 4))), (array(array(1, 2), cast(NULL as array<int>)), array(array(3, 4))), (array(array(1, NULL)), array(array(1, NULL))), (array(cast(NULL as array<int>)), array(cast(NULL as array<int>)))

query
SELECT a, b, arrays_overlap(a, b) FROM test_overlap_nested

-- struct element arrays
statement
CREATE TABLE test_overlap_struct(a array<struct<x:int, y:int>>, b array<struct<x:int, y:int>>) USING parquet

statement
INSERT INTO test_overlap_struct VALUES (array(named_struct('x', 1, 'y', 2)), array(named_struct('x', 1, 'y', 2))), (array(named_struct('x', 1, 'y', 2)), array(named_struct('x', 3, 'y', 4))), (array(named_struct('x', 1, 'y', cast(NULL as int))), array(named_struct('x', 1, 'y', cast(NULL as int)))), (array(cast(NULL as struct<x:int, y:int>)), array(cast(NULL as struct<x:int, y:int>)))

query
SELECT a, b, arrays_overlap(a, b) FROM test_overlap_struct

-- mixed column and literal with NULL elements
query
SELECT arrays_overlap(a, array(99, NULL)) FROM test_arrays_overlap

query
SELECT arrays_overlap(array(NULL, 99), b) FROM test_arrays_overlap

-- conditional (CASE WHEN) arrays
query
SELECT arrays_overlap(CASE WHEN a IS NOT NULL THEN a ELSE array(0) END, b) FROM test_arrays_overlap
