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
CREATE TABLE test_arrays_zip(
  ints array<int>,
  longs array<bigint>,
  strs array<string>,
  bools array<boolean>,
  doubles array<double>
) USING parquet

-- equal-length arrays, all types
statement
INSERT INTO test_arrays_zip VALUES
  (array(1, 2, 3),   array(10L, 20L, 30L), array('a', 'b', 'c'), array(true, false, true),  array(1.1, 2.2, 3.3)),
  (array(4, 5),      array(40L, 50L),       array('d', 'e'),      array(false, false),         array(4.4, 5.5)),
  (array(6),         array(60L),            array('f'),            array(true),                 array(6.6)),
  (array(),          array(),               array(),               array(),                     array()),
  (NULL,             array(1L),             array('x'),            array(false),                array(0.1)),
  (array(7, 8, 9),   NULL,                  array('g', 'h', 'i'), array(true, false, true),    array(7.7, 8.8, 9.9)),
  (array(1, NULL, 3),array(10L, NULL, 30L), array('a', NULL, 'c'),array(true, NULL, false),    array(1.1, NULL, 3.3))

-- two-array zip, equal lengths, column args
query spark_answer_only
SELECT arrays_zip(ints, longs) FROM test_arrays_zip

-- two-array zip with string and boolean columns
query spark_answer_only
SELECT arrays_zip(strs, bools) FROM test_arrays_zip

-- three-array zip
query spark_answer_only
SELECT arrays_zip(ints, strs, bools) FROM test_arrays_zip

-- four-array zip
query spark_answer_only
SELECT arrays_zip(ints, longs, strs, doubles) FROM test_arrays_zip

-- NULL elements within arrays produce null fields in output structs
query spark_answer_only
SELECT arrays_zip(ints, strs) FROM test_arrays_zip WHERE ints IS NOT NULL AND strs IS NOT NULL

-- NULL array input: entire result is null
query spark_answer_only
SELECT arrays_zip(ints, longs) FROM test_arrays_zip WHERE ints IS NULL OR longs IS NULL

-- unequal lengths: shorter arrays are padded with null
statement
CREATE TABLE test_arrays_zip_unequal(
  a array<int>,
  b array<bigint>
) USING parquet

statement
INSERT INTO test_arrays_zip_unequal VALUES
  (array(1, 2, 3),   array(10L)),
  (array(1),         array(10L, 20L, 30L)),
  (array(1, 2),      array(10L, 20L)),
  (array(),          array(1L, 2L)),
  (array(1, 2),      array())

query spark_answer_only
SELECT arrays_zip(a, b) FROM test_arrays_zip_unequal

-- literal arguments
query spark_answer_only
SELECT arrays_zip(array(1, 2, 3), array(4, 5, 6))

query spark_answer_only
SELECT arrays_zip(array(1, 2), array('a', 'b'), array(true, false))

-- one argument is an empty array
query spark_answer_only
SELECT arrays_zip(array(1, 2, 3), array())

-- NULL literal array
query spark_answer_only
SELECT arrays_zip(array(1, 2, 3), CAST(NULL AS array<int>))

-- zero-argument call returns empty array
query spark_answer_only
SELECT arrays_zip()

-- single-argument zip
query spark_answer_only
SELECT arrays_zip(array(1, 2, 3))

-- arrays whose elements are themselves arrays (nested)
statement
CREATE TABLE test_arrays_zip_nested(
  a array<array<int>>,
  b array<int>
) USING parquet

statement
INSERT INTO test_arrays_zip_nested VALUES
  (array(array(1, 2), array(3, 4)), array(10, 20)),
  (array(NULL, array(5, 6)),        array(30, 40)),
  (NULL,                            array(50, 60))

query spark_answer_only
SELECT arrays_zip(a, b) FROM test_arrays_zip_nested
