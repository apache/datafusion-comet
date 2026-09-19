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

-- ConfigMatrix: spark.sql.ansi.enabled=false,true

-- Read maps from columns so SimplifyExtractValueOps cannot rewrite m[k] into CASE.
-- String-to-float casts preserve negative zero (a decimal -0.0 literal does not).
statement
CREATE TABLE lookup_double(m MAP<DOUBLE, INT>, k DOUBLE) USING parquet

statement
INSERT INTO lookup_double VALUES
  (map(double('0.0'), 7), double('-0.0')),
  (map(double('-0.0'), 8), double('0.0')),
  (map(double('NaN'), 9), double('NaN')),
  (map(double('Infinity'), 10), double('Infinity')),
  (map(double('-Infinity'), 11), double('-Infinity')),
  (map(double('4.9E-324'), 12), double('4.9E-324')),
  (map(1D, 13), 2D),
  (map(1D, NULL), 1D),
  (map(), 1D),
  (NULL, 1D),
  (map(1D, 14), NULL)

query expect_dispatch(getmapvalue)
SELECT m[k] FROM lookup_double

query expect_dispatch(element_at)
SELECT element_at(m, k) FROM lookup_double

-- Exercise the Float Arrow input separately, including NULL maps, keys and values.
statement
CREATE TABLE lookup_float(m MAP<FLOAT, INT>, k FLOAT) USING parquet

statement
INSERT INTO lookup_float VALUES
  (map(float('0.0'), 7), float('-0.0')),
  (map(float('NaN'), 8), float('NaN')),
  (map(float('Infinity'), 9), float('Infinity')),
  (map(float('1.4E-45'), 10), float('1.4E-45')),
  (map(1F, 11), 2F), (map(1F, NULL), 1F), (map(), 1F), (NULL, 1F), (map(1F, 12), NULL)

query expect_dispatch(getmapvalue)
SELECT m[k] FROM lookup_float

query expect_dispatch(element_at)
SELECT element_at(m, k) FROM lookup_float

-- Complex keys compare structurally, including nested NULLs and different lengths.
-- Complex values exercise the dispatcher's output writer as well as key comparison.
statement
CREATE TABLE lookup_array(m MAP<ARRAY<INT>, ARRAY<STRING>>, k ARRAY<INT>) USING parquet

statement
INSERT INTO lookup_array VALUES
  (map(array(1, NULL), array('a', NULL)), array(1, NULL)),
  (map(array(1), array('b')), array(1, NULL)),
  (map(CAST(array() AS ARRAY<INT>), CAST(array() AS ARRAY<STRING>)), CAST(array() AS ARRAY<INT>)),
  (map(array(1), NULL), array(1)),
  (map(), array(1)), (NULL, array(1)), (map(array(1), array('c')), NULL)

query expect_dispatch(getmapvalue)
SELECT m[k] FROM lookup_array

query expect_dispatch(element_at)
SELECT element_at(m, k) FROM lookup_array

statement
CREATE TABLE lookup_struct(m MAP<STRUCT<a: INT, b: STRING>, INT>, k STRUCT<a: INT, b: STRING>) USING parquet

statement
INSERT INTO lookup_struct VALUES
  (map(named_struct('a', 1, 'b', NULL), 7), named_struct('a', 1, 'b', NULL)),
  (map(named_struct('a', 1, 'b', 'a'), 8), named_struct('a', 1, 'b', 'b')),
  (map(), named_struct('a', NULL, 'b', NULL)),
  (NULL, named_struct('a', 1, 'b', 'a')),
  (map(named_struct('a', 1, 'b', 'a'), 9), NULL)

query expect_dispatch(getmapvalue)
SELECT m[k] FROM lookup_struct

query expect_dispatch(element_at)
SELECT element_at(m, k) FROM lookup_struct

-- Floating-point equality must also hold inside complex keys.
statement
CREATE TABLE lookup_nested(m MAP<ARRAY<DOUBLE>, INT>, k ARRAY<DOUBLE>) USING parquet

statement
INSERT INTO lookup_nested VALUES
  (map(array(0D, double('NaN'), NULL), 7), array(double('-0.0'), double('NaN'), NULL)),
  (map(array(0D), 8), array(1D))

query expect_dispatch(getmapvalue)
SELECT m[k] FROM lookup_nested

query expect_dispatch(element_at)
SELECT element_at(m, k) FROM lookup_nested

-- Even with ANSI enabled, a NULL map must short-circuit a throwing key expression.
statement
CREATE TABLE lookup_short_circuit(id INT, m MAP<DOUBLE, INT>) USING parquet

statement
INSERT INTO lookup_short_circuit VALUES (1, map(0D, 7)), (2, NULL), (3, map(0D, 9))

query expect_dispatch(getmapvalue)
SELECT m[double(id % (id - 2))] FROM lookup_short_circuit

query expect_dispatch(element_at)
SELECT element_at(m, double(id % (id - 2))) FROM lookup_short_circuit
