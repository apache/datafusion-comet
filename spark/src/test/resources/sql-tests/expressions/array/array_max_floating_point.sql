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

-- Regression for https://github.com/apache/datafusion-comet/issues/5401.
-- Spark retains the first equal extremum: zero signs compare equal, as do NaNs.
-- Require exact native results in both floating-point modes without the codegen dispatcher.
-- The dictionary matrix varies the writer setting, not a guarantee of dictionary pages.
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false
-- Config: spark.comet.expression.ArrayMax.allowIncompatible=false
-- ConfigMatrix: spark.comet.exec.strictFloatingPoint=false,true
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_array_max_floating_point(id int, d array<double>, f array<float>) USING parquet

statement
INSERT INTO test_array_max_floating_point VALUES
  (1, array(double('0.0'), double('-0.0')), array(float('0.0'), float('-0.0'))),
  (2, array(double('-0.0'), double('0.0')), array(float('-0.0'), float('0.0'))),
  (3, array(NULL, double('0.0'), double('-0.0'), NULL), array(NULL, float('0.0'), float('-0.0'), NULL)),
  (4, array(NULL, double('-0.0'), double('0.0'), NULL), array(NULL, float('-0.0'), float('0.0'), NULL)),
  (5, array(double('-0.0')), array(float('-0.0'))),
  (6, array(double('0.0')), array(float('0.0'))),
  (7, array(), array()),
  (8, array(NULL), array(NULL)),
  (9, array(NULL, NULL), array(NULL, NULL)),
  (10, NULL, NULL),
  (11, array(double('-3.0'), double('1.0'), double('2.0')), array(float('-3.0'), float('1.0'), float('2.0'))),
  (12, array(NULL, double('-2.0'), double('4.0'), NULL), array(NULL, float('-2.0'), float('4.0'), NULL)),
  (13, array(double('NaN'), double('1.0'), double('2.0')), array(float('NaN'), float('1.0'), float('2.0'))),
  (14, array(double('1.0'), double('NaN'), double('2.0')), array(float('1.0'), float('NaN'), float('2.0'))),
  (15, array(double('1.0'), double('2.0'), double('NaN')), array(float('1.0'), float('2.0'), float('NaN'))),
  (16, array(double('NaN'), double('NaN')), array(float('NaN'), float('NaN'))),
  (17, array(NULL, double('NaN'), NULL), array(NULL, float('NaN'), NULL)),
  (18, array(double('-Infinity'), double('Infinity'), double('NaN')), array(float('-Infinity'), float('Infinity'), float('NaN'))),
  (19, array(double('NaN'), double('Infinity'), double('-Infinity')), array(float('NaN'), float('Infinity'), float('-Infinity'))),
  (20, array(double('-Infinity'), double('1.0'), double('Infinity')), array(float('-Infinity'), float('1.0'), float('Infinity'))),
  (21, array(double('Infinity'), double('1.0'), double('-Infinity')), array(float('Infinity'), float('1.0'), float('-Infinity'))),
  (22, array(double('Infinity'), double('Infinity')), array(float('Infinity'), float('Infinity'))),
  (23, array(double('-Infinity'), double('-Infinity')), array(float('-Infinity'), float('-Infinity')))

query
SELECT id, array_max(d), array_max(f) FROM test_array_max_floating_point

-- The harness disables constant folding, so literal inputs exercise native evaluation too.
query
SELECT array_max(array(double('0.0'), double('-0.0'))),
       array_max(array(double('-0.0'), double('0.0'))),
       array_max(array(float('0.0'), float('-0.0'))),
       array_max(array(float('-0.0'), float('0.0')))

query
SELECT array_max(CAST(NULL AS array<double>)),
       array_max(CAST(NULL AS array<float>)),
       array_max(CAST(array() AS array<double>)),
       array_max(CAST(array() AS array<float>)),
       array_max(array(CAST(NULL AS double), CAST(NULL AS double))),
       array_max(array(CAST(NULL AS float), CAST(NULL AS float)))

query
SELECT array_max(array(double('NaN'), double('Infinity'), double('-Infinity'))),
       array_max(array(float('NaN'), float('Infinity'), float('-Infinity'))),
       array_max(array(double('NaN'), double('NaN'))),
       array_max(array(float('NaN'), float('NaN'))),
       array_max(array(double('Infinity'), double('-Infinity'))),
       array_max(array(float('Infinity'), float('-Infinity')))

-- Exercise both tie orders around 32-element boundaries and across longer arrays.
-- Build the input during INSERT so the tested projection has only column arguments.
statement
CREATE TABLE test_array_max_floating_long(
  n int, negative_first boolean, d array<double>, f array<float>,
  nullable_d array<double>, nullable_f array<float>) USING parquet

statement
INSERT INTO test_array_max_floating_long
SELECT n, negative_first,
       concat(array(d0), array_repeat(d1, n - 1)),
       concat(array(f0), array_repeat(f1, n - 1)),
       concat(array_repeat(CAST(NULL AS double), n - 2), array(d0, d1)),
       concat(array_repeat(CAST(NULL AS float), n - 2), array(f0, f1))
FROM VALUES (31), (32), (33), (65), (129) AS sizes(n)
CROSS JOIN VALUES
  (false, double('0.0'), double('-0.0'), float('0.0'), float('-0.0')),
  (true, double('-0.0'), double('0.0'), float('-0.0'), float('0.0'))
AS signs(negative_first, d0, d1, f0, f1)

query
SELECT n, negative_first, array_max(d), array_max(f),
       array_max(nullable_d), array_max(nullable_f)
FROM test_array_max_floating_long

-- Outer null elements are skipped; nulls inside an array sort before non-null elements.
-- Equal nested zeros and NaNs must allow comparison to continue to later elements.
statement
CREATE TABLE test_array_max_floating_nested(
  id int, d array<array<double>>, f array<array<float>>) USING parquet

statement
INSERT INTO test_array_max_floating_nested VALUES
  (1, array(array(double('0.0')), array(double('-0.0'))),
      array(array(float('0.0')), array(float('-0.0')))),
  (2, array(array(double('-0.0')), array(double('0.0'))),
      array(array(float('-0.0')), array(float('0.0')))),
  (3, array(array(NULL, double('0.0')), array(NULL, double('-0.0'))),
      array(array(NULL, float('0.0')), array(NULL, float('-0.0')))),
  (4, array(array(NULL, double('-0.0')), array(NULL, double('0.0'))),
      array(array(NULL, float('-0.0')), array(NULL, float('0.0')))),
  (5, array(array(NULL), array(double('-Infinity'))),
      array(array(NULL), array(float('-Infinity')))),
  (6, array(array(double('-Infinity')), array(NULL)),
      array(array(float('-Infinity')), array(NULL))),
  (7, array(array(double('1.0'), NULL), array(double('1.0'), double('2.0'))),
      array(array(float('1.0'), NULL), array(float('1.0'), float('2.0')))),
  (8, array(array(double('1.0')), array(double('1.0'), NULL)),
      array(array(float('1.0')), array(float('1.0'), NULL))),
  (9, array(array(), array(NULL)), array(array(), array(NULL))),
  (10, array(NULL, array(double('-0.0')), array(double('0.0'))),
       array(NULL, array(float('-0.0')), array(float('0.0')))),
  (11, array(NULL, NULL), array(NULL, NULL)),
  (12, array(), array()),
  (13, NULL, NULL),
  (14, array(array(double('NaN'), double('2.0')), array(double('NaN'), double('1.0'))),
       array(array(float('NaN'), float('2.0')), array(float('NaN'), float('1.0')))),
  (15, array(array(double('0.0'), double('1.0')), array(double('-0.0'), double('2.0'))),
       array(array(float('0.0'), float('1.0')), array(float('-0.0'), float('2.0'))))

query
SELECT id, array_max(d), array_max(f) FROM test_array_max_floating_nested

-- A zero tie in the first struct field must not override the ordering of its payload.
-- When every field compares equal, preserve the first struct, including its zero sign.
statement
CREATE TABLE test_array_max_floating_struct(
  id int, d array<struct<v:double,payload:int>>, f array<struct<v:float,payload:int>>) USING parquet

statement
INSERT INTO test_array_max_floating_struct VALUES
  (1, array(named_struct('v', double('0.0'), 'payload', 1), named_struct('v', double('-0.0'), 'payload', 2)),
      array(named_struct('v', float('0.0'), 'payload', 1), named_struct('v', float('-0.0'), 'payload', 2))),
  (2, array(named_struct('v', double('-0.0'), 'payload', 2), named_struct('v', double('0.0'), 'payload', 1)),
      array(named_struct('v', float('-0.0'), 'payload', 2), named_struct('v', float('0.0'), 'payload', 1))),
  (3, array(named_struct('v', double('0.0'), 'payload', 1), named_struct('v', double('-0.0'), 'payload', 1)),
      array(named_struct('v', float('0.0'), 'payload', 1), named_struct('v', float('-0.0'), 'payload', 1))),
  (4, array(named_struct('v', double('-0.0'), 'payload', 1), named_struct('v', double('0.0'), 'payload', 1)),
      array(named_struct('v', float('-0.0'), 'payload', 1), named_struct('v', float('0.0'), 'payload', 1))),
  (5, array(named_struct('v', NULL, 'payload', 2), named_struct('v', double('-Infinity'), 'payload', 1)),
      array(named_struct('v', NULL, 'payload', 2), named_struct('v', float('-Infinity'), 'payload', 1))),
  (6, array(named_struct('v', double('0.0'), 'payload', NULL), named_struct('v', double('-0.0'), 'payload', 1)),
      array(named_struct('v', float('0.0'), 'payload', NULL), named_struct('v', float('-0.0'), 'payload', 1))),
  (7, array(named_struct('v', double('NaN'), 'payload', 2), named_struct('v', double('NaN'), 'payload', 1)),
      array(named_struct('v', float('NaN'), 'payload', 2), named_struct('v', float('NaN'), 'payload', 1))),
  (8, array(NULL, named_struct('v', NULL, 'payload', NULL)),
      array(NULL, named_struct('v', NULL, 'payload', NULL))),
  (9, array(NULL, NULL), array(NULL, NULL)),
  (10, array(), array()),
  (11, NULL, NULL)

query
SELECT id, array_max(d), array_max(f) FROM test_array_max_floating_struct

-- Recurse through a list of structs, not only structs or lists of primitive elements.
query
SELECT id, array_max(array(array(d[0]), array(d[1]))),
       array_max(array(array(f[0]), array(f[1])))
FROM test_array_max_floating_struct WHERE id IN (1, 2, 3, 4)

-- Recurse in the other direction too: an array-valued struct field tied on zero.
query
SELECT array_max(array(
         named_struct('v', array(double('0.0')), 'payload', 1),
         named_struct('v', array(double('-0.0')), 'payload', 2))),
       array_max(array(
         named_struct('v', array(float('0.0')), 'payload', 1),
         named_struct('v', array(float('-0.0')), 'payload', 2)))
