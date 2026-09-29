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
-- Spark preserves the first equal extremum, including its zero sign.
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false
-- Config: spark.comet.expression.ArrayMin.allowIncompatible=false
-- Config: spark.comet.expression.ArrayMax.allowIncompatible=false
-- ConfigMatrix: spark.comet.exec.strictFloatingPoint=false,true

statement
CREATE TABLE test_array_extrema_floating_point(id int, d array<double>, f array<float>) USING parquet

statement
INSERT INTO test_array_extrema_floating_point VALUES
  (1, array(double('0.0'), double('-0.0')), array(float('0.0'), float('-0.0'))),
  (2, array(double('-0.0'), double('0.0')), array(float('-0.0'), float('0.0'))),
  (3, array(NULL, double('-0.0'), double('0.0')), array(NULL, float('-0.0'), float('0.0'))),
  (4, array(), array()),
  (5, array(NULL, NULL), array(NULL, NULL)),
  (6, NULL, NULL),
  (7, array(double('NaN'), double('1.0')), array(float('NaN'), float('1.0'))),
  (8, array(double('1.0'), double('NaN')), array(float('1.0'), float('NaN'))),
  (9, array(double('NaN'), double('NaN')), array(float('NaN'), float('NaN'))),
  (10, array(double('-Infinity'), double('Infinity')), array(float('-Infinity'), float('Infinity')))

query expect_native(array_min,array_max)
SELECT id, array_min(d), array_max(d), array_min(f), array_max(f)
FROM test_array_extrema_floating_point

-- Constant folding is disabled by the harness, so literals exercise scalar evaluation.
query expect_native(array_min,array_max)
SELECT array_min(array(double('0.0'), double('-0.0'))),
       array_max(array(double('-0.0'), double('0.0'))),
       array_min(array(float('0.0'), float('-0.0'))),
       array_max(array(float('-0.0'), float('0.0')))

-- Nested ties must compare later fields; fully equal elements retain their original bits.
statement
CREATE TABLE test_array_extrema_nested(
  id int, d array<array<double>>, s array<struct<v:float,payload:int>>) USING parquet

statement
INSERT INTO test_array_extrema_nested VALUES
  (1, array(array(double('0.0'), double('1.0')), array(double('-0.0'), double('2.0'))),
      array(named_struct('v', float('0.0'), 'payload', 1), named_struct('v', float('-0.0'), 'payload', 2))),
  (2, array(array(double('-0.0')), array(double('0.0'))),
      array(named_struct('v', float('-0.0'), 'payload', 1), named_struct('v', float('0.0'), 'payload', 1))),
  (3, array(array(NULL), array(double('1.0'))),
      array(named_struct('v', NULL, 'payload', 1), named_struct('v', float('1.0'), 'payload', 1))),
  (4, array(array(double('NaN'), double('2.0')), array(double('NaN'), double('1.0'))),
      array(named_struct('v', float('NaN'), 'payload', 2), named_struct('v', float('NaN'), 'payload', 1))),
  (5, array(array(), array(NULL)),
      array(NULL, named_struct('v', NULL, 'payload', NULL)))

query expect_native(array_min,array_max)
SELECT id, array_min(d), array_max(d), array_min(s), array_max(s)
FROM test_array_extrema_nested

-- Non-floating nested values retain nulls-first ordering.
query expect_native(array_min,array_max)
SELECT array_min(array(array(1, NULL), array(1, 0))),
       array_max(array(array(1, NULL), array(1, 0))),
       array_min(array(named_struct('k', 1, 'v', NULL), named_struct('k', 1, 'v', 'a'))),
       array_max(array(named_struct('k', 1, 'v', NULL), named_struct('k', 1, 'v', 'a')))
