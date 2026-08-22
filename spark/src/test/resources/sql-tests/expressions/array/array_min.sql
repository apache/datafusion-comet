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

-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false
-- Config: spark.comet.expression.ArrayMin.allowIncompatible=false

statement
CREATE TABLE test_array_min(arr array<int>) USING parquet

statement
INSERT INTO test_array_min VALUES (array(1, 2, 3)), (array(3, 1, 2)), (array()), (NULL), (array(NULL, 1, 2)), (array(-1, -2, -3))

query
SELECT array_min(arr) FROM test_array_min

-- literal arguments
query
SELECT array_min(array(1, 2, 3)), array_min(array()), array_min(cast(NULL as array<int>))

-- ===== DOUBLE arrays with NaN/Infinity/-0.0 =====

statement
CREATE TABLE test_array_min_double(arr array<double>) USING parquet

statement
INSERT INTO test_array_min_double VALUES
  (array(CAST('NaN' AS DOUBLE), 1.0, 2.0)),
  (array(1.0, CAST('NaN' AS DOUBLE), 2.0)),
  (array(1.0, 2.0, CAST('NaN' AS DOUBLE))),
  (array(CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE))),
  (array(CAST('NaN' AS DOUBLE), NULL, 1.0)),
  (array(CAST('Infinity' AS DOUBLE), 1.0, 2.0)),
  (array(CAST('-Infinity' AS DOUBLE), 1.0, 2.0)),
  (array(CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE))),
  (NULL),
  (array())

query
SELECT array_min(arr) FROM test_array_min_double

-- Regression for https://github.com/apache/datafusion-comet/issues/5401:
-- Spark preserves the first equal zero (+0.0 here), and native execution must do the same.
statement
CREATE TABLE test_array_min_dbl_negzero(arr array<double>) USING parquet

statement
INSERT INTO test_array_min_dbl_negzero VALUES
  (array(0.0, double('-0.0'), 1.0))

query
SELECT array_min(arr) FROM test_array_min_dbl_negzero

-- ===== FLOAT arrays with NaN/Infinity/-0.0 =====

statement
CREATE TABLE test_array_min_float(arr array<float>) USING parquet

statement
INSERT INTO test_array_min_float VALUES
  (array(CAST('NaN' AS FLOAT), CAST(1.0 AS FLOAT), CAST(2.0 AS FLOAT))),
  (array(CAST('NaN' AS FLOAT), CAST('NaN' AS FLOAT))),
  (array(CAST('NaN' AS FLOAT), NULL, CAST(1.0 AS FLOAT))),
  (array(CAST('Infinity' AS FLOAT), CAST(1.0 AS FLOAT))),
  (array(CAST('-Infinity' AS FLOAT), CAST(1.0 AS FLOAT))),
  (NULL),
  (array())

query
SELECT array_min(arr) FROM test_array_min_float

-- Regression for https://github.com/apache/datafusion-comet/issues/5401:
-- Spark preserves the first equal zero (+0.0 here), and native execution must do the same.
statement
CREATE TABLE test_array_min_flt_negzero(arr array<float>) USING parquet

statement
INSERT INTO test_array_min_flt_negzero VALUES
  (array(CAST(0.0 AS FLOAT), float('-0.0')))

query
SELECT array_min(arr) FROM test_array_min_flt_negzero

-- Default-mode non-floating native controls, including nested nulls-first ordering.
statement
CREATE TABLE test_array_min_nested_non_fp(
  id int, a array<array<int>>, s array<struct<k:int,v:string>>) USING parquet

statement
INSERT INTO test_array_min_nested_non_fp VALUES
  (1, array(array(1, NULL), array(1, 0)),
      array(named_struct('k', 1, 'v', NULL), named_struct('k', 1, 'v', 'a'))),
  (2, array(array(1, 0), array(1, NULL)),
      array(named_struct('k', 1, 'v', 'a'), named_struct('k', 1, 'v', NULL))),
  (3, array(array(), array(NULL)),
      array(NULL, named_struct('k', NULL, 'v', 'a'))),
  (4, array(array(1), array(1, NULL)),
      array(named_struct('k', NULL, 'v', 'b'), named_struct('k', NULL, 'v', 'a'))),
  (5, array(NULL, array(0)), array(NULL, named_struct('k', NULL, 'v', NULL))),
  (6, array(NULL, NULL), array(NULL, NULL)),
  (7, array(), array()),
  (8, NULL, NULL)

query
SELECT id, array_min(a), array_min(s) FROM test_array_min_nested_non_fp

query
SELECT array_min(array(false, true, NULL)),
       array_min(array('z', 'A', 'a')),
       array_min(array(CAST(1.25 AS decimal(8, 2)), CAST(-2.5 AS decimal(8, 2)))),
       array_min(array(DATE '2024-01-01', DATE '1969-12-31'))
