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

-- Config: spark.comet.expression.ArrayExcept.allowIncompatible=true

statement
CREATE TABLE test_array_except(a array<int>, b array<int>) USING parquet

statement
INSERT INTO test_array_except VALUES (array(1, 2, 3), array(2, 3, 4)), (array(1, 2), array()), (array(), array(1)), (NULL, array(1)), (array(1, NULL), array(NULL))

query
SELECT array_except(a, b) FROM test_array_except

-- column + literal
query
SELECT array_except(a, array(2, 3)) FROM test_array_except

-- literal + column
query
SELECT array_except(array(1, 2, 3), b) FROM test_array_except

-- literal + literal
query ignore(https://github.com/apache/datafusion-comet/issues/3646)
SELECT array_except(array(1, 2, 3), array(2, 3, 4)), array_except(array(1, 2), array()), array_except(array(), array(1)), array_except(cast(NULL as array<int>), array(1))

-- double arrays with NaN/Infinity
statement
CREATE TABLE test_except_dbl(a array<double>, b array<double>) USING parquet

statement
INSERT INTO test_except_dbl VALUES
  (array(1.0, 2.0, double('NaN')), array(2.0, double('NaN'))),
  (array(double('NaN'), 1.0, 2.0), array(1.0)),
  (array(double('NaN'), double('NaN'), 1.0), array(double('NaN'))),
  (array(1.0, 2.0), array(double('NaN'))),
  (array(double('NaN'), 1.0), array(2.0)),
  (array(double('Infinity'), 1.0, double('-Infinity')), array(double('Infinity'))),
  (array(double('Infinity'), double('-Infinity')), array(double('-Infinity'))),
  (array(1.0, 2.0, NULL), array(2.0, NULL)),
  (array(double('NaN'), NULL), array(NULL))

query
SELECT a, b, array_except(a, b) FROM test_except_dbl

-- float arrays with NaN/Infinity
statement
CREATE TABLE test_except_float(a array<float>, b array<float>) USING parquet

statement
INSERT INTO test_except_float VALUES
  (array(cast(1.0 as float), cast(2.0 as float), float('NaN')), array(cast(2.0 as float), float('NaN'))),
  (array(float('NaN'), float('NaN'), cast(1.0 as float)), array(float('NaN'))),
  (array(cast(1.0 as float), cast(2.0 as float)), array(float('NaN'))),
  (array(float('Infinity'), cast(1.0 as float), float('-Infinity')), array(float('Infinity'))),
  (array(cast(1.0 as float), cast(2.0 as float), NULL), array(cast(2.0 as float), NULL))

query
SELECT a, b, array_except(a, b) FROM test_except_float

-- negative zero (literal). Same signed-zero divergence as the column-sourced
-- cases below: NormalizeFloatingNumbers does not rewrite array-function inputs,
-- so a plain SELECT keeps the -0.0 literal intact. Spark keeps -0.0 distinct
-- from 0.0 while Comet (DataFusion) collapses them. Skip until Spark normalizes
-- these zeros (Spark 4.2+, SPARK-54918).
query ignore(https://issues.apache.org/jira/browse/SPARK-54918)
SELECT array_except(array(0.0, double('-0.0'), 1.0), array(0.0)),
       array_except(array(0.0, 1.0), array(double('-0.0')))

-- negative zero (column-sourced). Spark keeps -0.0 distinct from 0.0 while Comet
-- (DataFusion) collapses them, so array_except([0.0, -0.0, 1.0], [0.0]) is [-0.0, 1.0]
-- in Spark but [1.0] in Comet. The divergence is not limited to parquet columns;
-- the literal case above is skipped for the same reason. Skip until Spark
-- normalizes these zeros (Spark 4.2+, SPARK-54918).
statement
CREATE TABLE test_except_dbl_negzero(a array<double>, b array<double>) USING parquet

statement
INSERT INTO test_except_dbl_negzero VALUES
  (array(0.0, double('-0.0'), 1.0), array(0.0)),
  (array(0.0, 1.0), array(double('-0.0')))

query ignore(https://issues.apache.org/jira/browse/SPARK-54918)
SELECT a, b, array_except(a, b) FROM test_except_dbl_negzero

statement
CREATE TABLE test_except_flt_negzero(a array<float>, b array<float>) USING parquet

statement
INSERT INTO test_except_flt_negzero VALUES
  (array(cast(0.0 as float), float('-0.0'), cast(1.0 as float)), array(cast(0.0 as float)))

query ignore(https://issues.apache.org/jira/browse/SPARK-54918)
SELECT a, b, array_except(a, b) FROM test_except_flt_negzero

-- Coverage for nested-list elements from two native producers, `map_keys` on one side and a
-- lambda on the other; the sides are cast to one deeply-nullable element type like the other set
-- ops. Not a regression witness: both producers already declare a nullable list item, so the
-- except kernel accepts the pair without the cast. A nested list rather than a struct, because
-- `isTypeSupported` declines struct elements before `convert` runs.
query expect_native(array_except)
SELECT array_except(array(map_keys(map(coalesce(b[0], 0), 1))), transform(array(coalesce(b[0], 0)), x -> array(x))) FROM test_array_except

-- Struct elements have no native kernel. The branch reports Unsupported, which the JVM codegen
-- dispatcher runs even with this file's allowIncompatible=true (the Incompatible branch would
-- hand them to the kernel instead).
query expect_dispatch(array_except)
SELECT array_except(transform(a, x -> named_struct('i', x)), transform(b, x -> named_struct('i', x))) FROM test_array_except
