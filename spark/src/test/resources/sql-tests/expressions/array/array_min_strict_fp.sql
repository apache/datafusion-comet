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

-- Strict mode uses Spark's codegen for signed-zero ties. Spark retains the first equal
-- extremum, so both input orders and floating-point widths must be checked without a tolerance.
-- Native parity is tracked by https://github.com/apache/datafusion-comet/issues/5401.

-- Config: spark.comet.exec.strictFloatingPoint=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.comet.expression.ArrayMin.allowIncompatible=false
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_array_min_strict_fp(id int, d array<double>, f array<float>) USING parquet

statement
INSERT INTO test_array_min_strict_fp VALUES
  (1, array(double('0.0'), double('-0.0')), array(float('0.0'), float('-0.0'))),
  (2, array(double('-0.0'), double('0.0')), array(float('-0.0'), float('0.0'))),
  (3, array(NULL, double('0.0'), double('-0.0')), array(NULL, float('0.0'), float('-0.0'))),
  (4, array(NULL, double('-0.0'), double('0.0')), array(NULL, float('-0.0'), float('0.0'))),
  (5, array(double('-0.0')), array(float('-0.0'))),
  (6, array(), array()),
  (7, array(NULL), array(NULL)),
  (8, NULL, NULL)

query
SELECT id, array_min(d), array_min(f) FROM test_array_min_strict_fp

-- The result type contains a float/double inside an array, so the strict-mode guard is recursive.
query
SELECT id, array_min(array(array(d[0]), array(d[1]))),
       array_min(array(array(f[0]), array(f[1])))
FROM test_array_min_strict_fp WHERE id IN (1, 2)

-- The SQL harness disables constant folding, so literal inputs exercise the dispatcher too.
query
SELECT array_min(array(double('0.0'), double('-0.0'))),
       array_min(array(double('-0.0'), double('0.0'))),
       array_min(array(float('0.0'), float('-0.0'))),
       array_min(array(float('-0.0'), float('0.0')))
