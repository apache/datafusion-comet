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

-- Disabling the codegen dispatcher must not send incompatible floating-point extrema
-- back to the native UDF. Non-floating extrema still have a native path.
-- https://github.com/apache/datafusion-comet/issues/5401

-- Config: spark.comet.exec.strictFloatingPoint=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false
-- Config: spark.comet.expression.ArrayMin.allowIncompatible=false
-- Config: spark.comet.expression.ArrayMax.allowIncompatible=false
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_array_extrema_strict_fallback(id int, d array<double>, f array<float>, a int, b int) USING parquet

statement
INSERT INTO test_array_extrema_strict_fallback VALUES
  (1, array(double('0.0'), double('-0.0')), array(float('0.0'), float('-0.0')), 1, -2),
  (2, array(double('-0.0'), double('0.0')), array(float('-0.0'), float('0.0')), -3, 4),
  (3, array(), array(), NULL, 5),
  (4, NULL, NULL, NULL, NULL)

query expect_fallback(spark.comet.exec.strictFloatingPoint=true)
SELECT id, array_min(d), array_min(f) FROM test_array_extrema_strict_fallback

query expect_fallback(spark.comet.exec.strictFloatingPoint=true)
SELECT id, array_max(d), array_max(f) FROM test_array_extrema_strict_fallback

query expect_fallback(spark.comet.exec.strictFloatingPoint=true)
SELECT id, array_min(array(array(d[0]), array(d[1]))),
       array_max(array(array(f[0]), array(f[1])))
FROM test_array_extrema_strict_fallback WHERE id IN (1, 2)

query
SELECT id, array_min(array(a, b)), array_max(array(a, b)) FROM test_array_extrema_strict_fallback
