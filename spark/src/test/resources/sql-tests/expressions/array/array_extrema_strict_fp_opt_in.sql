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

-- Explicitly allowing incompatible extrema must retain native execution in strict mode,
-- even with the codegen dispatcher disabled. These arrays have no signed-zero ties, whose
-- native parity remains tracked by https://github.com/apache/datafusion-comet/issues/5401.

-- Config: spark.comet.exec.strictFloatingPoint=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false
-- Config: spark.comet.expression.ArrayMin.allowIncompatible=true
-- Config: spark.comet.expression.ArrayMax.allowIncompatible=true
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_array_extrema_strict_opt_in(id int, d array<double>, f array<float>) USING parquet

statement
INSERT INTO test_array_extrema_strict_opt_in VALUES
  (1, array(double('-3.0'), double('1.0'), double('2.0')), array(float('-3.0'), float('1.0'), float('2.0'))),
  (2, array(double('0.0')), array(float('0.0'))),
  (3, array(double('-0.0')), array(float('-0.0'))),
  (4, array(NULL, double('-2.0'), double('4.0')), array(NULL, float('-2.0'), float('4.0'))),
  (5, array(), array()),
  (6, NULL, NULL)

query
SELECT id, array_min(d), array_max(d), array_min(f), array_max(f)
FROM test_array_extrema_strict_opt_in
