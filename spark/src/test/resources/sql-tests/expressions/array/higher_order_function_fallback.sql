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

-- Higher-order functions have no native rust path; they ride the codegen dispatcher. With the
-- dispatcher disabled they have no native path and the projection falls back to Spark while still
-- producing correct results.

-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false

statement
CREATE TABLE test_hof_fallback(a array<int>, m map<string, int>) USING parquet

statement
INSERT INTO test_hof_fallback VALUES
  (array(1, 2, 3), map('a', 1, 'b', 2)),
  (array(), map()),
  (NULL, NULL)

-- array higher-order function falls back to Spark
query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled)
SELECT transform(a, x -> x + 1) FROM test_hof_fallback

-- map higher-order function falls back to Spark
query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled)
SELECT transform_values(m, (k, v) -> v + 1) FROM test_hof_fallback
