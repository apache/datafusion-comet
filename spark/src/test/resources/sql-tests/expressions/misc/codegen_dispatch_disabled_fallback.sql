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

-- Safety net for the CodegenDispatchFallback routing. `Unsupported` cases on serdes that mix in
-- CodegenDispatchFallback normally stay native by routing through the JVM codegen dispatcher.
-- With the dispatcher disabled it has no path to take, so these same cases must fall back to
-- Spark cleanly (matching results) rather than erroring. The fallback reason names the disabled
-- flag. The serdes covered here (Concat, SortArray, TruncDate) exercise the shared routing in
-- QueryPlanSerde across three distinct serdes.

-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false

statement
CREATE TABLE test_dispatch_disabled_array(c1 array<int>, c2 array<int>) USING parquet

statement
INSERT INTO test_dispatch_disabled_array VALUES (array(0, 1), array(2, 3)), (array(1, 2), array(3, 4))

-- Concat over array (non-string) children
query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT concat(c1, c2) FROM test_dispatch_disabled_array

statement
CREATE TABLE test_dispatch_disabled_binary(c1 binary, c2 binary) USING parquet

statement
INSERT INTO test_dispatch_disabled_binary VALUES (unhex('0A0B'), unhex('0C0D')), (unhex('01'), unhex('02'))

-- Concat over binary (non-string) children
query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT concat(c1, c2) FROM test_dispatch_disabled_binary

statement
CREATE TABLE test_dispatch_disabled_nested(arr array<array<struct<a:int>>>) USING parquet

statement
INSERT INTO test_dispatch_disabled_nested VALUES
  (array(array(named_struct('a', 2)), array(named_struct('a', 1)))),
  (NULL)

-- SortArray over nested arrays with Struct children
query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT sort_array(arr) FROM test_dispatch_disabled_nested

statement
CREATE TABLE test_dispatch_disabled_date(d date) USING parquet

statement
INSERT INTO test_dispatch_disabled_date VALUES (date('2024-06-15')), (NULL)

-- TruncDate with a format outside the native set
query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT trunc(d, 'day') FROM test_dispatch_disabled_date
