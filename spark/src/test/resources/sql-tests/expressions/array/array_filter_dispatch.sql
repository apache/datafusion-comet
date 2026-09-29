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

-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

query
SELECT filter(array(1, 2), (x, i) -> i < 3)

statement
CREATE TABLE test_dispatch(a array<int>, b array<int>, c array<string>) USING parquet;

statement
INSERT INTO test_dispatch VALUES (array(1,2,3), array(10,20,30), array('abc','xyz','a1'));

query
SELECT filter(c, x -> x rlike '^a') FROM test_dispatch;

query
SELECT filter(a, x -> exists(b, y -> y > x)) FROM test_dispatch;

query
SELECT filter(a, x -> array_max(transform(b, y -> y + x)) > 31) FROM test_dispatch;

-- All elements are <= 0, so the THEN branch containing CAST('bad' AS INT)
-- is never reached at runtime. Eager constant evaluation must not abort planning.
statement
CREATE TABLE test_guarded_case(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_case VALUES (array(-1, 0));

query
SELECT filter(a, x -> CASE WHEN x > 0 THEN CAST('bad' AS INT) > 0 ELSE false END) FROM test_guarded_case;

-- Capturing outer complex types (like array 'b') causes quadratic memory replication
-- in DataFusion's `take_arrays`. This query must safely degrade to JVM codegen dispatch.
statement
CREATE TABLE test_captured_array(a ARRAY<INT>, b ARRAY<INT>) USING parquet;

statement
INSERT INTO test_captured_array VALUES (array(1, -1, 2), array(10, 20));

query
SELECT filter(a, x -> x >= 0 AND size(b) > 0) FROM test_captured_array;

-- Verifies that COALESCE in lambda bodies produces correct results via codegen fallback
-- (avoiding native DataFusion COALESCE evaluation discrepancy).
statement
CREATE TABLE test_coalesce_lambda(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_coalesce_lambda VALUES (array(1, 2, null, 4));

query
SELECT filter(a, x -> coalesce(x % 2 = 0, false)) FROM test_coalesce_lambda;

-- Verifies that CASE WHEN in lambda bodies produces correct results via codegen fallback.
statement
CREATE TABLE test_case_when_lambda(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_case_when_lambda VALUES (array(1, 2, 3));

query
SELECT filter(a, x -> CASE WHEN x = 1 THEN true ELSE x > 2 END) FROM test_case_when_lambda;

-- Verifies that IF in lambda bodies produces correct results via codegen fallback.
statement
CREATE TABLE test_if_lambda(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_if_lambda VALUES (array(1, 2, 3));

query
SELECT filter(a, x -> if(x = 2, false, true)) FROM test_if_lambda;
