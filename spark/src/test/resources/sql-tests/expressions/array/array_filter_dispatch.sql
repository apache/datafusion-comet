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

-- In DataFusion native execution, scalar CASE WHEN inside a lambda returned []
-- instead of [1]. This must safely route to JVM codegen dispatch.
statement
CREATE TABLE test_scalar_case(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_scalar_case VALUES (array(1, 2, 3));

query
SELECT filter(a, x -> (CASE WHEN x = 1 THEN 10 ELSE 20 END) = 10) FROM test_scalar_case;

-- In DataFusion native execution, scalar COALESCE inside a lambda returned [4]
-- instead of [2, 4]. This must safely route to JVM codegen dispatch.
statement
CREATE TABLE test_scalar_coalesce(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_scalar_coalesce VALUES (array(2, 3, 4));

query
SELECT filter(a, x -> coalesce(x % 2, 1) = 0) FROM test_scalar_coalesce;

-- In DataFusion native execution, null-safe equality returned [] instead of [0].
-- This must safely route to JVM codegen dispatch.
statement
CREATE TABLE test_null_safe_equal(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_null_safe_equal VALUES (array(0, 1, cast(null as int)));

query
SELECT filter(a, x -> x <=> 0) FROM test_null_safe_equal;

-- The inner lambda captures the outer lambda's array variable 'x'.
-- To avoid quadratic memory replication in DataFusion's `take_arrays`,
-- this must degrade to JVM codegen dispatch.
statement
CREATE TABLE test_nested_complex_capture(arr ARRAY<ARRAY<INT>>) USING parquet;

statement
INSERT INTO test_nested_complex_capture VALUES (array(array(1, 2), array(3)));

query
SELECT filter(arr, x -> size(filter(x, y -> size(x) > 1)) > 0) FROM test_nested_complex_capture;

-- =========================================================================
-- Ordinary equality with stateful monotonically_increasing_id() on [NULL, 0]
-- =========================================================================
-- In Spark, binary EqualTo does not evaluate RHS when LHS is NULL.
-- For [NULL, 0], monotonically_increasing_id() is not evaluated on NULL,
-- so for 0 it produces 0 (0 = 0 -> kept).
-- Must degrade to JVM codegen dispatch to avoid state advancement in DataFusion.
statement
CREATE TABLE test_null_ordinary_equality(a ARRAY<BIGINT>) USING parquet;

statement
INSERT INTO test_null_ordinary_equality VALUES (array(cast(null as bigint), 0L));

query
SELECT filter(a, x -> x = monotonically_increasing_id()) FROM test_null_ordinary_equality;

-- In partition 0, `spark_partition_id()` evaluates to 0, producing a scalar
-- division by zero (1 DIV 0) at runtime.
-- For empty arrays [] and NULL rows, Spark guarantees the predicate is never invoked.
statement
CREATE TABLE test_empty_arrays(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_empty_arrays VALUES (array()), (NULL);

query
SELECT filter(a, x -> (1 DIV spark_partition_id()) > 0) FROM test_empty_arrays;

-- rand() must not advance its PRNG sequence when short-circuited.
statement
CREATE TABLE test_guarded_rand(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_rand VALUES (array(0, 1));

query
SELECT filter(a, x -> x = 0 OR rand(42L) > 0.5) FROM test_guarded_rand;
