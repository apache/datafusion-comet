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

-- Config: spark.sql.ansi.enabled=true

--SET spark.sql.ansi.enabled=true

-- =================================================================
-- 1. Empty and mixed empty/null arrays (Zero-row lambda guard regression)
-- =========================================================================
-- In partition 0, `spark_partition_id()` evaluates to 0, producing a scalar
-- division by zero (1 DIV 0) at runtime.
-- For empty arrays [] and NULL rows, Spark guarantees the predicate is never invoked.
statement
CREATE TABLE test_empty_arrays(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_empty_arrays VALUES (array()), (NULL);

query
SELECT filter(a, x -> (1 DIV spark_partition_id()) > 0) FROM test_empty_arrays;


-- =========================================================================
-- 2. Guarded AND short-circuiting in ANSI mode
-- =========================================================================
-- For x = 0, the left-hand condition evaluates to false; the right-hand
-- expression (1 DIV x) must not be evaluated.
statement
CREATE TABLE test_guarded_and(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_and VALUES (array(0, 1));

query
SELECT filter(a, x -> x <> 0 AND (1 DIV x) > 0) FROM test_guarded_and;


-- =========================================================================
-- 3. Guarded OR short-circuiting in ANSI mode
-- =========================================================================
-- For x = 0, the left-hand condition evaluates to true; the right-hand
-- expression (1 DIV x) must not be evaluated.
statement
CREATE TABLE test_guarded_or(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_or VALUES (array(0, 1));

query
SELECT filter(a, x -> x = 0 OR (1 DIV x) > 0) FROM test_guarded_or;


-- =========================================================================
-- 4. Guarded CASE WHEN in ANSI mode (Speculative serialization regression)
-- =========================================================================
-- All elements are <= 0, so the THEN branch containing CAST('bad' AS INT)
-- is never reached at runtime.
statement
CREATE TABLE test_guarded_case(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_case VALUES (array(-1, 0));

query
SELECT filter(a, x -> CASE WHEN x > 0 THEN CAST('bad' AS INT) > 0 ELSE false END) FROM test_guarded_case;