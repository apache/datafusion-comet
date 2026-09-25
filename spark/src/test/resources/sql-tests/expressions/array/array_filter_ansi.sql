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

-- =========================================================================
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


-- ========================================================
-- 2. Guarded AND short-circuiting in ANSI mode
-- =================================================
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
-- is never reached at runtime. Eager constant evaluation must not abort planning.
statement
CREATE TABLE test_guarded_case(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_case VALUES (array(-1, 0));

query
SELECT filter(a, x -> CASE WHEN x > 0 THEN CAST('bad' AS INT) > 0 ELSE false END) FROM test_guarded_case;


-- =========================================================================
-- 5. Stateful / non-deterministic expression in guarded OR branch
-- =========================================================================
-- In Spark, for x = 0, the left-hand condition evaluates to true, so
-- monotonically_increasing_id() must NOT be evaluated for that element.
-- Evaluating it speculatively advances the internal counter and produces wrong results.
statement
CREATE TABLE test_guarded_stateful(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_stateful VALUES (array(0, 1));

query
SELECT filter(a, x -> x = 0 OR monotonically_increasing_id() = 0) FROM test_guarded_stateful;


-- =========================================================================
-- 6. Invalid index 0 with element_at in guarded OR branch (fails in all modes)
-- =========================================================================
-- In Spark, array indexing is 1-based. element_at(..., 0) throws INVALID_INDEX_VALUE
-- even when ANSI mode is disabled. For x = 0, this branch must not be evaluated.
statement
CREATE TABLE test_guarded_element_at(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_element_at VALUES (array(0));

query
SELECT filter(a, x -> x = 0 OR element_at(array(1, 2), 0) = 1) FROM test_guarded_element_at;


-- =========================================================================
-- 7. Arithmetic overflow with abs(INT_MIN) in guarded OR branch
-- =========================================================================
-- In ANSI mode, abs(-2147483648) throws ARITHMETIC_OVERFLOW.
-- For x = 0, the left condition is true, so the right branch must not be evaluated.
statement
CREATE TABLE test_guarded_abs_overflow(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_abs_overflow VALUES (array(0));

query
SELECT filter(a, x -> x = 0 OR abs(-2147483648) > 0) FROM test_guarded_abs_overflow;


-- =========================================================================
-- 8. Non-deterministic rand() in guarded OR branch
-- ========================================================
-- rand() must not advance its PRNG sequence when short-circuited.
statement
CREATE TABLE test_guarded_rand(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_rand VALUES (array(0, 1));

query
SELECT filter(a, x -> x = 0 OR rand(42L) > 0.5) FROM test_guarded_rand;


-- =========================================================================
-- 9. Safe whitelist control: pure comparisons continue to work natively
-- ========================================================
-- Verifies that standard comparison predicates are accepted by the whitelist
-- and evaluated natively without falling back.
statement
CREATE TABLE test_safe_predicates(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_safe_predicates VALUES (array(-5, 5, 15));

query
SELECT filter(a, x -> x > 0 AND x < 10) FROM test_safe_predicates;

-- =========================================================================
-- 10. Nullable boolean conditions (SQL Three-Valued Logic / 3VL)
-- =========================================================================
-- Verifies that RHS is evaluated when LHS is NULL:
-- - In OR:  NULL OR true  evaluates to TRUE  (element preserved)
-- - In AND: NULL AND false evaluates to FALSE (element dropped)
statement
CREATE TABLE test_guarded_nullable(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_nullable VALUES (array(cast(null as int), 0, 1));

-- For NULL element: LHS is null, RHS is true -> (null OR true) = TRUE -> kept
query
SELECT filter(a, x -> (x > 0) OR (x IS NULL)) FROM test_guarded_nullable;

-- For NULL element: LHS is null, RHS is false -> (null AND false) = FALSE -> dropped
query
SELECT filter(a, x -> (x > 0) AND (x IS NOT NULL AND x > 10)) FROM test_guarded_nullable;


-- =========================================================================
-- 11. Nested conditional predicates (recursive tree rewriting)
-- =========================================================================
-- Verifies that rewrite_short_circuit_binary correctly handles nested trees
-- combining both AND and OR with guarded fallible expressions.
statement
CREATE TABLE test_guarded_nested(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_nested VALUES (array(-1, 0, 1, 2));

-- Compound condition: x = 0 is safe, (x > 0 AND 1 DIV x > 0) guards division for x > 0
query
SELECT filter(a, x -> x = 0 OR (x > 0 AND (1 DIV x) > 0)) FROM test_guarded_nested;

-- Compound condition: guards with multiple logical levels
query
SELECT filter(a, x -> (x <> 0 AND (10 DIV x) > 0) AND (x > 0 OR x = -1)) FROM test_guarded_nested;