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
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false

-- For x = 0, the left-hand condition evaluates to false; the right-hand
-- expression (1 DIV x) must not be evaluated.
statement
CREATE TABLE test_guarded_and(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_and VALUES (array(0, 1));

query
SELECT filter(a, x -> x <> 0 AND (1 DIV x) > 0) FROM test_guarded_and;

-- For x = 0, the left-hand condition evaluates to true; the right-hand
-- expression (1 DIV x) must not be evaluated.
statement
CREATE TABLE test_guarded_or(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_or VALUES (array(0, 1));

query
SELECT filter(a, x -> x = 0 OR (1 DIV x) > 0) FROM test_guarded_or;

-- In Spark, array indexing is 1-based. element_at(..., 0) throws INVALID_INDEX_VALUE
-- even when ANSI mode is disabled. For x = 0, this branch must not be evaluated.
statement
CREATE TABLE test_guarded_element_at(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_element_at VALUES (array(0));

query
SELECT filter(a, x -> x = 0 OR element_at(array(1, 2), 0) = 1) FROM test_guarded_element_at;

-- In ANSI mode, abs(-2147483648) throws ARITHMETIC_OVERFLOW.
-- For x = 0, the left condition is true, so the right branch must not be evaluated.
statement
CREATE TABLE test_guarded_abs_overflow(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_guarded_abs_overflow VALUES (array(0));

query
SELECT filter(a, x -> x = 0 OR abs(-2147483648) > 0) FROM test_guarded_abs_overflow;

-- Verifies that standard comparison predicates are accepted by the whitelist
-- and evaluated natively without falling back.
statement
CREATE TABLE test_safe_predicates(a ARRAY<INT>) USING parquet;

statement
INSERT INTO test_safe_predicates VALUES (array(-5, 5, 15));

query
SELECT filter(a, x -> x > 0 AND x < 10) FROM test_safe_predicates;

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
