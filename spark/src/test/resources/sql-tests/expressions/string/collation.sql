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

-- MinSparkVersion: 4.0

-- collation(expr) returns the collation name of a string expression.
-- It folds to a string literal at planning time, so Comet evaluates it natively.

-- default collation on a string literal
query
SELECT collation('abc')

-- collation of an explicit UTF8_BINARY string
query
SELECT collation('hello' COLLATE UTF8_BINARY)

-- collation of a NULL string
query
SELECT collation(CAST(NULL AS STRING))

-- concat preserves a non-default collation in its result type, which Comet's native concat does
-- not, so concat is Incompatible. It is enrolled in the JVM codegen dispatcher, which runs Spark's
-- own doGenCode inside the Comet pipeline, so a collated concat is evaluated natively and matches
-- Spark.
query
SELECT concat('Hello' COLLATE UTF8_LCASE, 'World' COLLATE UTF8_LCASE)

-- reverse is enrolled in the JVM codegen dispatcher, which runs Spark's own doGenCode inside the
-- Comet pipeline, so a collated string is evaluated natively and matches Spark.
query
SELECT reverse('Hello' COLLATE UTF8_LCASE)

-- A standard ICU collation (UNICODE_CI) also dispatches and matches Spark, confirming the path
-- covers any non-UTF8_BINARY collation rather than just UTF8_LCASE.
query
SELECT concat('Hello' COLLATE UNICODE_CI, 'World' COLLATE UNICODE_CI)

query
SELECT reverse('Hello' COLLATE UNICODE_CI)

-- ============================================================================
-- Predicate fallback for non-UTF8_BINARY collated operands. Comet's native
-- kernels are byte-wise, so any collated operand must fall back to Spark;
-- expect_fallback(...) pins the shared reason substring.
-- ============================================================================

statement
CREATE TABLE test_collated_predicates(id INT, s STRING) USING parquet

statement
INSERT INTO test_collated_predicates VALUES
  (1, 'a'),
  (2, 'A'),
  (3, 'hello'),
  (4, 'HELLO'),
  (5, 'World'),
  (6, 'b'),
  (7, NULL)

-- ---------- EqualTo / EqualNullSafe / inequality ---------------------------

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) = 'A' FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE CAST(s AS STRING COLLATE UTF8_LCASE) = 'A' ORDER BY id

-- UNICODE_CI covers a second ICU collation to prove the guard is not UTF8_LCASE-only.
query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UNICODE_CI) = 'A' FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) != 'A' FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) <> 'A' FROM test_collated_predicates ORDER BY id

-- `x <=> NULL` folds via NullPropagation, so we test only a non-NULL RHS.
query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) <=> 'A' FROM test_collated_predicates ORDER BY id

-- ---------- Ordering ------------------------------------------------------

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) < 'b' FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) <= 'a' FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) > 'a' FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) >= 'A' FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UNICODE_CI) < 'b' FROM test_collated_predicates ORDER BY id

-- ---------- In / NotIn / InSet ---------------------------------------------

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE CAST(s AS STRING COLLATE UTF8_LCASE) IN ('A', 'HELLO') ORDER BY id

-- NOT IN exercises CometNot's collated fall-through into the generic path.
query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE CAST(s AS STRING COLLATE UTF8_LCASE) NOT IN ('A', 'HELLO') ORDER BY id

-- 12 elements forces InSet (spark.sql.optimizer.inSetConversionThreshold=10 by default).
query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE CAST(s AS STRING COLLATE UTF8_LCASE) IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'HELLO') ORDER BY id

-- ---------- NOT (EqualTo / EqualNullSafe) rewrites ------------------------

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE NOT (CAST(s AS STRING COLLATE UTF8_LCASE) = 'A') ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE NOT (CAST(s AS STRING COLLATE UTF8_LCASE) <=> 'A') ORDER BY id

-- ---------- Like -----------------------------------------------------------

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) LIKE '%LLO' FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE CAST(s AS STRING COLLATE UTF8_LCASE) LIKE 'H%' ORDER BY id

-- Spark 4.0 rejects UNICODE_CI for LIKE / Contains / StartsWith / EndsWith at analysis time
-- (StringTypeNonCSAICollation), so only UTF8_LCASE is covered for these.

-- ---------- Contains / StartsWith / EndsWith -------------------------------

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, contains(CAST(s AS STRING COLLATE UTF8_LCASE), 'HE') FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, startswith(CAST(s AS STRING COLLATE UTF8_LCASE), 'HE') FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, endswith(CAST(s AS STRING COLLATE UTF8_LCASE), 'LO') FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE contains(CAST(s AS STRING COLLATE UTF8_LCASE), 'ELL') ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE startswith(CAST(s AS STRING COLLATE UTF8_LCASE), 'HE') ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE endswith(CAST(s AS STRING COLLATE UTF8_LCASE), 'O') ORDER BY id

-- ---------- Nested-collated types ------------------------------------------

-- hasNonDefaultStringCollation recurses into array/struct element types.
query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, array(CAST(s AS STRING COLLATE UTF8_LCASE)) = array('A' COLLATE UTF8_LCASE) FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, struct(CAST(s AS STRING COLLATE UTF8_LCASE)) = struct('A' COLLATE UTF8_LCASE) FROM test_collated_predicates ORDER BY id

-- NULL-literal operands are omitted: NullPropagation folds them before Comet sees them. The
-- (7, NULL) row covers NULL-in-data on every query above.
