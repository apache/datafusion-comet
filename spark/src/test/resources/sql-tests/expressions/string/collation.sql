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
-- Predicate fallback for non-UTF8_BINARY collated operands.
--
-- Comet's native equality/ordering/hashing/substring kernels compare raw UTF-8 bytes, so any
-- predicate operand carrying a non-UTF8_BINARY collation would produce wrong answers on the
-- native path -- e.g. `'a' = 'A'` under UNICODE_CI returns true in Spark but false byte-wise.
-- Every binary comparison and `In`/`InSet`/`Like`/`Contains`/`StartsWith`/`EndsWith` serde now
-- guards its `getSupportLevel` so any collated operand triggers a clean fallback to Spark with a
-- shared "non-UTF8_BINARY collated operands" reason. `expect_fallback(...)` below pins that
-- reason substring so a regressed guard surfaces as either a wrong answer or a missing fallback.
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

-- UTF8_LCASE folds case, so 'a' and 'A' both match 'A'.
query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) = 'A' FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE CAST(s AS STRING COLLATE UTF8_LCASE) = 'A' ORDER BY id

-- UNICODE_CI is a distinct ICU collation, also case-insensitive.
query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UNICODE_CI) = 'A' FROM test_collated_predicates ORDER BY id

-- Not-equals should surface via `!=` (parsed as Not(EqualTo)) and `<>`.
query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) != 'A' FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) <> 'A' FROM test_collated_predicates ORDER BY id

-- Null-safe equality: <=> treats two NULLs as equal.
query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) <=> 'A' FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) <=> CAST(NULL AS STRING COLLATE UTF8_LCASE) FROM test_collated_predicates ORDER BY id

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

-- Small list of literals -> `In`.
query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE CAST(s AS STRING COLLATE UTF8_LCASE) IN ('A', 'HELLO') ORDER BY id

-- `NOT IN` exercises CometNot's special-case rewrite for In: the collated variant must fall
-- through to the generic path so the child serde's Unsupported cascades to a Spark fallback.
query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE CAST(s AS STRING COLLATE UTF8_LCASE) NOT IN ('A', 'HELLO') ORDER BY id

-- Optimizer promotes a large literal list to `InSet`; force it by widening past the threshold
-- (spark.sql.optimizer.inSetConversionThreshold defaults to 10).
query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE CAST(s AS STRING COLLATE UTF8_LCASE) IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'HELLO') ORDER BY id

-- ---------- NOT (EqualTo / EqualNullSafe) rewrites ------------------------

-- Exercises CometNot's guard so the collated child falls through to Spark.
query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE NOT (CAST(s AS STRING COLLATE UTF8_LCASE) = 'A') ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE NOT (CAST(s AS STRING COLLATE UTF8_LCASE) <=> 'A') ORDER BY id

-- ---------- Like -----------------------------------------------------------

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) LIKE '%LLO' FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE CAST(s AS STRING COLLATE UTF8_LCASE) LIKE 'H%' ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE CAST(s AS STRING COLLATE UNICODE_CI) LIKE 'h%' ORDER BY id

-- ---------- Contains / StartsWith / EndsWith -------------------------------

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, contains(CAST(s AS STRING COLLATE UTF8_LCASE), 'HE') FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, startswith(CAST(s AS STRING COLLATE UTF8_LCASE), 'HE') FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, endswith(CAST(s AS STRING COLLATE UTF8_LCASE), 'LO') FROM test_collated_predicates ORDER BY id

-- Filter-side use, exercising the WHERE-clause pushdown path.
query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE contains(CAST(s AS STRING COLLATE UTF8_LCASE), 'ELL') ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE startswith(CAST(s AS STRING COLLATE UNICODE_CI), 'h') ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id FROM test_collated_predicates WHERE endswith(CAST(s AS STRING COLLATE UNICODE_CI), 'O') ORDER BY id

-- ---------- Nested-collated types ------------------------------------------

-- hasNonDefaultStringCollation walks nested types, so a collated string buried inside an
-- array/struct operand must also trigger fallback. If the recursive check regressed, Comet would
-- attempt native array-/struct-equality with byte-wise element compares and diverge here.
query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, array(CAST(s AS STRING COLLATE UTF8_LCASE)) = array('A' COLLATE UTF8_LCASE) FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, struct(CAST(s AS STRING COLLATE UTF8_LCASE)) = struct('A' COLLATE UTF8_LCASE) FROM test_collated_predicates ORDER BY id

-- ---------- NULL propagation ------------------------------------------------

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) = CAST(NULL AS STRING COLLATE UTF8_LCASE) FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(NULL AS STRING COLLATE UTF8_LCASE) IN ('A', 'B') FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, CAST(s AS STRING COLLATE UTF8_LCASE) LIKE CAST(NULL AS STRING COLLATE UTF8_LCASE) FROM test_collated_predicates ORDER BY id

query expect_fallback(non-UTF8_BINARY collated operands)
SELECT id, contains(CAST(s AS STRING COLLATE UTF8_LCASE), CAST(NULL AS STRING COLLATE UTF8_LCASE)) FROM test_collated_predicates ORDER BY id
