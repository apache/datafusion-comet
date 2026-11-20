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

-- Whether Spark shares a subexpression among a filter's or a join's condition, a join's keys or an
-- aggregate's results depends on how it runs the operator, so a lookup there that would skip a
-- shared key on a NULL map or array falls back to Spark. A projection and an aggregate's function
-- inputs always share it. The prefix Spark computes for a sort's first key is one projection, so it
-- shares within that key as a project does, and later keys are compared one by one and never
-- share. The filter and join queries check the routing through the fallback reason, since their key
-- never fails. Only the queries keyed on `1 div i` divide by zero on the NULL rows and so observe
-- whether Comet raises where Spark does.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=true
-- A declined join keeps Spark's broadcast, adaptive execution wraps it in a query stage, and the
-- rerun declines the join for its Spark input before looking at the keys, so the final plan carries
-- no uncertain-scope reason (https://github.com/apache/datafusion-comet/issues/6442). Only the
-- join checks need adaptive execution off.
-- Config: spark.sql.adaptive.enabled=false

statement
CREATE TABLE map_null_short_circuit_uncertain(
  m1 map<int, int>, m2 map<int, int>, a1 array<int>, a2 array<int>, i int, j int)
USING parquet

-- `i` is 0 only where the maps and arrays are NULL. COALESCE(1) writes one file, so the NULL row
-- shares a batch with the other.
statement
INSERT INTO map_null_short_circuit_uncertain SELECT /*+ COALESCE(1) */ * FROM VALUES
  (NULL, NULL, NULL, NULL, 0, 0),
  (map(1, 5), map(1, 6), array(5, 7), array(6, 8), 1, 0)
  AS v(m1, m2, a1, a2, i, j)

statement
CREATE TABLE map_null_short_circuit_uncertain_other(j int, k int) USING parquet

statement
INSERT INTO map_null_short_circuit_uncertain_other VALUES (0, 1), (6, 5)

-- A projection's scope is certain, so the lookups stay native
query expect_native(getmapvalue)
SELECT m1[(i + 1) div 2], m2[(i + 1) div 2] FROM map_null_short_circuit_uncertain

query expect_native(getarrayitem)
SELECT a1[CAST((i + 1) div 2 AS INT)], a2[CAST((i + 1) div 2 AS INT)]
FROM map_null_short_circuit_uncertain

-- The filters and joins never fail, so Spark returns the same rows whether it shares or not
query expect_fallback(depends on how Spark runs this operator)
SELECT i FROM map_null_short_circuit_uncertain WHERE (m1[(i + 1) div 2] + m2[(i + 1) div 2]) IS NULL

query expect_fallback(depends on how Spark runs this operator)
SELECT i FROM map_null_short_circuit_uncertain
WHERE (a1[CAST((i + 1) div 2 AS INT)] + a2[CAST((i + 1) div 2 AS INT)]) IS NULL

-- array(...) cannot be NULL, so array_contains never skips the shared key
query expect_native(array_contains)
SELECT i FROM map_null_short_circuit_uncertain
WHERE array_contains(array(1, 2), (i + 1) div 2) OR array_contains(array(3), (i + 1) div 2)

-- The join condition is shared only without whole-stage codegen
query expect_fallback(depends on how Spark runs this operator)
SELECT /*+ BROADCAST(u) */ t.i
FROM map_null_short_circuit_uncertain t JOIN map_null_short_circuit_uncertain_other u
  ON t.j = u.j AND (t.m1[(t.i + 1) div 2] + t.m2[(t.i + 1) div 2] + u.k) IS NULL

-- The stream-side join keys likewise share only without whole-stage codegen
query expect_fallback(depends on how Spark runs this operator)
SELECT /*+ BROADCAST(u) */ t.i, u.k
FROM map_null_short_circuit_uncertain t JOIN map_null_short_circuit_uncertain_other u
  ON t.m1[(t.i + 1) div 2] = u.k AND t.m2[(t.i + 1) div 2] = u.j

-- Spark compares sort keys one by one, so each skips `1 div i` on the NULL maps
query expect_native(getmapvalue)
SELECT i FROM map_null_short_circuit_uncertain SORT BY m1[1 div i], m2[1 div i], i

-- The prefix Spark computes for the first sort key shares `1 div i` within it, so both raise
query expect_error_class(DIVIDE_BY_ZERO)
SELECT i FROM map_null_short_circuit_uncertain SORT BY m1[1 div i] + m2[1 div i]

-- An aggregate's function inputs are a certain scope, so the lookups stay native
query expect_native(getmapvalue)
SELECT max(m1[(i + 1) div 2]), min(m2[(i + 1) div 2]) FROM map_null_short_circuit_uncertain

-- The function's input and the result each hold `1 div i` once in their own scope, so neither
-- evaluates it on the NULL row
query expect_native(getmapvalue)
SELECT i, a2, max(m1[1 div i]), a2[CAST(1 div i AS INT)]
FROM map_null_short_circuit_uncertain GROUP BY i, a2

-- The next query's expectation holds with whole-stage codegen
statement
SET spark.sql.codegen.wholeStage=true

-- An aggregate's generated code evaluates each result expression alone, so Spark skips the shared
-- `1 div i` on the NULL arrays. Without whole-stage codegen it would share it and raise.
query expect_fallback(depends on how Spark runs this operator)
SELECT a1[CAST(1 div i AS INT)] + a2[CAST(1 div i AS INT)]
FROM map_null_short_circuit_uncertain GROUP BY a1, a2, i

-- Without ANSI `element_at` can still fail, on an index of 0, so a filter sharing it falls back.
-- The index here is never 0, so Spark returns the same rows whether it shares or not.
statement
SET spark.sql.ansi.enabled=false

query expect_fallback(depends on how Spark runs this operator)
SELECT i FROM map_null_short_circuit_uncertain
WHERE (m1[element_at(array(1), i + 1)] + m2[element_at(array(1), i + 1)]) IS NULL
