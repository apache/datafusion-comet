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

-- Spark evaluates a subexpression that a projection's or an aggregate's expressions share for every
-- row, before element_at or a map lookup checks for a NULL map, and skips the rest of the key on
-- such a row. Natively the lookups evaluate a key whose failing parts are all shared for every row,
-- and fall back to Spark for a key that also holds an unshared part that can fail.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=true
-- Config: spark.sql.codegen.wholeStage=true

statement
CREATE TABLE test_element_at_map_shared(
  m1 map<int, int>, m2 map<int, int>, i int, j int, k int, l int)
USING parquet

-- On the rows whose maps are NULL, `1 div i` divides by zero in the first, and only `1 div j` and
-- `1 div l` in the second
statement
INSERT INTO test_element_at_map_shared SELECT /*+ COALESCE(1) */ * FROM VALUES
  (CAST(NULL AS MAP<INT, INT>), CAST(NULL AS MAP<INT, INT>), 0, 1, 1, 1),
  (CAST(NULL AS MAP<INT, INT>), CAST(NULL AS MAP<INT, INT>), 1, 0, 1, 0),
  (map(1, 5, 2, 7), map(1, 6, 2, 8), 1, 1, 1, 1)
  AS v(m1, m2, i, j, k, l)

-- The shared `1 div i` divides by zero on a row whose maps are NULL, natively as in Spark
query expect_error_class(DIVIDE_BY_ZERO)
SELECT element_at(m1, 1 div i), element_at(m2, 1 div i) FROM test_element_at_map_shared

query expect_error_class(DIVIDE_BY_ZERO)
SELECT m1[1 div i], m2[1 div i] FROM test_element_at_map_shared

-- `i + 1` never overflows here, but the check is conservative and counts it as able to fail
query expect_native(element_at)
SELECT element_at(m1, i + 1), element_at(m2, i + 1) FROM test_element_at_map_shared

-- Beside the shared `1 div k`, the key holds an unshared divide and an ANSI sum that can fail,
-- and either sends it to Spark
query expect_fallback(expressions share and that can fail)
SELECT element_at(m1, (1 div k) + (1 div j)), element_at(m2, (1 div k) + (1 div l))
FROM test_element_at_map_shared

query expect_fallback(expressions share and that can fail)
SELECT m1[(1 div k) + (1 div j)], m2[(1 div k) + (1 div l)] FROM test_element_at_map_shared

-- Whether a filter shares depends on how Spark runs it, so the lookups fall back
query expect_fallback(depends on how Spark runs this operator)
SELECT i FROM test_element_at_map_shared
WHERE (element_at(m1, (i + 1) div 2) + element_at(m2, (i + 1) div 2) > 1) IS NULL

-- An aggregate's update evaluates a subexpression its functions share for every row too, with
-- elimination on or off
query expect_error_class(DIVIDE_BY_ZERO)
SELECT max(element_at(m1, 1 div i)), max(element_at(m2, 1 div i))
FROM test_element_at_map_shared

query expect_fallback(expressions share and that can fail)
SELECT max(element_at(m1, (1 div k) + (1 div j))), max(element_at(m2, (1 div k) + (1 div l)))
FROM test_element_at_map_shared

-- With elimination off a projection shares nothing, so the lookups skip the key
statement
SET spark.sql.subexpressionElimination.enabled=false

query expect_native(element_at)
SELECT element_at(m1, 1 div i), element_at(m2, 1 div i) FROM test_element_at_map_shared

-- An aggregate's update still shares its functions' inputs, so the lookups evaluate `1 div i` for
-- every row and raise as Spark does
query expect_error_class(DIVIDE_BY_ZERO)
SELECT max(element_at(m1, 1 div i)), max(element_at(m2, 1 div i)) FROM test_element_at_map_shared
