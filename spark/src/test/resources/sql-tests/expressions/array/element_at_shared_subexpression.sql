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
-- row, before element_at or an array subscript checks for a NULL array, and skips the rest of the
-- index on such a row. Natively the lookups evaluate an index whose failing parts are all shared
-- for every row, and fall back to Spark for an index that also holds an unshared part that can
-- fail. The map counterparts live in map/element_at_map_shared_subexpression.sql.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=true
-- Config: spark.sql.codegen.wholeStage=true

statement
CREATE TABLE test_element_at_array_shared(
  a1 array<int>, a2 array<int>, i int, j int, k int, l int)
USING parquet

-- On the rows whose arrays are NULL, `1 div i` divides by zero in the first, and only `1 div j`
-- and `1 div l` in the second
statement
INSERT INTO test_element_at_array_shared SELECT /*+ COALESCE(1) */ * FROM VALUES
  (CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<INT>), 0, 1, 1, 1),
  (CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<INT>), 1, 0, 1, 0),
  (array(5, 7), array(6, 8), 1, 1, 1, 1)
  AS v(a1, a2, i, j, k, l)

-- The shared `1 div i` divides by zero on a row whose arrays are NULL, natively as in Spark
query expect_error_class(DIVIDE_BY_ZERO)
SELECT element_at(a1, CAST(1 div i AS INT)), element_at(a2, CAST(1 div i AS INT))
FROM test_element_at_array_shared

query expect_error_class(DIVIDE_BY_ZERO)
SELECT a1[CAST(1 div i AS INT)], a2[CAST(1 div i AS INT)] FROM test_element_at_array_shared

-- `i * 2` and `i - 1` never overflow, but the check counts them as able to fail. On the NULL
-- arrays `i * 2` is 0 or 2 and `i - 1` is -1 or 0, which Spark returns NULL for.
query expect_native(element_at)
SELECT element_at(a1, i * 2), element_at(a2, i * 2) FROM test_element_at_array_shared

query expect_native(getarrayitem)
SELECT a1[i - 1], a2[i - 1] FROM test_element_at_array_shared

-- Beside the shared `1 div k`, the index holds an unshared divide, an ANSI sum and an outer ANSI
-- cast that can fail, and any of them sends it to Spark
query expect_fallback(expressions share and that can fail)
SELECT element_at(a1, CAST((1 div k) + (1 div j) AS INT)),
  element_at(a2, CAST((1 div k) + (1 div l) AS INT))
FROM test_element_at_array_shared

query expect_fallback(expressions share and that can fail)
SELECT a1[CAST((1 div k) + (1 div j) - 1 AS INT)], a2[CAST((1 div k) + (1 div l) - 1 AS INT)]
FROM test_element_at_array_shared

-- An aggregate's update evaluates a subexpression its functions share for every row too, with
-- elimination on or off
query expect_error_class(DIVIDE_BY_ZERO)
SELECT max(element_at(a1, CAST(1 div i AS INT))), max(element_at(a2, CAST(1 div i AS INT)))
FROM test_element_at_array_shared

query expect_fallback(expressions share and that can fail)
SELECT max(element_at(a1, CAST((1 div k) + (1 div j) AS INT))),
  max(element_at(a2, CAST((1 div k) + (1 div l) AS INT)))
FROM test_element_at_array_shared

-- With elimination off a projection shares nothing, so the lookups skip the index
statement
SET spark.sql.subexpressionElimination.enabled=false

query expect_native(element_at)
SELECT element_at(a1, CAST(1 div i AS INT)), element_at(a2, CAST(1 div i AS INT))
FROM test_element_at_array_shared

-- An aggregate's update still shares its functions' inputs, so the lookups evaluate `1 div i` for
-- every row and raise as Spark does
query expect_error_class(DIVIDE_BY_ZERO)
SELECT max(element_at(a1, CAST(1 div i AS INT))), max(element_at(a2, CAST(1 div i AS INT)))
FROM test_element_at_array_shared
