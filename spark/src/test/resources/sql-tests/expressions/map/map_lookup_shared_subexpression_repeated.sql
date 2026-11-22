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

-- The dispatcher eliminates a subexpression a lookup repeats in its key, so it evaluates the key
-- on a NULL-map row. Spark does the same only where it eliminates that subexpression however the
-- operator runs, such as in a project outside a conditional branch, so elsewhere such a lookup
-- falls back. Spark 4.2 can eliminate it in a whole-stage filter too, which the last config turns
-- off so the filter cases expect the same on every version.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=true
-- Config: spark.sql.codegen.wholeStage=true
-- Config: spark.sql.subexpressionElimination.filterExec.enabled=false

-- The key `CAST(s AS DOUBLE)` fails only on the row whose map is NULL.
statement
CREATE TABLE test_map_lookup_repeated(m map<double, int>, s string) USING parquet

statement
INSERT INTO test_map_lookup_repeated VALUES
  (CAST(NULL AS MAP<DOUBLE, INT>), 'abc'),
  (map(2.0D, 1), '1'),
  (map(4.0D, CAST(NULL AS INT)), '2')

-- A whole-stage filter evaluates the condition without elimination here and skips the key
query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT * FROM test_map_lookup_repeated
WHERE element_at(m, CAST(s AS DOUBLE) + CAST(s AS DOUBLE)) IS NULL

query expect_dispatch(element_at)
SELECT * FROM test_map_lookup_repeated WHERE element_at(m, CAST(s AS DOUBLE)) IS NULL

-- A project eliminates the repeat, so Spark raises on the NULL-map row as the dispatcher does
query expect_error(CAST_INVALID_INPUT)
SELECT element_at(m, CAST(s AS DOUBLE) + CAST(s AS DOUBLE)) IS NULL FROM test_map_lookup_repeated

query expect_dispatch(element_at)
SELECT element_at(m, CAST(s AS DOUBLE) + CAST(s AS DOUBLE)) IS NULL
FROM test_map_lookup_repeated WHERE s <> 'abc'

-- Spark does not eliminate a subexpression out of a conditional branch
query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT IF(s = 'abc', element_at(m, CAST(s AS DOUBLE) + CAST(s AS DOUBLE)), 0)
FROM test_map_lookup_repeated

-- A sort compares a later key one by one, without elimination
query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT s FROM test_map_lookup_repeated
SORT BY s, element_at(m, CAST(s AS DOUBLE) + CAST(s AS DOUBLE))

-- A top-k ordering evaluates each key on its own, without elimination
query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT s FROM test_map_lookup_repeated
ORDER BY element_at(m, CAST(s AS DOUBLE) + CAST(s AS DOUBLE)), s LIMIT 2

-- With elimination off the dispatcher keeps the repeat too
statement
SET spark.sql.subexpressionElimination.enabled=false

query expect_dispatch(element_at)
SELECT * FROM test_map_lookup_repeated
WHERE element_at(m, CAST(s AS DOUBLE) + CAST(s AS DOUBLE)) IS NULL
