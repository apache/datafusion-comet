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

-- A subexpression shared with the map or array argument does not stop a lookup from skipping its
-- key on a NULL map or array when the shared part cannot fail: Spark evaluates it for every row,
-- but evaluating it again or not at all is unobservable. Here only the unshared rest of the key
-- fails, on the row whose map or array is NULL.
-- https://github.com/apache/datafusion-comet/issues/6970

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=true

statement
CREATE TABLE map_null_short_circuit_shared(
  m map<int, int>, a array<int>, d decimal(10, 2), i int)
USING parquet

statement
INSERT INTO map_null_short_circuit_shared SELECT /*+ COALESCE(1) */ * FROM VALUES
  (NULL, NULL, 0.00BD, 0),
  (map(1, 5, 2, 7), array(5, 7), 1.00BD, 1)
  AS v(m, a, d, i)

-- Both arguments share `i = 0`
query expect_native(element_at)
SELECT element_at(CASE WHEN i = 0 THEN CAST(NULL AS MAP<INT, INT>) ELSE m END,
  CASE WHEN i = 0 THEN CAST(1 div i AS INT) ELSE 1 END)
FROM map_null_short_circuit_shared

query expect_native(getmapvalue)
SELECT (CASE WHEN i = 0 THEN CAST(NULL AS MAP<INT, INT>) ELSE m END)[
  CASE WHEN i = 0 THEN CAST(1 div i AS INT) ELSE 1 END]
FROM map_null_short_circuit_shared

query expect_native(getarrayitem)
SELECT (CASE WHEN i = 0 THEN CAST(NULL AS ARRAY<INT>) ELSE a END)[
  CASE WHEN i = 0 THEN CAST(1 div i AS INT) ELSE 0 END]
FROM map_null_short_circuit_shared

-- `d = 0` casts the literal 0 to DECIMAL(1, 0) and widens that to `d`'s DECIMAL(10, 2). A cast
-- of a literal that evaluates and a widening decimal cast cannot fail even with ANSI
query expect_native(getmapvalue)
SELECT (CASE WHEN d = 0 THEN CAST(NULL AS MAP<INT, INT>) ELSE m END)[
  element_at(array(1), CASE WHEN d = 0 THEN 0 ELSE 1 END)]
FROM map_null_short_circuit_shared

-- Without ANSI a divide returns NULL instead of failing, so the shared `1 div i` cannot fail. An
-- index of 0 fails in any mode.
statement
SET spark.sql.ansi.enabled=false

query expect_native(getmapvalue)
SELECT (CASE WHEN (1 div i) IS NULL THEN CAST(NULL AS MAP<INT, INT>) ELSE m END)[
  element_at(array(1), CASE WHEN (1 div i) IS NULL THEN 0 ELSE 1 END)]
FROM map_null_short_circuit_shared

-- A shared index of 0 fails even without ANSI, so Spark raises on it for every row
query expect_error(INVALID_INDEX_OF_ZERO)
SELECT m[element_at(array(1), i)], a[element_at(array(1), i)]
FROM map_null_short_circuit_shared
