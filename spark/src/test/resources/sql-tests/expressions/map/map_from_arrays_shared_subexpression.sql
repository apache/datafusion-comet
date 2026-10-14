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

-- Under `LAST_WIN` `map_from_arrays` runs through the JVM codegen dispatcher, which compiles each
-- call alone, so Spark's own `nullSafeCodeGen` there would skip the values on a row whose keys
-- are NULL. Spark's projection evaluates a subexpression of the values that several of its
-- expressions share for every row, so under ANSI mode it raises on that row where the dispatched
-- call would return NULL. Such a call falls back to Spark instead.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=true
-- Config: spark.sql.mapKeyDedupPolicy=LAST_WIN

statement
CREATE TABLE test_map_from_arrays_shared(k1 array<int>, k2 array<int>, x double) USING parquet

statement
INSERT INTO test_map_from_arrays_shared VALUES
  (CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<INT>), 0.0D),
  (array(1, 1), array(2), 2.0D)

-- The `1.0D / x` that both values arrays hold divides by zero on the row whose keys are NULL.
query expect_error(DIVIDE_BY_ZERO)
SELECT map_from_arrays(k1, array(1.0D / x, 1.0D)), map_from_arrays(k2, array(1.0D / x))
FROM test_map_from_arrays_shared

-- A single call shares nothing, so Spark skips the values on the NULL keys too.
query expect_dispatch(map_from_arrays)
SELECT map_from_arrays(k1, array(1.0D / x, 1.0D)) FROM test_map_from_arrays_shared

-- On rows where the divide cannot fail, the shared `1.0D / x` still sends the calls back to
-- Spark.
statement
CREATE TABLE test_map_from_arrays_shared_nonzero(k1 array<int>, k2 array<int>, x double)
USING parquet

statement
INSERT INTO test_map_from_arrays_shared_nonzero VALUES
  (CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<INT>), 4.0D),
  (array(1, 1), array(2), 2.0D)

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT map_from_arrays(k1, array(1.0D / x, 1.0D)), map_from_arrays(k2, array(1.0D / x))
FROM test_map_from_arrays_shared_nonzero

-- With subexpression elimination off Spark shares nothing, so the calls stay dispatched and both
-- engines return NULL on the row whose keys are NULL.
statement
SET spark.sql.subexpressionElimination.enabled=false

query expect_dispatch(map_from_arrays)
SELECT map_from_arrays(k1, array(1.0D / x, 1.0D)), map_from_arrays(k2, array(1.0D / x))
FROM test_map_from_arrays_shared
