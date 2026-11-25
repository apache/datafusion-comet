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
-- row, before map_from_arrays checks for NULL keys. The native call skips the values on NULL keys
-- and cannot evaluate them for every row instead, as it would build a map for NULL values, so
-- values holding a shared subexpression that can fail fall back to Spark. The dispatched LAST_WIN
-- path lives in map_from_arrays_shared_subexpression.sql.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=true
-- Config: spark.sql.codegen.wholeStage=true

statement
CREATE TABLE test_map_from_arrays_native_shared(k1 array<int>, k2 array<int>, x double)
USING parquet

statement
INSERT INTO test_map_from_arrays_native_shared SELECT /*+ COALESCE(1) */ * FROM VALUES
  (CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<INT>), 0.0D),
  (array(1), array(2), 1.0D)
  AS t(k1, k2, x)

statement
CREATE TABLE test_map_from_arrays_native_nulls(k1 array<int>, k2 array<int>, v array<int>)
USING parquet

-- Valid keys with NULL values, NULL keys with valid values, and both NULL all return NULL. The
-- NULL rows' other array has a different length, which Spark never compares.
statement
INSERT INTO test_map_from_arrays_native_nulls SELECT /*+ COALESCE(1) */ * FROM VALUES
  (CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<INT>), array(10)),
  (array(1, 2), array(3, 4), array(10, 20)),
  (array(1, 2, 3), array(4, 5, 6), CAST(NULL AS ARRAY<INT>)),
  (CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<INT>))
  AS t(k1, k2, v)

-- A single call shares nothing, so Spark skips the values on the NULL keys
query expect_native(map_from_arrays)
SELECT map_from_arrays(k1, array(1.0D / x)) FROM test_map_from_arrays_native_shared

-- The `1.0D / x` that both values arrays hold divides by zero on the row whose keys are NULL. The
-- calls fall back, and Spark raises.
query expect_error(DIVIDE_BY_ZERO)
SELECT map_from_arrays(k1, array(1.0D / x)), map_from_arrays(k2, array(1.0D / x))
FROM test_map_from_arrays_native_shared

-- The check counts `reverse(v)` as able to fail, so the calls fall back
query expect_fallback(expressions share and that can fail)
SELECT map_from_arrays(k1, reverse(v)), map_from_arrays(k2, reverse(v))
FROM test_map_from_arrays_native_nulls

-- An aggregate's update evaluates a subexpression its functions share for every row too. The calls
-- fall back, and Spark raises.
query expect_error(DIVIDE_BY_ZERO)
SELECT count(map_from_arrays(k1, array(1.0D / x))), count(map_from_arrays(k2, array(1.0D / x)))
FROM test_map_from_arrays_native_shared

query expect_fallback(expressions share and that can fail)
SELECT count(map_from_arrays(k1, reverse(v))), count(map_from_arrays(k2, reverse(v)))
FROM test_map_from_arrays_native_nulls

-- With elimination off a projection shares nothing, so the calls skip the values
statement
SET spark.sql.subexpressionElimination.enabled=false

query expect_native(map_from_arrays)
SELECT map_from_arrays(k1, array(1.0D / x)), map_from_arrays(k2, array(1.0D / x))
FROM test_map_from_arrays_native_shared

-- An aggregate's update still shares its functions' inputs. The calls fall back, and Spark raises.
query expect_error(DIVIDE_BY_ZERO)
SELECT count(map_from_arrays(k1, array(1.0D / x))), count(map_from_arrays(k2, array(1.0D / x)))
FROM test_map_from_arrays_native_shared
