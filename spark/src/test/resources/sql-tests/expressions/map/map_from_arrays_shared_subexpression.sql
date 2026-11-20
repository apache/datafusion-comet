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

-- Under `LAST_WIN` the call is dispatched, where `nullSafeCodeGen` skips the values on a NULL-keys
-- row, while Spark's projection evaluates a shared subexpression of them on every row, so such a
-- call falls back.

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

-- `x + 1.0D` is never zero, yet the shared values still decline the dispatcher
query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT map_from_arrays(k1, array(1.0D / (x + 1.0D), 1.0D)),
  map_from_arrays(k2, array(1.0D / (x + 1.0D)))
FROM test_map_from_arrays_shared

-- With elimination off nothing is shared, so Spark skips the values on the NULL-keys row too
statement
SET spark.sql.subexpressionElimination.enabled=false

query expect_dispatch(map_from_arrays)
SELECT map_from_arrays(k1, array(1.0D / x, 1.0D)), map_from_arrays(k2, array(1.0D / x))
FROM test_map_from_arrays_shared
