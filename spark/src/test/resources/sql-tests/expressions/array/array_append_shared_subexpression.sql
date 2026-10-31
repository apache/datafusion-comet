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

-- The nondeterministic nullable array forces the dispatcher, which compiles each call alone and so
-- can skip a subexpression Spark's projection shares. `OptimizeRand` would fold `rand(7) >= 0.0D`,
-- hence `rand(7) * 0.0D = 0.0D`. Spark 4.0 rewrites `array_append`, so this is Spark 3.x only.

-- MaxSparkVersion: 3.5

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=true

statement
CREATE TABLE test_array_append_shared(
  a1 array<double>, a2 array<double>, m1 map<double, double>, m2 map<double, double>, x double)
USING parquet

statement
INSERT INTO test_array_append_shared VALUES
  (CAST(NULL AS ARRAY<DOUBLE>), CAST(NULL AS ARRAY<DOUBLE>),
   CAST(NULL AS MAP<DOUBLE, DOUBLE>), CAST(NULL AS MAP<DOUBLE, DOUBLE>), 0.0D),
  (array(1.5D), array(2.5D), map(0.5D, 5.0D), map(0.5D, 6.0D), 2.0D)

-- The lookup skips its key on the NULL-map row, where the shared `1.0D / x` divides by zero
query expect_error(DIVIDE_BY_ZERO)
SELECT array_append(IF(rand(7) * 0.0D = 0.0D, a1, NULL), element_at(m1, 1.0D / x)),
  array_append(IF(rand(7) * 0.0D = 0.0D, a2, NULL), element_at(m2, 1.0D / x))
FROM test_array_append_shared

-- A single call shares nothing, so Spark skips the divide on the NULL map too.
query expect_dispatch(array_append)
SELECT array_append(IF(rand(7) * 0.0D = 0.0D, a1, NULL), element_at(m1, 1.0D / x))
FROM test_array_append_shared

-- `array_append` evaluates its item before its NULL check, so both paths raise here
query expect_error(DIVIDE_BY_ZERO)
SELECT array_append(IF(rand(7) * 0.0D = 0.0D, a1, NULL), 1.0D / x),
  array_append(IF(rand(7) * 0.0D = 0.0D, a2, NULL), 1.0D / x)
FROM test_array_append_shared

-- A divisor that is never zero: dispatched, unless a map lookup could skip it
query expect_dispatch(array_append)
SELECT array_append(IF(rand(7) * 0.0D = 0.0D, a1, NULL), 1.0D / (x + 1.0D)),
  array_append(IF(rand(7) * 0.0D = 0.0D, a2, NULL), 1.0D / (x + 1.0D))
FROM test_array_append_shared

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT array_append(IF(rand(7) * 0.0D = 0.0D, a1, NULL), element_at(m1, 1.0D / (x + 1.0D))),
  array_append(IF(rand(7) * 0.0D = 0.0D, a2, NULL), element_at(m2, 1.0D / (x + 1.0D)))
FROM test_array_append_shared

-- With elimination off nothing is shared, so Spark skips the divide on the NULL-map row too
statement
SET spark.sql.subexpressionElimination.enabled=false

query expect_dispatch(array_append)
SELECT array_append(IF(rand(7) * 0.0D = 0.0D, a1, NULL), element_at(m1, 1.0D / x)),
  array_append(IF(rand(7) * 0.0D = 0.0D, a2, NULL), element_at(m2, 1.0D / x))
FROM test_array_append_shared
