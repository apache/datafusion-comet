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

-- Spark's projection evaluates a subexpression that several of its expressions share for every
-- row, before the expressions that use it. A nullable array with an item that is not a literal
-- or column sends `array_append` through the JVM codegen dispatcher, which compiles each call
-- alone, so a node in its tree that skips a later argument on a NULL input would skip the shared
-- subexpression too, and an error Spark raises would go unraised. Such a call falls back to Spark
-- instead.
--
-- Spark 4.0 rewrites `array_append` to `array_insert(-1)` before serde, so `CometArrayAppend` is
-- only reachable on Spark 3.x.

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

-- The item is a map lookup, which skips its key on the row whose map is NULL, and the shared
-- `1.0D / x` divides by zero there.
query expect_error(DIVIDE_BY_ZERO)
SELECT array_append(a1, element_at(m1, 1.0D / x)), array_append(a2, element_at(m2, 1.0D / x))
FROM test_array_append_shared

-- A single call shares nothing, so Spark skips the divide on the NULL map too.
query expect_dispatch(array_append)
SELECT array_append(a1, element_at(m1, 1.0D / x)) FROM test_array_append_shared

-- `array_append` itself evaluates its item before its NULL check, so a shared `1.0D / x` directly
-- in the item raises on the row whose array is NULL on both paths, and the calls stay dispatched.
query expect_error(DIVIDE_BY_ZERO)
SELECT array_append(a1, 1.0D / x), array_append(a2, 1.0D / x) FROM test_array_append_shared

statement
CREATE TABLE test_array_append_shared_nonzero(
  a1 array<double>, a2 array<double>, m1 map<double, double>, m2 map<double, double>, x double)
USING parquet

statement
INSERT INTO test_array_append_shared_nonzero VALUES
  (CAST(NULL AS ARRAY<DOUBLE>), CAST(NULL AS ARRAY<DOUBLE>),
   CAST(NULL AS MAP<DOUBLE, DOUBLE>), CAST(NULL AS MAP<DOUBLE, DOUBLE>), 4.0D),
  (array(1.5D), array(2.5D), map(0.5D, 5.0D), map(0.5D, 6.0D), 2.0D)

query expect_dispatch(array_append)
SELECT array_append(a1, 1.0D / x), array_append(a2, 1.0D / x)
FROM test_array_append_shared_nonzero

-- The map lookup in the item still declines the dispatcher.
query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT array_append(a1, element_at(m1, 1.0D / x)), array_append(a2, element_at(m2, 1.0D / x))
FROM test_array_append_shared_nonzero

-- With subexpression elimination off a projection shares nothing, but a whole-stage codegen
-- aggregate still does, so the map lookup in the item still declines the dispatcher.
statement
SET spark.sql.subexpressionElimination.enabled=false

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT array_append(a1, element_at(m1, 1.0D / x)), array_append(a2, element_at(m2, 1.0D / x))
FROM test_array_append_shared
