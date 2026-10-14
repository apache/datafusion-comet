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
-- row, before the expressions that use it. A `size` over a lookup whose key holds a
-- nondeterministic `rand(7)` goes through the dispatcher, whose decline check walks the whole
-- tree and finds the lookup skipping the shared `1.0D / x` on the row whose map is NULL.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=true

statement
CREATE TABLE test_size_shared(m1 map<double, array<int>>, m2 map<double, array<int>>, x double)
USING parquet

statement
INSERT INTO test_size_shared VALUES
  (CAST(NULL AS MAP<DOUBLE, ARRAY<INT>>), CAST(NULL AS MAP<DOUBLE, ARRAY<INT>>), 0.0D),
  (map(0.5D, array(1, 2)), map(0.5D, array(3)), 2.0D)

-- Without a nondeterministic key `size` runs natively, so this query exercises the decline of the
-- dispatched `element_at`.
query expect_error(DIVIDE_BY_ZERO)
SELECT size(element_at(m1, 1.0D / x)), size(element_at(m2, 1.0D / x)) FROM test_size_shared

-- The `+ rand(7) * 0.0D` leaves the key's value unchanged.
query expect_error(DIVIDE_BY_ZERO)
SELECT size(element_at(m1, 1.0D / x + rand(7) * 0.0D)),
  size(element_at(m2, 1.0D / x + rand(7) * 0.0D))
FROM test_size_shared

-- A single call shares nothing, so Spark skips the divide on the NULL map too.
query expect_dispatch(size)
SELECT size(element_at(m1, 1.0D / x + rand(7) * 0.0D)) FROM test_size_shared

-- On rows where the divide cannot fail, the shared `1.0D / x` still sends the calls back to
-- Spark.
statement
CREATE TABLE test_size_shared_nonzero(
  m1 map<double, array<int>>, m2 map<double, array<int>>, x double)
USING parquet

statement
INSERT INTO test_size_shared_nonzero VALUES
  (CAST(NULL AS MAP<DOUBLE, ARRAY<INT>>), CAST(NULL AS MAP<DOUBLE, ARRAY<INT>>), 4.0D),
  (map(0.5D, array(1, 2)), map(0.5D, array(3)), 2.0D)

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT size(element_at(m1, 1.0D / x + rand(7) * 0.0D)),
  size(element_at(m2, 1.0D / x + rand(7) * 0.0D))
FROM test_size_shared_nonzero
