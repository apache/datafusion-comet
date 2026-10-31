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

-- A `size` over a lookup whose key holds `rand(7)` is dispatched, and the decline check finds the
-- lookup skipping the shared `1.0D / x` on the NULL-map row. `rand(7) * 0.0D` changes no value.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=true

-- On the last row both `1.0D / x` and `1.0D / (x + 1.0D)` find a key.
statement
CREATE TABLE test_size_shared(m1 map<double, array<int>>, m2 map<double, array<int>>, x double)
USING parquet

statement
INSERT INTO test_size_shared VALUES
  (CAST(NULL AS MAP<DOUBLE, ARRAY<INT>>), CAST(NULL AS MAP<DOUBLE, ARRAY<INT>>), 0.0D),
  (map(0.5D, array(1, 2)), map(0.5D, array(3)), 2.0D),
  (map(1.0D, array(1), 0.5D, array(1, 2, 3)), map(1.0D, array(4, 5), 0.5D, array(6)), 1.0D)

-- Without `rand(7)` `size` is native, so this exercises the dispatched `element_at`
query expect_error(DIVIDE_BY_ZERO)
SELECT size(element_at(m1, 1.0D / x)), size(element_at(m2, 1.0D / x)) FROM test_size_shared

query expect_error(DIVIDE_BY_ZERO)
SELECT size(element_at(m1, 1.0D / x + rand(7) * 0.0D)),
  size(element_at(m2, 1.0D / x + rand(7) * 0.0D))
FROM test_size_shared

-- A single call shares nothing, so Spark skips the divide on the NULL map too.
query expect_dispatch(size)
SELECT size(element_at(m1, 1.0D / x + rand(7) * 0.0D)) FROM test_size_shared

-- `x + 1.0D` is never zero, yet the shared key still declines the dispatcher
query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT size(element_at(m1, 1.0D / (x + 1.0D) + rand(7) * 0.0D)),
  size(element_at(m2, 1.0D / (x + 1.0D) + rand(7) * 0.0D))
FROM test_size_shared
