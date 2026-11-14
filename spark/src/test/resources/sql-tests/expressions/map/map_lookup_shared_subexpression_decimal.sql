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

-- As map_lookup_shared_subexpression.sql, but the shared key holds decimal arithmetic, which the
-- serde wraps in CheckOverflow before the dispatcher sees it.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=true

-- On the last row both `d / x` and `d / (x + 1)` find a key.
statement
CREATE TABLE test_map_lookup_shared_decimal(
  m1 map<double, int>, m2 map<double, int>, d decimal(10, 0), x decimal(10, 0))
USING parquet

statement
INSERT INTO test_map_lookup_shared_decimal VALUES
  (CAST(NULL AS MAP<DOUBLE, INT>), CAST(NULL AS MAP<DOUBLE, INT>), 1, 0),
  (map(0.5D, 5), map(0.5D, 6), 1, 3),
  (map(1.0D, 7, 0.5D, 8), map(1.0D, 9, 0.5D, 10), 1, 1)

-- The shared `d / x` divides by zero on the row whose maps are NULL.
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(m1, CAST(d / x AS DOUBLE)), element_at(m2, CAST(d / x AS DOUBLE))
FROM test_map_lookup_shared_decimal

query expect_error(DIVIDE_BY_ZERO)
SELECT m1[CAST(d / x AS DOUBLE)], m2[CAST(d / x AS DOUBLE)] FROM test_map_lookup_shared_decimal

-- `x + 1` is never zero, yet the shared key still declines the dispatcher
query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT element_at(m1, CAST(d / (x + 1) AS DOUBLE)), element_at(m2, CAST(d / (x + 1) AS DOUBLE))
FROM test_map_lookup_shared_decimal

-- The dispatcher eliminates a subexpression the lookup itself repeats, as Spark does
query expect_dispatch(element_at)
SELECT element_at(m1, CAST(d / (x + 1) AS DOUBLE) + CAST(d / (x + 1) AS DOUBLE))
FROM test_map_lookup_shared_decimal

-- A single lookup shares nothing, so Spark skips the divide on the NULL map too.
query expect_dispatch(element_at)
SELECT element_at(m1, CAST(d / x AS DOUBLE)) FROM test_map_lookup_shared_decimal
