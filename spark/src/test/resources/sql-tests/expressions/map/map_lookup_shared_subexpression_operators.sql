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

-- Shuffle keys and the projection of a TakeOrderedAndProject share subexpressions as a project
-- list does (see map_lookup_shared_subexpression.sql). The planner must decline a DOUBLE-keyed
-- lookup there in the same scope as the execution-time serialization, or it picks the native
-- path and execution then fails to serialize the lookup.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=true
-- Config: spark.sql.shuffle.partitions=10

statement
CREATE TABLE test_map_lookup_shared_ops(m1 map<double, int>, m2 map<double, int>, x double)
USING parquet

-- `1.0D / (x + 1.0D)`, whose divisor is never zero, finds a key in the last three rows.
statement
INSERT INTO test_map_lookup_shared_ops VALUES
  (CAST(NULL AS MAP<DOUBLE, INT>), CAST(NULL AS MAP<DOUBLE, INT>), 0.0D),
  (map(0.5D, 5), map(0.5D, 6), 2.0D),
  (map(0.5D, 7), map(0.5D, 8), 1.0D),
  (map(0.25D, 1), map(0.25D, 2), 3.0D),
  (map(0.125D, 3), map(0.125D, 4), 7.0D)

-- Hash partitioning keys: native shuffle declines, and the JVM columnar shuffle evaluates the
-- keys with Spark's projection.
query expect_error(DIVIDE_BY_ZERO)
SELECT x
FROM test_map_lookup_shared_ops
DISTRIBUTE BY element_at(m1, 1.0D / x), element_at(m2, 1.0D / x)

query
SELECT x
FROM test_map_lookup_shared_ops
DISTRIBUTE BY element_at(m1, 1.0D / (x + 1.0D)), element_at(m2, 1.0D / (x + 1.0D))

-- Range partitioning keys of a global sort. The sort itself declines the lookups too.
query expect_error(DIVIDE_BY_ZERO)
SELECT x
FROM test_map_lookup_shared_ops
ORDER BY element_at(m1, 1.0D / x), element_at(m2, 1.0D / x)

-- Two rows miss both lookups, so `x` breaks the tie.
query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT x
FROM test_map_lookup_shared_ops
ORDER BY element_at(m1, 1.0D / (x + 1.0D)), element_at(m2, 1.0D / (x + 1.0D)), x

-- The projection of a TakeOrderedAndProject. The subquery keeps the projection above the sort,
-- where the outer query's ORDER BY would put it in a Project below the sort.
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(m1, 1.0D / x), element_at(m2, 1.0D / x)
FROM (SELECT m1, m2, x FROM test_map_lookup_shared_ops ORDER BY x)
LIMIT 10

query expect_fallback(unsupported projection)
SELECT element_at(m1, 1.0D / (x + 1.0D)), element_at(m2, 1.0D / (x + 1.0D))
FROM (SELECT m1, m2, x FROM test_map_lookup_shared_ops ORDER BY x)
LIMIT 10

-- Without ANSI mode the divide returns NULL, but the decline does not depend on ANSI mode.
statement
SET spark.sql.ansi.enabled=false

query
SELECT x
FROM test_map_lookup_shared_ops
DISTRIBUTE BY element_at(m1, 1.0D / x), element_at(m2, 1.0D / x)

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT x
FROM test_map_lookup_shared_ops
ORDER BY element_at(m1, 1.0D / x), element_at(m2, 1.0D / x), x

query expect_fallback(unsupported projection)
SELECT element_at(m1, 1.0D / x), element_at(m2, 1.0D / x)
FROM (SELECT m1, m2, x FROM test_map_lookup_shared_ops ORDER BY x)
LIMIT 10
