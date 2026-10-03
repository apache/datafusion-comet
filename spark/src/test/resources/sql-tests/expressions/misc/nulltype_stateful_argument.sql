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

-- Spark's generated code skips an argument on rows another argument decides (a NULL operand, a
-- NULL divisor), and native evaluation computes every argument over the whole batch, so a stateful
-- expression advances on rows Spark skips. An operator whose native plan runs a codegen dispatcher
-- kernel with a NullType result used to fall back to Spark, together with the native operators
-- above it up to a shuffle, and stays there when it evaluates a non-deterministic expression
-- outside the dispatcher.

statement
CREATE TABLE test_nulltype_stateful(id bigint) USING parquet

-- One file, so the rows share a partition and the counter.
statement
INSERT INTO test_nulltype_stateful SELECT id FROM range(0, 4, 1, 1)

query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT id, arrays_overlap(IF(id = 0, CAST(NULL AS ARRAY<VOID>), transform(array(id), x -> NULL)), IF(monotonically_increasing_id() = 0, array(), array(NULL))) FROM test_nulltype_stateful

query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT id, arrays_overlap(IF(id = 0, CAST(NULL AS ARRAY<VOID>), transform(array(id), x -> NULL)), array_repeat(NULL, CAST(monotonically_increasing_id() AS INT))) FROM test_nulltype_stateful

-- The NullType value and the stateful argument in different columns of one projection.
query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT id, map(id, NULL), IF(id = 0, NULL, id) + monotonically_increasing_id() FROM test_nulltype_stateful

-- The NullType value computed by a native operator below the one with the stateful argument, in
-- the same native block (SORT BY keeps the two projections apart without an exchange).
query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT id, m, IF(id = 0, NULL, id) + monotonically_increasing_id() FROM (SELECT id, map(id, NULL) AS m FROM test_nulltype_stateful SORT BY id)

-- The NullType value computed below a broadcast or a union, which exist only over a native plan.
query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT /*+ BROADCAST(b) */ t.id, IF(t.id = 0, NULL, t.id) + monotonically_increasing_id(), b.a FROM test_nulltype_stateful t JOIN (SELECT id, transform(array(id), x -> NULL) AS a FROM test_nulltype_stateful) b ON t.id = b.id

query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT id, IF(id = 0, NULL, id) + monotonically_increasing_id(), a FROM (SELECT id, transform(array(id), x -> NULL) AS a FROM test_nulltype_stateful UNION ALL SELECT id, transform(array(id), x -> NULL) FROM test_nulltype_stateful)

-- Spark evaluates a divisor first and skips the dividend when the divisor is NULL.
query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT id, map(id, NULL), monotonically_increasing_id() DIV IF(id = 0, CAST(NULL AS BIGINT), 1L) FROM test_nulltype_stateful

-- ln serializes its argument twice, for its domain check.
query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT id, map(id, NULL), ln(CAST(monotonically_increasing_id() AS DOUBLE)) FROM test_nulltype_stateful

-- ORDER BY ... LIMIT becomes TakeOrderedAndProject, which builds its projection natively when it
-- runs. The optimizer collapses the second query's two projections into that projection too.
query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT id, map(id, NULL), IF(id = 0, CAST(NULL AS BIGINT), id) + monotonically_increasing_id() FROM (SELECT id FROM test_nulltype_stateful ORDER BY id) LIMIT 4

query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT id, m, IF(id = 0, CAST(NULL AS BIGINT), id) + monotonically_increasing_id() FROM (SELECT id, map(id, NULL) AS m FROM test_nulltype_stateful ORDER BY id) LIMIT 4

-- The kernel in TakeOrderedAndProject's sort keys, and in the projection of one below the operator.
query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT id, IF(id = 0, CAST(NULL AS BIGINT), id) + monotonically_increasing_id() FROM (SELECT id FROM test_nulltype_stateful ORDER BY size(transform(array(id), x -> NULL)), id) LIMIT 4

query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT id, m, IF(id = 0, CAST(NULL AS BIGINT), id) + monotonically_increasing_id() FROM (SELECT id, map(id, NULL) AS m FROM (SELECT id FROM test_nulltype_stateful ORDER BY id) LIMIT 4)

-- A kernel with a typed result can run a NullType value inside it: the whole coalesce goes to the
-- dispatcher (its guarded argument is non-deterministic), and holds a transform main refused.
query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT id, coalesce(IF(monotonically_increasing_id() >= 0, size(transform(array(id), x -> NULL)), NULL), 0) AS a, IF(id = 0, CAST(NULL AS BIGINT), id) + monotonically_increasing_id() AS v FROM test_nulltype_stateful

-- The kernel as the key of a shuffle below the operator: main refused the shuffle, so the
-- operator above it ran in Spark.
query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT id, IF(id = 0, NULL, id) + monotonically_increasing_id() FROM (SELECT id FROM test_nulltype_stateful DISTRIBUTE BY transform(array(id), x -> NULL))

-- A non-deterministic column the dispatcher runs as a whole evaluates Spark's own generated code,
-- so it stays native.
query
SELECT id, map(monotonically_increasing_id(), NULL) FROM test_nulltype_stateful

-- Any other non-deterministic expression falls back, even one native evaluation gets right: main
-- ran all of these in Spark.
query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT id, map(id, NULL), IF(id = 0, NULL, monotonically_increasing_id()) FROM test_nulltype_stateful

-- A NULL literal is not a dispatcher kernel, so this ran natively on main and still does.
query
SELECT id, NULL AS n, 1L + monotonically_increasing_id() FROM test_nulltype_stateful

-- A cast that only relabels nested nullability runs natively, but main ran this one (an
-- array<date> field) through the dispatcher, which evaluates the child as Spark's generated code
-- does. A non-deterministic child keeps that route.
query expect_dispatch(cast)
SELECT id, CAST(named_struct('a', array(DATE '2020-01-01'), 'b', IF(id = 0, NULL, id) + monotonically_increasing_id()) AS struct<a: array<date>, b: bigint>) FROM test_nulltype_stateful

-- The partition id does not depend on the rows evaluated before it.
query
SELECT id, array_union(array_repeat(NULL, CAST(id AS INT)), array_repeat(NULL, CAST(id AS INT))), spark_partition_id() FROM test_nulltype_stateful
