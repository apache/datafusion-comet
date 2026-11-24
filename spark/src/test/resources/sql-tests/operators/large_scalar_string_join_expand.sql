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

-- A large string scalar expanded by a join filter or by Expand, which #7316 left unbounded. A join
-- evaluates its filter over up to a batch size of the row pairs it matched, however wide its
-- inputs, so the planner runs the join at a smaller batch size. Expand evaluates its projections
-- over its input batch, so the planner splits that batch, as it does under a projection. A full
-- sort computes every row's key when it merges and holds them all, so a key with such a literal
-- falls back to Spark's sort, which compares keys row by row.
-- See https://github.com/apache/datafusion-comet/issues/7689.
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.sql.autoBroadcastJoinThreshold=-1
-- Config: spark.comet.exec.range.enabled=true
-- ConstantFolding: enabled

statement
CREATE TABLE test_scalar_join_a(k int, s string) USING parquet

statement
INSERT INTO test_scalar_join_a SELECT /*+ COALESCE(1) */ 1, CAST(id AS STRING) FROM range(8192)

statement
CREATE TABLE test_scalar_join_b(k int, s string) USING parquet

statement
INSERT INTO test_scalar_join_b VALUES (1, 'b')

-- a sort-merge join whose filter references both sides, over 8192 matched pairs
query
SELECT /*+ SHUFFLE_MERGE(b) */ count(*) FROM test_scalar_join_a a JOIN test_scalar_join_b b
  ON a.k = b.k AND length(concat(repeat('x', 262144), a.s, b.s)) > 0

-- the same as a shuffled hash join
query
SELECT /*+ SHUFFLE_HASH(b) */ count(*) FROM test_scalar_join_a a JOIN test_scalar_join_b b
  ON a.k = b.k AND length(concat(repeat('x', 262144), a.s, b.s)) > 0

-- a broadcast nested loop join, whose condition is all filter
query
SELECT /*+ BROADCAST(b) */ count(*) FROM test_scalar_join_a a JOIN test_scalar_join_b b
  ON length(concat(repeat('x', 262144), a.s, b.s)) > 0

-- the same with a range as the probe side, which emits batches of the size it was planned with
-- and which the join evaluates the filter against one build row at a time
query
SELECT /*+ BROADCAST(b) */ count(*) FROM range(0, 8192, 1, 1) r JOIN test_scalar_join_b b
  ON length(concat(repeat('x', 262144), CAST(r.id AS STRING), b.s)) > 0

-- Expand evaluates the distinct aggregate's argument, which expands the scalar
query
SELECT count(DISTINCT length(concat(repeat('x', 262144), CAST(id AS STRING)))), count(DISTINCT id % 7)
FROM range(0, 8192, 1, 1)

-- a full sort on a key that expands the literal
query expect_fallback(A sort key expands a string or binary literal of 262144 bytes)
SELECT id FROM range(0, 8192, 1, 1) SORT BY concat(repeat('x', 262144), CAST(id AS STRING))

-- a full sort on a key with a smaller literal stays native, evaluating its keys batch by batch
query
SELECT id FROM range(0, 20000, 1, 3) SORT BY concat(repeat('x', 1024), CAST(id AS STRING))
