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

-- A string scalar, from a scalar subquery or a literal, is expanded to the batch length by the
-- aggregate, the hash partitioner and string kernels. A 256 KiB string over the default 8192-row
-- batch needs 2 GiB of value bytes, past Arrow's 32-bit offsets. Spark evaluates row by row.
-- The ids are written in one task so they arrive in one batch. Constant folding is on so that
-- `repeat('x', 262144)` reaches Comet as the literal it is in a Spark session.
-- See https://github.com/apache/datafusion-comet/issues/7247.
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.comet.exec.range.enabled=true
-- ConstantFolding: enabled

statement
CREATE TABLE test_large_scalar_wide(s string) USING parquet

statement
INSERT INTO test_large_scalar_wide SELECT repeat('x', 262144)

statement
CREATE TABLE test_large_scalar_ids(k int) USING parquet

statement
INSERT INTO test_large_scalar_ids SELECT /*+ COALESCE(1) */ CAST(id % 2 AS INT) FROM range(8192)

-- count of the scalar in a grouped aggregate, which expands it to the batch length. A literal
-- rather than the subquery: the harness's missingInput check trips on a grouped count of a
-- scalar subquery whatever the string's size, which is a separate plan issue.
query
SELECT k, count(repeat('x', 262144)) FROM test_large_scalar_ids GROUP BY k ORDER BY k

-- the scalar as a hash partitioning key
query
SELECT count(*) FROM (SELECT k FROM test_large_scalar_ids DISTRIBUTE BY k, (SELECT max(s) FROM test_large_scalar_wide))

-- a string kernel whose output exceeds 2 GiB per 8192-row batch
query
SELECT sum(length(c)) FROM (SELECT concat((SELECT max(s) FROM test_large_scalar_wide), CAST(k AS STRING)) AS c FROM test_large_scalar_ids)

-- the same with a literal instead of a subquery
query
SELECT sum(length(c)) FROM (SELECT concat(repeat('x', 262144), CAST(k AS STRING)) AS c FROM test_large_scalar_ids)

-- the literal over the native range source, which sizes its own batches, in one slice
query
SELECT sum(length(concat(repeat('x', 262144), CAST(id AS STRING)))) FROM range(0, 8192, 1, 1)

-- a filter under the projection coalesces its output to its own batch size
query
SELECT sum(length(c)) FROM (SELECT concat((SELECT max(s) FROM test_large_scalar_wide), CAST(k AS STRING)) AS c FROM test_large_scalar_ids WHERE k >= 0)

-- a window under the aggregate emits its whole partition
query
SELECT sum(length(concat(repeat('x', 262144), CAST(tile AS STRING)))) FROM (SELECT ntile(2) OVER (ORDER BY id) AS tile FROM range(0, 8192, 1, 1))

-- a scalar built from the subquery result is wider than the result itself
query
SELECT sum(length(c)) FROM (SELECT concat(repeat((SELECT max(s) FROM test_large_scalar_wide), 2), CAST(k AS STRING)) AS c FROM test_large_scalar_ids)
