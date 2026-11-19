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

-- In LEGACY mode an integer add or multiply chain is rebalanced into a shallow tree, which must
-- still evaluate each operand only for the rows where every operand before it is not NULL, as
-- Spark does (https://github.com/apache/datafusion-comet/issues/7246). `element_at` fails for
-- index 0 in every mode. Every table is one Parquet file, so its rows arrive in one batch.

-- Config: spark.sql.ansi.enabled=false
-- Config: spark.sql.adaptive.enabled=false

-- `i` is 0 only on the rows where `n`, `m` or `k` is NULL
statement
CREATE TABLE skipped(id int, n int, m int, k int, arr array<int>, i int) USING parquet

statement
INSERT INTO skipped SELECT /*+ REPARTITION(1) */ * FROM VALUES
  (1, CAST(NULL AS INT), 1, 1, array(1, 2), 0),
  (2, 2, CAST(NULL AS INT), 1, array(1, 2), 0),
  (3, 2, 3, CAST(NULL AS INT), array(1, 2), 0),
  (4, 2, 3, 4, array(5, 6), 2)
  AS t(id, n, m, k, arr, i)

-- `n`, `m` and `k` are not NULL, so Spark evaluates every operand
statement
CREATE TABLE filled(id int, n int, m int, k int, arr array<int>, i int) USING parquet

statement
INSERT INTO filled SELECT /*+ REPARTITION(1) */ * FROM VALUES
  (1, 1, 1, 1, array(1, 2), 0) AS t(id, n, m, k, arr, i)

-- Two operands

query
SELECT id, n + element_at(arr, i) FROM skipped WHERE id IN (1, 4)

query
SELECT id, n * element_at(arr, i) FROM skipped WHERE id IN (1, 4)

-- Longer chains, where the operand that fails sits in a nested node

query
SELECT id, n + m + element_at(arr, i) FROM skipped WHERE id IN (1, 2, 4)

query
SELECT id, n * m * element_at(arr, i) FROM skipped WHERE id IN (1, 2, 4)

query
SELECT id, n + m + k + element_at(arr, i) FROM skipped

query
SELECT id, n * m * k * element_at(arr, i) FROM skipped

-- Five operands split unevenly, so the operand that fails sits two nodes deep in the right half
query
SELECT id, n + m + k + id + element_at(arr, i) FROM skipped

query
SELECT id, CAST(n AS BIGINT) + m + k + element_at(arr, i) FROM skipped

-- An operand that Spark evaluates still fails

query expect_error([INVALID_INDEX_OF_ZERO])
SELECT id, n + element_at(arr, i) FROM filled

query expect_error([INVALID_INDEX_OF_ZERO])
SELECT id, n + m + k + element_at(arr, i) FROM filled

-- The first operand comes first, so it fails even where a later one is NULL
query expect_error([INVALID_INDEX_OF_ZERO])
SELECT id, element_at(arr, i) + n FROM skipped
