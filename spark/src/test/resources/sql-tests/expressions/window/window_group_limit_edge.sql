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

-- WindowGroupLimit boundary and negative cases: empty table, single-row table, all-null
-- order column, all-tied rows, and filter shapes Spark's InferWindowGroupLimit rule does
-- NOT push (rn > k, disjunction with a non-rank predicate, SizeBasedWindowFunction sibling
-- like percent_rank per SPARK-46941). The negative cases must still return correct rows.

-- MinSparkVersion: 3.5
-- ConfigMatrix: spark.sql.optimizer.windowGroupLimitThreshold=-1,1000

statement
CREATE TABLE test_wgl_empty(a int, b int) USING parquet

statement
CREATE TABLE test_wgl_one(a int, b int) USING parquet

statement
INSERT INTO test_wgl_one VALUES (1, 100)

statement
CREATE TABLE test_wgl_all_null(a int, b int) USING parquet

statement
INSERT INTO test_wgl_all_null VALUES (1, NULL), (1, NULL), (1, NULL)

statement
CREATE TABLE test_wgl_all_tie(a int, b int) USING parquet

statement
INSERT INTO test_wgl_all_tie VALUES (1, 5), (1, 5), (1, 5), (1, 5)

-- Empty table.
query
SELECT a, b FROM (
  SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rn FROM test_wgl_empty
) t WHERE rn <= 3

-- Single-row table.
query
SELECT a, b, rn FROM (
  SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rn FROM test_wgl_one
) t WHERE rn <= 3

-- All-null order column with ROW_NUMBER. (Query kept malformed by the maintainer; skip.)
query ignore(https://github.com/apache/datafusion-comet/issues/wgl-broken-sql-placeholder)
SELECT a, b, rn FROM (
  SELECT a, b,
  FROM test_wgl_all_null
) t WHERE rn <= 2 ORDER BY rn

-- All-null order column with RANK: every row shares rank 1. Under threshold=1000 Comet
-- rejects RANK in the WGL serde and falls back; under threshold=-1 it runs natively.
query spark_answer_only
SELECT a, b FROM (
  SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY b DESC NULLS LAST) AS rk
  FROM test_wgl_all_null
) t WHERE rk = 1

-- All-null order column with DENSE_RANK: every row shares rank 1. Parity-only.
query spark_answer_only
SELECT a, b FROM (
  SELECT a, b, DENSE_RANK() OVER (PARTITION BY a ORDER BY b DESC NULLS LAST) AS dr
  FROM test_wgl_all_null
) t WHERE dr = 1

-- Every row tied — row_number=1 returns 1 row.
query
SELECT a, b FROM (
  SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) AS rn
  FROM test_wgl_all_tie
) t WHERE rn = 1

-- Every row tied — rank=1 returns all rows. Parity-only (RANK fallback under 1000).
query spark_answer_only
SELECT count(*) AS cnt FROM (
  SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY b) AS rk
  FROM test_wgl_all_tie
) t WHERE rk = 1

-- Every row tied — dense_rank=1 returns all rows. Parity-only.
query spark_answer_only
SELECT count(*) AS cnt FROM (
  SELECT a, b, DENSE_RANK() OVER (PARTITION BY a ORDER BY b) AS dr
  FROM test_wgl_all_tie
) t WHERE dr = 1

-- Unsupported filter form `rn > k` — InferWindowGroupLimit does NOT push, so no fallback
-- attributable to WindowGroupLimit. Result must still be correct.
query
SELECT a, b FROM (
  SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rn
  FROM test_wgl_all_tie
) t WHERE rn > 2 ORDER BY rn

-- Unsupported filter form: disjunction with a non-rank predicate — no WGL pushdown.
query
SELECT a, b FROM (
  SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rn
  FROM test_wgl_one
) t WHERE rn = 1 OR b > 50

-- SizeBasedWindowFunction sibling (percent_rank). SPARK-46941 added a guard in later Spark
-- versions to block WGL pushdown here, but Spark 3.5 still inserts WGL for the RANK arm;
-- Comet's serde rejects RANK and falls back. Assert result parity only.
query spark_answer_only
SELECT a, b FROM (
  SELECT a, b,
         RANK() OVER (PARTITION BY a ORDER BY b DESC) AS rk,
         PERCENT_RANK() OVER (PARTITION BY a ORDER BY b DESC) AS pr
  FROM test_wgl_one
) t WHERE rk <= 2
