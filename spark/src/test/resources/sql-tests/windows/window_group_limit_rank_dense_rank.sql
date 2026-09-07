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

-- WindowGroupLimit tie-semantics for RANK vs DENSE_RANK. RANK leaves gaps on ties
-- (`rk <= 2` skips rank 2 if two rows tied at 1), DENSE_RANK does not. Both rank-like
-- functions route to Comet's streaming `PartitionedRankLimitExec` on the native side,
-- which preserves Spark's tie ordering (via `RankLimitIterator`). Strict `query` mode
-- verifies both correctness and full native operator coverage.
-- The `rk = 0` case at the bottom collapses to an empty LocalRelation via Spark's
-- optimizer under threshold=1000, so it uses `spark_answer_only` — the resulting
-- LocalTableScan is not natively supported without `spark.comet.exec.localTableScan.enabled`.

-- MinSparkVersion: 3.5
-- ConfigMatrix: spark.sql.optimizer.windowGroupLimitThreshold=-1,1000
-- Config: spark.comet.exec.localTableScan.enabled=true

statement
CREATE TABLE test_wgl_rank(grp string, score int) USING parquet

statement
INSERT INTO test_wgl_rank VALUES
  ('a', 100), ('a', 100), ('a', 90), ('a', 90), ('a', 80), ('a', 70),
  ('b', 50),  ('b', 40),  ('b', 40), ('b', NULL),
  ('c', 1),
  (NULL, 42), (NULL, 42),
  ('d', NULL), ('d', NULL)

-- RANK <= 2: two tied #1 rows fill the "first two slots" and rank 2 is skipped.
query
SELECT grp, score FROM (
  SELECT grp, score,
         RANK() OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST) AS rk
  FROM test_wgl_rank
) t WHERE rk <= 2 ORDER BY grp NULLS LAST, rk, score DESC NULLS LAST

-- DENSE_RANK <= 2: top-2 distinct order values per partition (ties still counted once).
query
SELECT grp, score FROM (
  SELECT grp, score,
         DENSE_RANK() OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST) AS dr
  FROM test_wgl_rank
) t WHERE dr <= 2 ORDER BY grp NULLS LAST, dr, score DESC NULLS LAST

-- RANK = 1: all rows tied at the top.
query
SELECT grp, score FROM (
  SELECT grp, score,
         RANK() OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST) AS rk
  FROM test_wgl_rank
) t WHERE rk = 1 ORDER BY grp NULLS LAST, score DESC NULLS LAST

-- DENSE_RANK < 3.
query
SELECT grp, score FROM (
  SELECT grp, score,
         DENSE_RANK() OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST) AS dr
  FROM test_wgl_rank
) t WHERE dr < 3 ORDER BY grp NULLS LAST, dr, score DESC NULLS LAST

-- RANK with no PARTITION BY. Unlike ROW_NUMBER, Spark still inserts WGL here.
query
SELECT grp, score FROM (
  SELECT grp, score,
         RANK() OVER (ORDER BY score DESC NULLS LAST) AS rk
  FROM test_wgl_rank
) t WHERE rk <= 2 ORDER BY rk, grp NULLS LAST

-- DENSE_RANK with no PARTITION BY.
query
SELECT grp, score FROM (
  SELECT grp, score,
         DENSE_RANK() OVER (ORDER BY score DESC NULLS LAST) AS dr
  FROM test_wgl_rank
) t WHERE dr <= 2 ORDER BY dr, grp NULLS LAST

-- Two rank-like windows in one SELECT, both filtered. Spark picks the minimum implied
-- limit (row_number wins as cheaper), and both filters still apply to the result.
query
SELECT grp, score FROM (
  SELECT grp, score,
         ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST) AS rn,
         RANK()       OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST) AS rk
  FROM test_wgl_rank
) t WHERE rn <= 3 AND rk <= 2 ORDER BY grp NULLS LAST, rn

-- Limit = 0 collapses to empty.
query
SELECT grp, score FROM (
  SELECT grp, score,
         RANK() OVER (PARTITION BY grp ORDER BY score DESC) AS rk
  FROM test_wgl_rank
) t WHERE rk = 0
