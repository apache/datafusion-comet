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

-- WindowGroupLimit RANK() native pushdown tests. Comet routes RANK with a non-empty
-- PARTITION BY through the streaming `PartitionedRankLimitExec`, which relies on the
-- Spark-injected `[partition_keys, order_keys]` sort and retains all rows tied at the
-- K-th ORDER BY value -- matching Spark's `RankLimitIterator`.
--
-- Every case here uses `query` mode: results are asserted against Spark AND every
-- pushdown-eligible plan must be Comet-native. ConfigMatrix threshold=-1 exercises
-- the plain Window+Filter shape (no WGL); threshold=1000 exercises the WGL pushdown
-- through the new native operator. Both must pass.
--
-- Tie-collapsing note: since these tests project only the partition + order columns
-- (no per-row disambiguator), any row that shares (part, rk, ord) with another is
-- indistinguishable in the output, so the ORDER BY does not need a further tiebreaker
-- for `checkAnswer` to compare row-wise.

-- MinSparkVersion: 3.5
-- ConfigMatrix: spark.sql.optimizer.windowGroupLimitThreshold=-1,1000
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_rank(part string, ord int, extra int) USING parquet

statement
INSERT INTO test_rank VALUES
  ('a', 100, 1),
  ('a', 100, 2),   -- tied with row 1 at rank 1
  ('a', 90,  3),   -- rank 3 under RANK (gap after tie)
  ('a', 80,  4),
  ('a', 80,  5),   -- tied at rank 4 under RANK
  ('a', 70,  6),
  ('b', 50,  7),
  ('b', 40,  8),
  ('b', 40,  9),   -- tied at rank 2 under RANK
  ('b', NULL, 10),
  ('c', 1,   11),
  ('d', NULL, 12),
  ('d', NULL, 13)  -- all NULLs in one partition


-- ================================================================================
-- Basic tie semantics: RANK <= 2 must retain rows tied at rank 1, then advance to
-- the next distinct ORDER BY value. Under RANK's gap semantics, `rk <= 2` really
-- means "rank 1 rows only" when two rows tied at rank 1 push rank 2 out.
-- ================================================================================
query
SELECT part, ord FROM (
  SELECT part, ord,
         RANK() OVER (PARTITION BY part ORDER BY ord DESC NULLS LAST) AS rk
  FROM test_rank
) t WHERE rk <= 2 ORDER BY part, rk, ord DESC NULLS LAST

-- Same shape with `< k` -- Spark rewrites to `<= k-1`.
query
SELECT part, ord FROM (
  SELECT part, ord,
         RANK() OVER (PARTITION BY part ORDER BY ord DESC NULLS LAST) AS rk
  FROM test_rank
) t WHERE rk < 3 ORDER BY part, rk, ord DESC NULLS LAST

-- Literal on the left of the comparison (`k >= rk`).
query
SELECT part, ord FROM (
  SELECT part, ord,
         RANK() OVER (PARTITION BY part ORDER BY ord DESC NULLS LAST) AS rk
  FROM test_rank
) t WHERE 3 >= rk ORDER BY part, rk, ord DESC NULLS LAST

-- Equality: rk = 1 returns every row tied at the top of its partition, so partitions
-- with duplicate top values return multiple rows (partition 'a' has two 100s).
query
SELECT part, ord FROM (
  SELECT part, ord,
         RANK() OVER (PARTITION BY part ORDER BY ord DESC NULLS LAST) AS rk
  FROM test_rank
) t WHERE rk = 1 ORDER BY part, ord DESC NULLS LAST


-- ================================================================================
-- K variations
-- ================================================================================

-- Larger K covering every partition.
query
SELECT part, ord FROM (
  SELECT part, ord,
         RANK() OVER (PARTITION BY part ORDER BY ord DESC NULLS LAST) AS rk
  FROM test_rank
) t WHERE rk <= 100 ORDER BY part, rk, ord DESC NULLS LAST

-- Outer LIMIT after the WGL pushdown. Comet keeps the pushdown and stacks the
-- outer LIMIT above.
query
SELECT part, ord, rk FROM (
  SELECT part, ord,
         RANK() OVER (PARTITION BY part ORDER BY ord DESC NULLS LAST) AS rk
  FROM test_rank
) t WHERE rk <= 2 ORDER BY part, rk, ord DESC NULLS LAST LIMIT 5

-- Extra AND-predicate on a non-rank column. The predicate does not affect WGL
-- pushdown (Spark keeps rk<=k separate) but the answer must still be correct.
-- Project `extra` here so it survives the outer select and disambiguates output rows.
query
SELECT part, ord, extra FROM (
  SELECT part, ord, extra,
         RANK() OVER (PARTITION BY part ORDER BY ord DESC NULLS LAST) AS rk
  FROM test_rank
) t WHERE rk <= 3 AND extra >= 3 ORDER BY part, rk, ord DESC NULLS LAST, extra


-- ================================================================================
-- ORDER BY variations (ASC/DESC, NULLS FIRST/LAST, multi-column)
-- ================================================================================

-- ASC ordering with NULLS FIRST.
query
SELECT part, ord FROM (
  SELECT part, ord,
         RANK() OVER (PARTITION BY part ORDER BY ord ASC NULLS FIRST) AS rk
  FROM test_rank
) t WHERE rk <= 2 ORDER BY part, rk, ord ASC NULLS FIRST

-- ASC ordering with NULLS LAST.
query
SELECT part, ord FROM (
  SELECT part, ord,
         RANK() OVER (PARTITION BY part ORDER BY ord ASC NULLS LAST) AS rk
  FROM test_rank
) t WHERE rk <= 2 ORDER BY part, rk, ord ASC NULLS LAST

-- Two-column ORDER BY. The secondary key eliminates every intra-rank tie so
-- exactly the top-K distinct rows are returned per partition.
query
SELECT part, ord, extra FROM (
  SELECT part, ord, extra,
         RANK() OVER (PARTITION BY part ORDER BY ord DESC NULLS LAST, extra ASC NULLS FIRST) AS rk
  FROM test_rank
) t WHERE rk <= 2 ORDER BY part, rk, ord DESC NULLS LAST, extra


-- ================================================================================
-- Multi-column PARTITION BY
-- ================================================================================

statement
CREATE TABLE test_rank_multipart(a string, b int, score int) USING parquet

statement
INSERT INTO test_rank_multipart VALUES
  ('x', 1, 10), ('x', 1, 10), ('x', 1, 9),   -- (x,1): tie at top, then rank 3
  ('x', 2, 5),  ('x', 2, 5),
  ('y', 1, 100), ('y', 1, 100), ('y', 1, 100), -- (y,1): all tied
  (NULL, 1, 7),  (NULL, 1, 7)                  -- NULL in partition column

query
SELECT a, b, score FROM (
  SELECT a, b, score,
         RANK() OVER (PARTITION BY a, b ORDER BY score DESC NULLS LAST) AS rk
  FROM test_rank_multipart
) t WHERE rk <= 2 ORDER BY a NULLS LAST, b, rk, score DESC NULLS LAST


-- ================================================================================
-- FP edge cases in the ORDER BY column (NaN / +Inf / -Inf / 0 / NULL). NaN sorts
-- greater than any finite value in Spark, and Arrow's row-format total-ordering
-- encoding matches -- so NaN gets rank 1 under DESC. Note: -0.0 vs 0.0 is a known
-- Spark-vs-Arrow-row divergence (Spark treats them equal in ORDER BY; Arrow's
-- bitwise total_cmp treats them distinct), so this test avoids exercising that
-- boundary directly by keeping the cutoff (rk <= 3) above the 0-values.
-- ================================================================================

statement
CREATE TABLE test_rank_fp(part string, v double) USING parquet

statement
INSERT INTO test_rank_fp VALUES
  ('p', double('NaN')),
  ('p', double('Infinity')),
  ('p', double('Infinity')),           -- tied with prior +Inf under DESC
  ('p', 1.0),
  ('p', 0.0),
  ('p', double('-Infinity')),
  ('p', NULL)

-- DESC NULLS LAST -> NaN, +Inf, +Inf, 1.0, 0.0, -Inf, NULL.
-- RANK <= 3 keeps the NaN row and both +Inf rows (rank 2 tied, rank 3 empty).
query
SELECT part, v FROM (
  SELECT part, v,
         RANK() OVER (PARTITION BY part ORDER BY v DESC NULLS LAST) AS rk
  FROM test_rank_fp
) t WHERE rk <= 3 ORDER BY rk, v DESC NULLS LAST


-- ================================================================================
-- Decimal / bigint / integer boundary values in ORDER BY column
-- ================================================================================

statement
CREATE TABLE test_rank_num(part string, i32 int, i64 bigint, dec38 decimal(38, 4))
USING parquet

statement
INSERT INTO test_rank_num VALUES
  ('p', -2147483648, -9223372036854775808, cast('-9999999999999999999999999999999999.9999' AS decimal(38,4))),
  ('p', -2147483648, -9223372036854775808, cast('-9999999999999999999999999999999999.9999' AS decimal(38,4))),
  ('p',  2147483647,  9223372036854775807, cast(' 9999999999999999999999999999999999.9999' AS decimal(38,4))),
  ('p',  0,           0,                   cast('0.0000' AS decimal(38,4))),
  ('p',  NULL,        NULL,                NULL)

-- ORDER BY the max-boundary decimal column, then rank <= 2. Verifies the row-encoder
-- handles wide decimals across partition boundaries without truncation.
query
SELECT part, dec38 FROM (
  SELECT part, dec38,
         RANK() OVER (PARTITION BY part ORDER BY dec38 DESC NULLS LAST) AS rk
  FROM test_rank_num
) t WHERE rk <= 2 ORDER BY rk, dec38 DESC NULLS LAST

-- ORDER BY the int32/int64 extremes. RANK <= 2 with duplicate min values.
query
SELECT part, i32, i64 FROM (
  SELECT part, i32, i64,
         RANK() OVER (PARTITION BY part ORDER BY i64 ASC NULLS LAST) AS rk
  FROM test_rank_num
) t WHERE rk <= 2 ORDER BY rk, i64 ASC NULLS LAST, i32


-- ================================================================================
-- TPC-DS q67-shaped: PARTITION BY category ORDER BY sum DESC, rank <= 3
-- Distilled from tpc-ds/q67.sql (`rank() over (partition by i_category order by
-- sumsales desc) rk ... where rk <= 100`).
-- ================================================================================

statement
CREATE TABLE q67_sales(category string, item string, revenue double) USING parquet

statement
INSERT INTO q67_sales VALUES
  ('food', 'apple',   500.0),
  ('food', 'banana',  500.0),   -- tie with apple at rank 1
  ('food', 'cherry',  300.0),
  ('food', 'donut',   200.0),
  ('food', 'eclair',  100.0),
  ('food', 'fig',      50.0),
  ('elec', 'phone',  1000.0),
  ('elec', 'laptop',  900.0),
  ('elec', 'tablet',  900.0),   -- tie at rank 2
  ('elec', 'monitor', 500.0),
  ('elec', 'cable',    10.0),
  ('elec', 'mouse',    10.0),
  ('elec', 'kbd',      10.0),
  ('home', 'sofa',    750.0),
  ('home', 'lamp',    NULL),    -- NULL revenue
  (NULL,   'unknown', 42.0)     -- NULL category

query
SELECT category, item, revenue FROM (
  SELECT category, item, revenue,
         RANK() OVER (PARTITION BY category ORDER BY revenue DESC NULLS LAST) AS rk
  FROM q67_sales
) t WHERE rk <= 3 ORDER BY category NULLS LAST, rk, revenue DESC NULLS LAST, item


-- ================================================================================
-- TPC-DS q70-shaped: PARTITION BY state, rank <= 5, filtering to top-K states
-- Distilled from tpc-ds/q70.sql (`rank() over (partition by s_state order by
-- sum(ss_net_profit) desc) as ranking ... where ranking <= 5`).
-- ================================================================================

statement
CREATE TABLE q70_state_sales(state string, county string, net_profit int) USING parquet

statement
INSERT INTO q70_state_sales VALUES
  ('CA', 'LA',   100), ('CA', 'SF',    90), ('CA', 'SD',    80),
  ('CA', 'SJ',    70), ('CA', 'OA',    60), ('CA', 'FR',    50),
  ('TX', 'HTX',  200), ('TX', 'DAL',  150), ('TX', 'AUS',  150),  -- tie at rank 2
  ('TX', 'SAT',   50),
  ('NY', 'NYC',  300), ('NY', 'BUF',  100),
  ('WA', 'SEA',   80), ('WA', 'TAC',   80), ('WA', 'SPO',   80),  -- all tied
  ('OR', NULL,    10),                                             -- NULL county
  (NULL, 'X',     42)

-- Match q70's structure: rank each state's counties by profit, keep top-5.
query
SELECT state, county, net_profit FROM (
  SELECT state, county, net_profit,
         RANK() OVER (PARTITION BY state ORDER BY net_profit DESC NULLS LAST) AS ranking
  FROM q70_state_sales
) t WHERE ranking <= 5
ORDER BY state NULLS LAST, ranking, net_profit DESC NULLS LAST, county NULLS LAST

-- Same query with the outer LIMIT clause q70 uses.
query
SELECT state, county, net_profit FROM (
  SELECT state, county, net_profit,
         RANK() OVER (PARTITION BY state ORDER BY net_profit DESC NULLS LAST) AS ranking
  FROM q70_state_sales
) t WHERE ranking <= 3
ORDER BY state NULLS LAST, ranking, net_profit DESC NULLS LAST, county NULLS LAST LIMIT 10


-- ================================================================================
-- Grouped aggregate then RANK -- exercises the q67 pattern where the input to WGL
-- comes from an aggregation whose output feeds the ranked window.
-- ================================================================================

query
SELECT category, item, total FROM (
  SELECT category, item, total,
         RANK() OVER (PARTITION BY category ORDER BY total DESC NULLS LAST) AS rk
  FROM (
    SELECT category, item, sum(revenue) AS total
    FROM q67_sales
    GROUP BY category, item
  ) agg
) t WHERE rk <= 2 ORDER BY category NULLS LAST, rk, total DESC NULLS LAST, item


-- ================================================================================
-- Global RANK top-K (empty PARTITION BY) -- routes through Comet's streaming
-- `PartitionedRankLimitExec` with `partition_prefix_len == 0`. State never resets
-- across the DF partition; Spark's WGL Partial phase runs per input partition and
-- the Final phase runs after a SinglePartition shuffle. Distilled from TPC-DS q44
-- (`rank() over (order by rank_col asc) rnk ... where rnk < 11`).
-- ================================================================================

-- q44 shape: global RANK ASC, filter `rnk < 11`.
query
SELECT part, ord FROM (
  SELECT part, ord,
         RANK() OVER (ORDER BY ord ASC NULLS LAST) AS rnk
  FROM test_rank
) t WHERE rnk < 4 ORDER BY rnk, ord ASC NULLS LAST

-- Global RANK DESC with ties at the top: `rk <= 1` returns EVERY row tied at
-- the max, `rk <= 2` also returns them (rank 2 is skipped because two ties push it
-- out).
query
SELECT part, ord FROM (
  SELECT part, ord,
         RANK() OVER (ORDER BY ord DESC NULLS LAST) AS rk
  FROM test_rank
) t WHERE rk <= 1 ORDER BY rk, ord DESC NULLS LAST, part NULLS LAST

query
SELECT part, ord FROM (
  SELECT part, ord,
         RANK() OVER (ORDER BY ord DESC NULLS LAST) AS rk
  FROM test_rank
) t WHERE rk <= 2 ORDER BY rk, ord DESC NULLS LAST, part NULLS LAST

-- Global RANK with `<` filter form.
query
SELECT part, ord FROM (
  SELECT part, ord,
         RANK() OVER (ORDER BY ord DESC NULLS LAST) AS rk
  FROM test_rank
) t WHERE rk < 3 ORDER BY rk, ord DESC NULLS LAST, part NULLS LAST

-- Global RANK with outer LIMIT (q44 uses `LIMIT 100` on the outer query).
query
SELECT part, ord, rk FROM (
  SELECT part, ord,
         RANK() OVER (ORDER BY ord DESC NULLS LAST) AS rk
  FROM test_rank
) t WHERE rk <= 3 ORDER BY rk, ord DESC NULLS LAST, part NULLS LAST LIMIT 5

-- Global ROW_NUMBER without PARTITION BY should route to LocalLimitExec (Spark
-- rewrites this pattern to plain Limit under low thresholds, but the WGL path is
-- exercised at threshold=1000).
query
SELECT part, ord FROM (
  SELECT part, ord,
         ROW_NUMBER() OVER (ORDER BY ord DESC NULLS LAST, part) AS rn
  FROM test_rank
) t WHERE rn <= 3 ORDER BY rn
