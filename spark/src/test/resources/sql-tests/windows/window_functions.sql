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

-- ============================================================
-- Setup: shared tables
-- ============================================================

statement
CREATE TABLE emp(dept string, id int, salary int, hire_yr int) USING parquet

statement
INSERT INTO emp VALUES
  ('eng',   1,  100,  2020),
  ('eng',   2,  100,  2021),
  ('eng',   2,  100,  2021),
  ('eng',   3,  150,  2022),
  ('eng',   4,  NULL, 2023),
  ('eng',   5,  200,  2024),
  ('sales', 6,  90,   2020),
  ('sales', 7,  90,   2021),
  ('sales', 8,  NULL, 2022),
  ('sales', 9,  110,  2023),
  ('sales', 10, 120,  2024),
  ('ops',   11, NULL, 2020),
  ('ops',   11, NULL, 2020),
  ('ops',   12, NULL, 2021),
  ('ops',   13, 50,   2022)

statement
CREATE TABLE emp_allnull(dept string, id int, salary int) USING parquet

statement
INSERT INTO emp_allnull VALUES
  ('a', 1, NULL),
  ('a', 2, NULL),
  ('b', 3, NULL),
  ('b', 4, NULL)

statement
CREATE TABLE daily(dt string, amt int) USING parquet

statement
INSERT INTO daily VALUES
  ('2024-01-01', 10),
  ('2024-01-02', 20),
  ('2024-01-03', 30),
  ('2024-01-04', 40)

statement
CREATE TABLE scores(player string, game int, score int) USING parquet

statement
INSERT INTO scores VALUES
  ('alice',  1, 10),
  ('alice',  2, 10),
  ('alice',  3, 20),
  ('alice',  4, 30),
  ('bob',    1, 10),
  ('bob',    2, 10),
  ('bob',    3, 10),
  ('bob',    4, 40),
  ('carol',  1, 50),
  ('carol',  2, 50),
  ('carol',  3, 50),
  ('carol',  4, 50)

statement
CREATE TABLE date_events(val_date date, cate string) USING parquet

statement
INSERT INTO date_events VALUES
  (DATE '2017-08-01', 'a'),
  (DATE '2017-08-01', 'a'),
  (DATE '2017-08-02', 'a'),
  (DATE '2020-12-31', 'a'),
  (DATE '2017-08-01', 'b'),
  (DATE '2017-08-03', 'b'),
  (DATE '2020-12-31', 'b'),
  (NULL, NULL),
  (DATE '2017-08-01', NULL)

statement
CREATE TABLE t_single(val int) USING parquet

statement
INSERT INTO t_single VALUES (1)

-- ############################################################
-- Section 1: Basic window combinations
-- ############################################################

-- ============================================================
-- 1.1: aggregate over unbounded window (no ORDER BY)
-- ============================================================

query
SELECT id, dept, salary,
  COUNT(*)    OVER () AS c,
  SUM(salary) OVER () AS s,
  MAX(salary) OVER () AS mx,
  MIN(salary) OVER () AS mn
FROM emp

-- ============================================================
-- 1.2: aggregate with ORDER BY (running aggregates)
-- ============================================================

query
SELECT id, dept, salary,
  COUNT(*)    OVER (ORDER BY id) AS c,
  SUM(salary) OVER (ORDER BY id) AS s,
  MAX(salary) OVER (ORDER BY id) AS mx,
  MIN(salary) OVER (ORDER BY id) AS mn
FROM emp

-- ============================================================
-- 1.3: aggregate with PARTITION BY + ORDER BY
-- ============================================================

query
SELECT dept, id, salary,
  COUNT(*)    OVER (PARTITION BY dept ORDER BY id) AS c,
  SUM(salary) OVER (PARTITION BY dept ORDER BY id) AS s,
  AVG(salary) OVER (PARTITION BY dept ORDER BY id) AS a
FROM emp

-- ============================================================
-- 1.4: aggregate with ORDER BY DESC
-- ============================================================

query
SELECT id, dept, salary,
  SUM(salary) OVER (PARTITION BY dept ORDER BY id DESC) AS s
FROM emp

-- ============================================================
-- 1.5: ranking functions (ROW_NUMBER, RANK, DENSE_RANK)
-- ============================================================

query
SELECT dept, id, salary,
  ROW_NUMBER() OVER (PARTITION BY dept ORDER BY id) AS rn,
  RANK()       OVER (PARTITION BY dept ORDER BY id) AS rk,
  DENSE_RANK() OVER (PARTITION BY dept ORDER BY id) AS drk
FROM emp

-- ============================================================
-- 1.6: PERCENT_RANK and CUME_DIST
-- ============================================================

query tolerance=1e-6
SELECT dept, id, salary,
  PERCENT_RANK() OVER (PARTITION BY dept ORDER BY id) AS pr,
  CUME_DIST()    OVER (PARTITION BY dept ORDER BY id) AS cd
FROM emp

-- ============================================================
-- 1.7: LAG / LEAD with default offset
-- ============================================================

query
SELECT dept, id, salary,
  LAG(salary)  OVER (PARTITION BY dept ORDER BY id) AS lg,
  LEAD(salary) OVER (PARTITION BY dept ORDER BY id) AS ld
FROM emp

-- ============================================================
-- 1.8: LAG / LEAD with explicit offset and default value
-- ============================================================

query
SELECT dept, id, salary,
  LAG(salary, 2, -1)  OVER (PARTITION BY dept ORDER BY id) AS lg2,
  LEAD(salary, 2, -1) OVER (PARTITION BY dept ORDER BY id) AS ld2
FROM emp

-- ============================================================
-- 1.9: ROWS BETWEEN frames
-- Note: Spark parses `N PRECEDING` in ROWS frames as UnaryMinus(Literal(N)),
-- which only becomes a plain Literal(-N) after ConstantFolding runs.
-- CometSqlFileTestSuite disables ConstantFolding, so the ROWS frames below
-- only use bounds that parse directly (UNBOUNDED / CURRENT ROW / N FOLLOWING).
-- N PRECEDING in ROWS frames is covered by CometWindowExecSuite via the
-- DataFrame API, which emits Literal(-N) without needing the optimizer.
-- ============================================================

query
SELECT dept, id, salary,
  SUM(salary) OVER (PARTITION BY dept ORDER BY id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s_run,
  SUM(salary) OVER (PARTITION BY dept ORDER BY id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS s_lookahead,
  SUM(salary) OVER (PARTITION BY dept ORDER BY id
                    ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS s_window3,
  SUM(salary) OVER (PARTITION BY dept ORDER BY id
                    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS s_tail
FROM emp

-- ============================================================
-- 1.10: RANGE BETWEEN with numeric ORDER BY
-- ============================================================

query
SELECT dept, id, salary,
  SUM(salary) OVER (PARTITION BY dept ORDER BY id
                    RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s_rng
FROM emp

-- ============================================================
-- 1.11: RANGE BETWEEN with DATE ORDER BY
-- Spark interprets the integer frame offset N on a DATE ORDER BY column as
-- N days, so MAX(val_date) OVER (... RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING)
-- returns the largest val_date within the next 2 days of each row.
-- Mirrors the "max(val_date) ... RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING"
-- case from Spark's window.sql. Comet falls back because its native window
-- planner ships RANGE frame offsets as Int64 while arrow-arith requires an
-- Interval RHS for Date32 arithmetic. Once the native planner coerces Int ->
-- Interval for DATE sort keys (as DataFusion's type-coercion analyzer does),
-- the guard in CometWindowExec can be removed and this test will start
-- failing because Comet stops falling back — that's the signal to re-enable
-- native execution.
-- ============================================================

query expect_fallback(RANGE frame with explicit offset on DATE ORDER BY is not supported)
SELECT val_date, cate,
  MAX(val_date) OVER (PARTITION BY cate ORDER BY val_date
                      RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS mx_d
FROM date_events
ORDER BY cate, val_date

-- ============================================================
-- 1.12: RANGE BETWEEN with DECIMAL ORDER BY DESC
-- Ported from Spark's typeCoercion/native/windowFrameCoercion.sql:
--   SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as decimal(10, 0)) DESC
--     RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t
-- Comet falls back because Spark decimal arithmetic widens precision on
-- +/-, so the native frame-boundary arithmetic produces e.g. Decimal(11,0)
-- while the current row stays Decimal(10,0), and DataFusion's comparator
-- fails with "Uncomparable values". Once the native planner preserves the
-- ORDER BY column's precision when computing RANGE boundaries, the guard
-- in CometWindowExec can be removed and this test will start failing
-- because Comet stops falling back — that's the signal to re-enable it.
-- ============================================================

query expect_fallback(RANGE frame with explicit offset on DECIMAL ORDER BY is not supported)
SELECT COUNT(*) OVER (PARTITION BY 1
                      ORDER BY CAST(1 AS DECIMAL(10, 0)) DESC
                      RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS c
FROM t_single

-- ============================================================
-- 1.13: RANGE frame with BIGINT ORDER BY values near Long.MaxValue
-- Ported from Spark's postgreSQL/window_part2.sql:
--   select x.id, last(x.id) over (order by x.id range between current row and 4 following)
--   from range(9223372036854775804, 9223372036854775807) x;
-- ANSI is off here (the file default) so this is not an overflow
-- error case. When Spark computes the frame upper bound
-- `current + 4` for a row near Long.MaxValue the addition
-- overflows; Spark's non-ANSI Add wraps to a negative value, so
-- the upper bound ends up below the current row and the range
-- frame is empty — LAST_VALUE returns NULL for every overflowing
-- row. The test verifies Comet propagates that overflow through
-- the native RANGE frame and produces the same NULL result rather
-- than a wrapped id or a saturated Long.MaxValue bound.
-- ============================================================

statement
CREATE TABLE bigint_max_range(id bigint) USING parquet

statement
INSERT INTO bigint_max_range VALUES
  (9223372036854775804),
  (9223372036854775805),
  (9223372036854775806)

-- https://github.com/apache/datafusion-comet/issues/4307
query ignore(https://github.com/apache/datafusion-comet/issues/4307)
SELECT id,
  LAST_VALUE(id) OVER (ORDER BY id
                       RANGE BETWEEN CURRENT ROW AND 4 FOLLOWING) AS lv
FROM bigint_max_range

-- ============================================================
-- 1.14: multiple PARTITION BY + multiple ORDER BY
-- ============================================================

query
SELECT dept, id, hire_yr, salary,
  SUM(salary) OVER (PARTITION BY dept, hire_yr ORDER BY id) AS s1,
  RANK()      OVER (PARTITION BY dept ORDER BY hire_yr, id) AS rk
FROM emp

-- ============================================================
-- 1.15: complex expression in aggregate input
-- ============================================================

query
SELECT dept, id, salary,
  SUM(salary + 10) OVER (PARTITION BY dept ORDER BY id) AS s_plus_10
FROM emp

-- ============================================================
-- 1.16: multiple window functions with mixed specs in one query
-- ============================================================

query
SELECT dept, id, salary,
  ROW_NUMBER() OVER (PARTITION BY dept ORDER BY id) AS rn,
  RANK()       OVER (PARTITION BY dept ORDER BY salary) AS rk_salary,
  SUM(salary)  OVER (PARTITION BY dept ORDER BY id) AS run_sum,
  MAX(salary)  OVER () AS global_max
FROM emp

-- ############################################################
-- Section 2: Mixed nulls
-- ############################################################

-- ============================================================
-- 2.1: COUNT(*) vs COUNT(col) with nulls
-- ============================================================

query
SELECT dept, id, salary,
  COUNT(*)      OVER (PARTITION BY dept ORDER BY id) AS c_star,
  COUNT(salary) OVER (PARTITION BY dept ORDER BY id) AS c_col
FROM emp

-- ============================================================
-- 2.2: SUM / AVG / MIN / MAX skip nulls
-- ============================================================

query
SELECT dept, id, salary,
  SUM(salary) OVER (PARTITION BY dept ORDER BY id) AS s,
  AVG(salary) OVER (PARTITION BY dept ORDER BY id) AS a,
  MIN(salary) OVER (PARTITION BY dept ORDER BY id) AS mn,
  MAX(salary) OVER (PARTITION BY dept ORDER BY id) AS mx
FROM emp

-- ============================================================
-- 2.3: LAG / LEAD RESPECT NULLS (default) vs IGNORE NULLS
-- ============================================================

query
SELECT dept, id, salary,
  LAG(salary)              OVER (PARTITION BY dept ORDER BY id) AS lag_respect,
  LAG(salary) IGNORE NULLS OVER (PARTITION BY dept ORDER BY id) AS lag_ignore,
  LEAD(salary)              OVER (PARTITION BY dept ORDER BY id) AS lead_respect,
  LEAD(salary) IGNORE NULLS OVER (PARTITION BY dept ORDER BY id) AS lead_ignore
FROM emp

-- ============================================================
-- 2.4: FIRST_VALUE / LAST_VALUE RESPECT vs IGNORE NULLS
-- ============================================================
query
SELECT dept, id, salary,
  FIRST_VALUE(salary)              OVER (PARTITION BY dept ORDER BY id, salary
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv_respect,
  FIRST_VALUE(salary) IGNORE NULLS OVER (PARTITION BY dept ORDER BY id, salary
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv_ignore,
  LAST_VALUE(salary)              OVER (PARTITION BY dept ORDER BY id, salary
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv_respect,
  LAST_VALUE(salary) IGNORE NULLS OVER (PARTITION BY dept ORDER BY id, salary
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv_ignore
FROM emp

-- ============================================================
-- 2.5: all-null partition
-- ============================================================
query
SELECT dept, id, salary,
  SUM(salary)                      OVER (PARTITION BY dept ORDER BY id, salary) AS s,
  AVG(salary)                      OVER (PARTITION BY dept ORDER BY id, salary) AS a,
  COUNT(salary)                    OVER (PARTITION BY dept ORDER BY id, salary) AS c_col,
  FIRST_VALUE(salary) IGNORE NULLS OVER (PARTITION BY dept ORDER BY id, salary
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv_ig
FROM emp_allnull

-- ============================================================
-- 2.6: NTH_VALUE with nulls RESPECTed vs IGNORED
-- ============================================================

query
SELECT dept, id, salary,
  NTH_VALUE(salary, 2)              OVER (PARTITION BY dept ORDER BY id
                                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nth_respect,
  NTH_VALUE(salary, 2) IGNORE NULLS OVER (PARTITION BY dept ORDER BY id
                                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nth_ignore
FROM emp

query
SELECT dept, id, salary,
    first_value(salary) IGNORE NULLS OVER (PARTITION BY dept ORDER BY id, salary
                                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv_ignore,
    last_value(salary) IGNORE NULLS OVER (PARTITION BY dept ORDER BY id, salary
                                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lv_ignore,
    nth_value(salary, 1) IGNORE NULLS OVER (PARTITION BY dept ORDER BY id, salary
                                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nth_ignore
FROM emp

-- ============================================================
-- 2.7: AVG with ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
-- over trailing NULLs (inverse transition).
-- Ported from Spark's postgreSQL/window_part4.sql:
--   SELECT i, AVG(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
--   FROM (VALUES (1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);
-- As each row advances the frame shrinks from the front, so the
-- aggregate is updated by its inverse-transition path. The test
-- exercises that the running count correctly excludes NULL values
-- and that AVG returns NULL once only NULL rows remain in the
-- frame (expected 1.5, 2.0, NULL, NULL).
-- ============================================================

statement
CREATE TABLE avg_nulls_trailing(i int, v int) USING parquet

statement
INSERT INTO avg_nulls_trailing VALUES
  (1, 1),
  (2, 2),
  (3, NULL),
  (4, NULL)

query
SELECT i,
  AVG(v) OVER (ORDER BY i
               ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS a
FROM avg_nulls_trailing

-- ############################################################
-- Section 3: Window functions with GROUP BY
-- ############################################################

-- ============================================================
-- 3.1: window over aggregated rows (window in outer SELECT)
-- ============================================================

query
SELECT dept, total_salary,
  RANK() OVER (ORDER BY total_salary DESC) AS rk
FROM (
  SELECT dept, SUM(salary) AS total_salary
  FROM emp
  GROUP BY dept
) t

-- ============================================================
-- 3.2: window and GROUP BY in the same query (aggregated input to window)
-- ============================================================

query
SELECT dept,
  SUM(salary) AS dept_total,
  SUM(SUM(salary)) OVER () AS grand_total,
  SUM(salary) / SUM(SUM(salary)) OVER () AS share
FROM emp
GROUP BY dept

-- ============================================================
-- 3.3: window with GROUP BY over multiple keys
-- ============================================================

query
SELECT dept, hire_yr,
  SUM(salary) AS yr_total,
  SUM(SUM(salary)) OVER (PARTITION BY dept ORDER BY hire_yr) AS dept_running
FROM emp
WHERE salary IS NOT NULL
GROUP BY dept, hire_yr

-- ============================================================
-- 3.4: GROUP BY producing a single row per group, RANK compares groups
-- ============================================================

query
SELECT dept, cnt, avg_sal,
  DENSE_RANK() OVER (ORDER BY avg_sal DESC) AS rk_by_avg
FROM (
  SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal
  FROM emp
  GROUP BY dept
) t

-- ############################################################
-- Section 4: LAST_VALUE default frame semantics
-- ############################################################
-- SQL default frame when ORDER BY is present is
--   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
-- so LAST_VALUE without an explicit frame returns the current row's value,
-- NOT the last row of the partition. Most users intuitively expect the
-- latter; the explicit ROWS BETWEEN ... UNBOUNDED FOLLOWING form gives it.

-- ============================================================
-- 4.1: LAST_VALUE default frame == current row value
-- ============================================================

query
SELECT dt, amt,
  LAST_VALUE(amt) OVER (ORDER BY dt) AS lv_default
FROM daily

-- ============================================================
-- 4.2: LAST_VALUE explicit ROWS BETWEEN ... UNBOUNDED FOLLOWING
-- ============================================================

query
SELECT dt, amt,
  LAST_VALUE(amt) OVER (ORDER BY dt
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv_full
FROM daily

-- ============================================================
-- 4.3: contrast default vs explicit FIRST_VALUE / LAST_VALUE
-- ============================================================

query
SELECT dt, amt,
  FIRST_VALUE(amt) OVER (ORDER BY dt) AS fv_default,
  FIRST_VALUE(amt) OVER (ORDER BY dt
                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fv_full,
  LAST_VALUE(amt)  OVER (ORDER BY dt) AS lv_default,
  LAST_VALUE(amt)  OVER (ORDER BY dt
                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv_full
FROM daily

-- ============================================================
-- 4.4: LAST_VALUE default with PARTITION BY
-- ============================================================

query
SELECT dept, id, salary,
  LAST_VALUE(salary) OVER (PARTITION BY dept ORDER BY id) AS lv_default,
  LAST_VALUE(salary) OVER (PARTITION BY dept ORDER BY id
                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv_full
FROM emp

-- ============================================================
-- 4.5: LAST_VALUE default RANGE semantics over peer groups
-- Peer rows (same ORDER BY key) see each other's value in the default
-- RANGE frame, so LAST_VALUE within a peer group returns the last peer's
-- value rather than just the current row. Returning `score` (the ORDER BY
-- column) keeps the output deterministic since all peers in a peer group
-- share the same score by definition.
-- ============================================================

query
SELECT player, game, score,
  LAST_VALUE(score)  OVER (PARTITION BY player ORDER BY score) AS lv_default,
  FIRST_VALUE(score) OVER (PARTITION BY player ORDER BY score) AS fv_default
FROM scores

-- ############################################################
-- Section 5: Peer groups with duplicates
-- ############################################################
-- Peer groups = rows that share the same ORDER BY key within a partition.
-- RANK assigns the same rank to peers and then skips (1,1,3). DENSE_RANK
-- assigns the same rank to peers without skipping (1,1,2). ROW_NUMBER
-- breaks ties arbitrarily but deterministically within a single plan.

-- ============================================================
-- 5.1: RANK vs DENSE_RANK vs ROW_NUMBER with ties
-- ROW_NUMBER needs a tie-breaker (game) to be deterministic — tied rows
-- can get row-numbers in either order otherwise. RANK and DENSE_RANK
-- give peers the same rank regardless of peer order, so they stay on
-- the tied `ORDER BY score` to demonstrate tie handling.
-- ============================================================

query
SELECT player, game, score,
  ROW_NUMBER() OVER (PARTITION BY player ORDER BY score, game) AS rn,
  RANK()       OVER (PARTITION BY player ORDER BY score) AS rk,
  DENSE_RANK() OVER (PARTITION BY player ORDER BY score) AS drk
FROM scores

-- ============================================================
-- 5.2: PERCENT_RANK and CUME_DIST across peer groups
-- CUME_DIST returns (#rows with order_by <= current) / (#rows in partition),
-- so every peer gets the same CUME_DIST value. PERCENT_RANK uses (rank-1)/(n-1),
-- so peers share a PERCENT_RANK as well.
-- ============================================================

query tolerance=1e-6
SELECT player, game, score,
  PERCENT_RANK() OVER (PARTITION BY player ORDER BY score) AS pr,
  CUME_DIST()    OVER (PARTITION BY player ORDER BY score) AS cd
FROM scores

-- ============================================================
-- 5.3: peer group where all rows share the same ORDER BY key
-- Every row is a peer of every other, so RANK / DENSE_RANK = 1 for all
-- and CUME_DIST = 1 for all.
-- ============================================================

query tolerance=1e-6
SELECT player, game, score,
  RANK()         OVER (PARTITION BY player ORDER BY score) AS rk,
  DENSE_RANK()   OVER (PARTITION BY player ORDER BY score) AS drk,
  CUME_DIST()    OVER (PARTITION BY player ORDER BY score) AS cd,
  PERCENT_RANK() OVER (PARTITION BY player ORDER BY score) AS pr
FROM scores
WHERE player = 'carol'

-- ============================================================
-- 5.4: SUM / COUNT default RANGE frame over peer groups
-- Default frame (RANGE UNBOUNDED PRECEDING TO CURRENT ROW) includes all
-- peers, so running SUM jumps by the peer-group total at each peer
-- rather than row-by-row.
-- ============================================================

query
SELECT player, game, score,
  SUM(score)   OVER (PARTITION BY player ORDER BY score) AS run_sum,
  COUNT(score) OVER (PARTITION BY player ORDER BY score) AS run_count
FROM scores

-- ============================================================
-- 5.5: ROWS frame vs default RANGE frame over peer groups
-- ROWS frame is position-based: running SUM moves row-by-row ignoring peers.
-- The ROWS ordering adds `game` as a tie-breaker to make per-row output
-- deterministic (ROWS with tied ORDER BY can assign partial sums to peers
-- in either order). RANGE intentionally keeps ORDER BY score only, since
-- peer rows share the frame and therefore produce identical partial sums
-- regardless of intra-peer order.
-- ============================================================

query
SELECT player, game, score,
  SUM(score) OVER (PARTITION BY player ORDER BY score, game
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rows_sum,
  SUM(score) OVER (PARTITION BY player ORDER BY score
                   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_sum
FROM scores

-- ############################################################
-- Section 6: Other window / aggregate functions
-- ############################################################
-- Functions beyond Comet's native window-aggregate support
-- (Count/Min/Max/Sum/Average/First/Last). Everything below either runs natively
-- with a known correctness issue (NTILE) or falls back because Comet's native
-- WindowExec serde does not recognise the aggregate.

-- ============================================================
-- 6.1: NTILE (known correctness bug tracked in #4255)
-- Falls back to Spark via the NTile guard in CometWindowExec.
-- When the native bug is fixed and the guard removed, this test will fail
-- because Comet stops falling back — that's the signal to re-enable it.
-- ============================================================

query
SELECT dept, id, salary,
  NTILE(4) OVER (PARTITION BY dept ORDER BY id) AS bucket
FROM emp

-- ============================================================
-- 6.2: statistical aggregates over a window (STDDEV / VAR / SKEW / KURT)
-- ============================================================

query expect_fallback(is not supported for window function)
SELECT dept, id, salary,
  STDDEV_POP(salary)  OVER (PARTITION BY dept ORDER BY id, salary) AS sd_pop,
  STDDEV_SAMP(salary) OVER (PARTITION BY dept ORDER BY id, salary) AS sd_samp,
  VAR_POP(salary)     OVER (PARTITION BY dept ORDER BY id, salary) AS v_pop,
  VAR_SAMP(salary)    OVER (PARTITION BY dept ORDER BY id, salary) AS v_samp,
  SKEWNESS(salary)    OVER (PARTITION BY dept ORDER BY id, salary) AS skew,
  KURTOSIS(salary)    OVER (PARTITION BY dept ORDER BY id, salary) AS kurt
FROM emp

-- ============================================================
-- 6.3: collection aggregates (COLLECT_LIST)
-- id is unique per partition, so the ORDER BY makes COLLECT_LIST insertion
-- order deterministic.
-- ============================================================

query expect_fallback(is not supported for window function)
SELECT dept, id, salary,
  COLLECT_LIST(salary) OVER (PARTITION BY dept ORDER BY id, salary
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS so_far
FROM emp

-- ============================================================
-- 6.4: bitwise aggregates (BIT_AND / BIT_OR / BIT_XOR)
-- ============================================================

query expect_fallback(is not supported for window function)
SELECT dept, id, salary,
  BIT_AND(salary) OVER (PARTITION BY dept) AS b_and,
  BIT_OR(salary)  OVER (PARTITION BY dept) AS b_or,
  BIT_XOR(salary) OVER (PARTITION BY dept) AS b_xor
FROM emp

-- ============================================================
-- 6.5: correlation / covariance (CORR / COVAR_POP / COVAR_SAMP)
-- ============================================================

query expect_fallback(is not supported for window function)
SELECT dept, id, salary, hire_yr,
  CORR(salary, hire_yr)       OVER (PARTITION BY dept) AS corr,
  COVAR_POP(salary, hire_yr)  OVER (PARTITION BY dept) AS covp,
  COVAR_SAMP(salary, hire_yr) OVER (PARTITION BY dept) AS covs
FROM emp

-- ============================================================
-- 6.6: percentile aggregates (PERCENTILE / MEDIAN / APPROX_PERCENTILE)
-- ============================================================

query expect_fallback(is not supported for window function)
SELECT dept, id, salary,
  PERCENTILE(salary, 0.5)        OVER (PARTITION BY dept) AS p50,
  MEDIAN(salary)                 OVER (PARTITION BY dept) AS med,
  APPROX_PERCENTILE(salary, 0.5) OVER (PARTITION BY dept) AS app_p50
FROM emp

-- ============================================================
-- 6.7: value-selection aggregates (ANY_VALUE / MAX_BY / MIN_BY / MODE)
-- ============================================================

query expect_fallback(is not supported for window function)
SELECT dept, id, salary,
  ANY_VALUE(salary)    OVER (PARTITION BY dept ORDER BY id, salary) AS anyv,
  MAX_BY(id, salary)   OVER (PARTITION BY dept) AS max_id_by_salary,
  MIN_BY(id, salary)   OVER (PARTITION BY dept) AS min_id_by_salary,
  MODE(salary)         OVER (PARTITION BY dept) AS mode_salary
FROM emp

-- ############################################################
-- Section 7: LAG / LEAD respect null values (mirrors Spark
-- SQLWindowFunctionSuite "lead/lag should respect null values")
-- ############################################################

statement
CREATE TABLE lag_lead_nulls(a int, b int, c int) USING parquet

statement
INSERT INTO lag_lead_nulls VALUES
  (CAST(NULL AS INT), 1, 3),
  (CAST(NULL AS INT), 2, 4)

-- ============================================================
-- 7.1: LAG / LEAD with literal default — input is null but the
-- default still fires when the offset row does not exist
-- (RESPECT NULLS is the default behaviour).
-- ============================================================

query
SELECT
  b,
  LAG(a, 1, 321)  OVER (ORDER BY b) AS lg,
  LEAD(a, 1, 321) OVER (ORDER BY b) AS ld
FROM lag_lead_nulls

-- ============================================================
-- 7.2: LAG / LEAD with a non-literal (column-reference) default
-- falls back to Spark — Comet's native lag/lead only accepts a
-- literal default value.
-- ============================================================

query expect_fallback(default value must be a literal)
SELECT
  b,
  LAG(a, 1, c)  OVER (ORDER BY b) AS lg,
  LEAD(a, 1, c) OVER (ORDER BY b) AS ld
FROM lag_lead_nulls
