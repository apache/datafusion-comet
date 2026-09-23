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

-- Tests for ExistenceJoin: produced when EXISTS / NOT EXISTS is combined
-- with another predicate via OR, preventing rewrite to LeftSemi / LeftAnti.
--
-- Strategy hints are placed INSIDE the EXISTS subquery (on the subquery's own
-- relation alias), because an outer hint referencing the subquery table cannot
-- resolve it and silently falls back to a broadcast hash join. Subquery-local
-- hints (verified on Spark 4.1.3 with AQE off) select ShuffledHashJoin /
-- SortMergeJoin.
--
-- Native existence support is currently hash-only: BROADCAST and SHUFFLE_HASH
-- cases exercise CometBroadcastHashJoinExec / CometHashJoinExec, while MERGE
-- cases fall back to Spark's SortMergeJoin (existence SMJ is not yet native) and
-- verify result parity under the Comet-enabled config.

-- Native ExistenceJoin support is experimental and disabled by default.
-- Config: spark.comet.exec.existenceJoin.enabled=true

-- ============================================================
-- Setup: NULLs (both sides), duplicates, empty build, all-NULL build
-- ============================================================

statement
CREATE TABLE ex_left(id int, k int, region string) USING parquet

statement
INSERT INTO ex_left VALUES
  (1, 1, 'US'),
  (2, 2, 'EU'),
  (3, NULL, 'US'),
  (4, 4, 'EU'),
  (5, 5, 'EU'),
  (6, NULL, 'EU')

statement
CREATE TABLE ex_right(id int, k int) USING parquet

statement
INSERT INTO ex_right VALUES (10, 1), (11, 2), (12, 2), (13, NULL)

statement
CREATE TABLE ex_right_no_nulls(id int, k int) USING parquet

statement
INSERT INTO ex_right_no_nulls VALUES (10, 1), (11, 5)

statement
CREATE TABLE ex_right_empty(id int, k int) USING parquet

statement
CREATE TABLE ex_right_dups(id int, k int) USING parquet

statement
INSERT INTO ex_right_dups VALUES (10, 1), (11, 1), (12, 1), (13, 2)

statement
CREATE TABLE ex_right_all_null(id int, k int) USING parquet

statement
INSERT INTO ex_right_all_null VALUES (10, NULL), (11, NULL)

statement
CREATE TABLE ex_left_empty(id int, k int, region string) USING parquet

-- ============================================================
-- EXISTS with OR across all three strategies (hint in subquery)
-- ============================================================

query
SELECT * FROM ex_left l
WHERE l.region = 'US'
   OR EXISTS (SELECT /*+ BROADCAST(r) */ 1 FROM ex_right r WHERE r.k = l.k)
ORDER BY l.id

query
SELECT * FROM ex_left l
WHERE l.region = 'US'
   OR EXISTS (SELECT /*+ SHUFFLE_HASH(r) */ 1 FROM ex_right r WHERE r.k = l.k)
ORDER BY l.id

query
SELECT * FROM ex_left l
WHERE l.region = 'US'
   OR EXISTS (SELECT /*+ MERGE(r) */ 1 FROM ex_right r WHERE r.k = l.k)
ORDER BY l.id

-- ============================================================
-- Empty build: every left row is unmatched, only OR-arm rows survive
-- ============================================================

query
SELECT * FROM ex_left l
WHERE l.region = 'US'
   OR EXISTS (SELECT /*+ BROADCAST(r) */ 1 FROM ex_right_empty r WHERE r.k = l.k)
ORDER BY l.id

query
SELECT * FROM ex_left l
WHERE l.region = 'US'
   OR EXISTS (SELECT /*+ MERGE(r) */ 1 FROM ex_right_empty r WHERE r.k = l.k)
ORDER BY l.id

-- ============================================================
-- Empty left: no rows in, no rows out (each strategy)
-- ============================================================

query
SELECT * FROM ex_left_empty l
WHERE l.region = 'US'
   OR EXISTS (SELECT /*+ SHUFFLE_HASH(r) */ 1 FROM ex_right r WHERE r.k = l.k)
ORDER BY l.id

-- ============================================================
-- All-NULL build keys: EXISTS is false for every left row
-- (NULL = anything is NULL), so only the OR arm can qualify.
-- ============================================================

query
SELECT * FROM ex_left l
WHERE l.region = 'US'
   OR EXISTS (SELECT /*+ SHUFFLE_HASH(r) */ 1 FROM ex_right_all_null r WHERE r.k = l.k)
ORDER BY l.id

-- ============================================================
-- Right side has no NULL: NULL-keyed left rows (id 3, 6) reach the marker
-- evaluation but cannot match, so their exists tag is false.
-- ============================================================

query
SELECT * FROM ex_left l
WHERE l.region = 'US'
   OR EXISTS (SELECT /*+ BROADCAST(r) */ 1 FROM ex_right_no_nulls r WHERE r.k = l.k)
ORDER BY l.id

-- ============================================================
-- NOT EXISTS combined with OR: also lowers to ExistenceJoin.
-- Exercised across all three strategies.
-- ============================================================

query
SELECT * FROM ex_left l
WHERE l.region = 'US'
   OR NOT EXISTS (SELECT /*+ BROADCAST(r) */ 1 FROM ex_right r WHERE r.k = l.k)
ORDER BY l.id

query
SELECT * FROM ex_left l
WHERE l.region = 'US'
   OR NOT EXISTS (SELECT /*+ SHUFFLE_HASH(r) */ 1 FROM ex_right r WHERE r.k = l.k)
ORDER BY l.id

query
SELECT * FROM ex_left l
WHERE l.region = 'US'
   OR NOT EXISTS (SELECT /*+ MERGE(r) */ 1 FROM ex_right r WHERE r.k = l.k)
ORDER BY l.id

-- ============================================================
-- Duplicate build keys: marker is "at least one match", so duplicates on the
-- right must not multiply the output. Checked on SHJ and SMJ too.
-- ============================================================

query
SELECT * FROM ex_left l
WHERE l.region = 'US'
   OR EXISTS (SELECT /*+ SHUFFLE_HASH(r) */ 1 FROM ex_right_dups r WHERE r.k = l.k)
ORDER BY l.id

query
SELECT * FROM ex_left l
WHERE l.region = 'US'
   OR EXISTS (SELECT /*+ MERGE(r) */ 1 FROM ex_right_dups r WHERE r.k = l.k)
ORDER BY l.id

-- ============================================================
-- Two EXISTS OR'd together: two Existence joins stacked in one plan.
-- ============================================================

query
SELECT * FROM ex_left l
WHERE EXISTS (SELECT /*+ BROADCAST(r) */ 1 FROM ex_right r WHERE r.k = l.k)
   OR EXISTS (SELECT /*+ BROADCAST(r2) */ 1 FROM ex_right_no_nulls r2 WHERE r2.k = l.k)
ORDER BY l.id

-- ============================================================
-- Marker used inside a more complex predicate (AND over NOT EXISTS OR ...).
-- ============================================================

query
SELECT id, k, region FROM ex_left l
WHERE l.id > 1
  AND (l.region = 'US'
       OR NOT EXISTS (SELECT /*+ BROADCAST(r) */ 1 FROM ex_right r WHERE r.k = l.k))
ORDER BY l.id

-- ============================================================
-- Multi-column correlation
-- ============================================================

statement
CREATE TABLE ex_left_multi(id int, k1 int, k2 int) USING parquet

statement
INSERT INTO ex_left_multi VALUES (1, 1, 100), (2, 2, 200), (3, 1, 300)

statement
CREATE TABLE ex_right_multi(k1 int, k2 int) USING parquet

statement
INSERT INTO ex_right_multi VALUES (1, 100), (2, 999)

query
SELECT * FROM ex_left_multi l
WHERE l.id > 0
  AND (l.k1 = 1
       OR EXISTS (SELECT /*+ SHUFFLE_HASH(r) */ 1
                  FROM ex_right_multi r WHERE r.k1 = l.k1 AND r.k2 = l.k2))
ORDER BY l.id
