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
-- Each query runs against the three physical join strategies (BHJ, SHJ,
-- SMJ) via hints, so we exercise CometBroadcastHashJoinExec,
-- CometHashJoinExec, and CometSortMergeJoinExec all carrying joinType =
-- ExistenceJoin.

-- ============================================================
-- Setup: covers NULLs, duplicates, empty build side
-- ============================================================

statement
CREATE TABLE ex_left(id int, k int, region string) USING parquet

statement
INSERT INTO ex_left VALUES
  (1, 1, 'US'),
  (2, 2, 'EU'),
  (3, NULL, 'US'),
  (4, 4, 'EU'),
  (5, 5, 'EU')

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

-- ============================================================
-- EXISTS with OR: BHJ build-right
-- ============================================================

query
SELECT /*+ BROADCAST(ex_right) */ * FROM ex_left l
WHERE l.region = 'US' OR EXISTS (SELECT 1 FROM ex_right r WHERE r.k = l.k)
ORDER BY l.id

-- ============================================================
-- EXISTS with OR: SHJ build-right
-- ============================================================

query
SELECT /*+ SHUFFLE_HASH(ex_right) */ * FROM ex_left l
WHERE l.region = 'US' OR EXISTS (SELECT 1 FROM ex_right r WHERE r.k = l.k)
ORDER BY l.id

-- ============================================================
-- EXISTS with OR: SMJ
-- ============================================================

query
SELECT /*+ MERGE(ex_right) */ * FROM ex_left l
WHERE l.region = 'US' OR EXISTS (SELECT 1 FROM ex_right r WHERE r.k = l.k)
ORDER BY l.id

-- ============================================================
-- Empty build: every left row is unmatched, only OR-arm rows survive
-- ============================================================

query
SELECT /*+ BROADCAST(ex_right_empty) */ * FROM ex_left l
WHERE l.region = 'US' OR EXISTS (SELECT 1 FROM ex_right_empty r WHERE r.k = l.k)
ORDER BY l.id

query
SELECT /*+ MERGE(ex_right_empty) */ * FROM ex_left l
WHERE l.region = 'US' OR EXISTS (SELECT 1 FROM ex_right_empty r WHERE r.k = l.k)
ORDER BY l.id

-- ============================================================
-- Right side has no NULL: NULL-keyed left row reaches the marker
-- evaluation but cannot match (NULL = anything is NULL → false),
-- so its exists tag is false.
-- ============================================================

query
SELECT /*+ BROADCAST(ex_right_no_nulls) */ * FROM ex_left l
WHERE l.region = 'US' OR EXISTS (SELECT 1 FROM ex_right_no_nulls r WHERE r.k = l.k)
ORDER BY l.id

-- ============================================================
-- NOT EXISTS combined with OR: also lowers to ExistenceJoin
-- (the optimizer flips the marker via NOT in the filter).
-- ============================================================

query
SELECT /*+ BROADCAST(ex_right) */ * FROM ex_left l
WHERE l.region = 'US' OR NOT EXISTS (SELECT 1 FROM ex_right r WHERE r.k = l.k)
ORDER BY l.id

-- ============================================================
-- Build with duplicate keys: marker is "at least one match", so duplicates
-- on the right must not multiply the output.
-- ============================================================

query
SELECT /*+ BROADCAST(ex_right_dups) */ * FROM ex_left l
WHERE l.region = 'US' OR EXISTS (SELECT 1 FROM ex_right_dups r WHERE r.k = l.k)
ORDER BY l.id

-- ============================================================
-- Marker used inside a more complex predicate (NOT exists OR ...).
-- ============================================================

query
SELECT /*+ BROADCAST(ex_right) */ id, k, region FROM ex_left l
WHERE l.id > 1
  AND (l.region = 'US' OR NOT EXISTS (SELECT 1 FROM ex_right r WHERE r.k = l.k))
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
SELECT /*+ BROADCAST(ex_right_multi) */ * FROM ex_left_multi l
WHERE l.id > 0
  AND (l.k1 = 1
       OR EXISTS (SELECT 1 FROM ex_right_multi r WHERE r.k1 = l.k1 AND r.k2 = l.k2))
ORDER BY l.id
