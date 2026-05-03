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

-- Tests for BuildRight + LeftAnti hash joins, including:
-- - Null-aware anti-join (NOT IN subquery): the join becomes a
--   BroadcastHashJoinExec with isNullAwareAntiJoin=true.
-- - Regular broadcast LEFT ANTI JOIN.
-- - Shuffled hash LEFT ANTI JOIN (build-right).

-- Enable broadcast joins so NOT IN subqueries plan as BHJ + null-aware.
-- Config: spark.sql.adaptive.autoBroadcastJoinThreshold=10485760
-- Config: spark.sql.autoBroadcastJoinThreshold=10485760

-- ============================================================
-- Setup: tables covering NULL placement variations
-- ============================================================

statement
CREATE TABLE anti_left(id int, k int) USING parquet

statement
INSERT INTO anti_left VALUES (1, 1), (2, 2), (3, 3), (4, NULL), (5, 5)

statement
CREATE TABLE anti_right(id int, k int) USING parquet

statement
INSERT INTO anti_right VALUES (10, 2), (11, 4)

statement
CREATE TABLE anti_right_with_null(id int, k int) USING parquet

statement
INSERT INTO anti_right_with_null VALUES (10, 1), (11, NULL)

statement
CREATE TABLE anti_right_empty(id int, k int) USING parquet

-- ============================================================
-- NOT IN subquery (null-aware anti-join)
-- ============================================================

-- Right side has no NULL: regular anti-semantics, with NULL probe filtered out
query
SELECT * FROM anti_left WHERE k NOT IN (SELECT k FROM anti_right) ORDER BY id

-- Right side contains NULL: null-aware should suppress all left rows
query
SELECT * FROM anti_left WHERE k NOT IN (SELECT k FROM anti_right_with_null) ORDER BY id

-- Empty subquery: NOT IN against an empty set returns every left row,
-- including ones where the probe key is NULL.
query
SELECT * FROM anti_left WHERE k NOT IN (SELECT k FROM anti_right_empty) ORDER BY id

-- ============================================================
-- Regular LEFT ANTI JOIN with BROADCAST hint (BHJ build-right, non-null-aware)
-- ============================================================

query
SELECT /*+ BROADCAST(anti_right) */ *
FROM anti_left LEFT ANTI JOIN anti_right ON anti_left.k = anti_right.k
ORDER BY id

query
SELECT /*+ BROADCAST(anti_right_with_null) */ *
FROM anti_left LEFT ANTI JOIN anti_right_with_null ON anti_left.k = anti_right_with_null.k
ORDER BY id

-- ============================================================
-- Regular LEFT ANTI JOIN with SHUFFLE_HASH hint (SHJ build-right)
-- ============================================================

query
SELECT /*+ SHUFFLE_HASH(anti_right) */ *
FROM anti_left LEFT ANTI JOIN anti_right ON anti_left.k = anti_right.k
ORDER BY id
