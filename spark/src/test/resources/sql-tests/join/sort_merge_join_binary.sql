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

-- Tests for SortMergeJoin on BinaryType keys.
--
-- SortMergeJoin advances two sorted streams, so the sort order of the inputs must agree with the
-- join key comparator. Spark orders binary with `TypeUtils.compareBinary` (unsigned byte, shorter
-- prefix first) and Arrow orders `Binary` by byte slice, which is the same order. A disagreement
-- between the sort and the comparator drops matches silently rather than failing, so the keys below
-- separate the two candidate orderings: under signed comparison 0x80 and 0xff would sort before
-- 0x00.

-- Force a sort merge join: neither side may be broadcast, and hash joins are not preferred.
-- Config: spark.sql.autoBroadcastJoinThreshold=-1
-- Config: spark.sql.adaptive.autoBroadcastJoinThreshold=-1
-- Config: spark.sql.join.preferSortMergeJoin=true
-- More than one sorted stream per side.
-- Config: spark.sql.shuffle.partitions=3
-- Smaller than the row count of the smj_bin_wide_* tables, so their key groups span batches and
-- the join compares keys across batch boundaries.
-- Config: spark.comet.batchSize=16
-- Decides whether the scan hands the join a plain or a dictionary-backed binary array.
-- ConfigMatrix: parquet.enable.dictionary=false,true

-- ============================================================
-- Setup
-- ============================================================

-- 16-byte keys, the shape a UUID stored as binary takes. Rows 'a' and 'c' share a key, so a key
-- group with more than one row on the left is covered.
statement
CREATE TABLE smj_bin_left(name STRING, bkey BINARY) USING parquet

statement
INSERT INTO smj_bin_left VALUES
  ('a', X'000102030405060708090a0b0c0d0e0f'),
  ('b', X'0102030405060708090a0b0c0d0e0f10'),
  ('c', X'000102030405060708090a0b0c0d0e0f')

statement
CREATE TABLE smj_bin_right(name STRING, bkey BINARY) USING parquet

statement
INSERT INTO smj_bin_right VALUES
  ('a', X'000102030405060708090a0b0c0d0e0f'),
  ('d', X'f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff')

-- Keys that separate unsigned from signed byte ordering, plus an empty key and a shared prefix.
-- Both sides carry the same keys, so every key must find its match.
statement
CREATE TABLE smj_bin_ord_left(id INT, bkey BINARY) USING parquet

statement
INSERT INTO smj_bin_ord_left VALUES
  (0, CAST('' AS BINARY)),
  (1, X'00'),
  (2, X'01'),
  (3, X'0100'),
  (4, X'7f'),
  (5, X'80'),
  (6, X'ff'),
  (7, X'ff00')

statement
CREATE TABLE smj_bin_ord_right(id INT, bkey BINARY) USING parquet

statement
INSERT INTO smj_bin_ord_right VALUES
  (100, CAST('' AS BINARY)),
  (200, X'00'),
  (300, X'01'),
  (400, X'0100'),
  (500, X'7f'),
  (600, X'80'),
  (700, X'ff'),
  (800, X'ff00')

-- NULL keys on both sides.
statement
CREATE TABLE smj_bin_null_left(id INT, bkey BINARY) USING parquet

statement
INSERT INTO smj_bin_null_left VALUES (1, X'0a0b'), (2, CAST(NULL AS BINARY)), (3, X'0c0d')

statement
CREATE TABLE smj_bin_null_right(id INT, bkey BINARY) USING parquet

statement
INSERT INTO smj_bin_null_right VALUES (10, X'0a0b'), (20, CAST(NULL AS BINARY)), (30, X'1112')

-- Few distinct keys over many rows: with the batch size above, every key group spans many batches.
-- The 0x80 suffix keeps the high-byte ordering in play here too.
statement
CREATE TABLE smj_bin_wide_left(id BIGINT, bkey BINARY) USING parquet

statement
INSERT INTO smj_bin_wide_left
SELECT id, unhex(concat(lpad(hex(id % 3), 2, '0'), '80')) FROM range(200)

statement
CREATE TABLE smj_bin_wide_right(id BIGINT, bkey BINARY) USING parquet

statement
INSERT INTO smj_bin_wide_right
SELECT id * 10, unhex(concat(lpad(hex(id % 4), 2, '0'), '80')) FROM range(200)

-- ============================================================
-- Join types over a binary key
-- ============================================================

query
SELECT * FROM smj_bin_left l JOIN smj_bin_right r ON l.bkey = r.bkey

query
SELECT * FROM smj_bin_left l LEFT OUTER JOIN smj_bin_right r ON l.bkey = r.bkey

query
SELECT * FROM smj_bin_left l RIGHT OUTER JOIN smj_bin_right r ON l.bkey = r.bkey

query
SELECT * FROM smj_bin_left l FULL OUTER JOIN smj_bin_right r ON l.bkey = r.bkey

query
SELECT * FROM smj_bin_left l LEFT SEMI JOIN smj_bin_right r ON l.bkey = r.bkey

query
SELECT * FROM smj_bin_left l LEFT ANTI JOIN smj_bin_right r ON l.bkey = r.bkey

-- Composite key: the binary key is only part of the ordering, and ('c', ...) no longer matches.
query
SELECT * FROM smj_bin_left l JOIN smj_bin_right r ON l.name = r.name AND l.bkey = r.bkey

-- ============================================================
-- Byte ordering
-- ============================================================

query
SELECT * FROM smj_bin_ord_left l JOIN smj_bin_ord_right r ON l.bkey = r.bkey

-- ============================================================
-- NULL keys
-- ============================================================

-- Inner join: NULL = NULL must not match.
query
SELECT * FROM smj_bin_null_left l JOIN smj_bin_null_right r ON l.bkey = r.bkey

-- Full outer join: the NULL-keyed row on each side surfaces as unmatched.
query
SELECT * FROM smj_bin_null_left l FULL OUTER JOIN smj_bin_null_right r ON l.bkey = r.bkey

-- ============================================================
-- Key groups spanning batches
-- ============================================================

query
SELECT COUNT(*), SUM(l.id), MAX(r.id)
FROM smj_bin_wide_left l JOIN smj_bin_wide_right r ON l.bkey = r.bkey
