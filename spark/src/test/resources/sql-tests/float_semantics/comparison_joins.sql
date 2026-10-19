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

-- Comparison operators in the condition of each join type, over every ordered pair of the
-- edge values (see comparison.sql). The equality on id is the key of the three equi-joins. The
-- nested loop join has no key, so the comparison is its whole condition, over the edge values
-- alone; its = and <=> make it an equi-join, which Spark plans as a hash join, so they repeat
-- the join keys of keys.sql.

-- ConstantFolding: enabled

statement
CREATE TABLE fs_edge_double(id INT, c DOUBLE, neg BOOLEAN) USING parquet

statement
INSERT INTO fs_edge_double VALUES
  (1, 0.0D, false),
  (2, double('-0.0'), false),
  (3, double('NaN'), false),
  (4, double('NaN'), true),
  (5, 1.0D, false),
  (6, -1.0D, false),
  (7, double('Infinity'), false),
  (8, double('-Infinity'), false),
  (9, NULL, false)

statement
CREATE TABLE fs_pair_double USING parquet AS
SELECT a.id * 10 + b.id AS id, a.c AS l, b.c AS r, a.neg AS lneg, b.neg AS rneg
FROM fs_edge_double a CROSS JOIN fs_edge_double b

statement
CREATE TABLE fs_edge_float(id INT, c FLOAT, neg BOOLEAN) USING parquet

statement
INSERT INTO fs_edge_float VALUES
  (1, float(0.0), false),
  (2, float('-0.0'), false),
  (3, float('NaN'), false),
  (4, float('NaN'), true),
  (5, float(1.0), false),
  (6, float(-1.0), false),
  (7, float('Infinity'), false),
  (8, float('-Infinity'), false),
  (9, NULL, false)

statement
CREATE TABLE fs_pair_float USING parquet AS
SELECT a.id * 10 + b.id AS id, a.c AS l, b.c AS r, a.neg AS lneg, b.neg AS rneg
FROM fs_edge_float a CROSS JOIN fs_edge_float b

-- broadcast hash join condition, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ BROADCAST(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l = b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ BROADCAST(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l <=> b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ BROADCAST(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l != b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ BROADCAST(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l < b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ BROADCAST(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l <= b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ BROADCAST(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l > b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ BROADCAST(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l >= b.r

-- broadcast hash join condition, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ BROADCAST(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l = b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ BROADCAST(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l <=> b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ BROADCAST(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l != b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ BROADCAST(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l < b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ BROADCAST(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l <= b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ BROADCAST(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l > b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ BROADCAST(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l >= b.r

-- shuffled hash join condition, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ SHUFFLE_HASH(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l = b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ SHUFFLE_HASH(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l <=> b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ SHUFFLE_HASH(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l != b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ SHUFFLE_HASH(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l < b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ SHUFFLE_HASH(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l <= b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ SHUFFLE_HASH(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l > b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ SHUFFLE_HASH(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l >= b.r

-- shuffled hash join condition, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ SHUFFLE_HASH(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l = b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ SHUFFLE_HASH(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l <=> b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ SHUFFLE_HASH(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l != b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ SHUFFLE_HASH(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l < b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ SHUFFLE_HASH(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l <= b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ SHUFFLE_HASH(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l > b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ SHUFFLE_HASH(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l >= b.r

-- sort merge join condition, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ MERGE(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l = b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ MERGE(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l <=> b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ MERGE(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l != b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ MERGE(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l < b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ MERGE(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l <= b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ MERGE(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l > b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT /*+ MERGE(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l >= b.r

-- sort merge join condition, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ MERGE(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l = b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ MERGE(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l <=> b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ MERGE(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l != b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ MERGE(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l < b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ MERGE(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l <= b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ MERGE(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l > b.r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT /*+ MERGE(b) */ a.id FROM p a JOIN p b
ON a.id = b.id AND a.l >= b.r

-- nested loop join condition, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c = b.c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c <=> b.c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c != b.c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c < b.c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c <= b.c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c > b.c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c >= b.c

-- nested loop join condition, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c = b.c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c <=> b.c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c != b.c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c < b.c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c <= b.c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c > b.c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c >= b.c
