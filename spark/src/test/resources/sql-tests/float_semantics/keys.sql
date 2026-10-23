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

-- Contexts where Spark normalizes a float key (grouping, join and window partition keys
-- get NormalizeNaNAndZero) or compares it with SQL ordering (IN, ORDER BY, rank): -0.0 and
-- 0.0 are one key, and so are all NaNs. The sweep tracks #6385.
--
-- The edge values are 0.0, -0.0, a canonical NaN and a NaN with the sign bit set, plus 1.0,
-- -1.0, both infinities and NULL. The e CTE negates the flagged stored values to make the
-- sign-bit NaN at query time (see comparison.sql).
--
-- Nested ORDER BY and window order keys are in windows/nested_float_order_keys.sql, which
-- uses the same edge values.
--
-- ConstantFolding is on so that the IN list reaches Comet with folded literals, as Spark
-- plans it.

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

-- GROUP BY, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT c, count(*) FROM e GROUP BY c

-- GROUP BY, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT c, count(*) FROM e GROUP BY c

-- DISTINCT, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT DISTINCT c FROM e

-- DISTINCT, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT DISTINCT c FROM e

-- count(DISTINCT), DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT count(DISTINCT c) FROM e

-- count(DISTINCT), FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT count(DISTINCT c) FROM e

-- broadcast hash join key, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c = b.c

-- broadcast hash join key, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM e a JOIN e b ON a.c = b.c

-- shuffled hash join key, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT /*+ SHUFFLE_HASH(b) */ a.id, b.id FROM e a JOIN e b ON a.c = b.c

-- shuffled hash join key, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT /*+ SHUFFLE_HASH(b) */ a.id, b.id FROM e a JOIN e b ON a.c = b.c

-- sort merge join key, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT /*+ MERGE(b) */ a.id, b.id FROM e a JOIN e b ON a.c = b.c

-- sort merge join key, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT /*+ MERGE(b) */ a.id, b.id FROM e a JOIN e b ON a.c = b.c

-- null-safe join key, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT a.id, b.id FROM e a JOIN e b ON a.c <=> b.c

-- null-safe join key, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT a.id, b.id FROM e a JOIN e b ON a.c <=> b.c

-- left semi join key, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT a.id FROM e a LEFT SEMI JOIN (SELECT * FROM e WHERE id IN (2, 4)) b
ON a.c = b.c

-- left semi join key, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT a.id FROM e a LEFT SEMI JOIN (SELECT * FROM e WHERE id IN (2, 4)) b
ON a.c = b.c

-- left anti join key, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT a.id FROM e a LEFT ANTI JOIN (SELECT * FROM e WHERE id IN (2, 4)) b
ON a.c = b.c

-- left anti join key, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT a.id FROM e a LEFT ANTI JOIN (SELECT * FROM e WHERE id IN (2, 4)) b
ON a.c = b.c

-- INTERSECT, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT c FROM e INTERSECT SELECT c FROM e WHERE id IN (2, 4)

-- INTERSECT, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT c FROM e INTERSECT SELECT c FROM e WHERE id IN (2, 4)

-- EXCEPT, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT c FROM e EXCEPT SELECT c FROM e WHERE id IN (2, 4)

-- EXCEPT, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT c FROM e EXCEPT SELECT c FROM e WHERE id IN (2, 4)

-- IN list, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id FROM e WHERE c IN (CAST('0.0' AS DOUBLE), CAST('NaN' AS DOUBLE))

-- IN list, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id FROM e WHERE c IN (CAST('0.0' AS FLOAT), CAST('NaN' AS FLOAT))

-- IN subquery, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id FROM e WHERE c IN (SELECT c FROM e WHERE id IN (2, 4))

-- IN subquery, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id FROM e WHERE c IN (SELECT c FROM e WHERE id IN (2, 4))

-- ORDER BY, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id FROM e ORDER BY c, id

-- ORDER BY, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id FROM e ORDER BY c, id

-- ORDER BY DESC, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id FROM e ORDER BY c DESC NULLS LAST, id

-- ORDER BY DESC, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id FROM e ORDER BY c DESC NULLS LAST, id

-- window partition key, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, count(*) OVER (PARTITION BY c) FROM e

-- window partition key, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, count(*) OVER (PARTITION BY c) FROM e

-- window order key, rank, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, rank() OVER (ORDER BY c) FROM e

-- window order key, rank, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, rank() OVER (ORDER BY c) FROM e

-- window order key, dense_rank, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, dense_rank() OVER (ORDER BY c) FROM e

-- window order key, dense_rank, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, dense_rank() OVER (ORDER BY c) FROM e

-- hash repartition, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, spark_partition_id() FROM (SELECT /*+ REPARTITION(4, c) */ * FROM e)

-- hash repartition, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, spark_partition_id() FROM (SELECT /*+ REPARTITION(4, c) */ * FROM e)

-- nested GROUP BY, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT array(c), count(*) FROM e GROUP BY array(c)

-- nested GROUP BY, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT array(c), count(*) FROM e GROUP BY array(c)
