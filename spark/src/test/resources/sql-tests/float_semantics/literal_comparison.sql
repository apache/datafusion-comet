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

-- A column compared with each edge literal, on either side. The literals are 0.0, -0.0, a
-- canonical NaN, a NaN with the sign bit set (-CAST('NaN' AS ...)) and 1.0.
--
-- This file needs ConstantFolding: Spark folds each literal into a Literal, so the
-- sign-bit NaN reaches Comet as a constant. The Parquet scan filter is only pushed down
-- when its literal has been folded.
--
-- Each predicate is its own UNION ALL branch. With all of them in one projection, Spark
-- 4.1.3's whole-stage codegen returns false for 0.0 = 0.0 (it is right with subexpression
-- elimination turned off), which would hide a Comet difference behind a Spark bug.
--
-- A stored column compared with a literal is pushed into the Parquet reader, so the third
-- context reads the table directly, without the CTE.

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

-- Project, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, 0, c = CAST('0.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 1, CAST('0.0' AS DOUBLE) = c FROM e
UNION ALL
SELECT id, 2, c = CAST('-0.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 3, CAST('-0.0' AS DOUBLE) = c FROM e
UNION ALL
SELECT id, 4, c = CAST('NaN' AS DOUBLE) FROM e
UNION ALL
SELECT id, 5, CAST('NaN' AS DOUBLE) = c FROM e
UNION ALL
SELECT id, 6, c = -CAST('NaN' AS DOUBLE) FROM e
UNION ALL
SELECT id, 7, -CAST('NaN' AS DOUBLE) = c FROM e
UNION ALL
SELECT id, 8, c = CAST('1.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 9, CAST('1.0' AS DOUBLE) = c FROM e

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, 0, c <=> CAST('0.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 1, CAST('0.0' AS DOUBLE) <=> c FROM e
UNION ALL
SELECT id, 2, c <=> CAST('-0.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 3, CAST('-0.0' AS DOUBLE) <=> c FROM e
UNION ALL
SELECT id, 4, c <=> CAST('NaN' AS DOUBLE) FROM e
UNION ALL
SELECT id, 5, CAST('NaN' AS DOUBLE) <=> c FROM e
UNION ALL
SELECT id, 6, c <=> -CAST('NaN' AS DOUBLE) FROM e
UNION ALL
SELECT id, 7, -CAST('NaN' AS DOUBLE) <=> c FROM e
UNION ALL
SELECT id, 8, c <=> CAST('1.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 9, CAST('1.0' AS DOUBLE) <=> c FROM e

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, 0, c != CAST('0.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 1, CAST('0.0' AS DOUBLE) != c FROM e
UNION ALL
SELECT id, 2, c != CAST('-0.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 3, CAST('-0.0' AS DOUBLE) != c FROM e
UNION ALL
SELECT id, 4, c != CAST('NaN' AS DOUBLE) FROM e
UNION ALL
SELECT id, 5, CAST('NaN' AS DOUBLE) != c FROM e
UNION ALL
SELECT id, 6, c != -CAST('NaN' AS DOUBLE) FROM e
UNION ALL
SELECT id, 7, -CAST('NaN' AS DOUBLE) != c FROM e
UNION ALL
SELECT id, 8, c != CAST('1.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 9, CAST('1.0' AS DOUBLE) != c FROM e

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, 0, c < CAST('0.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 1, CAST('0.0' AS DOUBLE) < c FROM e
UNION ALL
SELECT id, 2, c < CAST('-0.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 3, CAST('-0.0' AS DOUBLE) < c FROM e
UNION ALL
SELECT id, 4, c < CAST('NaN' AS DOUBLE) FROM e
UNION ALL
SELECT id, 5, CAST('NaN' AS DOUBLE) < c FROM e
UNION ALL
SELECT id, 6, c < -CAST('NaN' AS DOUBLE) FROM e
UNION ALL
SELECT id, 7, -CAST('NaN' AS DOUBLE) < c FROM e
UNION ALL
SELECT id, 8, c < CAST('1.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 9, CAST('1.0' AS DOUBLE) < c FROM e

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, 0, c <= CAST('0.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 1, CAST('0.0' AS DOUBLE) <= c FROM e
UNION ALL
SELECT id, 2, c <= CAST('-0.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 3, CAST('-0.0' AS DOUBLE) <= c FROM e
UNION ALL
SELECT id, 4, c <= CAST('NaN' AS DOUBLE) FROM e
UNION ALL
SELECT id, 5, CAST('NaN' AS DOUBLE) <= c FROM e
UNION ALL
SELECT id, 6, c <= -CAST('NaN' AS DOUBLE) FROM e
UNION ALL
SELECT id, 7, -CAST('NaN' AS DOUBLE) <= c FROM e
UNION ALL
SELECT id, 8, c <= CAST('1.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 9, CAST('1.0' AS DOUBLE) <= c FROM e

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, 0, c > CAST('0.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 1, CAST('0.0' AS DOUBLE) > c FROM e
UNION ALL
SELECT id, 2, c > CAST('-0.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 3, CAST('-0.0' AS DOUBLE) > c FROM e
UNION ALL
SELECT id, 4, c > CAST('NaN' AS DOUBLE) FROM e
UNION ALL
SELECT id, 5, CAST('NaN' AS DOUBLE) > c FROM e
UNION ALL
SELECT id, 6, c > -CAST('NaN' AS DOUBLE) FROM e
UNION ALL
SELECT id, 7, -CAST('NaN' AS DOUBLE) > c FROM e
UNION ALL
SELECT id, 8, c > CAST('1.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 9, CAST('1.0' AS DOUBLE) > c FROM e

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, 0, c >= CAST('0.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 1, CAST('0.0' AS DOUBLE) >= c FROM e
UNION ALL
SELECT id, 2, c >= CAST('-0.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 3, CAST('-0.0' AS DOUBLE) >= c FROM e
UNION ALL
SELECT id, 4, c >= CAST('NaN' AS DOUBLE) FROM e
UNION ALL
SELECT id, 5, CAST('NaN' AS DOUBLE) >= c FROM e
UNION ALL
SELECT id, 6, c >= -CAST('NaN' AS DOUBLE) FROM e
UNION ALL
SELECT id, 7, -CAST('NaN' AS DOUBLE) >= c FROM e
UNION ALL
SELECT id, 8, c >= CAST('1.0' AS DOUBLE) FROM e
UNION ALL
SELECT id, 9, CAST('1.0' AS DOUBLE) >= c FROM e

-- Project, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, 0, c = CAST('0.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 1, CAST('0.0' AS FLOAT) = c FROM e
UNION ALL
SELECT id, 2, c = CAST('-0.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 3, CAST('-0.0' AS FLOAT) = c FROM e
UNION ALL
SELECT id, 4, c = CAST('NaN' AS FLOAT) FROM e
UNION ALL
SELECT id, 5, CAST('NaN' AS FLOAT) = c FROM e
UNION ALL
SELECT id, 6, c = -CAST('NaN' AS FLOAT) FROM e
UNION ALL
SELECT id, 7, -CAST('NaN' AS FLOAT) = c FROM e
UNION ALL
SELECT id, 8, c = CAST('1.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 9, CAST('1.0' AS FLOAT) = c FROM e

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, 0, c <=> CAST('0.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 1, CAST('0.0' AS FLOAT) <=> c FROM e
UNION ALL
SELECT id, 2, c <=> CAST('-0.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 3, CAST('-0.0' AS FLOAT) <=> c FROM e
UNION ALL
SELECT id, 4, c <=> CAST('NaN' AS FLOAT) FROM e
UNION ALL
SELECT id, 5, CAST('NaN' AS FLOAT) <=> c FROM e
UNION ALL
SELECT id, 6, c <=> -CAST('NaN' AS FLOAT) FROM e
UNION ALL
SELECT id, 7, -CAST('NaN' AS FLOAT) <=> c FROM e
UNION ALL
SELECT id, 8, c <=> CAST('1.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 9, CAST('1.0' AS FLOAT) <=> c FROM e

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, 0, c != CAST('0.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 1, CAST('0.0' AS FLOAT) != c FROM e
UNION ALL
SELECT id, 2, c != CAST('-0.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 3, CAST('-0.0' AS FLOAT) != c FROM e
UNION ALL
SELECT id, 4, c != CAST('NaN' AS FLOAT) FROM e
UNION ALL
SELECT id, 5, CAST('NaN' AS FLOAT) != c FROM e
UNION ALL
SELECT id, 6, c != -CAST('NaN' AS FLOAT) FROM e
UNION ALL
SELECT id, 7, -CAST('NaN' AS FLOAT) != c FROM e
UNION ALL
SELECT id, 8, c != CAST('1.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 9, CAST('1.0' AS FLOAT) != c FROM e

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, 0, c < CAST('0.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 1, CAST('0.0' AS FLOAT) < c FROM e
UNION ALL
SELECT id, 2, c < CAST('-0.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 3, CAST('-0.0' AS FLOAT) < c FROM e
UNION ALL
SELECT id, 4, c < CAST('NaN' AS FLOAT) FROM e
UNION ALL
SELECT id, 5, CAST('NaN' AS FLOAT) < c FROM e
UNION ALL
SELECT id, 6, c < -CAST('NaN' AS FLOAT) FROM e
UNION ALL
SELECT id, 7, -CAST('NaN' AS FLOAT) < c FROM e
UNION ALL
SELECT id, 8, c < CAST('1.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 9, CAST('1.0' AS FLOAT) < c FROM e

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, 0, c <= CAST('0.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 1, CAST('0.0' AS FLOAT) <= c FROM e
UNION ALL
SELECT id, 2, c <= CAST('-0.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 3, CAST('-0.0' AS FLOAT) <= c FROM e
UNION ALL
SELECT id, 4, c <= CAST('NaN' AS FLOAT) FROM e
UNION ALL
SELECT id, 5, CAST('NaN' AS FLOAT) <= c FROM e
UNION ALL
SELECT id, 6, c <= -CAST('NaN' AS FLOAT) FROM e
UNION ALL
SELECT id, 7, -CAST('NaN' AS FLOAT) <= c FROM e
UNION ALL
SELECT id, 8, c <= CAST('1.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 9, CAST('1.0' AS FLOAT) <= c FROM e

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, 0, c > CAST('0.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 1, CAST('0.0' AS FLOAT) > c FROM e
UNION ALL
SELECT id, 2, c > CAST('-0.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 3, CAST('-0.0' AS FLOAT) > c FROM e
UNION ALL
SELECT id, 4, c > CAST('NaN' AS FLOAT) FROM e
UNION ALL
SELECT id, 5, CAST('NaN' AS FLOAT) > c FROM e
UNION ALL
SELECT id, 6, c > -CAST('NaN' AS FLOAT) FROM e
UNION ALL
SELECT id, 7, -CAST('NaN' AS FLOAT) > c FROM e
UNION ALL
SELECT id, 8, c > CAST('1.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 9, CAST('1.0' AS FLOAT) > c FROM e

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, 0, c >= CAST('0.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 1, CAST('0.0' AS FLOAT) >= c FROM e
UNION ALL
SELECT id, 2, c >= CAST('-0.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 3, CAST('-0.0' AS FLOAT) >= c FROM e
UNION ALL
SELECT id, 4, c >= CAST('NaN' AS FLOAT) FROM e
UNION ALL
SELECT id, 5, CAST('NaN' AS FLOAT) >= c FROM e
UNION ALL
SELECT id, 6, c >= -CAST('NaN' AS FLOAT) FROM e
UNION ALL
SELECT id, 7, -CAST('NaN' AS FLOAT) >= c FROM e
UNION ALL
SELECT id, 8, c >= CAST('1.0' AS FLOAT) FROM e
UNION ALL
SELECT id, 9, CAST('1.0' AS FLOAT) >= c FROM e

-- Filter, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, 0 FROM e WHERE c = CAST('0.0' AS DOUBLE)
UNION ALL
SELECT id, 1 FROM e WHERE CAST('0.0' AS DOUBLE) = c
UNION ALL
SELECT id, 2 FROM e WHERE c = CAST('-0.0' AS DOUBLE)
UNION ALL
SELECT id, 3 FROM e WHERE CAST('-0.0' AS DOUBLE) = c
UNION ALL
SELECT id, 4 FROM e WHERE c = CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 5 FROM e WHERE CAST('NaN' AS DOUBLE) = c
UNION ALL
SELECT id, 6 FROM e WHERE c = -CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 7 FROM e WHERE -CAST('NaN' AS DOUBLE) = c
UNION ALL
SELECT id, 8 FROM e WHERE c = CAST('1.0' AS DOUBLE)
UNION ALL
SELECT id, 9 FROM e WHERE CAST('1.0' AS DOUBLE) = c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, 0 FROM e WHERE c <=> CAST('0.0' AS DOUBLE)
UNION ALL
SELECT id, 1 FROM e WHERE CAST('0.0' AS DOUBLE) <=> c
UNION ALL
SELECT id, 2 FROM e WHERE c <=> CAST('-0.0' AS DOUBLE)
UNION ALL
SELECT id, 3 FROM e WHERE CAST('-0.0' AS DOUBLE) <=> c
UNION ALL
SELECT id, 4 FROM e WHERE c <=> CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 5 FROM e WHERE CAST('NaN' AS DOUBLE) <=> c
UNION ALL
SELECT id, 6 FROM e WHERE c <=> -CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 7 FROM e WHERE -CAST('NaN' AS DOUBLE) <=> c
UNION ALL
SELECT id, 8 FROM e WHERE c <=> CAST('1.0' AS DOUBLE)
UNION ALL
SELECT id, 9 FROM e WHERE CAST('1.0' AS DOUBLE) <=> c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, 0 FROM e WHERE c != CAST('0.0' AS DOUBLE)
UNION ALL
SELECT id, 1 FROM e WHERE CAST('0.0' AS DOUBLE) != c
UNION ALL
SELECT id, 2 FROM e WHERE c != CAST('-0.0' AS DOUBLE)
UNION ALL
SELECT id, 3 FROM e WHERE CAST('-0.0' AS DOUBLE) != c
UNION ALL
SELECT id, 4 FROM e WHERE c != CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 5 FROM e WHERE CAST('NaN' AS DOUBLE) != c
UNION ALL
SELECT id, 6 FROM e WHERE c != -CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 7 FROM e WHERE -CAST('NaN' AS DOUBLE) != c
UNION ALL
SELECT id, 8 FROM e WHERE c != CAST('1.0' AS DOUBLE)
UNION ALL
SELECT id, 9 FROM e WHERE CAST('1.0' AS DOUBLE) != c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, 0 FROM e WHERE c < CAST('0.0' AS DOUBLE)
UNION ALL
SELECT id, 1 FROM e WHERE CAST('0.0' AS DOUBLE) < c
UNION ALL
SELECT id, 2 FROM e WHERE c < CAST('-0.0' AS DOUBLE)
UNION ALL
SELECT id, 3 FROM e WHERE CAST('-0.0' AS DOUBLE) < c
UNION ALL
SELECT id, 4 FROM e WHERE c < CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 5 FROM e WHERE CAST('NaN' AS DOUBLE) < c
UNION ALL
SELECT id, 6 FROM e WHERE c < -CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 7 FROM e WHERE -CAST('NaN' AS DOUBLE) < c
UNION ALL
SELECT id, 8 FROM e WHERE c < CAST('1.0' AS DOUBLE)
UNION ALL
SELECT id, 9 FROM e WHERE CAST('1.0' AS DOUBLE) < c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, 0 FROM e WHERE c <= CAST('0.0' AS DOUBLE)
UNION ALL
SELECT id, 1 FROM e WHERE CAST('0.0' AS DOUBLE) <= c
UNION ALL
SELECT id, 2 FROM e WHERE c <= CAST('-0.0' AS DOUBLE)
UNION ALL
SELECT id, 3 FROM e WHERE CAST('-0.0' AS DOUBLE) <= c
UNION ALL
SELECT id, 4 FROM e WHERE c <= CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 5 FROM e WHERE CAST('NaN' AS DOUBLE) <= c
UNION ALL
SELECT id, 6 FROM e WHERE c <= -CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 7 FROM e WHERE -CAST('NaN' AS DOUBLE) <= c
UNION ALL
SELECT id, 8 FROM e WHERE c <= CAST('1.0' AS DOUBLE)
UNION ALL
SELECT id, 9 FROM e WHERE CAST('1.0' AS DOUBLE) <= c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, 0 FROM e WHERE c > CAST('0.0' AS DOUBLE)
UNION ALL
SELECT id, 1 FROM e WHERE CAST('0.0' AS DOUBLE) > c
UNION ALL
SELECT id, 2 FROM e WHERE c > CAST('-0.0' AS DOUBLE)
UNION ALL
SELECT id, 3 FROM e WHERE CAST('-0.0' AS DOUBLE) > c
UNION ALL
SELECT id, 4 FROM e WHERE c > CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 5 FROM e WHERE CAST('NaN' AS DOUBLE) > c
UNION ALL
SELECT id, 6 FROM e WHERE c > -CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 7 FROM e WHERE -CAST('NaN' AS DOUBLE) > c
UNION ALL
SELECT id, 8 FROM e WHERE c > CAST('1.0' AS DOUBLE)
UNION ALL
SELECT id, 9 FROM e WHERE CAST('1.0' AS DOUBLE) > c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT id, 0 FROM e WHERE c >= CAST('0.0' AS DOUBLE)
UNION ALL
SELECT id, 1 FROM e WHERE CAST('0.0' AS DOUBLE) >= c
UNION ALL
SELECT id, 2 FROM e WHERE c >= CAST('-0.0' AS DOUBLE)
UNION ALL
SELECT id, 3 FROM e WHERE CAST('-0.0' AS DOUBLE) >= c
UNION ALL
SELECT id, 4 FROM e WHERE c >= CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 5 FROM e WHERE CAST('NaN' AS DOUBLE) >= c
UNION ALL
SELECT id, 6 FROM e WHERE c >= -CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 7 FROM e WHERE -CAST('NaN' AS DOUBLE) >= c
UNION ALL
SELECT id, 8 FROM e WHERE c >= CAST('1.0' AS DOUBLE)
UNION ALL
SELECT id, 9 FROM e WHERE CAST('1.0' AS DOUBLE) >= c

-- Filter, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, 0 FROM e WHERE c = CAST('0.0' AS FLOAT)
UNION ALL
SELECT id, 1 FROM e WHERE CAST('0.0' AS FLOAT) = c
UNION ALL
SELECT id, 2 FROM e WHERE c = CAST('-0.0' AS FLOAT)
UNION ALL
SELECT id, 3 FROM e WHERE CAST('-0.0' AS FLOAT) = c
UNION ALL
SELECT id, 4 FROM e WHERE c = CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 5 FROM e WHERE CAST('NaN' AS FLOAT) = c
UNION ALL
SELECT id, 6 FROM e WHERE c = -CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 7 FROM e WHERE -CAST('NaN' AS FLOAT) = c
UNION ALL
SELECT id, 8 FROM e WHERE c = CAST('1.0' AS FLOAT)
UNION ALL
SELECT id, 9 FROM e WHERE CAST('1.0' AS FLOAT) = c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, 0 FROM e WHERE c <=> CAST('0.0' AS FLOAT)
UNION ALL
SELECT id, 1 FROM e WHERE CAST('0.0' AS FLOAT) <=> c
UNION ALL
SELECT id, 2 FROM e WHERE c <=> CAST('-0.0' AS FLOAT)
UNION ALL
SELECT id, 3 FROM e WHERE CAST('-0.0' AS FLOAT) <=> c
UNION ALL
SELECT id, 4 FROM e WHERE c <=> CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 5 FROM e WHERE CAST('NaN' AS FLOAT) <=> c
UNION ALL
SELECT id, 6 FROM e WHERE c <=> -CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 7 FROM e WHERE -CAST('NaN' AS FLOAT) <=> c
UNION ALL
SELECT id, 8 FROM e WHERE c <=> CAST('1.0' AS FLOAT)
UNION ALL
SELECT id, 9 FROM e WHERE CAST('1.0' AS FLOAT) <=> c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, 0 FROM e WHERE c != CAST('0.0' AS FLOAT)
UNION ALL
SELECT id, 1 FROM e WHERE CAST('0.0' AS FLOAT) != c
UNION ALL
SELECT id, 2 FROM e WHERE c != CAST('-0.0' AS FLOAT)
UNION ALL
SELECT id, 3 FROM e WHERE CAST('-0.0' AS FLOAT) != c
UNION ALL
SELECT id, 4 FROM e WHERE c != CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 5 FROM e WHERE CAST('NaN' AS FLOAT) != c
UNION ALL
SELECT id, 6 FROM e WHERE c != -CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 7 FROM e WHERE -CAST('NaN' AS FLOAT) != c
UNION ALL
SELECT id, 8 FROM e WHERE c != CAST('1.0' AS FLOAT)
UNION ALL
SELECT id, 9 FROM e WHERE CAST('1.0' AS FLOAT) != c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, 0 FROM e WHERE c < CAST('0.0' AS FLOAT)
UNION ALL
SELECT id, 1 FROM e WHERE CAST('0.0' AS FLOAT) < c
UNION ALL
SELECT id, 2 FROM e WHERE c < CAST('-0.0' AS FLOAT)
UNION ALL
SELECT id, 3 FROM e WHERE CAST('-0.0' AS FLOAT) < c
UNION ALL
SELECT id, 4 FROM e WHERE c < CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 5 FROM e WHERE CAST('NaN' AS FLOAT) < c
UNION ALL
SELECT id, 6 FROM e WHERE c < -CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 7 FROM e WHERE -CAST('NaN' AS FLOAT) < c
UNION ALL
SELECT id, 8 FROM e WHERE c < CAST('1.0' AS FLOAT)
UNION ALL
SELECT id, 9 FROM e WHERE CAST('1.0' AS FLOAT) < c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, 0 FROM e WHERE c <= CAST('0.0' AS FLOAT)
UNION ALL
SELECT id, 1 FROM e WHERE CAST('0.0' AS FLOAT) <= c
UNION ALL
SELECT id, 2 FROM e WHERE c <= CAST('-0.0' AS FLOAT)
UNION ALL
SELECT id, 3 FROM e WHERE CAST('-0.0' AS FLOAT) <= c
UNION ALL
SELECT id, 4 FROM e WHERE c <= CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 5 FROM e WHERE CAST('NaN' AS FLOAT) <= c
UNION ALL
SELECT id, 6 FROM e WHERE c <= -CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 7 FROM e WHERE -CAST('NaN' AS FLOAT) <= c
UNION ALL
SELECT id, 8 FROM e WHERE c <= CAST('1.0' AS FLOAT)
UNION ALL
SELECT id, 9 FROM e WHERE CAST('1.0' AS FLOAT) <= c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, 0 FROM e WHERE c > CAST('0.0' AS FLOAT)
UNION ALL
SELECT id, 1 FROM e WHERE CAST('0.0' AS FLOAT) > c
UNION ALL
SELECT id, 2 FROM e WHERE c > CAST('-0.0' AS FLOAT)
UNION ALL
SELECT id, 3 FROM e WHERE CAST('-0.0' AS FLOAT) > c
UNION ALL
SELECT id, 4 FROM e WHERE c > CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 5 FROM e WHERE CAST('NaN' AS FLOAT) > c
UNION ALL
SELECT id, 6 FROM e WHERE c > -CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 7 FROM e WHERE -CAST('NaN' AS FLOAT) > c
UNION ALL
SELECT id, 8 FROM e WHERE c > CAST('1.0' AS FLOAT)
UNION ALL
SELECT id, 9 FROM e WHERE CAST('1.0' AS FLOAT) > c

query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT id, 0 FROM e WHERE c >= CAST('0.0' AS FLOAT)
UNION ALL
SELECT id, 1 FROM e WHERE CAST('0.0' AS FLOAT) >= c
UNION ALL
SELECT id, 2 FROM e WHERE c >= CAST('-0.0' AS FLOAT)
UNION ALL
SELECT id, 3 FROM e WHERE CAST('-0.0' AS FLOAT) >= c
UNION ALL
SELECT id, 4 FROM e WHERE c >= CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 5 FROM e WHERE CAST('NaN' AS FLOAT) >= c
UNION ALL
SELECT id, 6 FROM e WHERE c >= -CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 7 FROM e WHERE -CAST('NaN' AS FLOAT) >= c
UNION ALL
SELECT id, 8 FROM e WHERE c >= CAST('1.0' AS FLOAT)
UNION ALL
SELECT id, 9 FROM e WHERE CAST('1.0' AS FLOAT) >= c

-- scan filter, DOUBLE
query
SELECT id, 0 FROM fs_edge_double WHERE c = CAST('0.0' AS DOUBLE)
UNION ALL
SELECT id, 1 FROM fs_edge_double WHERE CAST('0.0' AS DOUBLE) = c
UNION ALL
SELECT id, 2 FROM fs_edge_double WHERE c = CAST('-0.0' AS DOUBLE)
UNION ALL
SELECT id, 3 FROM fs_edge_double WHERE CAST('-0.0' AS DOUBLE) = c
UNION ALL
SELECT id, 4 FROM fs_edge_double WHERE c = CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 5 FROM fs_edge_double WHERE CAST('NaN' AS DOUBLE) = c
UNION ALL
SELECT id, 6 FROM fs_edge_double WHERE c = -CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 7 FROM fs_edge_double WHERE -CAST('NaN' AS DOUBLE) = c
UNION ALL
SELECT id, 8 FROM fs_edge_double WHERE c = CAST('1.0' AS DOUBLE)
UNION ALL
SELECT id, 9 FROM fs_edge_double WHERE CAST('1.0' AS DOUBLE) = c

query
SELECT id, 0 FROM fs_edge_double WHERE c <=> CAST('0.0' AS DOUBLE)
UNION ALL
SELECT id, 1 FROM fs_edge_double WHERE CAST('0.0' AS DOUBLE) <=> c
UNION ALL
SELECT id, 2 FROM fs_edge_double WHERE c <=> CAST('-0.0' AS DOUBLE)
UNION ALL
SELECT id, 3 FROM fs_edge_double WHERE CAST('-0.0' AS DOUBLE) <=> c
UNION ALL
SELECT id, 4 FROM fs_edge_double WHERE c <=> CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 5 FROM fs_edge_double WHERE CAST('NaN' AS DOUBLE) <=> c
UNION ALL
SELECT id, 6 FROM fs_edge_double WHERE c <=> -CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 7 FROM fs_edge_double WHERE -CAST('NaN' AS DOUBLE) <=> c
UNION ALL
SELECT id, 8 FROM fs_edge_double WHERE c <=> CAST('1.0' AS DOUBLE)
UNION ALL
SELECT id, 9 FROM fs_edge_double WHERE CAST('1.0' AS DOUBLE) <=> c

query
SELECT id, 0 FROM fs_edge_double WHERE c != CAST('0.0' AS DOUBLE)
UNION ALL
SELECT id, 1 FROM fs_edge_double WHERE CAST('0.0' AS DOUBLE) != c
UNION ALL
SELECT id, 2 FROM fs_edge_double WHERE c != CAST('-0.0' AS DOUBLE)
UNION ALL
SELECT id, 3 FROM fs_edge_double WHERE CAST('-0.0' AS DOUBLE) != c
UNION ALL
SELECT id, 4 FROM fs_edge_double WHERE c != CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 5 FROM fs_edge_double WHERE CAST('NaN' AS DOUBLE) != c
UNION ALL
SELECT id, 6 FROM fs_edge_double WHERE c != -CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 7 FROM fs_edge_double WHERE -CAST('NaN' AS DOUBLE) != c
UNION ALL
SELECT id, 8 FROM fs_edge_double WHERE c != CAST('1.0' AS DOUBLE)
UNION ALL
SELECT id, 9 FROM fs_edge_double WHERE CAST('1.0' AS DOUBLE) != c

query
SELECT id, 0 FROM fs_edge_double WHERE c < CAST('0.0' AS DOUBLE)
UNION ALL
SELECT id, 1 FROM fs_edge_double WHERE CAST('0.0' AS DOUBLE) < c
UNION ALL
SELECT id, 2 FROM fs_edge_double WHERE c < CAST('-0.0' AS DOUBLE)
UNION ALL
SELECT id, 3 FROM fs_edge_double WHERE CAST('-0.0' AS DOUBLE) < c
UNION ALL
SELECT id, 4 FROM fs_edge_double WHERE c < CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 5 FROM fs_edge_double WHERE CAST('NaN' AS DOUBLE) < c
UNION ALL
SELECT id, 6 FROM fs_edge_double WHERE c < -CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 7 FROM fs_edge_double WHERE -CAST('NaN' AS DOUBLE) < c
UNION ALL
SELECT id, 8 FROM fs_edge_double WHERE c < CAST('1.0' AS DOUBLE)
UNION ALL
SELECT id, 9 FROM fs_edge_double WHERE CAST('1.0' AS DOUBLE) < c

query
SELECT id, 0 FROM fs_edge_double WHERE c <= CAST('0.0' AS DOUBLE)
UNION ALL
SELECT id, 1 FROM fs_edge_double WHERE CAST('0.0' AS DOUBLE) <= c
UNION ALL
SELECT id, 2 FROM fs_edge_double WHERE c <= CAST('-0.0' AS DOUBLE)
UNION ALL
SELECT id, 3 FROM fs_edge_double WHERE CAST('-0.0' AS DOUBLE) <= c
UNION ALL
SELECT id, 4 FROM fs_edge_double WHERE c <= CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 5 FROM fs_edge_double WHERE CAST('NaN' AS DOUBLE) <= c
UNION ALL
SELECT id, 6 FROM fs_edge_double WHERE c <= -CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 7 FROM fs_edge_double WHERE -CAST('NaN' AS DOUBLE) <= c
UNION ALL
SELECT id, 8 FROM fs_edge_double WHERE c <= CAST('1.0' AS DOUBLE)
UNION ALL
SELECT id, 9 FROM fs_edge_double WHERE CAST('1.0' AS DOUBLE) <= c

query
SELECT id, 0 FROM fs_edge_double WHERE c > CAST('0.0' AS DOUBLE)
UNION ALL
SELECT id, 1 FROM fs_edge_double WHERE CAST('0.0' AS DOUBLE) > c
UNION ALL
SELECT id, 2 FROM fs_edge_double WHERE c > CAST('-0.0' AS DOUBLE)
UNION ALL
SELECT id, 3 FROM fs_edge_double WHERE CAST('-0.0' AS DOUBLE) > c
UNION ALL
SELECT id, 4 FROM fs_edge_double WHERE c > CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 5 FROM fs_edge_double WHERE CAST('NaN' AS DOUBLE) > c
UNION ALL
SELECT id, 6 FROM fs_edge_double WHERE c > -CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 7 FROM fs_edge_double WHERE -CAST('NaN' AS DOUBLE) > c
UNION ALL
SELECT id, 8 FROM fs_edge_double WHERE c > CAST('1.0' AS DOUBLE)
UNION ALL
SELECT id, 9 FROM fs_edge_double WHERE CAST('1.0' AS DOUBLE) > c

query
SELECT id, 0 FROM fs_edge_double WHERE c >= CAST('0.0' AS DOUBLE)
UNION ALL
SELECT id, 1 FROM fs_edge_double WHERE CAST('0.0' AS DOUBLE) >= c
UNION ALL
SELECT id, 2 FROM fs_edge_double WHERE c >= CAST('-0.0' AS DOUBLE)
UNION ALL
SELECT id, 3 FROM fs_edge_double WHERE CAST('-0.0' AS DOUBLE) >= c
UNION ALL
SELECT id, 4 FROM fs_edge_double WHERE c >= CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 5 FROM fs_edge_double WHERE CAST('NaN' AS DOUBLE) >= c
UNION ALL
SELECT id, 6 FROM fs_edge_double WHERE c >= -CAST('NaN' AS DOUBLE)
UNION ALL
SELECT id, 7 FROM fs_edge_double WHERE -CAST('NaN' AS DOUBLE) >= c
UNION ALL
SELECT id, 8 FROM fs_edge_double WHERE c >= CAST('1.0' AS DOUBLE)
UNION ALL
SELECT id, 9 FROM fs_edge_double WHERE CAST('1.0' AS DOUBLE) >= c

-- scan filter, FLOAT
query
SELECT id, 0 FROM fs_edge_float WHERE c = CAST('0.0' AS FLOAT)
UNION ALL
SELECT id, 1 FROM fs_edge_float WHERE CAST('0.0' AS FLOAT) = c
UNION ALL
SELECT id, 2 FROM fs_edge_float WHERE c = CAST('-0.0' AS FLOAT)
UNION ALL
SELECT id, 3 FROM fs_edge_float WHERE CAST('-0.0' AS FLOAT) = c
UNION ALL
SELECT id, 4 FROM fs_edge_float WHERE c = CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 5 FROM fs_edge_float WHERE CAST('NaN' AS FLOAT) = c
UNION ALL
SELECT id, 6 FROM fs_edge_float WHERE c = -CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 7 FROM fs_edge_float WHERE -CAST('NaN' AS FLOAT) = c
UNION ALL
SELECT id, 8 FROM fs_edge_float WHERE c = CAST('1.0' AS FLOAT)
UNION ALL
SELECT id, 9 FROM fs_edge_float WHERE CAST('1.0' AS FLOAT) = c

query
SELECT id, 0 FROM fs_edge_float WHERE c <=> CAST('0.0' AS FLOAT)
UNION ALL
SELECT id, 1 FROM fs_edge_float WHERE CAST('0.0' AS FLOAT) <=> c
UNION ALL
SELECT id, 2 FROM fs_edge_float WHERE c <=> CAST('-0.0' AS FLOAT)
UNION ALL
SELECT id, 3 FROM fs_edge_float WHERE CAST('-0.0' AS FLOAT) <=> c
UNION ALL
SELECT id, 4 FROM fs_edge_float WHERE c <=> CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 5 FROM fs_edge_float WHERE CAST('NaN' AS FLOAT) <=> c
UNION ALL
SELECT id, 6 FROM fs_edge_float WHERE c <=> -CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 7 FROM fs_edge_float WHERE -CAST('NaN' AS FLOAT) <=> c
UNION ALL
SELECT id, 8 FROM fs_edge_float WHERE c <=> CAST('1.0' AS FLOAT)
UNION ALL
SELECT id, 9 FROM fs_edge_float WHERE CAST('1.0' AS FLOAT) <=> c

query
SELECT id, 0 FROM fs_edge_float WHERE c != CAST('0.0' AS FLOAT)
UNION ALL
SELECT id, 1 FROM fs_edge_float WHERE CAST('0.0' AS FLOAT) != c
UNION ALL
SELECT id, 2 FROM fs_edge_float WHERE c != CAST('-0.0' AS FLOAT)
UNION ALL
SELECT id, 3 FROM fs_edge_float WHERE CAST('-0.0' AS FLOAT) != c
UNION ALL
SELECT id, 4 FROM fs_edge_float WHERE c != CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 5 FROM fs_edge_float WHERE CAST('NaN' AS FLOAT) != c
UNION ALL
SELECT id, 6 FROM fs_edge_float WHERE c != -CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 7 FROM fs_edge_float WHERE -CAST('NaN' AS FLOAT) != c
UNION ALL
SELECT id, 8 FROM fs_edge_float WHERE c != CAST('1.0' AS FLOAT)
UNION ALL
SELECT id, 9 FROM fs_edge_float WHERE CAST('1.0' AS FLOAT) != c

query
SELECT id, 0 FROM fs_edge_float WHERE c < CAST('0.0' AS FLOAT)
UNION ALL
SELECT id, 1 FROM fs_edge_float WHERE CAST('0.0' AS FLOAT) < c
UNION ALL
SELECT id, 2 FROM fs_edge_float WHERE c < CAST('-0.0' AS FLOAT)
UNION ALL
SELECT id, 3 FROM fs_edge_float WHERE CAST('-0.0' AS FLOAT) < c
UNION ALL
SELECT id, 4 FROM fs_edge_float WHERE c < CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 5 FROM fs_edge_float WHERE CAST('NaN' AS FLOAT) < c
UNION ALL
SELECT id, 6 FROM fs_edge_float WHERE c < -CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 7 FROM fs_edge_float WHERE -CAST('NaN' AS FLOAT) < c
UNION ALL
SELECT id, 8 FROM fs_edge_float WHERE c < CAST('1.0' AS FLOAT)
UNION ALL
SELECT id, 9 FROM fs_edge_float WHERE CAST('1.0' AS FLOAT) < c

query
SELECT id, 0 FROM fs_edge_float WHERE c <= CAST('0.0' AS FLOAT)
UNION ALL
SELECT id, 1 FROM fs_edge_float WHERE CAST('0.0' AS FLOAT) <= c
UNION ALL
SELECT id, 2 FROM fs_edge_float WHERE c <= CAST('-0.0' AS FLOAT)
UNION ALL
SELECT id, 3 FROM fs_edge_float WHERE CAST('-0.0' AS FLOAT) <= c
UNION ALL
SELECT id, 4 FROM fs_edge_float WHERE c <= CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 5 FROM fs_edge_float WHERE CAST('NaN' AS FLOAT) <= c
UNION ALL
SELECT id, 6 FROM fs_edge_float WHERE c <= -CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 7 FROM fs_edge_float WHERE -CAST('NaN' AS FLOAT) <= c
UNION ALL
SELECT id, 8 FROM fs_edge_float WHERE c <= CAST('1.0' AS FLOAT)
UNION ALL
SELECT id, 9 FROM fs_edge_float WHERE CAST('1.0' AS FLOAT) <= c

query
SELECT id, 0 FROM fs_edge_float WHERE c > CAST('0.0' AS FLOAT)
UNION ALL
SELECT id, 1 FROM fs_edge_float WHERE CAST('0.0' AS FLOAT) > c
UNION ALL
SELECT id, 2 FROM fs_edge_float WHERE c > CAST('-0.0' AS FLOAT)
UNION ALL
SELECT id, 3 FROM fs_edge_float WHERE CAST('-0.0' AS FLOAT) > c
UNION ALL
SELECT id, 4 FROM fs_edge_float WHERE c > CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 5 FROM fs_edge_float WHERE CAST('NaN' AS FLOAT) > c
UNION ALL
SELECT id, 6 FROM fs_edge_float WHERE c > -CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 7 FROM fs_edge_float WHERE -CAST('NaN' AS FLOAT) > c
UNION ALL
SELECT id, 8 FROM fs_edge_float WHERE c > CAST('1.0' AS FLOAT)
UNION ALL
SELECT id, 9 FROM fs_edge_float WHERE CAST('1.0' AS FLOAT) > c

query
SELECT id, 0 FROM fs_edge_float WHERE c >= CAST('0.0' AS FLOAT)
UNION ALL
SELECT id, 1 FROM fs_edge_float WHERE CAST('0.0' AS FLOAT) >= c
UNION ALL
SELECT id, 2 FROM fs_edge_float WHERE c >= CAST('-0.0' AS FLOAT)
UNION ALL
SELECT id, 3 FROM fs_edge_float WHERE CAST('-0.0' AS FLOAT) >= c
UNION ALL
SELECT id, 4 FROM fs_edge_float WHERE c >= CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 5 FROM fs_edge_float WHERE CAST('NaN' AS FLOAT) >= c
UNION ALL
SELECT id, 6 FROM fs_edge_float WHERE c >= -CAST('NaN' AS FLOAT)
UNION ALL
SELECT id, 7 FROM fs_edge_float WHERE -CAST('NaN' AS FLOAT) >= c
UNION ALL
SELECT id, 8 FROM fs_edge_float WHERE c >= CAST('1.0' AS FLOAT)
UNION ALL
SELECT id, 9 FROM fs_edge_float WHERE CAST('1.0' AS FLOAT) >= c
