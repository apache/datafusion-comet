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

-- Aggregate functions over float input. Each pair of edge values is expanded from one
-- input row, so its two values reach the same partial aggregate in array order. Window
-- tests also order by pos to preserve signed-zero ties after a shuffle. mode gets each pair
-- as (l, r, r), so the mode is unique under every Spark equality rule.
--
-- The e and p CTEs negate the flagged stored values to make the sign-bit NaN at query time
-- (see comparison.sql). The sweep tracks #6385.
--
-- collect_set is in aggregates_collect_set_4_2.sql.


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

-- max, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- max, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- min, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, min(x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- min, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, min(x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- max_by, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max_by(pos, x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- max_by, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max_by(pos, x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- min_by, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, min_by(pos, x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- min_by, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, min_by(pos, x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- sum, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, sum(x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- sum, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, sum(x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- avg, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, avg(x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- avg, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, avg(x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- count(DISTINCT), DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, count(DISTINCT x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- count(DISTINCT), FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, count(DISTINCT x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- approx_count_distinct, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, approx_count_distinct(x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- approx_count_distinct, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, approx_count_distinct(x) FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x) GROUP BY id

-- mode, DOUBLE
-- mode falls back to Spark unless Mode.allowIncompatible is set, so only the answer is checked
-- here. expressions/aggregate/mode.sql runs it natively.
query spark_answer_only
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, mode(x) FROM (SELECT id, x FROM p LATERAL VIEW explode(array(l, r, r)) t AS x) GROUP BY id

-- mode, FLOAT
query spark_answer_only
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, mode(x) FROM (SELECT id, x FROM p LATERAL VIEW explode(array(l, r, r)) t AS x) GROUP BY id

-- global max and min, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT max(c), min(c) FROM e

-- global max and min, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT max(c), min(c) FROM e

-- global max_by and min_by, DOUBLE
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_double)
SELECT max_by(id, c), min_by(id, c) FROM e

-- global max_by and min_by, FLOAT
query
WITH e AS (SELECT id, IF(neg, -c, c) AS c FROM fs_edge_float)
SELECT max_by(id, c), min_by(id, c) FROM e

-- window max and min, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, pos, max(x) OVER w, min(x) OVER w FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x)
WINDOW w AS (PARTITION BY id ORDER BY pos
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)

-- window max and min, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, pos, max(x) OVER w, min(x) OVER w FROM (SELECT id, pos, x FROM p LATERAL VIEW posexplode(array(l, r)) t AS pos, x)
WINDOW w AS (PARTITION BY id ORDER BY pos
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
