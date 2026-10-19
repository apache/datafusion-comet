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

-- Spark compares floats with SQL ordering: -0.0 equals 0.0, every NaN equals every other NaN,
-- and NaN sorts above every other value. Each query applies one comparison operator to every
-- ordered pair of the edge values, in one operator context. The sweep tracks #6385.
--
-- The edge values are 0.0, -0.0, a canonical NaN and a NaN with the sign bit set, plus 1.0,
-- -1.0, both infinities and NULL. Spark's Parquet writer canonicalizes NaN, so the tables hold
-- the canonical NaN and a flag, and the p CTE negates the flagged values to make the sign-bit
-- NaN at query time. That works on every platform; on x86-64, every NaN that arithmetic
-- produces also has the sign bit set.
--
-- comparison_joins.sql covers join conditions and comparison_nested.sql covers comparisons of
-- arrays and structs. literal_comparison.sql compares a column with a literal.
--
-- The fixtures run with Spark's ConstantFolding on, which the sweep always did. The literal
-- queries need it for a literal to arrive as a sign-bit NaN, and nothing here is an
-- all-literal expression that the default exclusion would keep from being folded.

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

-- Project, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, l = r FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, l <=> r FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, l != r FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, l < r FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, l <= r FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, l > r FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, l >= r FROM p

-- Project, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, l = r FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, l <=> r FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, l != r FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, l < r FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, l <= r FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, l > r FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, l >= r FROM p

-- Filter, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id FROM p WHERE l = r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id FROM p WHERE l <=> r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id FROM p WHERE l != r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id FROM p WHERE l < r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id FROM p WHERE l <= r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id FROM p WHERE l > r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id FROM p WHERE l >= r

-- Filter, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id FROM p WHERE l = r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id FROM p WHERE l <=> r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id FROM p WHERE l != r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id FROM p WHERE l < r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id FROM p WHERE l <= r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id FROM p WHERE l > r

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id FROM p WHERE l >= r

-- aggregate argument, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(l = r, 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(l <=> r, 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(l != r, 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(l < r, 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(l <= r, 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(l > r, 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(l >= r, 1, 0)) FROM p GROUP BY id

-- aggregate argument, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(l = r, 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(l <=> r, 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(l != r, 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(l < r, 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(l <= r, 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(l > r, 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(l >= r, 1, 0)) FROM p GROUP BY id

-- aggregate FILTER, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, count(*) FILTER (WHERE l = r) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, count(*) FILTER (WHERE l <=> r) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, count(*) FILTER (WHERE l != r) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, count(*) FILTER (WHERE l < r) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, count(*) FILTER (WHERE l <= r) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, count(*) FILTER (WHERE l > r) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, count(*) FILTER (WHERE l >= r) FROM p GROUP BY id

-- aggregate FILTER, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, count(*) FILTER (WHERE l = r) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, count(*) FILTER (WHERE l <=> r) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, count(*) FILTER (WHERE l != r) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, count(*) FILTER (WHERE l < r) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, count(*) FILTER (WHERE l <= r) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, count(*) FILTER (WHERE l > r) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, count(*) FILTER (WHERE l >= r) FROM p GROUP BY id

-- sort key, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id FROM p ORDER BY l = r, id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id FROM p ORDER BY l <=> r, id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id FROM p ORDER BY l != r, id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id FROM p ORDER BY l < r, id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id FROM p ORDER BY l <= r, id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id FROM p ORDER BY l > r, id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id FROM p ORDER BY l >= r, id

-- sort key, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id FROM p ORDER BY l = r, id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id FROM p ORDER BY l <=> r, id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id FROM p ORDER BY l != r, id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id FROM p ORDER BY l < r, id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id FROM p ORDER BY l <= r, id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id FROM p ORDER BY l > r, id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id FROM p ORDER BY l >= r, id

-- Generate, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, x FROM p LATERAL VIEW explode(array(l = r)) t AS x

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, x FROM p LATERAL VIEW explode(array(l <=> r)) t AS x

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, x FROM p LATERAL VIEW explode(array(l != r)) t AS x

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, x FROM p LATERAL VIEW explode(array(l < r)) t AS x

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, x FROM p LATERAL VIEW explode(array(l <= r)) t AS x

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, x FROM p LATERAL VIEW explode(array(l > r)) t AS x

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, x FROM p LATERAL VIEW explode(array(l >= r)) t AS x

-- Generate, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, x FROM p LATERAL VIEW explode(array(l = r)) t AS x

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, x FROM p LATERAL VIEW explode(array(l <=> r)) t AS x

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, x FROM p LATERAL VIEW explode(array(l != r)) t AS x

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, x FROM p LATERAL VIEW explode(array(l < r)) t AS x

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, x FROM p LATERAL VIEW explode(array(l <= r)) t AS x

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, x FROM p LATERAL VIEW explode(array(l > r)) t AS x

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, x FROM p LATERAL VIEW explode(array(l >= r)) t AS x

-- window function argument, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(l = r, 1, 0)) OVER (PARTITION BY id % 7) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(l <=> r, 1, 0)) OVER (PARTITION BY id % 7) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(l != r, 1, 0)) OVER (PARTITION BY id % 7) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(l < r, 1, 0)) OVER (PARTITION BY id % 7) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(l <= r, 1, 0)) OVER (PARTITION BY id % 7) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(l > r, 1, 0)) OVER (PARTITION BY id % 7) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(l >= r, 1, 0)) OVER (PARTITION BY id % 7) FROM p

-- window function argument, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(l = r, 1, 0)) OVER (PARTITION BY id % 7) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(l <=> r, 1, 0)) OVER (PARTITION BY id % 7) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(l != r, 1, 0)) OVER (PARTITION BY id % 7) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(l < r, 1, 0)) OVER (PARTITION BY id % 7) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(l <= r, 1, 0)) OVER (PARTITION BY id % 7) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(l > r, 1, 0)) OVER (PARTITION BY id % 7) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(l >= r, 1, 0)) OVER (PARTITION BY id % 7) FROM p

-- window partition key, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, count(*) OVER (PARTITION BY l = r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, count(*) OVER (PARTITION BY l <=> r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, count(*) OVER (PARTITION BY l != r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, count(*) OVER (PARTITION BY l < r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, count(*) OVER (PARTITION BY l <= r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, count(*) OVER (PARTITION BY l > r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, count(*) OVER (PARTITION BY l >= r) FROM p

-- window partition key, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, count(*) OVER (PARTITION BY l = r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, count(*) OVER (PARTITION BY l <=> r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, count(*) OVER (PARTITION BY l != r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, count(*) OVER (PARTITION BY l < r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, count(*) OVER (PARTITION BY l <= r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, count(*) OVER (PARTITION BY l > r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, count(*) OVER (PARTITION BY l >= r) FROM p
