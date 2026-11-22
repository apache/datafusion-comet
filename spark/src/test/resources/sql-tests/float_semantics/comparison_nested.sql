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

-- Comparisons of arrays and structs that hold one float, over every ordered pair of the edge
-- values (see comparison.sql). Spark compares the elements and fields with SQL ordering.


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

-- array operands, Project, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array(l) = array(r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array(l) <=> array(r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array(l) != array(r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array(l) < array(r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array(l) <= array(r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array(l) > array(r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array(l) >= array(r) FROM p

-- array operands, Project, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array(l) = array(r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array(l) <=> array(r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array(l) != array(r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array(l) < array(r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array(l) <= array(r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array(l) > array(r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array(l) >= array(r) FROM p

-- array operands, aggregate argument, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(array(l) = array(r), 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(array(l) <=> array(r), 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(array(l) != array(r), 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(array(l) < array(r), 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(array(l) <= array(r), 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(array(l) > array(r), 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(array(l) >= array(r), 1, 0)) FROM p GROUP BY id

-- array operands, aggregate argument, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(array(l) = array(r), 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(array(l) <=> array(r), 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(array(l) != array(r), 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(array(l) < array(r), 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(array(l) <= array(r), 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(array(l) > array(r), 1, 0)) FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(array(l) >= array(r), 1, 0)) FROM p GROUP BY id

-- struct operands, Project, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, named_struct('x', l) = named_struct('x', r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, named_struct('x', l) <=> named_struct('x', r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, named_struct('x', l) != named_struct('x', r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, named_struct('x', l) < named_struct('x', r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, named_struct('x', l) <= named_struct('x', r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, named_struct('x', l) > named_struct('x', r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, named_struct('x', l) >= named_struct('x', r) FROM p

-- struct operands, Project, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, named_struct('x', l) = named_struct('x', r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, named_struct('x', l) <=> named_struct('x', r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, named_struct('x', l) != named_struct('x', r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, named_struct('x', l) < named_struct('x', r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, named_struct('x', l) <= named_struct('x', r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, named_struct('x', l) > named_struct('x', r) FROM p

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, named_struct('x', l) >= named_struct('x', r) FROM p

-- struct operands, aggregate argument, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(named_struct('x', l) = named_struct('x', r), 1, 0))
FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(named_struct('x', l) <=> named_struct('x', r), 1, 0))
FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(named_struct('x', l) != named_struct('x', r), 1, 0))
FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(named_struct('x', l) < named_struct('x', r), 1, 0))
FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(named_struct('x', l) <= named_struct('x', r), 1, 0))
FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(named_struct('x', l) > named_struct('x', r), 1, 0))
FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, max(IF(named_struct('x', l) >= named_struct('x', r), 1, 0))
FROM p GROUP BY id

-- struct operands, aggregate argument, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(named_struct('x', l) = named_struct('x', r), 1, 0))
FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(named_struct('x', l) <=> named_struct('x', r), 1, 0))
FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(named_struct('x', l) != named_struct('x', r), 1, 0))
FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(named_struct('x', l) < named_struct('x', r), 1, 0))
FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(named_struct('x', l) <= named_struct('x', r), 1, 0))
FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(named_struct('x', l) > named_struct('x', r), 1, 0))
FROM p GROUP BY id

query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, max(IF(named_struct('x', l) >= named_struct('x', r), 1, 0))
FROM p GROUP BY id
