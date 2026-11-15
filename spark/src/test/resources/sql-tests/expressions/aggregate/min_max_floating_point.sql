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

-- min and max order floats the way Spark's SQL ordering does: NaN is larger than every other
-- value, Infinity included, and -0.0 equals 0.0. Spark's Max keeps greatest(buffer, input), so
-- of equal values the first one seen is returned: max over -0.0 then 0.0 returns -0.0. Min is
-- the same with least.
--
-- `-d` of the NaN rows is a NaN with the sign bit set on every platform. Arithmetic produces
-- that NaN on x86-64, and IEEE 754 total order sorts it below -Infinity.

-- Strict floating-point mode no longer needs to fall back for min and max.
-- ConfigMatrix: spark.comet.exec.strictFloatingPoint=false,true

statement
CREATE TABLE mm_float(id INT, g INT, d DOUBLE, f FLOAT) USING parquet

-- Which zero max and min return depends on the order they read the rows in. COALESCE(1) writes
-- the table as a single file, in id order, which both Spark and Comet read in one task, so of two
-- equal values both see the one with the smaller id first.
statement
INSERT INTO mm_float SELECT /*+ COALESCE(1) */ * FROM VALUES
  (1, 1, 1.0D, float('1.0')),
  (2, 1, double('NaN'), float('NaN')),
  (3, 1, -1.0D, float('-1.0')),
  (4, 2, double('-0.0'), float('-0.0')),
  (5, 2, 0.0D, float('0.0')),
  (6, 3, 0.0D, float('0.0')),
  (7, 3, double('-0.0'), float('-0.0')),
  (8, 4, double('-Infinity'), float('-Infinity')),
  (9, 5, double('Infinity'), float('Infinity')),
  (10, 6, double('NaN'), float('NaN')),
  (11, 7, NULL, NULL),
  (12, 8, double('Infinity'), float('Infinity')),
  (13, 8, double('NaN'), float('NaN'))
  AS t(id, g, d, f)

-- Without grouping
query
SELECT max(d), min(d), max(-d), min(-d), max(f), min(f), max(-f), min(-f) FROM mm_float

-- Of equal values the first one seen wins. Group 2 holds -0.0 and then 0.0, and group 3 the same
-- zeros the other way round. Each group is a query of its own, because over both groups the first
-- and the last zero have the same sign.
query
SELECT max(d), min(d), max(-d), min(-d), max(f), min(f), max(-f), min(-f) FROM mm_float WHERE g = 2

query
SELECT max(d), min(d), max(-d), min(-d), max(f), min(f), max(-f), min(-f) FROM mm_float WHERE g = 3

-- Grouped. Groups 2 and 3 hold both zeros, groups 4 and 5 only an infinity, group 6 only NaN and
-- group 7 only NULL.
query
SELECT g, max(d), min(d), max(-d), min(-d), max(f), min(f), max(-f), min(-f)
FROM mm_float GROUP BY g ORDER BY g

-- Window frames: growing, sliding and the whole partition. Each frame is ordered by id, so the
-- first of two equal values is well defined. The sliding frames use FOLLOWING offsets because the
-- suite turns off constant folding, which leaves `1 PRECEDING` as an expression Spark's own window
-- operator cannot evaluate.
query
SELECT id,
  max(d) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
  min(-d) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
  max(-d) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING),
  min(d) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING),
  max(-f) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING),
  min(f) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING),
  max(f) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
  min(-f) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM mm_float
