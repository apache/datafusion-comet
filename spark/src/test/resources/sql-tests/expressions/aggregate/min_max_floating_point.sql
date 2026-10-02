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

statement
INSERT INTO mm_float VALUES
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

-- Without grouping. The extrema are NaN and infinities, so scan/merge order cannot change them.
query
SELECT max(d), min(d), max(-d), min(-d), max(f), min(f), max(-f), min(-f) FROM mm_float

-- Signed-zero ties need an explicit input order, not just an ORDER BY on the aggregate result.
-- Use windows for groups 2 and 3: neither file layout nor partial-aggregate merge order is fixed.
-- The native accumulator tests check first-value preservation by bits for ordinary and grouped
-- aggregates, including partial-state merges.
query
SELECT id, g,
  max(d) OVER w, min(d) OVER w, max(-d) OVER w, min(-d) OVER w,
  max(f) OVER w, min(f) OVER w, max(-f) OVER w, min(-f) OVER w
FROM mm_float WHERE g IN (2, 3)
WINDOW w AS (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)

-- Grouped, excluding the order-sensitive signed-zero groups. Groups 4 and 5 hold only an
-- infinity, group 6 only NaN and group 7 only NULL.
query
SELECT g, max(d), min(d), max(-d), min(-d), max(f), min(f), max(-f), min(-f)
FROM mm_float WHERE g NOT IN (2, 3) GROUP BY g ORDER BY g

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
