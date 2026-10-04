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

-- Sort and window keys that nest floats in arrays and structs follow Spark's ordering at every
-- depth: -0.0 equals 0.0, every NaN equals every other NaN, and NaN sorts above every other
-- value. Only the comparison keys are normalized, so returned values keep their bits.
--
-- The keys use IF(s, -d, d): rows 1 and 2 hold -0.0 and 0.0, and rows 4 and 5 a canonical NaN
-- and a NaN with the sign bit set, the NaN that arithmetic produces on x86-64, which Arrow's
-- total order sorts below -Infinity. Each ORDER BY ends with a unique tiebreaker, so peers come
-- out in the tiebreaker's order.

-- Strict floating-point mode declines these keys, because their types can hold a null element or
-- field: see nested_float_order_keys_strict.sql.

statement
CREATE TABLE nested_float_keys(id INT, g INT, d DOUBLE, f FLOAT, s BOOLEAN) USING parquet

statement
INSERT INTO nested_float_keys VALUES
  (1, 1, 0.0D, float('0.0'), true),
  (2, 1, 0.0D, float('0.0'), false),
  (3, 1, 1.0D, float('1.0'), false),
  (4, 2, double('NaN'), float('NaN'), false),
  (5, 2, double('NaN'), float('NaN'), true),
  (6, 2, double('Infinity'), float('Infinity'), false),
  (7, 2, -1.0D, float('-1.0'), false),
  (8, 1, NULL, NULL, false),
  (9, 2, double('-Infinity'), float('-Infinity'), false)

-- Sort. Row 8's keys hold a null element, which Spark orders below every other value whatever
-- the key's null order. Arrow ties the order of nested nulls to NULLS FIRST or LAST, so these
-- queries keep the default null order, where the two agree (#6476).
query
SELECT id FROM nested_float_keys ORDER BY array(IF(s, -d, d)), id DESC

query
SELECT id FROM nested_float_keys ORDER BY array(IF(s, -f, f)) DESC, id

query
SELECT id FROM nested_float_keys ORDER BY named_struct('x', IF(s, -d, d)) DESC, id DESC

query
SELECT id FROM nested_float_keys ORDER BY named_struct('x', IF(s, -f, f)), id

-- Nested more deeply, and more than one nested key
query
SELECT id FROM nested_float_keys
ORDER BY array(named_struct('x', IF(s, -d, d))), named_struct('a', array(IF(s, -f, f))) DESC, id

-- Returning the keys as well. CometExpressionSuite checks that their bits come back unchanged.
query
SELECT id, array(IF(s, -d, d)) AS k, named_struct('x', IF(s, -f, f)) AS t
FROM nested_float_keys ORDER BY k, id DESC

-- TopK
query
SELECT id FROM nested_float_keys ORDER BY array(IF(s, -d, d)) DESC, id LIMIT 4

query
SELECT id FROM nested_float_keys ORDER BY named_struct('x', IF(s, -f, f)), id DESC LIMIT 4

-- Window order keys: peers share a rank, and the default RANGE frame of a running sum spans
-- all of them. The running sums leave out row 8: DataFusion finds a RANGE frame's end by ordering
-- a null element above every value, while the sort puts it first, so the frame of every row
-- after it would run to the end of the partition, with or without floats (#6477).
query
SELECT id,
  RANK() OVER (ORDER BY array(IF(s, -d, d))) AS r,
  DENSE_RANK() OVER (PARTITION BY g ORDER BY named_struct('x', IF(s, -f, f)) DESC) AS dr
FROM nested_float_keys

-- Comet declines to sort on a lone struct column, which Arrow cannot sort, so every struct key
-- comes with a second key or a partition.
query
SELECT id,
  RANK() OVER (PARTITION BY g ORDER BY named_struct('x', IF(s, -d, d))) AS r,
  SUM(id) OVER (ORDER BY array(IF(s, -d, d))) AS running,
  SUM(id) OVER (PARTITION BY g ORDER BY array(IF(s, -f, f)) DESC) AS by_group
FROM nested_float_keys WHERE id <> 8

-- A rank limit keeps every peer of the last rank it admits
query
SELECT id FROM (
  SELECT id, RANK() OVER (ORDER BY array(IF(s, -d, d))) AS r FROM nested_float_keys
) WHERE r <= 3

query
SELECT id FROM (
  SELECT id, DENSE_RANK() OVER (PARTITION BY g ORDER BY named_struct('x', IF(s, -f, f)) DESC) AS r
  FROM nested_float_keys
) WHERE r <= 1
