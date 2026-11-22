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

-- The set operations on arrays of floats, over the edge values (see expressions.sql). Every 4.2+
-- release normalizes signed zeros in these functions (array_set_signed_zero.sql), so native
-- distinct and union, and the except and intersect kernels with allowIncompatible, must run
-- natively and agree with Spark. Before 4.2 the signed-zero routing differs by release
-- (array_set_signed_zero_spark_3.sql, array_set_signed_zero_spark_4_0_4_1.sql), so the sweep does
-- not cover those releases.
-- The sweep tracks #6385.
--
-- MinSparkVersion: 4.2
-- Config: spark.comet.expression.ArrayExcept.allowIncompatible=true
-- Config: spark.comet.expression.ArrayIntersect.allowIncompatible=true

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

-- array_distinct, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array_distinct(array(l, r)) FROM p

-- array_distinct, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array_distinct(array(l, r)) FROM p

-- array_union, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array_union(array(l), array(r)) FROM p

-- array_union, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array_union(array(l), array(r)) FROM p

-- array_intersect, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array_intersect(array(l), array(r)) FROM p

-- array_intersect, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array_intersect(array(l), array(r)) FROM p

-- array_except, DOUBLE
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_double)
SELECT id, array_except(array(l), array(r)) FROM p

-- array_except, FLOAT
query
WITH p AS (SELECT id, IF(lneg, -l, l) AS l, IF(rneg, -r, r) AS r FROM fs_pair_float)
SELECT id, array_except(array(l), array(r)) FROM p
