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

-- Comparisons follow Spark's SQL ordering for floats (SQLOrderingUtil.compareDoubles): -0.0
-- equals 0.0, every NaN equals every other NaN, and NaN sorts above every other value. This
-- must hold in every operator that evaluates a comparison, not only in Project and Filter.
--
-- `-d` flips the sign bit, so `-d` of the NaN row is a NaN with the sign bit set on every
-- platform. That is the NaN that arithmetic produces on x86-64, and Arrow's total order sorts
-- it below -Infinity.
--
-- A scan pushes its filters into the Parquet reader, which with row-level pushdown drops rows
-- itself, before Spark's Filter above the scan sees them.
-- ConfigMatrix: spark.comet.parquet.rowFilterPushdown.enabled=false,true

statement
CREATE TABLE float_cmp(id INT, d DOUBLE, f FLOAT) USING parquet

statement
INSERT INTO float_cmp VALUES
  (1, 0.0D, float('0.0')),
  (2, double('-0.0'), float('-0.0')),
  (3, double('NaN'), float('NaN')),
  (4, 1.0D, float('1.0')),
  (5, -1.0D, float('-1.0')),
  (6, double('Infinity'), float('Infinity')),
  (7, NULL, NULL)

-- Project. `IS DISTINCT FROM` is `NOT (a <=> b)`, which Comet plans as one native operator.
query
SELECT id, -d = d, -d <=> d, -d < d, -d <= d, -d > d, -d >= d, -d != d, -d IS DISTINCT FROM d
FROM float_cmp

query
SELECT id, -f = f, -f <=> f, -f < f, -f <= f, -f > f, -f >= f, -f != f, -f IS DISTINCT FROM f
FROM float_cmp

-- Comparisons against a constant on either side. `-0.0D` and `-0.0F` are literals with the sign
-- bit set, while `double('NaN')` is a cast, because the suite turns off constant folding.
query
SELECT id, -d > 0.0D, -d >= -0.0D, -0.0D = d, -d = double('NaN'), double('NaN') <= -d,
  -d < double('-0.0'), -d IS DISTINCT FROM double('NaN'), -0.0D IS DISTINCT FROM d,
  -f > 0.0F, -0.0F <=> f, -f = float('NaN'), float('-0.0') >= -f
FROM float_cmp

-- Filter
query
SELECT id FROM float_cmp WHERE -d > 0.0D

query
SELECT id FROM float_cmp WHERE -f >= f

-- A stored column compared with a constant, the filter shape that Parquet pruning reads. The
-- scan still returns the -0.0 and NaN rows that Spark's ordering keeps.
query
SELECT id FROM float_cmp WHERE d >= 0.0D

query
SELECT id FROM float_cmp WHERE f <= -0.0F

query
SELECT id FROM float_cmp WHERE d = double('NaN')

query
SELECT id FROM float_cmp WHERE d > double('Infinity')

-- Aggregate argument
query
SELECT sum(if(-d > 0.0D, 1, 0)), sum(if(-d = d, 1, 0)), sum(if(-f < f, 1, 0)) FROM float_cmp

-- Aggregate FILTER clause
query
SELECT count(*) FILTER (WHERE -d >= d), count(*) FILTER (WHERE -f <=> f),
  count(*) FILTER (WHERE -d IS DISTINCT FROM d)
FROM float_cmp

-- Grouping by a comparison
query
SELECT -d > 0.0D AS k, count(*) FROM float_cmp GROUP BY -d > 0.0D

-- Join conditions, for each join strategy
query
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM float_cmp a JOIN float_cmp b
  ON a.id = b.id AND -a.d >= b.d

query
SELECT /*+ SHUFFLE_HASH(b) */ a.id, b.id FROM float_cmp a JOIN float_cmp b
  ON a.id = b.id AND -a.d >= b.d

query
SELECT /*+ MERGE(b) */ a.id, b.id FROM float_cmp a JOIN float_cmp b
  ON a.id = b.id AND -a.f >= b.f

query
SELECT /*+ BROADCAST(b) */ a.id, b.id FROM float_cmp a JOIN float_cmp b
  ON -a.d > b.d WHERE a.id = 3

-- Sort key
query
SELECT id FROM float_cmp ORDER BY -d > 0.0D, id

-- Generate
query
SELECT id, x FROM float_cmp LATERAL VIEW explode(array(-d > 0.0D, -d = d)) v AS x

-- Window function argument, which Spark evaluates in a Project below the Window
query
SELECT id, sum(if(-d > 0.0D, 1, 0)) OVER (ORDER BY id) FROM float_cmp

-- Arrays and structs compare their float leaves the same way, for every operator
statement
CREATE TABLE float_cmp_nested(
  id INT,
  a ARRAY<DOUBLE>, b ARRAY<DOUBLE>,
  s STRUCT<v: DOUBLE>, u STRUCT<v: DOUBLE>,
  x ARRAY<FLOAT>, y ARRAY<FLOAT>) USING parquet

statement
INSERT INTO float_cmp_nested VALUES
  (1, array(double('-0.0')), array(0.0D),
    named_struct('v', double('-0.0')), named_struct('v', 0.0D),
    array(float('-0.0')), array(float('0.0'))),
  (2, array(double('NaN')), array(double('Infinity')),
    named_struct('v', double('NaN')), named_struct('v', double('Infinity')),
    array(float('NaN')), array(float('Infinity'))),
  (3, array(1.0D, NULL), array(1.0D, 2.0D),
    named_struct('v', NULL), named_struct('v', 1.0D),
    array(float('1.0'), NULL), array(float('1.0'), float('2.0'))),
  (4, array(1.0D), array(1.0D, double('-0.0')),
    named_struct('v', 1.0D), named_struct('v', 1.0D),
    array(), array(float('-0.0'))),
  (5, NULL, array(0.0D), NULL, named_struct('v', 0.0D), NULL, array(float('0.0')))

query
SELECT id, a = b, a <=> b, a < b, a <= b, a > b, a >= b, a IS DISTINCT FROM b
FROM float_cmp_nested

query
SELECT id, s = u, s <=> u, s < u, s <= u, s > u, s >= u, s IS DISTINCT FROM u
FROM float_cmp_nested

query
SELECT id, x = y, x <=> y, x < y, x <= y, x > y, x >= y, x IS DISTINCT FROM y
FROM float_cmp_nested

-- Negating the elements gives the NaN in row 2 the sign bit
query
SELECT id, transform(a, e -> -e) < b, transform(a, e -> -e) <=> a,
  transform(a, e -> -e) IS DISTINCT FROM a
FROM float_cmp_nested

-- Nested comparisons outside Project and Filter
query
SELECT sum(if(a < b, 1, 0)), count(*) FILTER (WHERE s <=> u) FROM float_cmp_nested

-- Floats two levels deep, in an array of structs and in a struct of arrays
statement
CREATE TABLE float_cmp_deep(
  id INT,
  a ARRAY<STRUCT<v: DOUBLE>>, b ARRAY<STRUCT<v: DOUBLE>>,
  s STRUCT<a: ARRAY<DOUBLE>>, u STRUCT<a: ARRAY<DOUBLE>>) USING parquet

statement
INSERT INTO float_cmp_deep VALUES
  (1, array(named_struct('v', double('-0.0'))), array(named_struct('v', 0.0D)),
    named_struct('a', array(double('-0.0'))), named_struct('a', array(0.0D))),
  (2, array(named_struct('v', double('NaN'))), array(named_struct('v', double('Infinity'))),
    named_struct('a', array(double('NaN'))), named_struct('a', array(double('Infinity')))),
  (3, array(named_struct('v', 1.0D), named_struct('v', NULL)),
    array(named_struct('v', 1.0D), named_struct('v', 2.0D)),
    named_struct('a', array(1.0D, NULL)), named_struct('a', array(1.0D, 2.0D))),
  (4, array(named_struct('v', 1.0D)),
    array(named_struct('v', 1.0D), named_struct('v', double('-0.0'))),
    named_struct('a', NULL), named_struct('a', array(double('-0.0')))),
  (5, NULL, array(named_struct('v', 0.0D)), NULL, named_struct('a', array(0.0D)))

query
SELECT id, a = b, a <=> b, a < b, a <= b, a > b, a >= b, a IS DISTINCT FROM b
FROM float_cmp_deep

query
SELECT id, s = u, s <=> u, s < u, s <= u, s > u, s >= u, s IS DISTINCT FROM u
FROM float_cmp_deep

-- Negating the leaves gives the NaN in row 2 the sign bit
query
SELECT id, transform(a, e -> named_struct('v', -e.v)) < b,
  transform(a, e -> named_struct('v', -e.v)) <=> a,
  named_struct('a', transform(s.a, e -> -e)) <=> s
FROM float_cmp_deep

query
SELECT sum(if(a < b, 1, 0)), count(*) FILTER (WHERE s <=> u) FROM float_cmp_deep
