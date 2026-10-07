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

-- greatest and least order floats the way Spark's SQL ordering does: NaN is larger than every
-- other value, Infinity included, and -0.0 equals 0.0. An argument replaces the running result
-- only when it is strictly greater (or smaller), so of equal arguments the first one wins:
-- greatest(-0.0, 0.0) is -0.0 and greatest(0.0, -0.0) is 0.0.
--
-- `-a` of the NaN rows is a NaN with the sign bit set on every platform. Arithmetic produces
-- that NaN on x86-64, and IEEE 754 total order sorts it below -Infinity.

statement
CREATE TABLE gl_float(id INT, a DOUBLE, b DOUBLE, x FLOAT, y FLOAT) USING parquet

statement
INSERT INTO gl_float VALUES
  (1, 0.0D, double('-0.0'), float('0.0'), float('-0.0')),
  (2, double('-0.0'), 0.0D, float('-0.0'), float('0.0')),
  (3, double('NaN'), 1.0D, float('NaN'), float('1.0')),
  (4, 1.0D, double('NaN'), float('1.0'), float('NaN')),
  (5, double('Infinity'), double('NaN'), float('Infinity'), float('NaN')),
  (6, NULL, double('-0.0'), NULL, float('-0.0')),
  (7, NULL, NULL, NULL, NULL),
  (8, double('-Infinity'), -1.0D, float('-Infinity'), float('-1.0'))

-- Spark treats greatest and least as commutative when it compares expressions, so a query that
-- holds both greatest(a, b) and greatest(b, a) evaluates only one of them. Each query here uses
-- one argument order, and rows 1 and 2 put the two zeros both ways round.
query
SELECT id, greatest(a, b), least(a, b) FROM gl_float

query
SELECT id, greatest(x, y), least(x, y) FROM gl_float

-- A NaN with the sign bit set, on either side
query
SELECT id, greatest(-a, b), least(-a, b), greatest(-x, y), least(y, -x) FROM gl_float

query
SELECT id, greatest(b, -a), least(b, -a) FROM gl_float

-- Literals before, between and after the columns. `-0.0D` is a literal with the sign bit set.
query
SELECT id, greatest(a, 0.0D), least(-0.0D, a), greatest(-0.0D, a, 0.0D), least(b, 0.0D, a),
  greatest(a, double('NaN'), b)
FROM gl_float

query
SELECT id, greatest(0.0D, a), least(a, -0.0D) FROM gl_float

-- More than two arguments
query
SELECT id, greatest(a, b, -a), least(-b, a, b) FROM gl_float

query
SELECT id, greatest(-a, b, a), least(b, a, -b) FROM gl_float

-- Only literals, which the suite does not constant fold
query
SELECT greatest(-0.0D, 0.0D), least(0.0D, -0.0D), greatest(double('NaN'), double('Infinity')),
  least(double('NaN'), double('Infinity')), greatest(-0.0F, 0.0F), least(0.0F, -0.0F),
  greatest(CAST(NULL AS DOUBLE), -0.0D, 0.0D)

query
SELECT greatest(0.0D, -0.0D), least(-0.0D, 0.0D)

-- Arrays and structs compare their float leaves the same way
statement
CREATE TABLE gl_float_nested(id INT, a ARRAY<DOUBLE>, b ARRAY<DOUBLE>, s STRUCT<v: DOUBLE>,
  t STRUCT<v: DOUBLE>) USING parquet

statement
INSERT INTO gl_float_nested VALUES
  (1, array(0.0D), array(double('-0.0')), named_struct('v', 0.0D), named_struct('v', double('-0.0'))),
  (2, array(double('-0.0')), array(0.0D), named_struct('v', double('-0.0')), named_struct('v', 0.0D)),
  (3, array(double('NaN')), array(double('Infinity')), named_struct('v', double('NaN')),
    named_struct('v', double('Infinity'))),
  (4, array(1.0D, NULL), array(1.0D, 2.0D), named_struct('v', NULL), named_struct('v', 1.0D)),
  (5, NULL, array(double('-0.0')), NULL, named_struct('v', double('-0.0')))

query
SELECT id, greatest(a, b), least(a, b), greatest(s, t), least(s, t) FROM gl_float_nested

query
SELECT id, greatest(transform(a, e -> -e), b), least(transform(a, e -> -e), b)
FROM gl_float_nested
