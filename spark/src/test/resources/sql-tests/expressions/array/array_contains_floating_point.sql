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

-- array_contains compares floats the way Spark's generated code does: -0.0 equals 0.0, and every
-- NaN equals every other NaN, at any depth of an array or struct element. Flat FLOAT and DOUBLE
-- arrays run natively; nested float elements go through the codegen dispatcher. The result is true when
-- an element matches, null when none does and the array holds a null element, and false
-- otherwise; a null array or value gives null.
--
-- `-d` flips the sign bit, so `-d` of the NaN row is a NaN with the sign bit set on every
-- platform. That is the NaN that arithmetic produces on x86-64.

statement
CREATE TABLE ac_float(id INT, d DOUBLE, f FLOAT) USING parquet

statement
INSERT INTO ac_float VALUES
  (1, 0.0D, float('0.0')),
  (2, double('-0.0'), float('-0.0')),
  (3, double('NaN'), float('NaN')),
  (4, 1.0D, float('1.0')),
  (5, NULL, NULL)

-- A constant value: either zero finds both zeros, and a NaN finds both NaNs
query expect_native(array_contains)
SELECT id, array_contains(array(d, -d), 0.0D), array_contains(array(d, -d), -0.0D),
  array_contains(array(-d, 1.0D), double('NaN')), array_contains(array(d, 1.0D), 2.0D)
FROM ac_float

query expect_native(array_contains)
SELECT id, array_contains(array(f, -f), 0.0F), array_contains(array(-f, 2.0F), float('NaN'))
FROM ac_float

-- A value per row
query expect_native(array_contains)
SELECT id, array_contains(array(0.0D, 1.0D), d), array_contains(array(double('NaN'), 1.0D), -d),
  array_contains(array(float('-0.0'), 2.0F), f)
FROM ac_float

-- Null elements: true when another element matches, null when nothing matches
query expect_native(array_contains)
SELECT id, array_contains(array(d, NULL), 0.0D), array_contains(array(2.0D, NULL), d),
  array_contains(array(NULL, -d), double('NaN'))
FROM ac_float

-- A null array or a null value gives null; an empty array gives false
query expect_native(array_contains)
SELECT id, array_contains(IF(d IS NULL, NULL, array(d, -d)), 0.0D),
  array_contains(array(d, -d), CAST(NULL AS DOUBLE)), array_contains(array(), d)
FROM ac_float

-- Arrays as elements compare their floats the same way. Nested float elements stay on the codegen
-- dispatcher.
query expect_dispatch(array_contains)
SELECT id, array_contains(array(array(-d), array(1.0D)), array(d)),
  array_contains(array(array(d, 1.0D), array(2.0D)), array(-d, 1.0D)),
  array_contains(array(array(d), NULL), array(2.0D))
FROM ac_float

-- Struct elements too
query expect_dispatch(array_contains)
SELECT id, array_contains(array(named_struct('x', -d, 'y', 1), named_struct('x', 2.0D, 'y', 1)),
  named_struct('x', d, 'y', 1))
FROM ac_float

-- Only literals, which the suite does not constant fold. `-0.0D` is a literal with the sign bit
-- set, while `double('NaN')` is a cast.
query expect_native(array_contains)
SELECT array_contains(array(0.0D, 1.0D), -0.0D), array_contains(array(-0.0D), 0.0D),
  array_contains(array(double('NaN'), 1.0D), double('NaN')),
  array_contains(array(1.0D, NULL), -0.0D)
