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

-- array_remove compares floats the way Spark's generated code does: -0.0 equals 0.0, and every
-- NaN equals every other NaN, at any depth of an array or struct element. Null elements stay, and
-- a null array or value gives null.
--
-- `-d` flips the sign bit, so `-d` of the NaN row is a NaN with the sign bit set on every
-- platform. That is the NaN that arithmetic produces on x86-64.

statement
CREATE TABLE ar_float(id INT, d DOUBLE, f FLOAT) USING parquet

statement
INSERT INTO ar_float VALUES
  (1, 0.0D, float('0.0')),
  (2, double('-0.0'), float('-0.0')),
  (3, double('NaN'), float('NaN')),
  (4, 1.0D, float('1.0')),
  (5, NULL, NULL)

-- Removing a zero removes both zeros, and removing a NaN removes both NaNs
query
SELECT id, array_remove(array(d, -d, 1.0D, NULL), 0.0D), array_remove(array(d, -d, 1.0D), d),
  array_remove(array(d, -d, 1.0D), -d)
FROM ar_float

query
SELECT id, array_remove(array(f, -f, 1.0F, NULL), 0.0F), array_remove(array(f, -f), -f)
FROM ar_float

-- A literal array with a value from a column, and a null array
query
SELECT id, array_remove(array(0.0D, -0.0D, double('NaN'), 1.0D), d),
  array_remove(IF(d IS NULL, NULL, array(d, -d, 1.0D)), 0.0D)
FROM ar_float

-- Arrays as elements compare their floats the same way. Struct elements fall back to Spark
-- (#1307).
query
SELECT id, array_remove(array(array(d), array(-d), array(1.0D)), array(d)),
  array_remove(array(array(-d, 1.0D), array(d, 1.0D)), array(-d, 1.0D))
FROM ar_float

-- Only literals, which the suite does not constant fold. `-0.0D` is a literal with the sign bit
-- set, while `double('NaN')` is a cast.
query
SELECT array_remove(array(0.0D, -0.0D, 1.0D), -0.0D), array_remove(array(-0.0D, 0.0D), 0.0D),
  array_remove(array(double('NaN'), 1.0D), double('NaN'))
