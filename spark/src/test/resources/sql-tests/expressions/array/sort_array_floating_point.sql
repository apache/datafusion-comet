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

-- sort_array orders floats the way Spark does, which depends on whether the elements can be null.
-- When they can, or when sorting descending, Spark compares in its SQL ordering with a stable
-- sort: -0.0 and 0.0 tie and keep their order, and every NaN ties with every other NaN and sorts
-- after every other value. When an ascending array's elements cannot be null, Spark's generated
-- code calls java.util.Arrays.sort instead, which puts -0.0 before 0.0.
--
-- `-d` flips the sign bit, so `-d` of the NaN row is a NaN with the sign bit set on every
-- platform. That is the NaN that arithmetic produces on x86-64.

statement
CREATE TABLE sa_float(id INT, d DOUBLE, f FLOAT) USING parquet

statement
INSERT INTO sa_float VALUES
  (1, 0.0D, float('0.0')),
  (2, double('-0.0'), float('-0.0')),
  (3, double('NaN'), float('NaN')),
  (4, 1.0D, float('1.0')),
  (5, NULL, NULL),
  (6, double('-Infinity'), float('-Infinity'))

-- Elements that can be null, with both zeros and both NaNs in both orders
query
SELECT id, sort_array(array(d, -d, 1.0D)), sort_array(array(-d, d, 1.0D)),
  sort_array(array(d, -d, 1.0D), false), sort_array(array(-d, d, NULL), false)
FROM sa_float

query
SELECT id, sort_array(array(f, -f, 1.0F, NULL)), sort_array(array(-f, f), false)
FROM sa_float

-- A null array
query
SELECT id, sort_array(IF(d IS NULL, NULL, array(d, -d, 1.0D))) FROM sa_float

-- Elements that cannot be null
query
SELECT id, sort_array(array(coalesce(d, 0.0D), coalesce(-d, 0.0D), 1.0D)),
  sort_array(array(coalesce(-d, 0.0D), coalesce(d, 0.0D)), false),
  sort_array(array(coalesce(f, 0.0F), coalesce(-f, 0.0F)))
FROM sa_float

query
SELECT sort_array(array(0.0D, -0.0D, 1.0D)), sort_array(array(0.0F, -0.0F)),
  sort_array(array(0.0D, -0.0D), false)

-- Arrays and structs as elements compare their floats in Spark's order, with a stable sort
query
SELECT id, sort_array(array(array(d), array(-d), array(1.0D))),
  sort_array(array(named_struct('x', d), named_struct('x', -d)), false)
FROM sa_float
