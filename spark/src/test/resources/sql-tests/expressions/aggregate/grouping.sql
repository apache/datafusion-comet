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

-- Tests for the grouping() and grouping_id() indicator functions used with
-- ROLLUP, CUBE, and GROUPING SETS. These are Unevaluable expressions that the
-- analyzer rewrites into arithmetic over the virtual spark_grouping_id column,
-- so the whole query (Expand + aggregate + projection) should run natively.

statement
CREATE TABLE test_grouping(name string, dept string, age int) USING parquet

statement
INSERT INTO test_grouping VALUES
  ('Alice', 'eng',   30),
  ('Bob',   'eng',   40),
  ('Carol', 'sales', 25),
  ('Dave',  'sales', 35),
  ('Eve',   NULL,    50),
  (NULL,    'eng',   20),
  (NULL,    NULL,    NULL)

-- grouping(col) with CUBE
query
SELECT name, grouping(name), sum(age)
FROM test_grouping
GROUP BY cube(name)
ORDER BY grouping(name), name

-- grouping(col) with ROLLUP
query
SELECT dept, grouping(dept), sum(age)
FROM test_grouping
GROUP BY rollup(dept)
ORDER BY grouping(dept), dept

-- grouping over two columns with CUBE
query
SELECT name, dept, grouping(name), grouping(dept), sum(age)
FROM test_grouping
GROUP BY cube(name, dept)
ORDER BY grouping(name), grouping(dept), name, dept

-- grouping with GROUPING SETS
query
SELECT name, dept, grouping(name), grouping(dept), count(*)
FROM test_grouping
GROUP BY GROUPING SETS ((name), (dept), ())
ORDER BY grouping(name), grouping(dept), name, dept

-- grouping_id() with CUBE
query
SELECT name, dept, grouping_id(), sum(age)
FROM test_grouping
GROUP BY cube(name, dept)
ORDER BY grouping_id(), name, dept

-- grouping_id(cols) with explicit columns
query
SELECT name, dept, grouping_id(name, dept), sum(age)
FROM test_grouping
GROUP BY cube(name, dept)
ORDER BY grouping_id(name, dept), name, dept

-- grouping and grouping_id together with ROLLUP
query
SELECT name, dept, grouping(name), grouping(dept), grouping_id(), sum(age)
FROM test_grouping
GROUP BY rollup(name, dept)
ORDER BY grouping_id(), name, dept

-- grouping used in filter (HAVING) and select
query
SELECT dept, grouping(dept), sum(age)
FROM test_grouping
GROUP BY rollup(dept)
HAVING grouping(dept) = 1

-- grouping_id used to distinguish aggregation levels
query
SELECT name, dept, sum(age)
FROM test_grouping
GROUP BY cube(name, dept)
HAVING grouping_id() = 0
ORDER BY name, dept
