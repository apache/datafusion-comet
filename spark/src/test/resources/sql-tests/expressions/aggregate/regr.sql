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

-- SQL standard linear-regression aggregate functions: regr_count, regr_avgx,
-- regr_avgy, regr_sxx, regr_syy, regr_sxy, regr_slope, regr_intercept, regr_r2.
-- All functions take (y, x) and operate only on rows where BOTH y and x are non-null.

statement
CREATE TABLE test_regr(y double, x double, grp string) USING parquet

statement
INSERT INTO test_regr VALUES
  (1.0,  2.0, 'a'),
  (3.0,  4.0, 'a'),
  (5.0,  6.0, 'a'),
  (7.0,  8.0, 'a'),
  (9.0, 10.0, 'a'),
  (2.0,  1.0, 'b'),
  (4.0,  3.0, 'b'),
  (6.0,  5.0, 'b'),
  (NULL,  3.0, 'b'),
  (4.0,  NULL, 'b'),
  (NULL, NULL, 'b')

-- regr_count: counts rows where both y and x are non-null
query
SELECT regr_count(y, x) FROM test_regr

query
SELECT grp, regr_count(y, x) FROM test_regr GROUP BY grp ORDER BY grp

-- regr_avgx: average of x over non-null (y, x) pairs
query tolerance=1e-6
SELECT regr_avgx(y, x) FROM test_regr

query tolerance=1e-6
SELECT grp, regr_avgx(y, x) FROM test_regr GROUP BY grp ORDER BY grp

-- regr_avgy: average of y over non-null (y, x) pairs
query tolerance=1e-6
SELECT regr_avgy(y, x) FROM test_regr

query tolerance=1e-6
SELECT grp, regr_avgy(y, x) FROM test_regr GROUP BY grp ORDER BY grp

-- regr_sxx: sum of squares of deviation of x from mean(x)
query tolerance=1e-6
SELECT regr_sxx(y, x) FROM test_regr

query tolerance=1e-6
SELECT grp, regr_sxx(y, x) FROM test_regr GROUP BY grp ORDER BY grp

-- regr_syy: sum of squares of deviation of y from mean(y)
query tolerance=1e-6
SELECT regr_syy(y, x) FROM test_regr

query tolerance=1e-6
SELECT grp, regr_syy(y, x) FROM test_regr GROUP BY grp ORDER BY grp

-- regr_sxy: sum of products of deviations of x and y from their means
query tolerance=1e-6
SELECT regr_sxy(y, x) FROM test_regr

query tolerance=1e-6
SELECT grp, regr_sxy(y, x) FROM test_regr GROUP BY grp ORDER BY grp

-- regr_slope: slope of the least-squares regression line
query tolerance=1e-6
SELECT regr_slope(y, x) FROM test_regr

query tolerance=1e-6
SELECT grp, regr_slope(y, x) FROM test_regr GROUP BY grp ORDER BY grp

-- regr_intercept: y-intercept of the least-squares regression line
query tolerance=1e-6
SELECT regr_intercept(y, x) FROM test_regr

query tolerance=1e-6
SELECT grp, regr_intercept(y, x) FROM test_regr GROUP BY grp ORDER BY grp

-- regr_r2: square of the correlation coefficient (coefficient of determination)
query tolerance=1e-6
SELECT regr_r2(y, x) FROM test_regr

query tolerance=1e-6
SELECT grp, regr_r2(y, x) FROM test_regr GROUP BY grp ORDER BY grp

-- literal dependent variable mixed with a column independent variable
query tolerance=1e-6
SELECT regr_slope(2.0, x), regr_intercept(2.0, x), regr_sxy(2.0, x) FROM test_regr

-- literal independent variable mixed with a column dependent variable
query tolerance=1e-6
SELECT regr_slope(y, 3.0), regr_sxx(y, 3.0), regr_r2(y, 3.0) FROM test_regr

-- edge case: all-NULL input returns NULL for all functions
statement
CREATE TABLE test_regr_all_null(y double, x double) USING parquet

statement
INSERT INTO test_regr_all_null VALUES (NULL, NULL), (NULL, NULL)

query
SELECT regr_count(y, x), regr_avgx(y, x), regr_avgy(y, x) FROM test_regr_all_null

-- regr_sxx/syy/sxy return NULL when there are no non-null pairs
query tolerance=1e-6
SELECT regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x) FROM test_regr_all_null

-- regr_slope/intercept/r2 return NULL when there are no non-null pairs
query tolerance=1e-6
SELECT regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x) FROM test_regr_all_null

-- edge case: single non-null pair (slope/intercept/r2 require >= 2 rows)
statement
CREATE TABLE test_regr_single(y double, x double) USING parquet

statement
INSERT INTO test_regr_single VALUES (3.0, 5.0), (NULL, 2.0), (1.0, NULL)

query
SELECT regr_count(y, x), regr_avgx(y, x), regr_avgy(y, x) FROM test_regr_single

-- sxx/syy/sxy are 0 for a single pair; slope/intercept/r2 are NULL
query tolerance=1e-6
SELECT regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x) FROM test_regr_single

query tolerance=1e-6
SELECT regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x) FROM test_regr_single

-- edge case: independent variable (x) is constant.
-- var_pop(x) = 0, so slope/intercept/r2 are all NULL, and regr_sxx = 0.
statement
CREATE TABLE test_regr_const_x(y double, x double) USING parquet

statement
INSERT INTO test_regr_const_x VALUES (1.0, 5.0), (2.0, 5.0), (3.0, 5.0), (4.0, 5.0)

query tolerance=1e-6
SELECT regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x) FROM test_regr_const_x

query tolerance=1e-6
SELECT regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x) FROM test_regr_const_x

-- edge case: dependent variable (y) is constant but x varies.
-- A horizontal line is a perfect fit, so Spark's regr_r2 returns 1.0 (not NULL).
-- The slope is 0 and the intercept equals the constant y.
statement
CREATE TABLE test_regr_const_y(y double, x double) USING parquet

statement
INSERT INTO test_regr_const_y VALUES (7.0, 1.0), (7.0, 2.0), (7.0, 3.0), (7.0, 4.0)

query tolerance=1e-6
SELECT regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x) FROM test_regr_const_y

query tolerance=1e-6
SELECT regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x) FROM test_regr_const_y
