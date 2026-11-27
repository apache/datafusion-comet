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
query
SELECT regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x) FROM test_regr_all_null

-- regr_slope/intercept/r2 return NULL when there are no non-null pairs
query
SELECT regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x) FROM test_regr_all_null

-- edge case: single non-null pair (slope/intercept/r2 require >= 2 rows)
statement
CREATE TABLE test_regr_single(y double, x double) USING parquet

statement
INSERT INTO test_regr_single VALUES (3.0, 5.0), (NULL, 2.0), (1.0, NULL)

query
SELECT regr_count(y, x), regr_avgx(y, x), regr_avgy(y, x) FROM test_regr_single

-- sxx/syy/sxy are 0 for a single pair; slope/intercept/r2 are NULL
query
SELECT regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x) FROM test_regr_single

query
SELECT regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x) FROM test_regr_single

-- edge case: independent variable (x) is constant but y varies.
-- var_pop(x) = 0, so slope/intercept are NULL and regr_sxx = 0. regr_r2 is a
-- degenerate case whose value depends on the Spark patch release (1.0 before
-- SPARK-55969, NULL from 3.5.9, 4.0.3, 4.1.2 and 4.2.0), which Comet matches.
statement
CREATE TABLE test_regr_const_x(y double, x double) USING parquet

statement
INSERT INTO test_regr_const_x VALUES (1.0, 5.0), (2.0, 5.0), (3.0, 5.0), (4.0, 5.0)

query
SELECT regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x) FROM test_regr_const_x

query
SELECT regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x) FROM test_regr_const_x

-- edge case: dependent variable (y) is constant but x varies.
-- The slope is 0 and the intercept equals the constant y. regr_r2 is a degenerate
-- case whose value depends on the Spark patch release (NULL before SPARK-55969,
-- 1.0 from 3.5.9, 4.0.3, 4.1.2 and 4.2.0), which Comet matches.
statement
CREATE TABLE test_regr_const_y(y double, x double) USING parquet

statement
INSERT INTO test_regr_const_y VALUES (7.0, 1.0), (7.0, 2.0), (7.0, 3.0), (7.0, 4.0)

query
SELECT regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x) FROM test_regr_const_y

query
SELECT regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x) FROM test_regr_const_y

-- Nullable aggregate FILTERs must preserve group positions and combine with
-- each function's pair-null rules. Group 1 exists but has no qualifying rows;
-- group 2 has x values whose y is NULL, which slope/intercept counted before SPARK-48719.
statement
CREATE TABLE test_regr_filtered(g int, y double, x double, keep boolean) USING parquet

statement
INSERT INTO test_regr_filtered VALUES
  (0, 3.0, 1.0, true), (0, 9.0, 4.0, true), (0, 8.0, NULL, true),
  (0, NULL, 20.0, true), (0, 90.0, 8.0, false), (0, 70.0, 9.0, NULL),
  (1, 2.0, 5.0, false), (1, 8.0, 9.0, NULL),
  (2, NULL, 1.0, true), (2, NULL, 4.0, true),
  (3, 7.0, 1.0, true), (3, 7.0, 4.0, true)

query
SELECT g,
  regr_sxx(y, x) FILTER (WHERE keep), regr_syy(y, x) FILTER (WHERE keep),
  regr_sxy(y, x) FILTER (WHERE keep), regr_slope(y, x) FILTER (WHERE keep),
  regr_intercept(y, x) FILTER (WHERE keep), regr_r2(y, x) FILTER (WHERE keep)
FROM test_regr_filtered GROUP BY g ORDER BY g

query
SELECT g,
  regr_sxx(x, y) FILTER (WHERE keep), regr_syy(x, y) FILTER (WHERE keep),
  regr_sxy(x, y) FILTER (WHERE keep), regr_slope(x, y) FILTER (WHERE keep),
  regr_intercept(x, y) FILTER (WHERE keep), regr_r2(x, y) FILTER (WHERE keep)
FROM test_regr_filtered GROUP BY g ORDER BY g

-- NaN, infinities and -0.0 through the grouped path, in y (groups 0, 2, 4) and in
-- x (groups 1, 3, 5). Group 6 has a NaN and an infinity only in rows the FILTER drops.
-- Group 7 has exactly one non-null pair.
statement
CREATE TABLE test_regr_special(g int, y double, x double, keep boolean) USING parquet

statement
INSERT INTO test_regr_special VALUES
  (0, cast('NaN' as double), 1.0, true), (0, 2.0, 2.0, true), (0, 4.0, 3.0, true),
  (1, 1.0, cast('NaN' as double), true), (1, 2.0, 2.0, true), (1, 4.0, 3.0, true),
  (2, cast('Infinity' as double), 1.0, true), (2, 2.0, 2.0, true), (2, 4.0, 3.0, true),
  (3, 1.0, cast('-Infinity' as double), true), (3, 2.0, 2.0, true), (3, 4.0, 3.0, true),
  (4, -0.0D, 1.0, true), (4, -0.0D, 2.0, true), (4, -0.0D, 3.0, true),
  (5, 1.0, -0.0D, true), (5, 2.0, -0.0D, true), (5, 4.0, -0.0D, true),
  (6, 1.0, 1.0, true), (6, 2.0, 2.0, true), (6, 4.0, 3.0, true),
  (6, cast('NaN' as double), 4.0, false), (6, 5.0, cast('Infinity' as double), NULL),
  (7, 3.0, 5.0, true), (7, NULL, 2.0, true)

query
SELECT g,
  regr_sxx(y, x) FILTER (WHERE keep), regr_syy(y, x) FILTER (WHERE keep),
  regr_sxy(y, x) FILTER (WHERE keep), regr_slope(y, x) FILTER (WHERE keep),
  regr_intercept(y, x) FILTER (WHERE keep), regr_r2(y, x) FILTER (WHERE keep)
FROM test_regr_special GROUP BY g ORDER BY g

-- Both moments are nonzero, but their product underflows to zero.
statement
CREATE TABLE test_regr_underflow(v double, grp int) USING parquet

statement
INSERT INTO test_regr_underflow VALUES (1e-100, 0), (NULL, 0), (2e-100, 0)

query
SELECT regr_r2(v, v), regr_r2(v, -v) FROM test_regr_underflow

query
SELECT grp, regr_r2(v, v), regr_r2(v, -v) FROM test_regr_underflow GROUP BY grp

statement
SET spark.sql.ansi.enabled=true

query expect_error(DIVIDE_BY_ZERO)
SELECT regr_r2(v, v) FROM test_regr_underflow

query expect_error(DIVIDE_BY_ZERO)
SELECT grp, regr_r2(v, -v) FROM test_regr_underflow GROUP BY grp

-- The existing constant-input guards precede division, even in ANSI mode.
query
SELECT regr_r2(y, x) FROM test_regr_const_x

query
SELECT regr_r2(y, x) FROM test_regr_const_y

query
SELECT x, regr_r2(y, x) FROM test_regr_const_x GROUP BY x

query
SELECT y, regr_r2(y, x) FROM test_regr_const_y GROUP BY y
