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
-- Falls back to Spark: aggregate not yet accelerated by Comet
query spark_answer_only
SELECT regr_sxx(y, x) FROM test_regr

query spark_answer_only
SELECT grp, regr_sxx(y, x) FROM test_regr GROUP BY grp ORDER BY grp

-- regr_syy: sum of squares of deviation of y from mean(y)
-- Falls back to Spark: aggregate not yet accelerated by Comet
query spark_answer_only
SELECT regr_syy(y, x) FROM test_regr

query spark_answer_only
SELECT grp, regr_syy(y, x) FROM test_regr GROUP BY grp ORDER BY grp

-- regr_sxy: sum of products of deviations of x and y from their means
-- Falls back to Spark: aggregate not yet accelerated by Comet
query spark_answer_only
SELECT regr_sxy(y, x) FROM test_regr

query spark_answer_only
SELECT grp, regr_sxy(y, x) FROM test_regr GROUP BY grp ORDER BY grp

-- regr_slope: slope of the least-squares regression line
-- Falls back to Spark: aggregate not yet accelerated by Comet
query spark_answer_only
SELECT regr_slope(y, x) FROM test_regr

query spark_answer_only
SELECT grp, regr_slope(y, x) FROM test_regr GROUP BY grp ORDER BY grp

-- regr_intercept: y-intercept of the least-squares regression line
-- Falls back to Spark: aggregate not yet accelerated by Comet
query spark_answer_only
SELECT regr_intercept(y, x) FROM test_regr

query spark_answer_only
SELECT grp, regr_intercept(y, x) FROM test_regr GROUP BY grp ORDER BY grp

-- regr_r2: square of the correlation coefficient (coefficient of determination)
-- Falls back to Spark: aggregate not yet accelerated by Comet
query spark_answer_only
SELECT regr_r2(y, x) FROM test_regr

query spark_answer_only
SELECT grp, regr_r2(y, x) FROM test_regr GROUP BY grp ORDER BY grp

-- edge case: all-NULL input returns NULL for all functions
statement
CREATE TABLE test_regr_all_null(y double, x double) USING parquet

statement
INSERT INTO test_regr_all_null VALUES (NULL, NULL), (NULL, NULL)

query
SELECT regr_count(y, x), regr_avgx(y, x), regr_avgy(y, x) FROM test_regr_all_null

-- Falls back to Spark: regr_sxx/syy/sxy not yet accelerated by Comet
query spark_answer_only
SELECT regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x) FROM test_regr_all_null

-- Falls back to Spark: regr_slope/intercept/r2 not yet accelerated by Comet
query spark_answer_only
SELECT regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x) FROM test_regr_all_null

-- edge case: single non-null pair (slope/intercept/r2 require >= 2 rows)
statement
CREATE TABLE test_regr_single(y double, x double) USING parquet

statement
INSERT INTO test_regr_single VALUES (3.0, 5.0), (NULL, 2.0), (1.0, NULL)

query
SELECT regr_count(y, x), regr_avgx(y, x), regr_avgy(y, x) FROM test_regr_single

-- Falls back to Spark: regr_sxx/syy/sxy not yet accelerated by Comet
query spark_answer_only
SELECT regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x) FROM test_regr_single

-- Falls back to Spark: regr_slope/intercept/r2 not yet accelerated by Comet
query spark_answer_only
SELECT regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x) FROM test_regr_single
