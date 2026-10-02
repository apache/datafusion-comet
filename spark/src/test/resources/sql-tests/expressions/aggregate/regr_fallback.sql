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

-- regr_slope, regr_intercept, regr_r2, regr_sxx, regr_syy and regr_sxy fall back to Spark by
-- default, because their native merge of partial aggregates orders its floating-point operations
-- differently from Spark's (https://github.com/apache/datafusion-comet/issues/6423). Each query
-- below holds a single regr function, so it checks that function's own fallback. regr.sql opts in
-- and covers the native path.

-- The data from #6423: x is constant at 0.1, and each INSERT writes one file of three rows, so the
-- rows are merged from two partial aggregates. The native merge leaves x with a tiny non-zero
-- variance there, and returns wrong values where Spark returns NULL, 0.0 or 1.0.
statement
CREATE TABLE test_regr_fallback(y double, x double) USING parquet

statement
INSERT INTO test_regr_fallback SELECT CAST(id AS DOUBLE), 0.1D FROM range(0, 3, 1, 1)

statement
INSERT INTO test_regr_fallback SELECT CAST(id AS DOUBLE), 0.1D FROM range(3, 6, 1, 1)

query expect_fallback(issues/6423)
SELECT regr_slope(y, x) FROM test_regr_fallback

query expect_fallback(issues/6423)
SELECT regr_intercept(y, x) FROM test_regr_fallback

query expect_fallback(issues/6423)
SELECT regr_r2(y, x) FROM test_regr_fallback

-- The constant as the dependent variable
query expect_fallback(issues/6423)
SELECT regr_r2(x, y) FROM test_regr_fallback

query expect_fallback(issues/6423)
SELECT regr_sxx(y, x) FROM test_regr_fallback

query expect_fallback(issues/6423)
SELECT regr_sxy(y, x) FROM test_regr_fallback

query expect_fallback(issues/6423)
SELECT regr_syy(x, y) FROM test_regr_fallback
