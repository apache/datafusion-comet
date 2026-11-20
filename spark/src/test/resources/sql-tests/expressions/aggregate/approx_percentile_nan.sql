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

-- One input partition, with more than the 50000-value head-buffer threshold
-- for both scalar and grouped aggregates. Negate NaN after the Parquet scan:
-- Spark canonicalizes NaNs on write, so storing -NaN would miss this bug.
-- Plain queries assert native execution as well as exact Spark results.
-- Config: spark.sql.files.maxPartitionBytes=134217728

statement
CREATE TABLE test_approx_percentile_nan(d double, f float, g int) USING parquet

statement
INSERT INTO test_approx_percentile_nan
SELECT IF(id = 0, double('NaN'), cast(id AS double)),
       IF(id = 0, float('NaN'), cast(id AS float)),
       cast(id % 2 AS int)
FROM range(0, 300000, 1, 1)

-- Canonical NaN control, with both numeric input types and array output.
query
SELECT percentile_approx(d, array(0.0D, 0.25D, 0.5D, 0.75D, 1.0D)),
       approx_percentile(f, array(0.0D, 0.25D, 0.5D, 0.75D, 1.0D))
FROM test_approx_percentile_nan

-- Negative-sign NaN must not change any rank or replace the maximum with a
-- finite value. Six head-buffer flushes reproduce issue #6519.
query
SELECT percentile_approx(IF(isnan(d), -d, d), array(0.0D, 0.25D, 0.5D, 0.75D, 1.0D))
FROM test_approx_percentile_nan

query
SELECT approx_percentile(IF(isnan(f), -f, f), array(0.0D, 0.25D, 0.5D, 0.75D, 1.0D))
FROM test_approx_percentile_nan

-- Grouped accumulators and their partial-to-final serialized states. Each
-- group exceeds the head-buffer threshold; only group 0 contains a NaN.
query
SELECT g,
       percentile_approx(IF(isnan(d), -d, d), array(0.0D, 0.25D, 0.5D, 0.75D, 1.0D)),
       approx_percentile(IF(isnan(f), -f, f), array(0.0D, 0.25D, 0.5D, 0.75D, 1.0D))
FROM test_approx_percentile_nan GROUP BY g ORDER BY g

-- Scalar output uses the same ordering, including the NaN endpoint.
query
SELECT percentile_approx(IF(isnan(d), -d, d), 0.5D),
       isnan(percentile_approx(IF(isnan(d), -d, d), 1.0D)),
       approx_percentile(IF(isnan(f), -f, f), 0.5D),
       isnan(approx_percentile(IF(isnan(f), -f, f), 1.0D))
FROM test_approx_percentile_nan
