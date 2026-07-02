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

-- Native exact percentile via DataFusion percentile_cont (same (n-1)*p interpolation as Spark).
-- Marked Incompatible because DataFusion quantizes the interpolation weight to 6 decimal places
-- (#4719); allow it here so the native path is exercised.
-- Config: spark.comet.expression.Percentile.allowIncompatible=true

statement
CREATE TABLE test_percentile(g int, v double, i int) USING parquet

statement
INSERT INTO test_percentile VALUES
  (1, 1.0, 10), (1, 2.0, 20), (1, 3.0, 30), (1, 4.0, 40),
  (2, 10.0, 5), (2, 20.0, 15), (2, NULL, 25)

-- global percentile, interpolated and exact-rank cases
query
SELECT percentile(v, 0.5) FROM test_percentile

query
SELECT percentile(v, 0.0), percentile(v, 1.0), percentile(v, 0.25), percentile(v, 0.9) FROM test_percentile

-- grouped
query
SELECT g, percentile(v, 0.5) FROM test_percentile GROUP BY g ORDER BY g

-- integer input (cast to double)
query
SELECT percentile(i, 0.5) FROM test_percentile

-- all-null group yields null
query
SELECT percentile(v, 0.5) FROM test_percentile WHERE v IS NULL

-- median is a RuntimeReplaceable that rewrites to percentile(col, 0.5), so it also runs natively
query
SELECT median(v) FROM test_percentile

query
SELECT g, median(i) FROM test_percentile GROUP BY g ORDER BY g

-- ============================================================
-- Other numeric input types (all cast to double, matching Spark)
-- ============================================================

statement
CREATE TABLE test_percentile_types(l bigint, f float, d decimal(10, 2), s smallint, b tinyint) USING parquet

statement
INSERT INTO test_percentile_types VALUES
  (1, 1.5, 1.25, 1, 1), (2, 2.5, 2.50, 2, 2), (3, 3.5, 3.75, 3, 3), (4, 4.5, 4.00, 4, 4)

query
SELECT percentile(l, 0.5), percentile(f, 0.5), percentile(d, 0.5), percentile(s, 0.5), percentile(b, 0.5) FROM test_percentile_types

query
SELECT percentile(l, 0.25), percentile(f, 0.75) FROM test_percentile_types

-- ============================================================
-- Negative values and special float values
-- ============================================================

statement
CREATE TABLE test_percentile_neg(v double) USING parquet

statement
INSERT INTO test_percentile_neg VALUES (-10.0), (-5.0), (0.0), (5.0), (10.0)

query
SELECT percentile(v, 0.5), percentile(v, 0.1), percentile(v, 0.9) FROM test_percentile_neg

-- ============================================================
-- Unsupported forms fall back to Spark cleanly
-- ============================================================

query expect_fallback(An array of percentages is not supported.)
SELECT percentile(v, array(0.25, 0.5, 0.75)) FROM test_percentile

query expect_fallback(A frequency argument is not supported.)
SELECT percentile(v, 0.5, 2) FROM test_percentile
