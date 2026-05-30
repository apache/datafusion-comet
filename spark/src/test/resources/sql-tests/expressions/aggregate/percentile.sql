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

-- unsupported forms fall back to Spark cleanly
query expect_fallback(an array of percentages is not supported)
SELECT percentile(v, array(0.25, 0.5, 0.75)) FROM test_percentile

query expect_fallback(a frequency argument is not supported)
SELECT percentile(v, 0.5, 2) FROM test_percentile
