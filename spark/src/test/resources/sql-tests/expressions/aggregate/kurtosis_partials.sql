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

-- Each INSERT coalesces to one task, so it writes one file, and an open cost as large as a split
-- keeps every file in its own partition. Every query below then merges two partials, and plain
-- `query` compares the merged result with Spark's exactly.
-- Config: spark.sql.files.openCostInBytes=134217728

-- Ordinary values in both files. Group 3 is NULL in the first file, so its first partial is
-- empty. Merging these partials rounds differently if `moments4_merge` reorders the arithmetic
-- for `avg`, `m2` or `m4`, and every query gives the same result in either arrival order.

statement
CREATE TABLE test_kurtosis_partials(g int, v double) USING parquet

statement
INSERT INTO test_kurtosis_partials SELECT /*+ COALESCE(1) */ * FROM VALUES
  (1, -13.44), (1, -13.9), (1, 0.94), (1, -2.92),
  (2, -19.6), (2, -2.6), (2, 11.5),
  (3, NULL) AS t(g, v)

statement
INSERT INTO test_kurtosis_partials SELECT /*+ COALESCE(1) */ * FROM VALUES
  (1, 13.3), (1, 19.0), (1, 2.39),
  (2, -13.3), (2, -15.54), (2, -3.4),
  (3, 4.0), (3, 8.0), (3, 16.0), (3, 23.0) AS t(g, v)

query
SELECT kurtosis(v) FROM test_kurtosis_partials

query
SELECT g, kurtosis(v) FROM test_kurtosis_partials GROUP BY g

-- The moments of these values overflow when merged, so Spark returns NaN. Merging the all-NULL
-- file's empty partial must not hide that.

statement
CREATE TABLE test_kurtosis_empty_partial(g int, v double) USING parquet

statement
INSERT INTO test_kurtosis_empty_partial SELECT /*+ COALESCE(1) */ 0, 5e77 * (1 + id * 1e-9)
  FROM range(10)

statement
INSERT INTO test_kurtosis_empty_partial SELECT /*+ COALESCE(1) */ 0, CAST(NULL AS double)

query
SELECT kurtosis(v) FROM test_kurtosis_empty_partial

query
SELECT g, kurtosis(v) FROM test_kurtosis_empty_partial GROUP BY g
