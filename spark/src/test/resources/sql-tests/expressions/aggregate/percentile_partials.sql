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

-- MinSparkVersion: 3.5

-- When a group holds both 0.0 and -0.0, Spark ranks first the zero its final hash table reaches
-- first, which depends on each partition's partial buffer, not only on the rows. Each INSERT
-- coalesces to one task, so it writes one file in row order, and an open cost as large as a
-- split keeps every file in its own partition.
-- Spark ranks -0.0 first for these rows in either arrival order. Replaying them as one partial
-- ranks 0.0 first when the first file is merged first, so these queries catch a regression to
-- that for one shuffle order only.
-- Config: spark.sql.files.openCostInBytes=134217728

statement
CREATE TABLE test_percentile_partials(g int, d double) USING parquet

statement
INSERT INTO test_percentile_partials SELECT /*+ COALESCE(1) */ * FROM VALUES
  (1, 369513.0), (1, 998311.0), (1, 8278.0), (1, 156933.0), (1, 286454.0), (1, 349279.0),
  (1, 125841.0), (1, 796806.0), (1, 556996.0), (1, 0.0), (1, 667190.0), (1, 624668.0),
  (1, 335166.0), (1, 220654.0), (1, 315604.0) AS t(g, d)

statement
INSERT INTO test_percentile_partials SELECT /*+ COALESCE(1) */ * FROM VALUES
  (1, 223815.0), (1, 201661.0), (1, 68860.0), (1, 928422.0), (1, double('-0.0')) AS t(g, d)

query
SELECT percentile(d, 0.0), percentile(d, 0.05) FROM test_percentile_partials

query
SELECT g, percentile(d, 0.0), percentile(d, 0.05) FROM test_percentile_partials GROUP BY g
