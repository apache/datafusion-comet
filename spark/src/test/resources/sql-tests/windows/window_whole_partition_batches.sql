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

-- Window functions that need a whole window partition run in CometWindowAggExec, which evaluates
-- a window partition once the sorted input moves past it. With 7-row batches most window
-- partitions span several batches, with null-key and single-row window partitions in between.
-- Without PARTITION BY the whole input is one window partition, which the Rust tests cover.
-- Config: spark.comet.batchSize=7

statement
CREATE TABLE whole_partition(id INT, k INT, o INT, v BIGINT) USING parquet

statement
INSERT INTO whole_partition
SELECT
  CAST(id AS INT),
  CASE WHEN id % 13 = 0 THEN NULL WHEN id < 1500 THEN CAST(id * 7 % 61 AS INT) ELSE CAST(id AS INT) END,
  CAST(id % 17 AS INT),
  CASE WHEN id % 11 = 0 THEN NULL ELSE id END
FROM range(2000)

-- NTILE orders by the unique id too, so that peers cannot swap buckets.
query
SELECT id, k, o, v,
  PERCENT_RANK() OVER (PARTITION BY k ORDER BY o) AS pct_rnk,
  CUME_DIST() OVER (PARTITION BY k ORDER BY o) AS cume_dist,
  NTILE(4) OVER (PARTITION BY k ORDER BY o, id) AS ntile_4,
  SUM(v) OVER (PARTITION BY k) AS total,
  COUNT(v) OVER (PARTITION BY k ORDER BY o
                 RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS remaining
FROM whole_partition
