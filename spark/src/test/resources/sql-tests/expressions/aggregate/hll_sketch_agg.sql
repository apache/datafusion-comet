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

-- MinSparkVersion: 4.0
-- Config: spark.comet.expression.HllSketchAgg.allowIncompatible=true
-- Config: spark.comet.expression.HllSketchEstimate.allowIncompatible=true
-- Config: spark.sql.files.openCostInBytes=134217728

-- Until a group's sketch holds the register array, its estimate depends only on the coupons it
-- collected. Merging two or more partials that already hold it leaves the estimate to the
-- registers alone. Comet matches Spark exactly in both cases.

statement
CREATE TABLE hll_types(g int, i int, l bigint, s string, b binary) USING parquet

statement
INSERT INTO hll_types VALUES
  (0, 1, 1, 'a', X'01'),
  (0, 1, 1, 'a', X'01'),
  (0, -2147483648, -9223372036854775808, '', X''),
  (0, 2147483647, 9223372036854775807, 'héllo', X'00FF'),
  (0, NULL, NULL, NULL, NULL),
  (1, 0, 0, 'b', X'02'),
  (1, NULL, NULL, NULL, NULL),
  (2, NULL, NULL, NULL, NULL)

-- NULLs, empty strings and empty binaries are skipped, so the all-NULL group estimates to 0.
query
SELECT g,
  hll_sketch_estimate(hll_sketch_agg(i)),
  hll_sketch_estimate(hll_sketch_agg(l)),
  hll_sketch_estimate(hll_sketch_agg(s)),
  hll_sketch_estimate(hll_sketch_agg(b))
FROM hll_types GROUP BY g ORDER BY g

-- Each INSERT coalesces to one task, so it writes one file, and an open cost as large as a split
-- keeps every file in its own partition. Each file then becomes one partial sketch per group, of
-- 10,000 distinct values, and the Final aggregation merges four of them.
statement
CREATE TABLE hll_partials(g int, v bigint) USING parquet

statement
INSERT INTO hll_partials SELECT /*+ COALESCE(1) */ CAST(id % 2 AS INT), id FROM range(0, 20000)

statement
INSERT INTO hll_partials SELECT /*+ COALESCE(1) */ CAST(id % 2 AS INT), id FROM range(20000, 40000)

statement
INSERT INTO hll_partials SELECT /*+ COALESCE(1) */ CAST(id % 2 AS INT), id FROM range(40000, 60000)

statement
INSERT INTO hll_partials SELECT /*+ COALESCE(1) */ CAST(id % 2 AS INT), id FROM range(60000, 80000)

-- At lgConfigK 21 a sketch holds 196,608 coupons before the register array, so the partials and
-- the unions all stay coupon sets.
query
SELECT g, hll_sketch_estimate(hll_sketch_agg(v, 21)) FROM hll_partials GROUP BY g ORDER BY g

-- At lgConfigK 12 every partial already holds the register array.
query
SELECT g, hll_sketch_estimate(hll_sketch_agg(v, 12)) FROM hll_partials GROUP BY g ORDER BY g

query
SELECT hll_sketch_estimate(hll_sketch_agg(v, 12)) FROM hll_partials

-- At lgConfigK 18 the partials are coupon sets, and a group's union moves to the register array
-- during the third merge, past 24,576 coupons. From there the estimate depends on the order the
-- remaining coupons arrive in, in Spark too, which gives different estimates for different orders
-- of the four partials. So the estimates are compared within 1% of the 40,000 distinct values.
query tolerance=400
SELECT g, CAST(hll_sketch_estimate(hll_sketch_agg(v, 18)) AS DOUBLE)
FROM hll_partials GROUP BY g ORDER BY g
