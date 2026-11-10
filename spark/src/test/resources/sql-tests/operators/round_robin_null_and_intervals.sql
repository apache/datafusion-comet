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

-- Config: spark.comet.shuffle.mode=native
-- Config: spark.comet.shuffle.native.partitioning.roundrobin.enabled=true
-- Positional placement is off by default, but pinned here on purpose: these queries test the
-- content-hash placement, which positional placement would skip.
-- Config: spark.comet.shuffle.native.partitioning.roundrobin.positional.enabled=false

-- Without positional placement, native round-robin shuffle places each row by a murmur3 hash of
-- all its columns, so a NULL (NullType) column and ANSI interval columns go through the native
-- hash. The default query mode checks that the exchange is native and compares rows with Spark.

statement
CREATE TABLE test_rr_null_intervals(id int, y int, m int, d int, h int) USING parquet

statement
INSERT INTO test_rr_null_intervals VALUES
  (1, 1, 2, 1, 2), (2, 0, 0, 0, 0), (3, -1, 6, -1, 23), (4, 5, -3, 3, -4), (5, NULL, NULL, NULL, NULL)

query
SELECT /*+ REPARTITION(3) */ id, NULL AS n FROM test_rr_null_intervals

query
SELECT /*+ REPARTITION(3) */ id, make_ym_interval(y, m) AS ym, make_dt_interval(d, h) AS dt
FROM test_rr_null_intervals

query
SELECT /*+ REPARTITION(3) */ NULL AS n, make_ym_interval(y, m) AS ym, make_dt_interval(d, h) AS dt
FROM test_rr_null_intervals
