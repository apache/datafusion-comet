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

-- Under LAST_WIN the dedup difference is an opt-in (`allowIncompatible`), and the opt-in hands the
-- expression straight to the native kernel. The kernel's own gate (a stateful input under the
-- null guard) has nothing to do with dedup, so it must still refuse that shape with the opt-in on.

-- Config: spark.sql.mapKeyDedupPolicy=LAST_WIN
-- Config: spark.comet.expression.MapFromArrays.allowIncompatible=true

statement
CREATE TABLE test_map_from_arrays_lw(id bigint, k array<bigint>, v array<int>) USING parquet

statement
INSERT INTO test_map_from_arrays_lw VALUES
  (0, array(0), array(10)), (1, array(1), array(11)), (2, array(2), array(12)), (3, array(3), NULL)

-- No duplicate keys, so the opt-in takes the native kernel.
query expect_native(map_from_arrays)
SELECT map_from_arrays(k, v) FROM test_map_from_arrays_lw

-- The null guard would evaluate the stateful key array twice, so it stays in Spark, where row 2
-- keeps {2: NULL}.
query expect_fallback(non-deterministic child under a null guard is evaluated on different rows than Spark's)
SELECT map_from_arrays(IF(monotonically_increasing_id() % 2 = 0, array(id), NULL), transform(array(id), x -> NULL)) FROM test_map_from_arrays_lw

-- A literal array beside a per-row one takes the native kernel too.
query expect_native(map_from_arrays)
SELECT map_from_arrays(k, array(1)) FROM test_map_from_arrays_lw
