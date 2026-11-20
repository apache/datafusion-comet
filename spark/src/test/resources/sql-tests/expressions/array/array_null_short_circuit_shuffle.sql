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

-- array_union evaluates shuffle(b) only for the rows whose array is not NULL, as in
-- array_null_short_circuit.sql. shuffle keeps one random generator for every batch of a partition,
-- so its permutations have to carry on from a batch with no NULL array to one with some. The two
-- argument shuffle(array, seed) form only exists in Spark 4.0+.
-- https://github.com/apache/datafusion-comet/issues/6613

-- MinSparkVersion: 4.0
-- Config: spark.comet.batchSize=8192

statement
CREATE TABLE array_null_short_circuit_shuffle(id bigint, a array<int>, b array<int>) USING parquet

-- A single file, which the scan reads in batches of 8192 rows (the batch size is pinned above). Only
-- the second batch has NULL arrays.
statement
INSERT INTO array_null_short_circuit_shuffle
SELECT id, IF(id >= 8192 AND id % 2 = 0, NULL, array(0)), array(1, 2, 3, 4)
FROM range(0, 8200, 1, 1)

query expect_native(array_union, shuffle)
SELECT id, array_union(a, shuffle(b, 42)) FROM array_null_short_circuit_shuffle
