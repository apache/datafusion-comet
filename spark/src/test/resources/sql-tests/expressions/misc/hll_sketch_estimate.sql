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
-- ConfigMatrix: parquet.enable.dictionary=false,true

-- hll_sketch_estimate runs natively without an opt-in. hll_sketch_agg and hll_union_agg still
-- need one, so the sketches below are built by Spark, and the estimates read Spark's bytes.

statement
CREATE TABLE hll_input(lgk int, n int, v bigint) USING parquet

statement
INSERT INTO hll_input
SELECT lgk, n, id FROM
  (SELECT explode(array(4, 12, 21)) AS lgk) k,
  (SELECT explode(array(5, 100, 1000, 10000)) AS n) c,
  range(0, 10000) r
WHERE id < n

statement
CREATE TABLE hll_sketches(lgk int, n int, sk binary) USING parquet

-- Few values stay in LIST mode, more move to SET and then HLL mode, which differs by lgConfigK.
statement
INSERT INTO hll_sketches
SELECT 4, n, hll_sketch_agg(v, 4) FROM hll_input WHERE lgk = 4 GROUP BY n
UNION ALL
SELECT 12, n, hll_sketch_agg(v, 12) FROM hll_input WHERE lgk = 12 GROUP BY n
UNION ALL
SELECT 21, n, hll_sketch_agg(v, 21) FROM hll_input WHERE lgk = 21 GROUP BY n

-- Repeat every sketch so the dictionary-encoded run of the matrix has repeated values.
statement
INSERT INTO hll_sketches SELECT * FROM hll_sketches

-- An empty sketch, from a group with only NULL input, and a NULL sketch.
statement
INSERT INTO hll_sketches
SELECT 12, 0, hll_sketch_agg(CAST(NULL AS BIGINT), 12) FROM range(1)
UNION ALL
SELECT 12, -1, CAST(NULL AS BINARY)

query
SELECT lgk, n, hll_sketch_estimate(sk) FROM hll_sketches ORDER BY lgk, n

-- A sketch unioned by Spark from sketches with different lgConfigK values.
statement
CREATE TABLE hll_unioned(u binary) USING parquet

statement
INSERT INTO hll_unioned SELECT hll_union_agg(sk, true) FROM hll_sketches WHERE n = 10000

query
SELECT hll_sketch_estimate(u) FROM hll_unioned

statement
CREATE TABLE hll_bad(b binary) USING parquet

statement
INSERT INTO hll_bad VALUES (X'0102030405')

-- Both engines fail, but Comet raises a Comet error instead of HLL_INVALID_INPUT_SKETCH_BUFFER.
-- Switch to expect_error_class(HLL_INVALID_INPUT_SKETCH_BUFFER) once the issue is fixed.
query ignore(https://github.com/apache/datafusion-comet/issues/7549)
SELECT hll_sketch_estimate(b) FROM hll_bad
