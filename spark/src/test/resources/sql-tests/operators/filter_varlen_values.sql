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

-- A filter over string and binary values, including values nested in arrays, structs and maps,
-- with a small batch size. The native filter merges small output batches into bigger ones, so
-- these check the merged batches against Spark. The batch size limit itself is tested in Rust,
-- since SQL can't see batch sizes.

-- Config: spark.comet.batchSize=4

statement
CREATE TABLE test_filter_varlen_values(
  id int,
  b binary,
  s string,
  a array<struct<x: binary, y: string>>,
  m map<string, binary>) USING parquet

-- Values of about 0.5 to 2.5 KB, plus empty and NULL rows that the filters must drop.
statement
INSERT INTO test_filter_varlen_values
SELECT
  id,
  CASE WHEN id % 10 = 3 THEN NULL WHEN id % 10 = 7 THEN X'' ELSE CAST(repeat(cast(id AS string), 500 + id * 10) AS binary) END,
  CASE WHEN id % 10 = 3 THEN NULL WHEN id % 10 = 7 THEN '' ELSE repeat(concat('数据', cast(id AS string), 'é'), 100 + id * 2) END,
  CASE WHEN id % 10 = 5 THEN NULL ELSE array(
    named_struct('x', CAST(repeat('ab', 200 + id) AS binary), 'y', repeat('データ', 50 + id)),
    named_struct('x', CAST(NULL AS binary), 'y', CAST(NULL AS string))) END,
  CASE WHEN id % 10 = 5 THEN NULL ELSE map(concat('k', cast(id AS string)), CAST(repeat('é', 300 + id) AS binary)) END
FROM range(64)

-- About one row in three passes, so each 4-row input batch leaves 1 or 2 rows and the filter
-- merges them. md5 checks every byte.
query
SELECT id, length(b), md5(b), length(s), md5(s) FROM test_filter_varlen_values WHERE id % 3 = 0 AND length(b) > 0 ORDER BY id

query
SELECT id, a, m FROM test_filter_varlen_values WHERE id % 3 = 0 ORDER BY id

-- Nearly every row passes.
query
SELECT id, length(b), md5(b), length(s), md5(s) FROM test_filter_varlen_values WHERE length(b) > 0 ORDER BY id

-- Only a few rows pass.
query
SELECT id, b, s FROM test_filter_varlen_values WHERE id % 16 = 0 AND length(b) > 0 ORDER BY id

query
SELECT count(*), sum(length(b)), sum(length(s)) FROM test_filter_varlen_values WHERE b IS NOT NULL
