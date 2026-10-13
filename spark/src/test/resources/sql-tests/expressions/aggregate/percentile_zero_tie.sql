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

-- When both zeros are present Spark keeps them as separate keys and ranks first the one its
-- hash table reaches first, which changes as the table grows. Spark does so from 3.5.2
-- (SPARK-45599). Earlier releases can merge the two zeros and drop a count. Each INSERT
-- coalesces to one task, so each table is one file in row order with a single partial buffer.
statement
CREATE TABLE test_percentile_zero_tie(d double) USING parquet

statement
INSERT INTO test_percentile_zero_tie SELECT /*+ COALESCE(1) */ * FROM VALUES
  (0.0), (2.0), (153.0), (168.0), (3252411229536261.0), (7.205759403792794e+16),
  (1.7976931348623157e+308), (0.25), (double('NaN')), (double('NaN')), (double('-0.0')),
  (-128.0), (double('NaN')), (double('NaN')) AS t(d)

query
SELECT percentile(d, 0.1), percentile(d, 0.15), percentile(d, 0.5) FROM test_percentile_zero_tie

-- Spark ranks -0.0 first in a small table, whichever zero arrives first
statement
CREATE TABLE test_percentile_zero_tie_grouped(g int, f float, d double) USING parquet

statement
INSERT INTO test_percentile_zero_tie_grouped SELECT /*+ COALESCE(1) */ * FROM VALUES
  (1, float('-0.0'), double('-0.0')), (1, float('0.0'), double('0.0')), (1, 1.0, 1.0),
  (2, float('0.0'), double('0.0')), (2, float('-0.0'), double('-0.0')), (2, 1.0, 1.0) AS t(g, f, d)

query
SELECT g, percentile(f, 0.0), percentile(d, 0.0), percentile(d, 0.25) FROM test_percentile_zero_tie_grouped GROUP BY g ORDER BY g

statement
CREATE TABLE test_percentile_zero_tie_large(f float, d double) USING parquet

statement
INSERT INTO test_percentile_zero_tie_large
SELECT /*+ COALESCE(1) */ * FROM (
  SELECT cast(id AS float), cast(id AS double) FROM range(1, 501)
  UNION ALL SELECT float('-0.0'), double('-0.0')
  UNION ALL SELECT float('0.0'), double('0.0'))

query
SELECT percentile(f, 0.0), percentile(d, 0.0), percentile(f, 0.001), percentile(d, 0.001) FROM test_percentile_zero_tie_large
