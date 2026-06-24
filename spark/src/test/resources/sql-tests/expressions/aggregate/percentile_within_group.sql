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

-- percentile_cont(p) WITHIN GROUP (ORDER BY col) was added in Spark 4.0. It is a
-- RuntimeReplaceable that rewrites to Percentile(col, p, reverse), so the ascending form runs
-- natively through Comet, while the descending (DESC) form falls back to Spark because the
-- native percentile_cont always interpolates in ascending order.
-- MinSparkVersion: 4.0

statement
CREATE TABLE test_pct_wg(g int, v double) USING parquet

statement
INSERT INTO test_pct_wg VALUES
  (1, 1.0), (1, 2.0), (1, 3.0), (1, 4.0), (2, 10.0), (2, 20.0), (2, NULL)

-- ascending WITHIN GROUP runs natively
query
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM test_pct_wg

query
SELECT g, percentile_cont(0.25) WITHIN GROUP (ORDER BY v) FROM test_pct_wg GROUP BY g ORDER BY g

-- descending WITHIN GROUP falls back to Spark (reverse is not supported natively)
query expect_fallback(Descending order in `WITHIN GROUP (ORDER BY ... DESC)` is not supported.)
SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC) FROM test_pct_wg
