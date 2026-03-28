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

-- Tests for SQL aggregate FILTER (WHERE ...) clause support.
-- See https://github.com/apache/datafusion-comet/issues/XXXX

statement
CREATE TABLE test_agg_filter(
  grp string,
  i int,
  l long,
  d decimal(10, 2),
  flag boolean
) USING parquet

statement
INSERT INTO test_agg_filter VALUES
  ('a', 1,  10,  1.00, true),
  ('a', 2,  20,  2.00, false),
  ('a', 3,  30,  3.00, true),
  ('b', 4,  40,  4.00, false),
  ('b', 5,  50,  5.00, true),
  ('b', NULL, NULL, NULL, true)

-- Basic FILTER on SUM(int)
query
SELECT SUM(i) FILTER (WHERE flag = true) FROM test_agg_filter

-- FILTER on SUM with GROUP BY
query
SELECT grp, SUM(i) FILTER (WHERE flag = true) FROM test_agg_filter GROUP BY grp ORDER BY grp

-- FILTER on SUM(long)
query
SELECT SUM(l) FILTER (WHERE flag = true) FROM test_agg_filter

-- FILTER on SUM(decimal)
query
SELECT SUM(d) FILTER (WHERE flag = true) FROM test_agg_filter

-- Multiple aggregates: one with filter, one without
query
SELECT SUM(i), SUM(i) FILTER (WHERE flag = true) FROM test_agg_filter

-- FILTER with NULL rows: NULLs should not be included even when filter passes
query
SELECT grp, SUM(i) FILTER (WHERE flag = true) FROM test_agg_filter GROUP BY grp ORDER BY grp

-- FILTER with COUNT
query
SELECT COUNT(*) FILTER (WHERE flag = true) FROM test_agg_filter

-- FILTER with COUNT GROUP BY
query
SELECT grp, COUNT(*) FILTER (WHERE flag = true) FROM test_agg_filter GROUP BY grp ORDER BY grp
