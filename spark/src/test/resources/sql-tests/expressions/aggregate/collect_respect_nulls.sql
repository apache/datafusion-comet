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

-- MinSparkVersion: 4.2

-- collect_list / collect_set with RESPECT NULLS (ignoreNulls = false).
--
-- Spark 4.2 added the `ignoreNulls` field to CollectList / CollectSet, settable
-- to false via `RESPECT NULLS`, which preserves null inputs in the result array.
-- Comet's native aggregate always drops nulls, so CometCollectList / CometCollectSet
-- report Unsupported for this case and fall back to Spark. The RESPECT NULLS queries
-- use expect_fallback to assert both the fallback reason and Spark-identical results;
-- the closing default-behavior query stays native to prove the fallback is specific to
-- RESPECT NULLS.
--
-- Result order is non-deterministic across partitions, so every query wraps the
-- result in sort_array for stable comparison. sort_array orders nulls first.

-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE crn_src(v int, grp string) USING parquet

statement
INSERT INTO crn_src VALUES
  (1, 'a'), (2, 'a'), (NULL, 'a'), (1, 'a'),
  (NULL, 'b'), (NULL, 'b'),
  (3, 'c')

-- ============================================================
-- collect_list RESPECT NULLS: nulls are kept (global aggregate)
-- ============================================================

query expect_fallback(collect_list with RESPECT NULLS (ignoreNulls = false) is not supported)
SELECT sort_array(collect_list(v) RESPECT NULLS) FROM crn_src

-- ============================================================
-- collect_list RESPECT NULLS: per group, including an all-null group
-- ============================================================

query expect_fallback(collect_list with RESPECT NULLS (ignoreNulls = false) is not supported)
SELECT grp, sort_array(collect_list(v) RESPECT NULLS) FROM crn_src GROUP BY grp ORDER BY grp

-- ============================================================
-- collect_set RESPECT NULLS: keeps a single null alongside distinct values
-- ============================================================

query expect_fallback(collect_set with RESPECT NULLS (ignoreNulls = false) is not supported)
SELECT grp, sort_array(collect_set(v) RESPECT NULLS) FROM crn_src GROUP BY grp ORDER BY grp

-- ============================================================
-- Sanity check: default (IGNORE NULLS) still drops nulls, so RESPECT NULLS
-- above is genuinely exercising the null-preserving path.
-- ============================================================

query
SELECT grp, sort_array(collect_list(v)) FROM crn_src GROUP BY grp ORDER BY grp
