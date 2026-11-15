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

-- WindowGroupLimit fallback when a partition or order key carries a non-default string
-- collation. Comet's streaming operator compares row-encoded bytes for equality, which loses the
-- collation semantics that make Spark tie e.g. 'A' with 'a' under UTF8_LCASE. Falling
-- back to Spark keeps peer equality intact.
--
-- Pin the threshold so these queries exercise WindowGroupLimit plans. The sort below the
-- window must also reject collated keys (#6158); its fallback can happen before the
-- WindowGroupLimit converter, so either operator may supply the collation fallback reason.

-- MinSparkVersion: 4.0
-- Config: spark.sql.optimizer.windowGroupLimitThreshold=1000

statement
CREATE TABLE test_wgl_collation(grp int, s string) USING parquet

statement
INSERT INTO test_wgl_collation VALUES (1, 'A'), (1, 'a'), (1, 'b'), (1, 'B')

-- Byte order places 'B' before 'a', while UTF8_LCASE places it after. Spark's fallback window
-- operators must receive collation-aware ordering, including for multi-column sort keys.

-- Case-insensitive ORDER BY key: 'A' and 'a' must tie at rank 1 (Spark keeps both).
query expect_fallback(non-default string collation)
SELECT grp, s FROM (
  SELECT grp, s,
         RANK() OVER (
           PARTITION BY grp
           ORDER BY CAST(s AS STRING COLLATE UTF8_LCASE)
         ) AS rk
  FROM test_wgl_collation
) t WHERE rk <= 1 ORDER BY grp, s

-- Same shape with DENSE_RANK to pin both rank functions.
query expect_fallback(non-default string collation)
SELECT grp, s FROM (
  SELECT grp, s,
         DENSE_RANK() OVER (
           PARTITION BY grp
           ORDER BY CAST(s AS STRING COLLATE UTF8_LCASE)
         ) AS rk
  FROM test_wgl_collation
) t WHERE rk <= 1 ORDER BY grp, s

-- Collated PARTITION BY key: 'A' and 'a' belong to the same partition under UTF8_LCASE, so
-- ROW_NUMBER() <= 1 keeps only one of them for Spark, while Comet's byte-equality partition
-- detection would treat them as two partitions and keep both.
--
-- This must stay the only window function in the query. A second window over the same collated
-- key (e.g. COUNT(*) OVER (PARTITION BY ...)) lands between the `WindowGroupLimit` and the scan
-- and forces its own Spark shuffle exchange, so no `WindowGroupLimit` node has Comet-native
-- children any more. `CometExecRule.transform` only offers an operator to its serde when every
-- child is a `CometNativeExec`, so the guard below would never run and the reason would never be
-- recorded.
query expect_fallback(non-default string collation)
SELECT s, rn FROM (
  SELECT s,
         ROW_NUMBER() OVER (
           PARTITION BY CAST(s AS STRING COLLATE UTF8_LCASE)
           ORDER BY s
         ) AS rn
  FROM test_wgl_collation
) t WHERE rn <= 1 ORDER BY s
